"""emotion_classifier.py — Firewall-screened LLM emotion classifier with VADER fallback.

Pipeline:
    1. screen_ingest(raw)         — firewall-sanitize BEFORE the LLM sees the text
    2. If quarantined (HIGH/CRITICAL injection): fall back to VADER immediately
    3. Otherwise: call LLM → parse JSON → validate vocab + range → return
    4. On any LLM error / bad output: fall back to VADER (fail-open)

The classifier NEVER raises — bad input, network errors, or corrupt LLM responses
all collapse cleanly to the VADER fallback.

VOCAB: delighted, satisfied, neutral, frustrated, disappointed, angry

Note: VADER maps compound scores to a 5-bucket label set and never emits
"frustrated" directly — that label is only reachable via the LLM path.

Usage (CLI):
    python -m ml.emotion_classifier \\
        --db-url postgresql://... --client-id CLT-001 --update-unscored
"""
from __future__ import annotations

import argparse
import concurrent.futures as _futures
import json
import os
import random
import time
from typing import Optional, Tuple

from sqlalchemy import create_engine, text

from app.llm_gateway import screen_ingest

# Default thread-pool size for the batch scorer. The per-row work is a blocking
# llm.invoke() HTTP round-trip, so threads (GIL released on I/O) overlap those
# waits. Tunable without a code change, mirroring ML_STAGE_TIMEOUT_SECS.
_DEFAULT_EMOTION_WORKERS = int(os.getenv("EMOTION_MAX_WORKERS", "4"))

# Under concurrent load at scale the provider throttles by returning EMPTY
# content (a 200 with no body), not a 429 — so json.loads() fails. langchain's
# max_retries only fires on API *exceptions*, never on an empty success, which
# is why a bigger client retry budget alone doesn't help. We still raise it
# (cheap insurance for genuine 429s) and add a per-request timeout so a hung
# call can't pin a worker thread.
_EMOTION_LLM_MAX_RETRIES = int(os.getenv("EMOTION_LLM_MAX_RETRIES", "8"))
_EMOTION_LLM_TIMEOUT = float(os.getenv("EMOTION_LLM_TIMEOUT", "30"))

# Application-level attempts around invoke+parse. An empty/unparseable/out-of-
# vocab response is treated as a TRANSIENT throttle and retried with backoff +
# jitter (which also spreads load to ease the throttle) before VADER fallback.
_EMOTION_LLM_ATTEMPTS = int(os.getenv("EMOTION_LLM_ATTEMPTS", "4"))


def _backoff_sleep(attempt: int) -> None:
    """Exponential backoff + jitter between retry attempts. Isolated in its own
    function so tests can neutralize it without touching the global time.sleep."""
    time.sleep(min(8.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.5))

# ── VADER singleton (constructed once at module load if available) ──────────────
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as _SIA
    _vader_analyzer: Optional[object] = _SIA()
except Exception:
    _vader_analyzer = None

# ── Public vocabulary ──────────────────────────────────────────────────────────
VOCAB = ("delighted", "satisfied", "neutral", "frustrated", "disappointed", "angry")

# Distress prior per label; the LLM refines within [0, 1].
_PRIOR = {
    "delighted": 0.05,
    "satisfied": 0.15,
    "neutral": 0.30,
    "frustrated": 0.65,
    "disappointed": 0.80,
    "angry": 0.95,
}

_PROMPT = (
    "You classify the emotion of a customer support ticket or review.\n"
    "Respond with STRICT JSON only: "
    '{{"emotion": one of '
    + ", ".join(VOCAB)
    + ', "distress_score": a number 0.0-1.0 where 1.0 = maximum churn-risk distress}}.\n'
    "Text:\n{body}"
)


# ── VADER fallback ─────────────────────────────────────────────────────────────
def _vader(text_in: str) -> Tuple[str, float]:
    """Map VADER compound score to an emotion + distress_score."""
    global _vader_analyzer
    if _vader_analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as _SIA
        _vader_analyzer = _SIA()

    c = _vader_analyzer.polarity_scores(text_in or "")["compound"]
    if c <= -0.6:
        emo = "angry"
    elif c <= -0.2:
        emo = "disappointed"
    elif c < 0.2:
        emo = "neutral"
    elif c < 0.6:
        emo = "satisfied"
    else:
        emo = "delighted"
    distress = max(0.0, min(1.0, (1.0 - c) / 2.0))
    return emo, round(distress, 3)


# ── Public classifier ──────────────────────────────────────────────────────────
def classify_text(raw: str, llm=None, use_llm: bool = True) -> Tuple[str, float, str]:
    """Firewall-screen then classify; fall back to VADER on any error.

    Args:
        raw: Raw customer text (review body, ticket text, etc.)
        llm: Optional pre-built LangChain chat model. If None, built on demand.
        use_llm: Use the LLM path (default for this primitive), with VADER as
            fallback. Pass False to skip the LLM and use VADER directly — local,
            instant, deterministic, no provider rate limit. NOTE: the *bulk*
            VADER-by-default policy lives in classify_unscored (which passes
            use_llm=False unless EMOTION_USE_LLM opts in); this function itself
            stays LLM-first so callers that hand it an `llm` actually use it.

    Returns:
        (emotion, distress_score, model_tag) where:
            emotion       ∈ VOCAB
            distress_score ∈ [0.0, 1.0]
            model_tag     ∈ {"llm", "vader", "vader-fallback", "vader-quarantined"}
    """
    # ① Firewall MUST run before the LLM sees the text
    quarantine, clean, _meta = screen_ingest(raw or "")
    body = clean or ""

    # ② HIGH/CRITICAL injection → VADER (never feed weaponized text to the LLM)
    if quarantine:
        emo, dist = _vader(body)
        return emo, dist, "vader-quarantined"

    # ②a VADER-only mode (default for bulk) — deliberate, not a fallback
    if not use_llm:
        emo, dist = _vader(body)
        return emo, dist, "vader"

    # ③ Build the LLM lazily if not supplied
    if llm is None:
        from agent.llm import build_chat_model
        try:
            llm = build_chat_model(temperature=0.0)
        except Exception:
            emo, dist = _vader(body)
            return emo, dist, "vader-fallback"

    # ④ LLM inference — the sanitized `body` (not the raw text) goes into the
    #    prompt. Retry transient failures (empty/unparseable/out-of-vocab
    #    responses — the throttle signature under concurrent load) with backoff
    #    + jitter before conceding to VADER, so we keep LLM-quality coverage.
    from langchain_core.messages import HumanMessage

    for attempt in range(_EMOTION_LLM_ATTEMPTS):
        try:
            resp = llm.invoke([HumanMessage(content=_PROMPT.format(body=body))])
            content = (resp.content or "").strip()
            if not content:
                raise ValueError("empty LLM response")  # throttle signature
            data = json.loads(content)
            emo = str(data["emotion"]).strip().lower()
            dist = float(data["distress_score"])

            # Clamp distress into [0, 1] before validation so minor float drift
            # (e.g. 1.0000001) doesn't needlessly push us to the VADER fallback.
            dist = max(0.0, min(1.0, dist))

            # Validate contract — out-of-vocab is retriable, then VADER.
            if emo not in VOCAB:
                raise ValueError("LLM returned out-of-vocab emotion")

            return emo, round(dist, 3), "llm"

        except Exception:
            if attempt < _EMOTION_LLM_ATTEMPTS - 1:
                _backoff_sleep(attempt)   # spreads load to ease the throttle
                continue
            emo, dist = _vader(body)
            return emo, dist, "vader-fallback"

    # Unreachable (loop always returns), but keeps the type checker happy.
    emo, dist = _vader(body)
    return emo, dist, "vader-fallback"


# ── Parallel batch classifier (pure, DB-free) ───────────────────────────────────
def _classify_batch(rows, *, llm, workers: int, client_id: str, use_llm: bool = True) -> list:
    """Classify one batch of fetched rows, parallelizing the network-bound LLM
    calls. Returns one UPDATE param dict per row, **input order preserved**.

    Pure and DB-free by design — the caller owns all database access, so there
    are no SQLAlchemy connections shared across threads here. classify_text
    never raises (it fails open to VADER), so pool.map drops no row.
    """
    def _score(r):
        emo, dist, model = classify_text(r.body, llm=llm, use_llm=use_llm)
        return {"emo": emo, "dist": dist, "model": model, "c": client_id, "rid": r.rid}

    if workers <= 1:
        return [_score(r) for r in rows]
    # pool.map preserves input order, so results line up with `rows`.
    with _futures.ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_score, rows))


# ── Batch scorer (compute-once) ────────────────────────────────────────────────
def classify_unscored(
    engine,
    client_id: str,
    *,
    batch_size: int = 50,
    limit: Optional[int] = None,
    llm=None,
    use_llm: Optional[bool] = None,
    max_workers: Optional[int] = None,
) -> dict:
    """Score rows with emotion_scored_at IS NULL (compute-once) across both tables.

    Writes: emotion, distress_score, emotion_scored_at, emotion_model per row.

    Mode (resolved from `use_llm`, else the EMOTION_USE_LLM env flag, default
    OFF):
      • VADER (default) — local, instant, deterministic, no provider rate limit.
        The right choice for bulk historical scoring; runs single-threaded since
        VADER is CPU-bound and gains nothing from threads.
      • LLM (opt-in)    — richer labels, but one API call per row. Batches run
        concurrently (up to `max_workers`, default EMOTION_MAX_WORKERS) with
        retry-on-throttle. Only worthwhile where the account has rate-limit
        headroom — otherwise the provider throttles and rows VADER-fall-back.

    `limit`, when given, is applied PER TABLE (up to `limit` tickets AND up to
    `limit` reviews) — a debugging cap, not a global budget; the real
    compute-once gate is `emotion_scored_at IS NULL`.

    Returns:
        {"tickets": <n>, "reviews": <n>}  — counts of rows scored this run.
    """
    if use_llm is None:
        use_llm = os.getenv("EMOTION_USE_LLM", "0").strip().lower() in ("1", "true", "yes", "on")

    if use_llm:
        workers = max(1, int(max_workers if max_workers is not None else _DEFAULT_EMOTION_WORKERS))
        # Build the chat model ONCE and share it across worker threads — the
        # OpenAI client is thread-safe.
        if llm is None:
            try:
                from agent.llm import build_chat_model
                llm = build_chat_model(
                    temperature=0.0,
                    max_retries=_EMOTION_LLM_MAX_RETRIES,
                    timeout=_EMOTION_LLM_TIMEOUT,
                )
            except Exception:
                llm = None
        if llm is None:
            # Build failed (e.g. no API key). Degrade the WHOLE run to VADER
            # instead of letting each worker thread re-attempt the lazy build
            # once PER ROW (N futile builds). VADER is the safe fast default.
            use_llm = False
            workers = 1
    else:
        workers = 1      # VADER is local + CPU-bound; threads don't help
        llm = None

    counts = {"tickets": 0, "reviews": 0}

    for table, id_col, text_col, count_key in (
        ("support_tickets", "ticket_id", "description", "tickets"),
        ("customer_reviews", "review_id", "review_text", "reviews"),
    ):
        upd = text(f"""
            UPDATE {table}
            SET emotion = :emo,
                distress_score = :dist,
                emotion_scored_at = NOW(),
                emotion_model = :model
            WHERE client_id = :c
              AND {id_col} = :rid
        """)

        processed = 0
        while True:
            remaining = None if limit is None else max(0, limit - processed)
            if remaining == 0:
                break
            take = batch_size if remaining is None else min(batch_size, remaining)
            sel = text(
                f"SELECT {id_col} AS rid, {text_col} AS body FROM {table} "
                f"WHERE client_id=:c AND emotion_scored_at IS NULL AND {text_col} IS NOT NULL "
                f"ORDER BY {id_col} LIMIT {int(take)}"
            )
            # 1) fetch the batch — short read txn, released BEFORE the slow LLM
            #    calls so we never hold a transaction open across the network.
            #    Assumes ONE classify_unscored run per client at a time (the
            #    pipeline's contract). The SELECT isn't row-locked, so two
            #    overlapping same-client runs could re-classify a batch —
            #    wasteful but never corrupting (the UPDATE is idempotent and
            #    re-gated by emotion_scored_at on the next pass).
            with engine.begin() as conn:
                rows = conn.execute(sel, {"c": client_id}).fetchall()
            if not rows:
                break
            # 2) classify — DB is untouched here (no cross-thread conns). LLM
            #    mode runs concurrently; VADER mode is single-threaded (workers=1).
            results = _classify_batch(rows, llm=llm, workers=workers,
                                      client_id=client_id, use_llm=use_llm)
            # 3) write the batch back single-threaded — one connection = safe
            with engine.begin() as conn:
                for params in results:
                    conn.execute(upd, params)
            processed += len(rows)
            counts[count_key] += len(rows)

    return counts


# ── CLI entry point ────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Classify external-signal emotion")
    ap.add_argument("--db-url", default=os.getenv("DB_URL"))
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--update-unscored", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--max-workers", type=int, default=None,
                    help="LLM-call concurrency when --use-llm (default: EMOTION_MAX_WORKERS or 4)")
    ap.add_argument("--use-llm", dest="use_llm", action="store_true", default=None,
                    help="Opt into the LLM path (default: VADER, or the EMOTION_USE_LLM env flag)")
    a = ap.parse_args()

    if not a.update_unscored:
        print("Hint: pass --update-unscored to score rows with emotion_scored_at IS NULL.")
        return

    eng = create_engine(a.db_url, future=True)
    print(classify_unscored(eng, a.client_id, limit=a.limit,
                            use_llm=a.use_llm, max_workers=a.max_workers))


if __name__ == "__main__":
    main()
