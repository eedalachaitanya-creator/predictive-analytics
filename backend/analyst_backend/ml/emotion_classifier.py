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
import json
import os
from typing import Optional, Tuple

from sqlalchemy import create_engine, text

from app.llm_gateway import screen_ingest

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
def classify_text(raw: str, llm=None) -> Tuple[str, float, str]:
    """Firewall-screen then LLM-classify; fall back to VADER on any error.

    Args:
        raw: Raw customer text (review body, ticket text, etc.)
        llm: Optional pre-built LangChain chat model. If None, built on demand.

    Returns:
        (emotion, distress_score, model_tag) where:
            emotion       ∈ VOCAB
            distress_score ∈ [0.0, 1.0]
            model_tag     ∈ {"llm", "vader-fallback", "vader-quarantined"}
    """
    # ① Firewall MUST run before the LLM sees the text
    quarantine, clean, _meta = screen_ingest(raw or "")
    body = clean or ""

    # ② HIGH/CRITICAL injection → VADER (never feed weaponized text to the LLM)
    if quarantine:
        emo, dist = _vader(body)
        return emo, dist, "vader-quarantined"

    # ③ Build the LLM lazily if not supplied
    if llm is None:
        from agent.llm import build_chat_model
        try:
            llm = build_chat_model(temperature=0.0)
        except Exception:
            emo, dist = _vader(body)
            return emo, dist, "vader-fallback"

    # ④ LLM inference — the sanitized `body` (not the raw text) goes into the prompt
    try:
        from langchain_core.messages import HumanMessage

        resp = llm.invoke([HumanMessage(content=_PROMPT.format(body=body))])
        data = json.loads(resp.content)
        emo = str(data["emotion"]).strip().lower()
        dist = float(data["distress_score"])

        # Clamp distress into [0, 1] before validation so minor float drift
        # (e.g. 1.0000001) doesn't needlessly push us to the VADER fallback.
        dist = max(0.0, min(1.0, dist))

        # Validate contract — out-of-vocab → VADER fallback (range is now guaranteed)
        if emo not in VOCAB:
            raise ValueError("LLM returned out-of-vocab emotion")

        return emo, round(dist, 3), "llm"

    except Exception:
        emo, dist = _vader(body)
        return emo, dist, "vader-fallback"


# ── Batch scorer (compute-once) ────────────────────────────────────────────────
def classify_unscored(
    engine,
    client_id: str,
    *,
    batch_size: int = 50,
    limit: Optional[int] = None,
    llm=None,
) -> dict:
    """Score rows with emotion_scored_at IS NULL (compute-once) across both tables.

    Writes: emotion, distress_score, emotion_scored_at, emotion_model per row.

    Returns:
        {"tickets": <n>, "reviews": <n>}  — counts of rows scored this run.
    """
    counts = {"tickets": 0, "reviews": 0}

    for table, id_col, text_col, count_key in (
        ("support_tickets", "ticket_id", "ticket_text", "tickets"),
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
            with engine.begin() as conn:
                rows = conn.execute(sel, {"c": client_id}).fetchall()
                if not rows:
                    break
                for r in rows:
                    emo, dist, model = classify_text(r.body, llm=llm)
                    conn.execute(
                        upd,
                        {"emo": emo, "dist": dist, "model": model, "c": client_id, "rid": r.rid},
                    )
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
    a = ap.parse_args()

    if not a.update_unscored:
        print("Hint: pass --update-unscored to score rows with emotion_scored_at IS NULL.")
        return

    eng = create_engine(a.db_url, future=True)
    print(classify_unscored(eng, a.client_id, limit=a.limit))


if __name__ == "__main__":
    main()
