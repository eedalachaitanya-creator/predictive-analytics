"""
langfuse_tracker.py — LLM Cost Tracking via LangFuse
=====================================================
Provides a centralized LangFuse callback handler for all LLM calls
in the Analyst Agent. Tracks:
    - Token usage (input/output)
    - Cost per call (target: $0.08/analyst call)
    - Latency
    - Model name and parameters
    - Session and trace grouping

Integration points:
    1. agent/graph.py   — Analyst agent LLM calls
    2. ml/alerts.py     — Outreach email generation LLM calls

Usage:
    from app.langfuse_tracker import get_langfuse_handler, flush_langfuse

    # Add to LLM call
    handler = get_langfuse_handler(session_id="pipeline-run-123")
    llm.invoke(messages, config={"callbacks": [handler]})

    # At shutdown
    flush_langfuse()

Environment variables:
    LANGFUSE_PUBLIC_KEY   — Your LangFuse public key
    LANGFUSE_SECRET_KEY   — Your LangFuse secret key
    LANGFUSE_HOST         — LangFuse host (default: https://cloud.langfuse.com)
    LANGFUSE_ENABLED      — Set to "false" to disable (default: true)
"""

import os
import logging
from typing import Optional
from datetime import datetime

from sqlalchemy import text

log = logging.getLogger("langfuse_tracker")

# ── Local cost-log table DDL ──────────────────────────────────────────────
# We dual-write: every LLM call's cost goes to cloud.langfuse.com (for deep
# trace exploration) AND into this Postgres table (so the Cost Tracking page
# can show per-client aggregates without hitting LangFuse's read API).
_COST_LOG_DDL = """
CREATE TABLE IF NOT EXISTS llm_cost_log (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(20) NOT NULL,
    call_type       VARCHAR(50) NOT NULL,
    model           VARCHAR(100) NOT NULL,
    input_tokens    INT NOT NULL DEFAULT 0,
    output_tokens   INT NOT NULL DEFAULT 0,
    total_tokens    INT NOT NULL DEFAULT 0,
    input_cost_usd  NUMERIC(12,8) NOT NULL DEFAULT 0,
    output_cost_usd NUMERIC(12,8) NOT NULL DEFAULT 0,
    total_cost_usd  NUMERIC(12,8) NOT NULL DEFAULT 0,
    over_budget     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_llm_cost_log_client_created
    ON llm_cost_log (client_id, created_at DESC);
"""
_cost_log_ready = False

# ── Configuration ─────────────────────────────────────────────────────────

LANGFUSE_ENABLED = os.getenv("LANGFUSE_ENABLED", "true").lower() != "false"
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "")
LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")

# Cost per token (Groq llama-3.3-70b-versatile pricing)
# Groq is free tier / pay-per-token — adjust these as needed
COST_PER_INPUT_TOKEN = 0.00000059    # $0.59 per 1M input tokens
COST_PER_OUTPUT_TOKEN = 0.00000079   # $0.79 per 1M output tokens

# Target cost budget
TARGET_COST_PER_CALL = 0.08  # $0.08 per analyst call


# ── Singleton LangFuse client ─────────────────────────────────────────────

_langfuse_client = None
_callback_handler_class = None


def _init_langfuse():
    """Initialize the LangFuse client (lazy singleton).

    We only use the direct Langfuse SDK here — NOT the LangChain CallbackHandler.
    The callback handler depends on `langchain_core.pydantic_v1`, a compatibility
    shim that was removed in langchain-core 1.x. Importing it crashes the agent
    at LLM-invocation time. The direct SDK has no such dependency, so traces
    via `client.trace()` / `client.generation()` continue to work across
    langchain upgrades.
    """
    global _langfuse_client

    if _langfuse_client is not None:
        return _langfuse_client

    if not LANGFUSE_ENABLED:
        log.info("LangFuse tracking disabled (LANGFUSE_ENABLED=false)")
        return None

    if not LANGFUSE_PUBLIC_KEY or not LANGFUSE_SECRET_KEY:
        log.warning(
            "LangFuse keys not configured. Set LANGFUSE_PUBLIC_KEY and "
            "LANGFUSE_SECRET_KEY in .env to enable cost tracking."
        )
        return None

    try:
        from langfuse import Langfuse

        _langfuse_client = Langfuse(
            public_key=LANGFUSE_PUBLIC_KEY,
            secret_key=LANGFUSE_SECRET_KEY,
            host=LANGFUSE_HOST,
        )
        log.info("LangFuse initialized: %s", LANGFUSE_HOST)
        return _langfuse_client

    except ImportError:
        log.warning(
            "langfuse package not installed. "
            "Run: pip install langfuse"
        )
        return None
    except Exception as e:
        log.error("Failed to initialize LangFuse: %s", e)
        return None


def get_langfuse_handler(
    session_id: Optional[str] = None,
    trace_name: Optional[str] = None,
    user_id: Optional[str] = None,
    metadata: Optional[dict] = None,
):
    """
    DEPRECATED — always returns None.

    Previously returned a `langfuse.callback.CallbackHandler` for LangChain
    LLM calls, but that handler imports `langchain_core.pydantic_v1` which
    was removed in langchain-core 1.x and crashes the agent at runtime.

    Callers have been migrated to `track_cost()` (direct SDK path) instead.
    This stub is kept so existing `llm.with_config({"callbacks": [handler]})`
    guards that check `if handler:` continue to work.
    """
    return None


def _ensure_cost_log_table(engine):
    """Create llm_cost_log table + index if missing (one-time per process)."""
    global _cost_log_ready
    if _cost_log_ready:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(_COST_LOG_DDL))
        _cost_log_ready = True
        log.info("llm_cost_log table ready.")
    except Exception as e:
        log.debug("Could not ensure llm_cost_log table: %s", e)


def track_cost(
    input_tokens: int,
    output_tokens: int,
    model: str = "llama-3.3-70b-versatile",
    call_type: str = "analyst_call",
    client_id: str = "CLT-001",
) -> dict:
    """
    Track cost for one LLM call. Dual-writes:
      1. Sends a generation to cloud.langfuse.com (if configured)
      2. Inserts a row into Postgres table `llm_cost_log` so the Cost Tracking
         UI can aggregate without hitting LangFuse's read API.

    Returns cost breakdown dict.
    """
    input_cost = input_tokens * COST_PER_INPUT_TOKEN
    output_cost = output_tokens * COST_PER_OUTPUT_TOKEN
    total_cost = input_cost + output_cost
    over_budget = total_cost > TARGET_COST_PER_CALL

    cost_info = {
        "model": model,
        "call_type": call_type,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "input_cost_usd": round(input_cost, 6),
        "output_cost_usd": round(output_cost, 6),
        "total_cost_usd": round(total_cost, 6),
        "target_cost_usd": TARGET_COST_PER_CALL,
        "within_budget": not over_budget,
        "timestamp": datetime.now().isoformat(),
    }

    if over_budget:
        log.warning(
            "Cost OVER budget: $%.4f (target: $%.2f) for %s",
            total_cost, TARGET_COST_PER_CALL, call_type
        )
    else:
        log.info(
            "Cost: $%.4f / $%.2f budget (%d tokens) — %s",
            total_cost, TARGET_COST_PER_CALL,
            input_tokens + output_tokens, call_type
        )

    # ── 1. Log to LangFuse cloud if configured ──
    client = _init_langfuse()
    if client:
        try:
            trace = client.trace(
                name=call_type,
                metadata=cost_info,
                user_id=client_id,
            )
            trace.generation(
                name=f"{call_type}_generation",
                model=model,
                usage={
                    "input": input_tokens,
                    "output": output_tokens,
                    "total": input_tokens + output_tokens,
                    "input_cost": input_cost,
                    "output_cost": output_cost,
                    "total_cost": total_cost,
                },
            )
        except Exception as e:
            log.debug("Failed to log cost to LangFuse: %s", e)

    # ── 2. Log to local Postgres so the UI can aggregate ──
    try:
        from app.database import engine
        _ensure_cost_log_table(engine)
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO llm_cost_log (
                    client_id, call_type, model,
                    input_tokens, output_tokens, total_tokens,
                    input_cost_usd, output_cost_usd, total_cost_usd,
                    over_budget
                ) VALUES (
                    :client_id, :call_type, :model,
                    :input_tokens, :output_tokens, :total_tokens,
                    :input_cost, :output_cost, :total_cost,
                    :over_budget
                )
            """), {
                "client_id": client_id,
                "call_type": call_type,
                "model": model,
                "input_tokens": int(input_tokens),
                "output_tokens": int(output_tokens),
                "total_tokens": int(input_tokens + output_tokens),
                "input_cost": input_cost,
                "output_cost": output_cost,
                "total_cost": total_cost,
                "over_budget": over_budget,
            })
    except Exception as e:
        log.debug("Failed to log cost to Postgres: %s", e)

    return cost_info


def get_cost_aggregates(engine, client_id: str) -> dict:
    """
    Query the llm_cost_log table for the Cost Tracking page.

    Returns:
        {
            "today":    {"calls": int, "cost": float, "tokens": int},
            "week":     {...},
            "month":    {...},
            "all_time": {...},
            "avg_cost_per_call": float,
            "budget_usd_per_call": float,
            "over_budget_pct": float,     # % of calls over budget (last 30d)
            "per_model": [{model, calls, tokens, cost}, ...],
            "daily_trend": [{date, cost, calls}, ...],   # last 14 days
            "recent_calls": [{created_at, call_type, model, tokens, cost, over_budget}, ...],
        }
    """
    _ensure_cost_log_table(engine)

    result = {
        "today":    {"calls": 0, "cost": 0.0, "tokens": 0},
        "week":     {"calls": 0, "cost": 0.0, "tokens": 0},
        "month":    {"calls": 0, "cost": 0.0, "tokens": 0},
        "all_time": {"calls": 0, "cost": 0.0, "tokens": 0},
        "avg_cost_per_call": 0.0,
        "budget_usd_per_call": TARGET_COST_PER_CALL,
        "over_budget_pct": 0.0,
        "per_model": [],
        "daily_trend": [],
        "recent_calls": [],
    }

    with engine.connect() as conn:
        # Rolling totals
        buckets = [
            ("today",    "created_at >= date_trunc('day',   NOW())"),
            ("week",     "created_at >= NOW() - INTERVAL '7 days'"),
            ("month",    "created_at >= NOW() - INTERVAL '30 days'"),
            ("all_time", "1=1"),
        ]
        for key, where in buckets:
            row = conn.execute(text(f"""
                SELECT COUNT(*) AS c,
                       COALESCE(SUM(total_cost_usd), 0) AS cost,
                       COALESCE(SUM(total_tokens), 0) AS tokens
                FROM llm_cost_log
                WHERE client_id = :cid AND {where}
            """), {"cid": client_id}).mappings().first()
            if row:
                result[key] = {
                    "calls": int(row["c"] or 0),
                    "cost": float(row["cost"] or 0.0),
                    "tokens": int(row["tokens"] or 0),
                }

        if result["all_time"]["calls"]:
            result["avg_cost_per_call"] = round(
                result["all_time"]["cost"] / result["all_time"]["calls"], 6
            )

        # Over-budget percentage (last 30 days)
        row = conn.execute(text("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN over_budget THEN 1 ELSE 0 END) AS over
            FROM llm_cost_log
            WHERE client_id = :cid AND created_at >= NOW() - INTERVAL '30 days'
        """), {"cid": client_id}).mappings().first()
        if row and row["total"]:
            result["over_budget_pct"] = round(
                100.0 * float(row["over"] or 0) / float(row["total"]), 2
            )

        # Per-model breakdown (last 30 days)
        rows = conn.execute(text("""
            SELECT model,
                   COUNT(*) AS calls,
                   SUM(total_tokens) AS tokens,
                   SUM(total_cost_usd) AS cost
            FROM llm_cost_log
            WHERE client_id = :cid AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY model
            ORDER BY cost DESC
        """), {"cid": client_id}).mappings().all()
        result["per_model"] = [
            {
                "model": r["model"],
                "calls": int(r["calls"] or 0),
                "tokens": int(r["tokens"] or 0),
                "cost": round(float(r["cost"] or 0.0), 6),
            }
            for r in rows
        ]

        # Daily trend (last 14 days)
        rows = conn.execute(text("""
            SELECT date_trunc('day', created_at)::date AS d,
                   COUNT(*) AS calls,
                   SUM(total_cost_usd) AS cost
            FROM llm_cost_log
            WHERE client_id = :cid AND created_at >= NOW() - INTERVAL '14 days'
            GROUP BY d
            ORDER BY d
        """), {"cid": client_id}).mappings().all()
        result["daily_trend"] = [
            {
                "date": r["d"].isoformat() if r["d"] else None,
                "calls": int(r["calls"] or 0),
                "cost": round(float(r["cost"] or 0.0), 6),
            }
            for r in rows
        ]

        # Recent calls (last 20)
        rows = conn.execute(text("""
            SELECT created_at, call_type, model,
                   total_tokens, total_cost_usd, over_budget
            FROM llm_cost_log
            WHERE client_id = :cid
            ORDER BY created_at DESC
            LIMIT 20
        """), {"cid": client_id}).mappings().all()
        result["recent_calls"] = [
            {
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "call_type": r["call_type"],
                "model": r["model"],
                "tokens": int(r["total_tokens"] or 0),
                "cost": round(float(r["total_cost_usd"] or 0.0), 6),
                "over_budget": bool(r["over_budget"]),
            }
            for r in rows
        ]

    return result


def get_per_client_cost_summary(engine) -> list:
    """
    Cross-tenant cost aggregation for the admin Cost Monitoring page.

    Unlike `get_cost_aggregates()`, which is scoped to one client, this
    returns one row per client_id found in `llm_cost_log`, LEFT JOINed to
    `client_config` so the UI can display the human-readable client name.

    Returns:
        [
            {
                "client_id":         "CLT-001",
                "client_name":       "Walmart",          # None if not in client_config
                "total_calls":       int,
                "total_cost":        float,
                "total_tokens":      int,
                "calls_today":       int,
                "cost_today":        float,
                "calls_30d":         int,
                "cost_30d":          float,
                "over_budget_count": int,                # lifetime count of over-budget calls
                "over_budget_pct":   float,              # % over budget (lifetime)
                "avg_cost_per_call": float,
                "last_call":         ISO-8601 string or None,
            },
            ...
        ]
    Sorted by total_cost DESC so the biggest spenders appear first.
    """
    _ensure_cost_log_table(engine)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    l.client_id,
                    c.client_name,
                    COUNT(*)                                             AS total_calls,
                    COALESCE(SUM(l.total_cost_usd), 0)                   AS total_cost,
                    COALESCE(SUM(l.total_tokens), 0)                     AS total_tokens,
                    COALESCE(SUM(CASE WHEN l.created_at >= date_trunc('day', NOW())
                                       THEN 1 ELSE 0 END), 0)            AS calls_today,
                    COALESCE(SUM(CASE WHEN l.created_at >= date_trunc('day', NOW())
                                       THEN l.total_cost_usd ELSE 0 END), 0) AS cost_today,
                    COALESCE(SUM(CASE WHEN l.created_at >= NOW() - INTERVAL '30 days'
                                       THEN 1 ELSE 0 END), 0)            AS calls_30d,
                    COALESCE(SUM(CASE WHEN l.created_at >= NOW() - INTERVAL '30 days'
                                       THEN l.total_cost_usd ELSE 0 END), 0) AS cost_30d,
                    COALESCE(SUM(CASE WHEN l.over_budget THEN 1 ELSE 0 END), 0) AS over_budget_count,
                    MAX(l.created_at)                                    AS last_call
                FROM llm_cost_log l
                LEFT JOIN client_config c ON c.client_id = l.client_id
                GROUP BY l.client_id, c.client_name
                ORDER BY total_cost DESC, l.client_id
            """)).mappings().all()
    except Exception as e:
        log.debug("Per-client cost summary query failed: %s", e)
        return []

    out = []
    for r in rows:
        total_calls = int(r["total_calls"] or 0)
        total_cost = float(r["total_cost"] or 0.0)
        over_count = int(r["over_budget_count"] or 0)
        out.append({
            "client_id":         r["client_id"],
            "client_name":       r["client_name"],
            "total_calls":       total_calls,
            "total_cost":        round(total_cost, 6),
            "total_tokens":      int(r["total_tokens"] or 0),
            "calls_today":       int(r["calls_today"] or 0),
            "cost_today":        round(float(r["cost_today"] or 0.0), 6),
            "calls_30d":         int(r["calls_30d"] or 0),
            "cost_30d":          round(float(r["cost_30d"] or 0.0), 6),
            "over_budget_count": over_count,
            "over_budget_pct":   round(100.0 * over_count / total_calls, 2) if total_calls else 0.0,
            "avg_cost_per_call": round(total_cost / total_calls, 6) if total_calls else 0.0,
            "last_call":         r["last_call"].isoformat() if r["last_call"] else None,
        })
    return out


def flush_langfuse():
    """Flush pending events to LangFuse. Call at application shutdown."""
    if _langfuse_client:
        try:
            _langfuse_client.flush()
            log.info("LangFuse events flushed.")
        except Exception as e:
            log.debug("LangFuse flush error: %s", e)


def get_cost_summary() -> dict:
    """
    Get a summary of LLM costs from LangFuse.

    Falls back to local tracking if LangFuse is not available.
    """
    return {
        "target_per_call": TARGET_COST_PER_CALL,
        "cost_per_input_token": COST_PER_INPUT_TOKEN,
        "cost_per_output_token": COST_PER_OUTPUT_TOKEN,
        "langfuse_enabled": LANGFUSE_ENABLED,
        "langfuse_configured": bool(LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY),
    }
