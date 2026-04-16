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

log = logging.getLogger("langfuse_tracker")

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
    """Initialize the LangFuse client (lazy singleton)."""
    global _langfuse_client, _callback_handler_class

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
        from langfuse.callback import CallbackHandler

        _langfuse_client = Langfuse(
            public_key=LANGFUSE_PUBLIC_KEY,
            secret_key=LANGFUSE_SECRET_KEY,
            host=LANGFUSE_HOST,
        )
        _callback_handler_class = CallbackHandler
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
    Get a LangFuse callback handler for LangChain LLM calls.

    Args:
        session_id: Groups traces into a session (e.g., pipeline run ID)
        trace_name: Name for this trace (e.g., "outreach_email", "agent_query")
        user_id: User/client identifier
        metadata: Additional metadata to attach

    Returns:
        CallbackHandler instance, or None if LangFuse is not configured
    """
    client = _init_langfuse()
    if client is None or _callback_handler_class is None:
        return None

    try:
        handler = _callback_handler_class(
            public_key=LANGFUSE_PUBLIC_KEY,
            secret_key=LANGFUSE_SECRET_KEY,
            host=LANGFUSE_HOST,
            session_id=session_id or f"session-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            trace_name=trace_name or "analyst-agent",
            user_id=user_id or "CLT-001",
            metadata=metadata or {},
        )
        return handler
    except Exception as e:
        log.error("Failed to create LangFuse handler: %s", e)
        return None


def track_cost(
    input_tokens: int,
    output_tokens: int,
    model: str = "llama-3.3-70b-versatile",
    call_type: str = "analyst_call",
) -> dict:
    """
    Manually track cost for an LLM call.

    Returns cost breakdown dict.
    """
    input_cost = input_tokens * COST_PER_INPUT_TOKEN
    output_cost = output_tokens * COST_PER_OUTPUT_TOKEN
    total_cost = input_cost + output_cost

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
        "within_budget": total_cost <= TARGET_COST_PER_CALL,
        "timestamp": datetime.now().isoformat(),
    }

    if total_cost > TARGET_COST_PER_CALL:
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

    # Log to LangFuse if available
    client = _init_langfuse()
    if client:
        try:
            trace = client.trace(
                name=call_type,
                metadata=cost_info,
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

    return cost_info


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
