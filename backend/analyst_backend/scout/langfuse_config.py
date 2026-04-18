"""
scout/langfuse_config.py — Central Langfuse observability configuration

Provides:
  - get_langfuse()              → singleton Langfuse client
  - get_openai_client()         → OpenAI client wrapped with Langfuse tracing
  - get_langchain_handler()     → LangChain CallbackHandler for agent tracing
  - trace_llm_call()            → decorator/context for manual LLM call tracking
  - trace_scrape()              → context manager for scrape operation tracking
  - flush()                     → flush pending traces (call on shutdown)

Environment variables (set in .env):
  LANGFUSE_PUBLIC_KEY    — your Langfuse public key
  LANGFUSE_SECRET_KEY    — your Langfuse secret key
  LANGFUSE_HOST          — Langfuse host (default: https://cloud.langfuse.com)
  LANGFUSE_ENABLED       — set to "false" to disable (default: "true")

All tracing is optional — if Langfuse keys are not set or LANGFUSE_ENABLED=false,
everything falls back to plain OpenAI calls with zero overhead.
"""

import os
import time
import logging
from typing import Optional, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────

def _clean_env(key: str, default: str = "") -> str:
    """Get env var and strip surrounding quotes that .env files sometimes include."""
    val = os.getenv(key, default)
    return val.strip().strip('"').strip("'")


_langfuse_client = None
_langfuse_available = False


# ── Singleton client ──────────────────────────────────────────────────

def get_langfuse():
    """Get or create the singleton Langfuse client. Returns None if disabled."""
    global _langfuse_client, _langfuse_available

    if _langfuse_client is not None:
        return _langfuse_client

    # Read env vars LAZILY — not at import time.
    # This ensures load_dotenv() has already run by the time we check.
    enabled = os.getenv("LANGFUSE_ENABLED", "true").lower() != "false"
    if not enabled:
        return None

    public_key = _clean_env("LANGFUSE_PUBLIC_KEY")
    secret_key = _clean_env("LANGFUSE_SECRET_KEY")
    host = (
        _clean_env("LANGFUSE_HOST") or
        _clean_env("LANGFUSE_BASE_URL") or
        "https://cloud.langfuse.com"
    )

    if not public_key or not secret_key:
        logger.info("[langfuse] Keys not set — tracing disabled. "
                    "Set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY in .env")
        _langfuse_available = False
        return None

    try:
        from langfuse import Langfuse
        _langfuse_client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=host,
        )
        _langfuse_available = True
        logger.info(f"[langfuse] ✅ Initialized — host={host}")
        return _langfuse_client
    except Exception as e:
        logger.warning(f"[langfuse] Failed to initialize: {e}")
        _langfuse_available = False
        return None


def is_available() -> bool:
    """Check if Langfuse is configured and available."""
    if _langfuse_client is not None:
        return True
    get_langfuse()  # Try to init
    return _langfuse_available


# ── Instrumented OpenAI client ────────────────────────────────────────

def get_openai_client():
    """
    Get an OpenAI client. If Langfuse is available, returns an instrumented
    client that auto-tracks all completions. Otherwise returns plain OpenAI.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    from openai import OpenAI

    if not is_available():
        return OpenAI(api_key=api_key)

    try:
        from langfuse.openai import OpenAI as LangfuseOpenAI
        client = LangfuseOpenAI(api_key=api_key)
        logger.debug("[langfuse] Using instrumented OpenAI client")
        return client
    except ImportError:
        logger.debug("[langfuse] langfuse.openai not available — using plain OpenAI")
        return OpenAI(api_key=api_key)
    except Exception as e:
        logger.warning(f"[langfuse] Instrumented client failed: {e} — using plain OpenAI")
        return OpenAI(api_key=api_key)


# ── LangChain callback handler ───────────────────────────────────────

def get_langchain_handler(session_id: Optional[str] = None):
    """
    Get a LangChain CallbackHandler for the Scout agent.
    Returns None if Langfuse is not available.
    
    Usage in scout_agent.py:
        handler = get_langchain_handler(session_id="user-123")
        if handler:
            agent.run(message, callbacks=[handler])
    """
    if not is_available():
        return None

    try:
        from langfuse.langchain import CallbackHandler

        kwargs = {}
        if session_id:
            kwargs["trace_context"] = {"trace_id": f"scout-{session_id}-{int(time.time())}"}

        handler = CallbackHandler(**kwargs)
        return handler
    except ImportError:
        logger.debug("[langfuse] langfuse.langchain not available")
        return None
    except Exception as e:
        logger.warning(f"[langfuse] CallbackHandler failed: {e}")
        return None


# ── Manual trace helpers ──────────────────────────────────────────────

@contextmanager
def trace_llm_call(
    name: str,
    model: str = "gpt-4o-mini",
    input_data: Optional[Any] = None,
    metadata: Optional[dict] = None,
):
    """
    Context manager to trace a direct OpenAI call with Langfuse.
    
    Usage:
        with trace_llm_call("product_validation", input_data={"query": q}):
            response = openai_client.chat.completions.create(...)
    
    If Langfuse is not available, this is a no-op.
    """
    lf = get_langfuse()
    if not lf:
        yield None
        return

    try:
        generation = lf.start_observation(
            name=name,
            as_type="generation",
            model=model,
            input=input_data,
            metadata=metadata or {},
        )
        yield generation
        generation.end(output="completed")
    except Exception as e:
        logger.debug(f"[langfuse] trace_llm_call error: {e}")
        yield None


@contextmanager
def trace_scrape(
    platform: str,
    product_name: str,
    metadata: Optional[dict] = None,
):
    """
    Context manager to trace a full scrape operation.
    
    Usage:
        with trace_scrape("amazon", "NIVEA lip balm"):
            result = _scrape_sync(site, product_name)
    """
    lf = get_langfuse()
    if not lf:
        yield None
        return

    try:
        span = lf.start_observation(
            name=f"scrape_{platform}",
            as_type="span",
            input={"platform": platform, "product": product_name},
            metadata=metadata or {},
        )
        yield span
        span.end(output={"status": "completed"})
    except Exception as e:
        logger.debug(f"[langfuse] trace_scrape error: {e}")
        yield None


@contextmanager
def trace_search(
    product_name: str,
    platforms: list,
    session_id: Optional[str] = None,
):
    """
    Context manager to trace a full cross-platform search.
    Creates a top-level trace that child spans nest under.
    """
    lf = get_langfuse()
    if not lf:
        yield None
        return

    try:
        # Use start_observation with as_type="span" for the top-level search
        span = lf.start_observation(
            name="search_across_sites",
            as_type="span",
            input={
                "product": product_name,
                "platforms": platforms,
            },
            metadata={"session_id": session_id} if session_id else {},
        )
        yield span
        span.end(output={"status": "completed"})
    except Exception as e:
        logger.debug(f"[langfuse] trace_search error: {e}")
        yield None


# ── Shutdown ──────────────────────────────────────────────────────────

def flush():
    """Flush pending Langfuse traces. Call on app shutdown."""
    if _langfuse_client:
        try:
            _langfuse_client.flush()
            logger.info("[langfuse] Flushed pending traces")
        except Exception as e:
            logger.warning(f"[langfuse] Flush error: {e}")


def shutdown():
    """Shutdown Langfuse client. Call on app shutdown."""
    global _langfuse_client
    if _langfuse_client:
        try:
            _langfuse_client.shutdown()
            logger.info("[langfuse] Shut down")
        except Exception:
            pass
        _langfuse_client = None