"""
main.py — CRP Analyst Agent | FastAPI entry point
Sprint 0 | Task 1.3
"""
from dotenv import load_dotenv
load_dotenv()  # Load .env BEFORE any other imports read os.getenv()

import sys
import asyncio
if sys.platform == "win32":     
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
 
import logging
import time
from contextlib import asynccontextmanager

import redis as redis_client
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import check_db_connection
from app.pipeline_router import router as pipeline_router
from app.customers_router import router as customers_router
from app.orders_router import router as orders_router
from app.analytics_router import router as analytics_router
from app.dashboard_router import router as dashboard_router
from app.auth_router import router as auth_router
from app.client_router import router as client_router
from app.upload_router import router as upload_router
from app.users_router import router as users_router
from app.settings_router import router as settings_router
from app.integrations_router import router as integrations_router
from app.churn_router import router as churn_router
from app.downloads_router import router as downloads_router
from app.validation_router import router as validation_router
from app.chat_router import router as chat_router
from app.rag_router import router as rag_router
from app.messages_router import router as messages_router
from app.audit_router import router as audit_router
from scout.router import scout_router
from scout_agent.routes import router as agent_router

# ── Strategist Agent integration ──────────────────────────────────────────────
# Strategist provides /api/strategist/* endpoints for pricing recommendations.
# It uses its own asyncpg pool (lazily) to query Scout's entity_listings +
# price_history tables and write to pricing_recommendations +
# customer_price_context (both already exist in Scout DB).
from strategist.routers.strategist_router import router as strategist_router
from strategist.routers.retention_router  import router as retention_router
from strategist.routers.db_router         import router as db_router
from strategist.db.connection import (
    create_pools as strategist_create_pools,
    close_pools as strategist_close_pools,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=settings.log_level.upper())
log = logging.getLogger("crp_api")

# ── Startup / shutdown ────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 CRP Analyst Agent starting up ...")

    # Initialise Strategist's asyncpg pool (reads SCOUT_DB_URL or DATABASE_URL).
    # Non-fatal: if the DB is unreachable, Strategist endpoints return 503,
    # but the rest of the app (scout, analyst, pipelines) keeps working.
    try:
        await strategist_create_pools()
        log.info("✅ Strategist DB pool ready.")
    except Exception as exc:
        log.error(
            "❌ Strategist DB pool failed to initialise: %s — "
            "/api/strategist/* endpoints will return 503.", exc
        )

    # Best-effort: ensure the RAG vector store exists so the chat agent's
    # search_customer_feedback tool doesn't error before the first reindex. On
    # managed deploys where the app user can't CREATE EXTENSION, this logs and
    # continues — run db/migration_rag_documents.sql as a superuser there.
    try:
        from rag.store import ensure_schema
        from app.database import engine
        ensure_schema(engine)
        log.info("✅ RAG vector store ready (rag_documents).")
    except Exception as exc:
        log.warning("RAG schema ensure failed (run migration manually?): %s", exc)

    yield

    # Shutdown — close Strategist pool gracefully
    try:
        await strategist_close_pools()
        log.info("Strategist DB pool closed.")
    except Exception as exc:
        log.warning("Strategist DB pool close failed: %s", exc)

    log.info("🛑 CRP Analyst Agent shutting down ...")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Churn prediction and retention analytics API for the Walmart CRP platform.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten this per environment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(client_router)
app.include_router(upload_router)
app.include_router(users_router)
app.include_router(settings_router)
app.include_router(integrations_router)
app.include_router(churn_router)
app.include_router(pipeline_router)
app.include_router(customers_router)
app.include_router(orders_router)
app.include_router(analytics_router)
app.include_router(dashboard_router)
app.include_router(downloads_router)
app.include_router(validation_router)
app.include_router(chat_router)
app.include_router(rag_router)
app.include_router(messages_router)
app.include_router(audit_router)
app.include_router(scout_router)
app.include_router(agent_router, prefix="/agent", tags=["agent"])
app.include_router(strategist_router)  # /api/strategist/* — prefix built into router
app.include_router(retention_router)   # /api/retention/*  — prefix built into router
app.include_router(db_router)          # /api/db/*         — prefix built into router
# ── LangFuse Cost Tracking ────────────────────────────────────────────────────

from fastapi import Query as _Query

@app.get("/api/v1/cost-tracking", tags=["ops"])
def get_cost_tracking(clientId: str = _Query("CLT-001")):
    """
    Return LLM cost summary + per-client aggregates for the Cost Tracking UI.

    Combines static config (budget, per-token pricing, langfuse enabled flag)
    from `get_cost_summary()` with live per-client aggregates pulled from the
    `llm_cost_log` Postgres table via `get_cost_aggregates()`.
    """
    payload: dict = {}
    try:
        from app.langfuse_tracker import get_cost_summary
        payload.update(get_cost_summary())
    except Exception as e:
        payload["summary_error"] = str(e)
        payload["langfuse_enabled"] = False

    try:
        from app.langfuse_tracker import get_cost_aggregates
        from app.database import engine
        payload["client_id"] = clientId
        payload["aggregates"] = get_cost_aggregates(engine, clientId)
    except Exception as e:
        payload["aggregates_error"] = str(e)
        payload["aggregates"] = None

    return payload


@app.get("/api/v1/cost-tracking/per-client", tags=["ops"])
def get_cost_tracking_per_client():
    """
    Admin Cost Monitoring — cross-tenant cost breakdown.

    Returns one row per client_id in `llm_cost_log`, joined to
    `client_config` for client_name. The admin UI renders this as a table
    so super admins can see which clients are burning the most LLM budget.
    """
    payload: dict = {"clients": [], "totals": {
        "total_calls": 0, "total_cost": 0.0, "total_tokens": 0,
        "calls_today": 0, "cost_today": 0.0,
        "calls_30d": 0,   "cost_30d":   0.0,
        "over_budget_count": 0,
    }}
    try:
        from app.langfuse_tracker import get_per_client_cost_summary, get_cost_summary
        from app.database import engine
        clients = get_per_client_cost_summary(engine)
        payload["clients"] = clients

        # Roll up grand totals so the page can show one summary row.
        for c in clients:
            payload["totals"]["total_calls"]       += c["total_calls"]
            payload["totals"]["total_cost"]        += c["total_cost"]
            payload["totals"]["total_tokens"]      += c["total_tokens"]
            payload["totals"]["calls_today"]       += c["calls_today"]
            payload["totals"]["cost_today"]        += c["cost_today"]
            payload["totals"]["calls_30d"]         += c["calls_30d"]
            payload["totals"]["cost_30d"]          += c["cost_30d"]
            payload["totals"]["over_budget_count"] += c["over_budget_count"]
        # Round totals after summing.
        payload["totals"]["total_cost"] = round(payload["totals"]["total_cost"], 6)
        payload["totals"]["cost_today"] = round(payload["totals"]["cost_today"], 6)
        payload["totals"]["cost_30d"]   = round(payload["totals"]["cost_30d"], 6)

        # Static config so the UI can show the per-call budget on the page.
        payload.update(get_cost_summary())
    except Exception as e:
        payload["error"] = str(e)
    return payload


@app.on_event("shutdown")
def shutdown_langfuse():
    """Flush LangFuse events on shutdown."""
    try:
        from app.langfuse_tracker import flush_langfuse
        flush_langfuse()
    except Exception:
        pass


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health_check():
    """
    Health check endpoint — Task 1.3 requirement.
    Returns 200 when the API, database, and Redis are all reachable.
    """
    start = time.perf_counter()

    # Check PostgreSQL
    db_ok = check_db_connection()

    # Check Redis
    redis_ok = False
    try:
        r = redis_client.from_url(settings.redis_url, socket_connect_timeout=2)
        redis_ok = r.ping()
    except Exception:
        redis_ok = False

    elapsed_ms = round((time.perf_counter() - start) * 1000, 1)

    status = "healthy" if (db_ok and redis_ok) else "degraded"

    return {
        "status":      status,
        "version":     settings.app_version,
        "environment": settings.environment,
        "checks": {
            "postgres": "ok" if db_ok    else "unreachable",
            "redis":    "ok" if redis_ok else "unreachable",
        },
        "response_time_ms": elapsed_ms,
    }


@app.get("/", tags=["ops"])
def root():
    return {
        "app":     settings.app_name,
        "version": settings.app_version,
        "docs":    "/docs",
        "health":  "/health",
    }