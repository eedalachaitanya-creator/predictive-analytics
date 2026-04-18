"""
main.py — CRP Analyst Agent | FastAPI entry point
Sprint 0 | Task 1.3
"""
from dotenv import load_dotenv
load_dotenv()  # Load .env BEFORE any other imports read os.getenv()

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
from app.churn_router import router as churn_router
from app.downloads_router import router as downloads_router
from app.validation_router import router as validation_router
from app.chat_router import router as chat_router
from app.messages_router import router as messages_router
from scout.router import scout_router

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=settings.log_level.upper())
log = logging.getLogger("crp_api")

# ── Startup / shutdown ────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 CRP Analyst Agent starting up ...")
    yield
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
app.include_router(churn_router)
app.include_router(pipeline_router)
app.include_router(customers_router)
app.include_router(orders_router)
app.include_router(analytics_router)
app.include_router(dashboard_router)
app.include_router(downloads_router)
app.include_router(validation_router)
app.include_router(chat_router)
app.include_router(messages_router)
app.include_router(scout_router)

# ── LangFuse Cost Tracking ────────────────────────────────────────────────────

@app.get("/api/v1/cost-tracking", tags=["ops"])
def get_cost_tracking():
    """Return LLM cost tracking configuration and summary."""
    try:
        from app.langfuse_tracker import get_cost_summary
        return get_cost_summary()
    except Exception as e:
        return {"error": str(e), "langfuse_enabled": False}


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
