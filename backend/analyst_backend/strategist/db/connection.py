"""
db/connection.py — Customer Retention Platform
===============================================

Manages two asyncpg connection pools shared across BOTH agents:

  SCOUT pool  → Scout/Strategist DB
      Tables read: entity_listings, price_history, pricing_recommendations,
                   customer_price_context (written by Strategist, read by Retention)

  ANALYST pool → Analyst DB
      Tables read:  churn_scores, client_config, customer_rfm_features,
                    value_propositions, customers
      Tables write: retention_interventions, pricing_recommendations (via Strategist)

If SCOUT_DB_URL == ANALYST_DB_URL (same Postgres instance), we reuse
one pool for both — no duplicate connections.

Environment variables (see .env.example):
    SCOUT_DB_URL    — full DSN for Scout/Strategist DB
    ANALYST_DB_URL  — full DSN for Analyst DB
    DATABASE_URL    — fallback if above are not set

All connection strings are normalised to asyncpg-compatible format
(strips the 'postgresql+asyncpg://' SQLAlchemy prefix if present).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Module-level pool references — initialised in create_pools(), never before
_scout_pool:   Optional[asyncpg.Pool] = None
_analyst_pool: Optional[asyncpg.Pool] = None


def _build_dsn(env_key: str, fallback_key: Optional[str] = None) -> str:
    url = os.getenv(env_key, "").strip()

    if not url and fallback_key:
        url = os.getenv(fallback_key, "").strip()

    if not url:
        raise RuntimeError(
            f"No database URL found. Set {env_key} or {fallback_key} in .env"
        )

    url = url.replace("postgresql+asyncpg://", "postgresql://")
    url = url.replace("postgres://", "postgresql://")
    return url


async def create_pools() -> None:
    """
    Create asyncpg connection pools on application startup.
    Called once by the FastAPI lifespan context manager.

    No min_size/max_size limits — asyncpg opens connections on demand
    and closes them when idle. This prevents connection exhaustion on
    the DB server during development (frequent uvicorn restarts) and
    on QA where multiple developers share the same Postgres instance.

    max_inactive_connection_lifetime=60 ensures idle connections are
    automatically closed after 60 seconds and returned to the OS,
    cleaning up stale connections from previous uvicorn restarts.

    If both DSNs point to the same host/database, the scout pool is reused
    for analyst queries — avoids duplicate connections on small instances.
    """
    global _scout_pool, _analyst_pool

    scout_dsn   = _build_dsn("SCOUT_DB_URL",   "DATABASE_URL")
    analyst_dsn = _build_dsn("ANALYST_DB_URL", "DATABASE_URL")

    logger.info("Connecting to Scout/Strategist DB …")
    _scout_pool = await asyncpg.create_pool(
        dsn=scout_dsn,
        min_size=1,
        max_size=10,
        command_timeout=30,
        statement_cache_size=0,              # required for pgBouncer compatibility
        max_inactive_connection_lifetime=60, # auto-close idle connections after 60s
    )
    logger.info("Scout pool ready.")

    if analyst_dsn != scout_dsn:
        # Different databases → separate pool
        logger.info("Connecting to Analyst DB (separate instance) …")
        _analyst_pool = await asyncpg.create_pool(
            dsn=analyst_dsn,
            min_size=1,
            max_size=10,
            command_timeout=30,
            statement_cache_size=0,
            max_inactive_connection_lifetime=60, # auto-close idle connections after 60s
        )
        logger.info("Analyst pool ready.")
    else:
        # Same instance → share the scout pool to avoid double connections
        logger.info("Scout + Analyst on same Postgres instance — sharing pool.")
        _analyst_pool = _scout_pool


async def close_pools() -> None:
    """
    Gracefully close all connection pools on application shutdown.
    Handles the shared-pool case to avoid double-closing.
    """
    global _scout_pool, _analyst_pool

    if _scout_pool:
        await _scout_pool.close()
        logger.info("Scout pool closed.")

    # Only close analyst pool separately if it's a different object
    if _analyst_pool and _analyst_pool is not _scout_pool:
        await _analyst_pool.close()
        logger.info("Analyst pool closed.")

    _scout_pool = None
    _analyst_pool = None


async def get_scout_pool() -> asyncpg.Pool:
    """
    Return the Scout/Strategist DB pool.
    Raises RuntimeError if create_pools() was not called first.
    """
    if not _scout_pool:
        raise RuntimeError(
            "Scout DB pool is not initialised. "
            "Ensure create_pools() is called during app startup."
        )
    return _scout_pool


async def get_analyst_pool() -> asyncpg.Pool:
    """
    Return the Analyst DB pool.
    Raises RuntimeError if create_pools() was not called first.
    """
    if not _analyst_pool:
        raise RuntimeError(
            "Analyst DB pool is not initialised. "
            "Ensure create_pools() is called during app startup."
        )
    return _analyst_pool


async def health_check() -> dict[str, str]:
    """
    Ping both databases with 'SELECT 1'.
    Returns {"scout": "ok", "analyst": "ok"} on success,
    or {"scout": "error: ...", ...} on failure.
    Used by the /health endpoint and startup logging.
    """
    results: dict[str, str] = {}

    for name, getter in [("scout", get_scout_pool), ("analyst", get_analyst_pool)]:
        try:
            pool = await getter()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")   # lightweight connectivity check
            results[name] = "ok"
        except Exception as exc:
            results[name] = f"error: {exc}"

    return results