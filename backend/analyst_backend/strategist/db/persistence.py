"""
db/persistence.py — Customer Retention Platform
================================================

Orchestration layer between routers and repositories.
Routers call functions here; never touch repositories directly.

This module handles:
  - All DB reads needed BEFORE an agent runs (load config, load data)
  - All DB writes needed AFTER an agent runs (persist results)
  - Fallback defaults when DB is unavailable (agents degrade gracefully)
  - Logging at appropriate levels

Strategist Agent uses:
  fetch_client_config()     → load guardrail thresholds
  fetch_market_trends()     → compute market trend from price_history
  persist_run()             → write recommendations + customer_price_context

Retention Agent uses:
  fetch_client_config()     → same function (shared config)
  fetch_at_risk_customers() → pull HIGH+MEDIUM churn scores
  fetch_value_props()       → load discount rules
  fetch_price_contexts()    → detect existing Strategist retention prices
  persist_interventions()   → write retention_interventions
  update_outcome()          → CRM webhook: record offer response
  get_escalations()         → human review queue
  get_retention_summary()   → aggregate stats
"""

from __future__ import annotations

import logging
from typing import Optional

from strategist.db.connection import get_analyst_pool, get_scout_pool
from strategist.db.repositories import (
    ChurnScoresRepo,
    ClientConfigRepo,
    CustomerPriceContextRepo,
    PriceHistoryRepo,
    PricingRecommendationsRepo,
    RetentionRepo,
    ValuePropositionsRepo,
)
from strategist.models.schemas import (
    ChurnScore,
    ClientConfig,
    CustomerPriceContext,
    PricingRecommendation,
    RetentionBatch,
    RetentionIntervention,
    StrategistRequest,
    ValueProposition,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults — used when Analyst DB is unreachable (graceful degradation)
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG = ClientConfig(
    client_id          = "CLT-001",
    client_name        = "Default Client",
    currency           = "USD",
    max_discount_pct   = 30.0,   # never give more than 30% discount
    high_ltv_threshold = 500.0,  # USD — escalate customers above this spend
    mid_ltv_threshold  = 250.0,
    churn_window_days  = 90,
)


# ===========================================================================
# Shared: Client Config
# ===========================================================================

async def fetch_client_config(client_id: str) -> ClientConfig:
    """
    Load client-level guardrails from Analyst DB.
    Falls back to _DEFAULT_CONFIG when DB is unavailable or client not found.
    Both agents call this — it's the single source of truth for max_discount_pct.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            cfg = await ClientConfigRepo.get(conn, client_id)
            if cfg:
                logger.info("Loaded client_config for %s (max_discount=%.0f%%).",
                            client_id, cfg.max_discount_pct)
                return cfg
            logger.warning("client_config not found for %s — using defaults.", client_id)
    except Exception as exc:
        logger.error("Cannot fetch client_config: %s — using defaults.", exc)

    return _DEFAULT_CONFIG


# ===========================================================================
# Strategist Agent: Market Trends
# ===========================================================================

async def fetch_market_trends(product_names: list[str]) -> dict[str, str]:
    """
    Compute market trend for each product by comparing recent vs historical prices.

    Returns: {product_name: "rising" | "falling" | "stable"}

    If DB is unreachable, returns "stable" for all products — the pricing engine
    makes sensible decisions without trend data (it just won't be trend-aware).
    """
    if not product_names:
        return {}

    trends: dict[str, str] = {}

    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            for name in product_names:
                trend = await PriceHistoryRepo.get_trend(conn, name)
                trends[name] = trend
                if trend != "stable":
                    logger.info("Market trend for '%s': %s", name, trend)
    except Exception as exc:
        logger.warning("Cannot fetch market trends: %s — defaulting all to 'stable'.", exc)
        return {name: "stable" for name in product_names}

    return trends


# ===========================================================================
# Strategist Agent: Persist Run
# ===========================================================================

async def persist_run(
    request: StrategistRequest,
    recommendations: list[PricingRecommendation],
    run_id: str,
) -> dict:
    """
    Persist Strategist Agent results to both databases after a successful run.

    Scout DB writes:
      - pricing_recommendations: one row per product
      - customer_price_context:  one row per HIGH-risk customer (strategy='retention')
        → this is the handoff to Retention Agent (double-discount prevention)

    Returns a summary dict for logging.
    """
    if not recommendations:
        return {"rows_written": 0}

    client_id = request.client_id
    recs_written = 0
    contexts_written = 0

    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            for rec in recommendations:
                # Write pricing recommendation
                await PricingRecommendationsRepo.insert_recommendation(
                    conn, rec, run_id, client_id
                )
                recs_written += 1

                # Write customer_price_context for retention-discounted customers
                # Only when a HIGH-risk churn customer received a special price
                if rec.strategy == "retention" and rec.churn_context:
                    await CustomerPriceContextRepo.insert_price_context(
                        conn,
                        rec,
                        customer_id=rec.churn_context.customer_id,
                        client_id=client_id,
                        run_id=run_id,
                    )
                    contexts_written += 1

    except Exception as exc:
        logger.error("persist_run failed for run_id=%s: %s", run_id, exc)
        return {"rows_written": recs_written, "error": str(exc)}

    logger.info(
        "persist_run: %d recommendations + %d price contexts written (run_id=%s).",
        recs_written, contexts_written, run_id
    )
    return {
        "recommendations_written": recs_written,
        "price_contexts_written":  contexts_written,
    }


# ===========================================================================
# Retention Agent: Load Data
# ===========================================================================

async def fetch_at_risk_customers(
    client_id: str,
    risk_tiers: tuple[str, ...] = ("HIGH", "MEDIUM"),
) -> list[ChurnScore]:
    """
    Pull the latest churn scores for HIGH and MEDIUM risk customers.

    Called when the retention API request does NOT include an inline churn_batch
    (i.e. the agent pulls from DB automatically).

    Returns empty list if DB is unavailable — caller raises 404.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            scores = await ChurnScoresRepo.get_at_risk(conn, client_id, risk_tiers)
            return scores
    except Exception as exc:
        logger.error("Cannot fetch churn scores: %s", exc)
        return []


async def fetch_value_props(client_id: str = "default") -> list[ValueProposition]:
    """
    Load discount rules from value_propositions table.
    Client-specific rules take priority over default rules.
    Returns empty list on failure — agents use hardcoded _VP_DISCOUNTS fallback.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            return await ValuePropositionsRepo.get_all(conn, client_id)
    except Exception as exc:
        logger.warning("Cannot fetch value_propositions: %s — using hardcoded table.", exc)
        return []


async def fetch_price_contexts(
    client_id: str,
    customer_ids: list[str],
) -> dict[str, CustomerPriceContext]:
    """
    Check Scout/Strategist DB for customers that already received a retention price.

    This is the double-discount prevention mechanism:
      1. Strategist Agent sets strategy='retention' and writes customer_price_context
      2. Retention Agent reads this table BEFORE generating its own discount
      3. If a context exists, Retention skips the discount — only sends a message

    Returns empty dict on failure — Retention Agent proceeds with full pricing.
    """
    if not customer_ids:
        return {}

    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            contexts = await CustomerPriceContextRepo.get_latest_retention_prices(
                conn, client_id, customer_ids
            )
            if contexts:
                logger.info(
                    "%d customers already have a Strategist retention price. "
                    "Retention Agent will skip pricing for these.",
                    len(contexts)
                )
            return contexts
    except Exception as exc:
        logger.warning(
            "Cannot fetch customer_price_context: %s — Retention will price normally.", exc
        )
        return {}


# ===========================================================================
# Retention Agent: Persist Results
# ===========================================================================

async def persist_interventions(batch: RetentionBatch) -> dict:
    """
    Write all generated retention interventions to retention_interventions table.
    Returns a dict with rows_written count for logging.
    """
    if not batch.interventions:
        logger.info("No interventions to persist.")
        return {"rows_written": 0}

    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            ids = await RetentionRepo.insert_interventions(
                conn, batch.interventions, batch.run_id
            )
        return {"rows_written": len(ids)}

    except Exception as exc:
        logger.error("persist_interventions failed (run_id=%s): %s", batch.run_id, exc)
        return {"rows_written": 0, "error": str(exc)}


async def update_outcome(
    intervention_id: int,
    offer_status: str,
    revenue_recovered: Optional[float],
) -> bool:
    """
    Update the outcome of a sent retention offer.
    Called by CRM/marketing automation webhook.
    Returns True if intervention found and updated, False if not found.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            return await RetentionRepo.update_outcome(
                conn, intervention_id, offer_status, revenue_recovered
            )
    except Exception as exc:
        logger.error("update_outcome failed for id=%d: %s", intervention_id, exc)
        return False


async def get_escalations(client_id: str) -> list[dict]:
    """
    Return all pending human-escalation interventions for a client.
    Human escalation: Platinum/Gold customers with churn_prob >= 0.90.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            return await RetentionRepo.get_pending_escalations(conn, client_id)
    except Exception as exc:
        logger.error("get_escalations failed for client=%s: %s", client_id, exc)
        return []


async def get_retention_summary(client_id: str) -> dict:
    """
    Aggregate retention stats for a client (total offers, conversion rate, revenue recovered).
    Auto-expires pending offers older than 7 days → marks them as 'no_response'.
    """
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            # Auto-expire: pending offers older than 7 days → no_response
            expired = await conn.execute(
                """
                UPDATE retention_interventions
                SET offer_status = 'no_response'
                WHERE client_id    = $1
                  AND offer_status = 'pending'
                  AND created_at   < NOW() - INTERVAL '7 days'
                """,
                client_id,
            )
            if expired != "UPDATE 0":
                logger.info(
                    "get_retention_summary: auto-expired pending offers for client=%s (%s)",
                    client_id, expired
                )
            return await RetentionRepo.get_summary(conn, client_id)
    except Exception as exc:
        logger.error("get_retention_summary failed for client=%s: %s", client_id, exc)
        return {}