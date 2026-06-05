"""
db/repositories.py — Customer Retention Platform
=================================================

All asyncpg repository classes used by both agents.
Each class is stateless (all methods are @staticmethod) and receives
an open asyncpg.Connection from the caller — no pool management here.

Repository map:
  Scout/Strategist DB (read + write):
    PriceHistoryRepo          — market trend calculation from price_history
    PricingRecommendationsRepo — write strategy output
    CustomerPriceContextRepo  — write retention prices (Strategist), read them (Retention)

  Analyst DB (read + write):
    ClientConfigRepo          — load guardrail config
    ChurnScoresRepo           — pull HIGH/MEDIUM at-risk customers
    CustomerRfmRepo           — supplemental RFM features
    ValuePropositionsRepo     — discount rules per tier/risk
    RetentionRepo             — write interventions, update outcomes, query escalations

Design principles:
  - All queries use parameterised $1, $2, ... (never f-string SQL — SQL injection safe)
  - DISTINCT ON (customer_id) ORDER BY ... DESC → always gets the LATEST record per customer
  - All methods log at INFO level so queries are traceable without a debugger
  - Errors propagate to the caller (persistence.py) which handles fallbacks
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg

from strategist.models.schemas import (
    ChurnScore,
    ClientConfig,
    CustomerPriceContext,
    PricingRecommendation,
    RetentionIntervention,
    ValueProposition,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# Scout/Strategist DB — price_history
# ===========================================================================

class PriceHistoryRepo:

    @staticmethod
    async def get_trend(
        conn: asyncpg.Connection,
        product_name: str,
        short_window_days: int = 14,
        long_window_days:  int = 30,
    ) -> str:
        """
        Determine market trend for a product by comparing recent vs older prices.

        Logic:
          - short_avg = average price in last 14 days
          - long_avg  = average price in last 30 days
          - If short_avg > long_avg × 1.02 → "rising"  (2% threshold avoids noise)
          - If short_avg < long_avg × 0.98 → "falling"
          - Otherwise                       → "stable"

        Returns "stable" if insufficient data (< 2 records in either window).
        """
        now        = datetime.now(timezone.utc)
        short_from = now - timedelta(days=short_window_days)
        long_from  = now - timedelta(days=long_window_days)

        # Fetch average prices in both windows in a single query
        row = await conn.fetchrow(
            """
            SELECT
                AVG(price) FILTER (WHERE scraped_at >= $2) AS short_avg,
                AVG(price) FILTER (WHERE scraped_at >= $3) AS long_avg,
                COUNT(*)   FILTER (WHERE scraped_at >= $2) AS short_count,
                COUNT(*)   FILTER (WHERE scraped_at >= $3) AS long_count
            FROM price_history
            WHERE product_name = $1
              AND scraped_at  >= $3
            """,
            product_name,
            short_from,
            long_from,
        )

        if not row or not row["short_avg"] or not row["long_avg"]:
            return "stable"   # insufficient data → neutral assumption

        short_count = row["short_count"] or 0
        long_count  = row["long_count"]  or 0

        # Need at least 2 data points in each window to be reliable
        if short_count < 2 or long_count < 2:
            return "stable"

        short_avg = float(row["short_avg"])
        long_avg  = float(row["long_avg"])

        if short_avg > long_avg * 1.02:
            return "rising"
        elif short_avg < long_avg * 0.98:
            return "falling"
        else:
            return "stable"


# ===========================================================================
# Scout/Strategist DB — pricing_recommendations
# ===========================================================================

class PricingRecommendationsRepo:

    @staticmethod
    async def insert_recommendation(
        conn: asyncpg.Connection,
        rec: PricingRecommendation,
        run_id: str,
        client_id: str,
    ) -> Optional[int]:
        """
        Write one pricing recommendation to the pricing_recommendations table.
        platform_breakdown is serialised to JSON (stored as JSONB).
        Returns the generated recommendation_id (None on failure).
        """
        try:
            rec_id = await conn.fetchval(
                """
                INSERT INTO pricing_recommendations (
                    run_id,           client_id,        product_name,
                    suggested_price,  pre_retention_price, floor_price,
                    target_price,     our_cost,         raw_cogs,
                    competitor_min,   competitor_avg,   competitor_max,
                    competitor_median, strategy,        confidence,
                    margin_percent,   market_trend,     flag,
                    reasoning,        platform_breakdown
                ) VALUES (
                    $1,  $2,  $3,
                    $4,  $5,  $6,
                    $7,  $8,  $9,
                    $10, $11, $12,
                    $13, $14, $15,
                    $16, $17, $18,
                    $19, $20::jsonb
                )
                RETURNING recommendation_id
                """,
                run_id, client_id, rec.product_name,
                rec.suggested_price, rec.pre_retention_price, rec.floor_price,
                rec.target_price, rec.our_cost, rec.raw_cogs,
                rec.competitor_min, rec.competitor_avg, rec.competitor_max,
                rec.competitor_median, rec.strategy, rec.confidence,
                rec.margin_percent, rec.market_trend, rec.flag,
                rec.reasoning,
                # Convert platform breakdown list to JSON string for JSONB column
                json.dumps([p.model_dump() for p in rec.platform_breakdown]),
            )
            return rec_id
        except Exception as exc:
            logger.error("Failed to insert recommendation for %s: %s", rec.product_name, exc)
            return None


# ===========================================================================
# Scout/Strategist DB — customer_price_context
# Cross-agent bridge: Strategist writes, Retention reads
# ===========================================================================

class CustomerPriceContextRepo:

    @staticmethod
    async def insert_price_context(
        conn: asyncpg.Connection,
        rec: PricingRecommendation,
        customer_id: str,
        client_id: str,
        run_id: str,
    ) -> None:
        """
        Write a customer-specific retention price to customer_price_context.

        Called by Strategist Agent when strategy = "retention" (churn discount applied).
        The Retention Agent reads this table to avoid double-discounting.

        Upsert logic: if the same customer already has a retention price for this
        run, update it (prevents duplicates if the endpoint is called twice).
        """
        churn_ctx = rec.churn_context
        if not churn_ctx:
            return   # Only write when churn fusion was actually applied

        await conn.execute(
            """
            INSERT INTO customer_price_context (
                customer_id,         client_id,          product_name,
                strategy,            suggested_price,    pre_retention_price,
                discount_pct_applied, churn_probability, risk_tier,
                run_id,              created_at
            ) VALUES (
                $1,  $2,  $3,
                $4,  $5,  $6,
                $7,  $8,  $9,
                $10, NOW()
            )
            ON CONFLICT (customer_id, product_name)
            DO UPDATE SET
                strategy             = EXCLUDED.strategy,
                suggested_price      = EXCLUDED.suggested_price,
                pre_retention_price  = EXCLUDED.pre_retention_price,
                discount_pct_applied = EXCLUDED.discount_pct_applied,
                churn_probability    = EXCLUDED.churn_probability,
                risk_tier            = EXCLUDED.risk_tier,
                run_id               = EXCLUDED.run_id,
                created_at           = NOW()
            """,
            customer_id, client_id, rec.product_name,
            rec.strategy, rec.suggested_price, rec.pre_retention_price,
            churn_ctx.discount_applied, churn_ctx.churn_probability,
            churn_ctx.risk_level,
            run_id,
        )

    @staticmethod
    async def get_latest_retention_prices(
        conn: asyncpg.Connection,
        client_id: str,
        customer_ids: list[str],
    ) -> dict[str, CustomerPriceContext]:
        """
        Read Strategist-applied retention prices for a list of customers.

        Used by Retention Agent to detect customers that already received
        a churn discount — those customers get a message but no additional discount.

        Returns: {customer_id: CustomerPriceContext} for customers with strategy='retention'.
        Customers not found are absent from the dict (Retention Agent handles pricing normally).
        """
        if not customer_ids:
            return {}

        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (customer_id)
                customer_id,
                product_name,
                strategy,
                suggested_price,
                pre_retention_price,
                discount_pct_applied,
                churn_probability,
                risk_tier,
                run_id,
                created_at
            FROM customer_price_context
            WHERE client_id   = $1
              AND customer_id = ANY($2::text[])
              AND strategy    = 'retention'
            ORDER BY customer_id, created_at DESC
            """,
            client_id,
            customer_ids,
        )

        result = {
            r["customer_id"]: CustomerPriceContext(**dict(r))
            for r in rows
        }
        logger.info(
            "Found %d/%d customers with existing strategist retention prices.",
            len(result), len(customer_ids)
        )
        return result


# ===========================================================================
# Analyst DB — client_config (read-only)
# ===========================================================================

class ClientConfigRepo:

    @staticmethod
    async def get(
        conn: asyncpg.Connection,
        client_id: str,
    ) -> Optional[ClientConfig]:
        """
        Load client-level guardrail config.
        Returns None if the client_id is not found — caller uses defaults.
        """
        row = await conn.fetchrow(
            """
            SELECT
                client_id,
                client_name,
                currency,
                max_discount_pct,
                high_ltv_threshold,
                mid_ltv_threshold,
                churn_window_days
            FROM client_config
            WHERE client_id = $1
            """,
            client_id,
        )
        if not row:
            logger.warning("No client_config found for client_id=%s", client_id)
            return None
        return ClientConfig(**dict(row))


# ===========================================================================
# Analyst DB — churn_scores (read-only)
# ===========================================================================

class ChurnScoresRepo:

    @staticmethod
    async def get_at_risk(
        conn: asyncpg.Connection,
        client_id: str,
        risk_tiers: tuple[str, ...] = ("HIGH", "MEDIUM"),
        limit: int = 500,
    ) -> list[ChurnScore]:
        """
        Pull the most-recent churn score for every at-risk customer.

        Uses DISTINCT ON (customer_id) ORDER BY scored_at DESC to ensure
        we get exactly one row per customer (the latest score), even if
        the Analyst Agent has run multiple times.

        JOINs:
          - mv_customer_features (materialized view) → customer_tier, is_high_value,
            avg_rating, spending, order history, recency signals.
            This view already computes all derived features from orders + reviews + rfm.
            Falls back to customer_rfm_features if mv not populated.

        DB column 'risk_tier' is aliased to 'risk_level' to match the JSON
        field name used in ChurnScore and churn_scores.json.
        """
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (cs.customer_id)
                cs.client_id,
                cs.customer_id,
                cs.churn_probability,
                cs.risk_tier                              AS risk_level,
                COALESCE(mv.customer_tier,
                         rf.customer_tier, 'Bronze')      AS customer_tier,
                COALESCE(mv.total_spend_usd,
                         rf.total_spend_usd,       0)     AS total_spend_usd,
                COALESCE(mv.total_orders,
                         rf.total_orders,          0)     AS total_orders,
                COALESCE(mv.avg_order_value_usd,
                         rf.avg_order_value_usd,   0)     AS avg_order_value_usd,
                COALESCE(mv.avg_rating,            0)     AS avg_rating,
                COALESCE(mv.days_since_last_order,
                         rf.days_since_last_order, 0)     AS days_since_last_order,
                0                                          AS is_high_value,
                COALESCE(mv.rfm_total_score,
                         rf.rfm_total_score,       0)     AS rfm_total_score
            FROM churn_scores cs
            LEFT JOIN mv_customer_features mv
                   ON mv.customer_id = cs.customer_id
                  AND mv.client_id   = cs.client_id
            LEFT JOIN customer_rfm_features rf
                   ON rf.customer_id = cs.customer_id
                  AND rf.client_id   = cs.client_id
            WHERE cs.client_id  = $1
              AND cs.risk_tier  = ANY($2::text[])
            ORDER BY cs.customer_id, cs.scored_at DESC
            LIMIT $3
            """,
            client_id,
            list(risk_tiers),
            limit,
        )
        scores = [ChurnScore(**dict(r)) for r in rows]
        logger.info("Fetched %d at-risk customers (tiers: %s).", len(scores), risk_tiers)
        return scores


# ===========================================================================
# Analyst DB — value_propositions (read-only)
# ===========================================================================

# Analyst Agent uses "At-Risk"/"Reactivated"/"New" as risk_level values.
# Strategist and Retention agents need "HIGH"/"MEDIUM"/"LOW".
# We map at read time so the DB is never touched.
_VP_RISK_LEVEL_MAP: dict[str, str] = {
    "At-Risk":    "HIGH",
    "Reactivated": "LOW",
    "New":        "LOW",
    # Pass-through for already-correct values
    "HIGH":   "HIGH",
    "MEDIUM": "MEDIUM",
    "LOW":    "LOW",
}


class ValuePropositionsRepo:

    @staticmethod
    async def get_all(conn: asyncpg.Connection, client_id: str = "default") -> list[ValueProposition]:
        rows = await conn.fetch(
            """
            SELECT
                tier_name,
                risk_level,
                action_type,
                message_template,
                discount_pct,
                channel,
                priority
            FROM value_propositions
            WHERE client_id = $1
               OR client_id = 'default'
            ORDER BY
                CASE WHEN client_id = $1 THEN 0 ELSE 1 END,
                priority ASC
            """,
            client_id,
        )
        vps = []
        for r in rows:
            row_dict = dict(r)
            # Map Analyst Agent risk_level names → agent-expected names
            row_dict["risk_level"] = _VP_RISK_LEVEL_MAP.get(
                row_dict["risk_level"], "LOW"
            )
            try:
                vps.append(ValueProposition(**row_dict))
            except Exception:
                # Skip rows that don't pass schema validation after mapping
                logger.debug(
                    "ValuePropositionsRepo: skipped row with risk_level=%s",
                    row_dict.get("risk_level"),
                )
        logger.info("Loaded %d value propositions (after risk_level mapping).", len(vps))
        return vps


# ===========================================================================
# Analyst DB — retention_interventions (read + write)
# ===========================================================================

class RetentionRepo:

    @staticmethod
    async def insert_interventions(
        conn: asyncpg.Connection,
        interventions: list[RetentionIntervention],
        run_id: str,
    ) -> list[int]:
        """
        Bulk-insert all interventions generated in a single run.
        Executes one INSERT per row (asyncpg does not support true bulk COPY here
        without additional setup — acceptable for typical batch sizes of 10-500).
        Returns list of generated intervention_ids for reference.
        """
        ids: list[int] = []

        for i in interventions:
            row_id = await conn.fetchval(
                """
                INSERT INTO retention_interventions (
                    client_id,           customer_id,         churn_probability,
                    risk_tier,           offer_type,          discount_pct,
                    offer_message,       channel,             customer_ltv_usd,
                    max_allowed_discount, guardrail_passed,   escalated_to_human,
                    offer_status,        langfuse_trace_id,   agent_cost_usd
                ) VALUES (
                    $1,  $2,  $3,
                    $4,  $5,  $6,
                    $7,  $8,  $9,
                    $10, $11, $12,
                    $13, $14, $15
                )
                RETURNING intervention_id
                """,
                # Customer identity
                i.client_id, i.customer_id, i.churn_probability,
                # Risk + offer details
                i.risk_tier, i.offer_type, i.discount_pct,
                i.offer_message, i.channel, i.customer_ltv_usd,
                # Guardrail audit fields
                i.max_allowed_discount, i.guardrail_passed, i.escalated_to_human,
                # Status + tracing
                i.offer_status,
                run_id,          # stored as langfuse_trace_id for cross-agent tracing
                i.agent_cost_usd,
            )
            i.intervention_id = row_id   # mutate in-place so caller can read IDs
            ids.append(row_id)

        logger.info("Inserted %d interventions (run_id=%s).", len(ids), run_id)
        return ids

    @staticmethod
    async def update_outcome(
        conn: asyncpg.Connection,
        intervention_id: int,
        offer_status: str,
        revenue_recovered: Optional[float],
    ) -> bool:
        """
        Update offer outcome when CRM reports customer response.
        Returns True if a row was updated, False if intervention_id not found.
        """
        result = await conn.execute(
            """
            UPDATE retention_interventions
            SET offer_status        = $1,
                revenue_recovered   = $2,
                outcome_recorded_at = NOW()
            WHERE intervention_id = $3
            """,
            offer_status,
            revenue_recovered,
            intervention_id,
        )
        # asyncpg returns "UPDATE N" — extract N to check if any row was updated
        updated = int(result.split()[-1]) > 0

        if updated:
            logger.info(
                "Outcome recorded: intervention_id=%d status=%s revenue=%s",
                intervention_id, offer_status, revenue_recovered
            )
        else:
            logger.warning("update_outcome: intervention_id=%d not found.", intervention_id)

        return updated

    @staticmethod
    async def get_pending_escalations(
        conn: asyncpg.Connection,
        client_id: str,
    ) -> list[dict]:
        """
        Return all interventions flagged for human review.

        Human escalation is triggered for Platinum/Gold customers with
        churn_probability >= 0.90 — these are too valuable to lose to
        automated messaging alone and need a personal outreach.

        Results ordered by churn_probability DESC (worst cases first).
        """
        rows = await conn.fetch(
            """
            SELECT
                intervention_id,
                customer_id,
                churn_probability,
                risk_tier,
                offer_type,
                discount_pct,
                offer_message,
                channel,
                customer_ltv_usd,
                created_at
            FROM retention_interventions
            WHERE client_id          = $1
              AND escalated_to_human = TRUE
              AND offer_status       = 'pending'
            ORDER BY churn_probability DESC
            """,
            client_id,
        )
        return [dict(r) for r in rows]

    @staticmethod
    async def get_summary(
        conn: asyncpg.Connection,
        client_id: str,
    ) -> dict:
        """
        Aggregate summary of retention activity for a client.
        Used by GET /api/retention/summary/{client_id}.
        """
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                                   AS total_interventions,
                COUNT(*) FILTER (WHERE risk_tier = 'HIGH')                 AS high_risk_count,
                COUNT(*) FILTER (WHERE risk_tier = 'MEDIUM')               AS medium_risk_count,
                COUNT(*) FILTER (WHERE escalated_to_human = TRUE)          AS escalated_count,
                COUNT(*) FILTER (WHERE offer_status = 'accepted')          AS accepted_count,
                COUNT(*) FILTER (WHERE offer_status = 'declined')          AS declined_count,
                COUNT(*) FILTER (WHERE offer_status = 'no_response')       AS no_response_count,
                COALESCE(SUM(revenue_recovered), 0)                        AS total_revenue_recovered,
                ROUND(AVG(discount_pct), 2)                                AS avg_discount_pct
            FROM retention_interventions
            WHERE client_id = $1
            """,
            client_id,
        )
        return dict(row) if row else {}