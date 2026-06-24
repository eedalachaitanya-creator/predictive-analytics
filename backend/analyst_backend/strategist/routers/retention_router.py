"""
routers/retention_router.py — Customer Retention Platform
==========================================================

Endpoints:

  POST /api/retention/run
      Full pipeline: load config → score churn → check Strategist prices →
      generate offers → apply guardrails → persist interventions.

  GET  /api/retention/escalations
      Return all interventions flagged for human outreach.
      Use this to populate the CRM's manual outreach queue.

  PATCH /api/retention/{intervention_id}/outcome
      CRM webhook: record what happened after an offer was sent.
      Updates offer_status and revenue_recovered.

  GET  /api/retention/summary/{client_id}
      Aggregate retention stats: total offers, conversion rate, revenue recovered.
"""

from __future__ import annotations
from strategist.services.email_service import send_retention_emails

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException

from app.auth_router import get_current_user
from strategist.agents.retention_agent import RetentionAgent, RetentionConfig
from strategist.db.persistence import (
    fetch_at_risk_customers,
    fetch_client_config,
    fetch_price_contexts,
    fetch_value_props,
    get_escalations,
    get_retention_summary,
    persist_interventions,
    update_outcome,
)
from strategist.models.schemas import (
    OutcomeUpdate,
    RetentionRequest,
    RetentionResponse,
)

logger = logging.getLogger(__name__)
# SECURITY: require a valid session token for every route (was unauthenticated —
# /recommend and /run also WRITE per-tenant rows). Tenant-scoping is a follow-up.
router = APIRouter(prefix="/api/retention", tags=["Retention Agent"],
                   dependencies=[Depends(get_current_user)])


# ---------------------------------------------------------------------------
# POST /run — main retention pipeline endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/run",
    response_model=RetentionResponse,
    summary="Run retention offer pipeline",
#     description="""
# **Full pipeline:**

# 1. Load `client_config` from Analyst DB (max_discount_pct, LTV thresholds)
# 2. Load `value_propositions` discount rules from Analyst DB
# 3. Pull HIGH + MEDIUM churn scores (from inline `churn_batch` OR Analyst DB query)
# 4. Check Strategist DB `customer_price_context` — skip discount for customers  
#    where the Strategist already applied a retention price (prevents double-discounting)
# 5. Generate personalised retention offers: discount_pct, message, channel
# 6. Apply guardrails: cap at max_discount_pct, escalate high-value at-risk customers
# 7. Persist to `retention_interventions` (Analyst DB)

# **Passing churn data:**
# - **Inline**: Include `churn_batch` in the request body (paste churn_scores.json directly)
# - **From DB**: Omit `churn_batch` — agent queries Analyst DB automatically

# **`dry_run=true`** — generates offers but skips DB write. Use for testing/previewing.

# **`min_risk`** — set to `"HIGH"` to only process HIGH-risk customers (skip MEDIUM).
# """,
)
async def run_retention(
    request: RetentionRequest,
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> RetentionResponse:

    # Header takes precedence over body client_id
    client_id = x_client_id or request.client_id

    # ── Step 1: Load guardrails from Analyst DB ──────────────────────────
    client_config = await fetch_client_config(client_id)
    logger.info(
        "Client config loaded: client=%s max_discount=%.0f%%",
        client_id, client_config.max_discount_pct
    )

    # ── Step 2: Load discount rules ──────────────────────────────────────
    value_props = await fetch_value_props(client_id)
    logger.info("Loaded %d value propositions.", len(value_props))

    # ── Step 3: Get churn scores ──────────────────────────────────────────
    if request.churn_batch and request.churn_batch.scores:
        # Inline churn batch provided in request body
        churn_scores = request.churn_batch.scores
        logger.info("Using inline churn batch: %d scores.", len(churn_scores))
    else:
        # Pull from Analyst DB
        risk_tiers = ("HIGH",) if request.min_risk == "HIGH" else ("HIGH", "MEDIUM")
        churn_scores = await fetch_at_risk_customers(client_id, risk_tiers)

        if not churn_scores:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No at-risk customers found. Please run the pipeline first "
                    "(Configure & Run → Process Data) to generate churn scores, "
                    "then try again."
                ),
            )

    # ── Step 3a: Filter by specific customer_ids if provided ─────────────
    if request.customer_ids:
        allowed = set(request.customer_ids)
        churn_scores = [s for s in churn_scores if s.customer_id in allowed]
        logger.info(
            "customer_ids filter: %d customers selected out of available pool.",
            len(churn_scores)
        )

    logger.info("Processing %d churn scores.", len(churn_scores))

    # ── Step 3b: Dedup ───────────────────────────────────────────────────
    try:
        from strategist.db.connection import get_analyst_pool
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            existing_rows = await conn.fetch(
                """
                SELECT DISTINCT customer_id
                FROM retention_interventions
                WHERE client_id = $1
                  AND created_at > NOW() - INTERVAL '30 days'
                """,
                client_id,
            )
        already_offered = {r["customer_id"] for r in existing_rows}
        before_dedup = len(churn_scores)
        churn_scores = [s for s in churn_scores if s.customer_id not in already_offered]
        logger.info(
            "Dedup: %d customers already offered in last 30 days — %d remaining to process.",
            before_dedup - len(churn_scores), len(churn_scores)
        )
        if not churn_scores:
            raise HTTPException(
                status_code=404,
                detail=f"All at-risk customers for '{client_id}' already received offers in the last 30 days."
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Dedup check failed (non-fatal): %s — proceeding without dedup.", exc)

    # ── Step 4: Check Strategist DB for prior retention prices ────────────
    # This is the double-discount prevention step.
    # Customers with strategy='retention' in customer_price_context already
    # received a price discount from the Strategist — we skip our discount.
    customer_ids   = [s.customer_id for s in churn_scores]
    price_contexts = await fetch_price_contexts(client_id, customer_ids)

    if price_contexts:
        logger.info(
            "Found %d customers with existing Strategist retention prices — "
            "skipping discount for these, sending message only.",
            len(price_contexts)
        )

    # ── Step 5+6: Run agent (generates offers + applies guardrails) ───────
    agent = RetentionAgent(RetentionConfig(
        dry_run=request.dry_run,
        min_probability_medium=request.min_probability_medium,
    ))
    batch = agent.run(
        churn_scores     = churn_scores,
        client_config    = client_config,
        value_props      = value_props or None,
        price_contexts   = price_contexts,
        custom_discounts = request.custom_discounts,
    )

    logger.info(
        "Retention run complete: run_id=%s interventions=%d escalated=%d",
        batch.run_id,
        len(batch.interventions),
        batch.summary.get("escalated_to_human", 0)
    )

    # ── Step 7: Persist to Analyst DB ────────────────────────────────────
    if not request.dry_run:
        db_result = await persist_interventions(batch)
        logger.info("DB persist: %s", db_result)

        # ── Step 8: Send retention emails ─────────────────────────────────
        try:
            from strategist.db.connection import get_analyst_pool
            pool = await get_analyst_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT customer_id, customer_email, customer_name
                    FROM customers
                    WHERE client_id = $1
                      AND customer_id = ANY($2::text[])
                      AND customer_email IS NOT NULL
                    """,
                    client_id,
                    [i.customer_id for i in batch.interventions],
                )
            customer_emails = {
                r["customer_id"]: r["customer_email"] for r in rows
            }
            customer_names = {
                r["customer_id"]: r["customer_name"]
                for r in rows if r["customer_name"]
            }
            email_result = await send_retention_emails(
                batch.interventions, customer_emails, customer_names
            )
            logger.info("Email sending result: %s", email_result)
        except Exception as exc:
            logger.error("Email sending failed (non-fatal): %s", exc)
    else:
        logger.info("dry_run=True — skipping DB write and email sending.")

    return RetentionResponse(
        run_id        = batch.run_id,
        client_id     = batch.client_id,
        generated_at  = batch.generated_at,
        dry_run       = request.dry_run,
        summary       = batch.summary,
        interventions = batch.interventions,
    )


# ---------------------------------------------------------------------------
# GET /escalations — human outreach queue
# ---------------------------------------------------------------------------

@router.get(
    "/escalations",
    summary="Get pending human-escalation interventions",
#     description="""
# Returns all interventions flagged for human review.

# **Escalation criteria:**
# - churn_probability >= 0.90 (very high likelihood of churning)
# - AND customer tier is Platinum or Gold (OR is_high_value flag is set)

# These customers are too valuable to leave to automated messaging alone.
# Add them to the CRM manual outreach queue for personal contact by an account manager.

# Results are ordered by churn_probability DESC (most urgent first).
# """,
)
async def escalations(
    client_id: str,
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> dict:
    cid = x_client_id or client_id
    rows = await get_escalations(cid)
    return {
        "client_id":   cid,
        "count":       len(rows),
        "escalations": rows,
    }


# ---------------------------------------------------------------------------
# PATCH /{intervention_id}/outcome — CRM webhook
# ---------------------------------------------------------------------------

@router.patch(
    "/{intervention_id}/outcome",
    summary="Record outcome of a sent retention offer",
#     description="""
# Called by the CRM or marketing automation platform when a customer responds  
# (or doesn't) to a retention offer.

# **offer_status values:**
# - `accepted`    — customer re-engaged and made a purchase
# - `declined`    — customer explicitly opted out
# - `no_response` — no action taken after 7 days
# - `bounced`     — message delivery failed (bad email/phone)

# `revenue_recovered` should be filled when status is `accepted`.
# """,
)
async def record_outcome(
    intervention_id: int,
    body: OutcomeUpdate,
) -> dict:
    # Sanity check: path param and body must agree
    if body.intervention_id != intervention_id:
        raise HTTPException(
            status_code=422,
            detail="intervention_id in path and body must match.",
        )

    updated = await update_outcome(
        intervention_id   = intervention_id,
        offer_status      = body.offer_status,
        revenue_recovered = body.revenue_recovered,
    )

    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"Intervention {intervention_id} not found.",
        )

    return {
        "intervention_id":  intervention_id,
        "offer_status":     body.offer_status,
        "revenue_recovered": body.revenue_recovered,
        "updated":          True,
    }


# ---------------------------------------------------------------------------
# GET /summary/{client_id} — aggregate retention stats
# ---------------------------------------------------------------------------

@router.get(
    "/summary/{client_id}",
    summary="Get aggregate retention stats for a client",
#     description="""
# Returns aggregated retention statistics:
# - Total offers generated (by risk tier)
# - Escalation count
# - Conversion rates (accepted / declined / no_response / bounced)
# - Total revenue recovered
# - Average discount given

# Use this for the retention dashboard or weekly reporting.
# """,
)
async def retention_summary(
    client_id: str,
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> dict:
    cid = x_client_id or client_id

    # Belt-and-suspenders: never raise a 5xx to the UI. Empty summary or
    # "table doesn't exist" both mean the same thing to the user: no data yet.
    try:
        summary = await get_retention_summary(cid)
    except Exception as exc:
        logger.warning("retention_summary: get_retention_summary failed: %s", exc)
        summary = {}

    if not summary:
        return {
            "client_id": cid,
            "message":   "No retention interventions found for this client. "
                         "Run the pipeline in save-mode to populate.",
            "total_interventions": 0,
            "high_risk_count":     0,
            "medium_risk_count":   0,
            "escalated_count":     0,
            "accepted_count":      0,
            "declined_count":      0,
            "no_response_count":   0,
            "conversion_rate_pct": 0.0,
            "total_revenue_recovered": 0.0,
            "avg_discount_pct":    0.0,
        }

    # Compute conversion rate (accepted / total with a known outcome)
    total = summary.get("total_interventions", 0)
    accepted = summary.get("accepted_count", 0)

    return {
        "client_id":             cid,
        "total_interventions":   total,
        "high_risk_count":       summary.get("high_risk_count", 0),
        "medium_risk_count":     summary.get("medium_risk_count", 0),
        "escalated_count":       summary.get("escalated_count", 0),
        "accepted_count":        accepted,
        "declined_count":        summary.get("declined_count", 0),
        "no_response_count":     summary.get("no_response_count", 0),
        "conversion_rate_pct":   round(accepted / total * 100, 1) if total > 0 else 0.0,
        "total_revenue_recovered": float(summary.get("total_revenue_recovered") or 0),
        "avg_discount_pct":      float(summary.get("avg_discount_pct") or 0),
    }