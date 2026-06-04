"""
routers/strategist_router.py — Customer Retention Platform
===========================================================

Endpoints:

  POST /api/strategist/recommend
      Main pricing endpoint. Accepts Scout output + optional churn batch.
      Auto-loads market trends and client config from DB.
      Returns price recommendations with strategies and margin analysis.

  POST /api/strategist/ingest-churn
      Receive and validate Analyst Agent churn_scores.json.
      Returns a summary — the churn data is NOT persisted here (it lives
      in Analyst DB, written by the Analyst Agent directly).

  GET  /api/strategist/market-trend/{product_name}
      Current market trend for a specific product.

  GET  /api/strategist/costs
      LangFuse cost and latency summary (requires LangFuse to be configured).
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from strategist.agents.strategist_graph import run_strategist_graph
from strategist.models.schemas import (
    ChurnBatch,
    CostSummaryResponse,
    StrategistRequest,
    StrategistResponse,
)
from strategist.services.langfuse_service import get_langfuse_safe, get_cost_summary

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/strategist", tags=["Strategist Agent"])


# ---------------------------------------------------------------------------
# POST /recommend — main pricing endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/recommend",
    response_model=StrategistResponse,
    summary="Generate price recommendations",
#     description="""
# **Full pricing pipeline:**

# 1. Load `client_config` from Analyst DB (guardrails: max_discount_pct, LTV thresholds)
# 2. Compute market trend per product from `price_history` (Scout DB, 14-day vs 30-day avg)
# 3. Run 5-layer pricing engine: cost-plus → competitor anchoring → trend → strategy → churn fusion
# 4. Apply charm pricing post-processing (₹247 → ₹249)
# 5. Persist to Scout DB: `pricing_recommendations` + `customer_price_context`

# **`scout_output`** — paste the Scout Agent's `/search/products` response directly.

# **`our_costs`** — COGS per product name (INR): `{"Product Name": 120.0}`  
# Products without COGS get `strategy: no_cost_data` and are flagged.

# **`churn_batch`** — optional. Paste the Analyst Agent's `churn_scores.json` payload.  
# When included, HIGH-risk customers receive a retention-discounted price  
# (capped at `client_config.max_discount_pct`). This price is also written to  
# `customer_price_context` so the Retention Agent doesn't double-discount.

# **`X-Client-Id` header** — used to load client-specific config from Analyst DB.  
# client_id is required — pass the client you want to generate a sample for.
# """,
)
async def recommend(
    request: StrategistRequest,
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> StrategistResponse:

    if not request.scout_output.products:
        raise HTTPException(status_code=422, detail="scout_output.products is empty.")

    # Header overrides body client_id (header is authoritative)
    if x_client_id:
        request.client_id = x_client_id

    # ── Run the LangGraph pipeline ────────────────────────────────────────
    # The graph handles: validation → market context → churn lookup →
    # pricing engine → charm pricing → churn routing → DB persistence
    import time as _time
    _t0 = _time.perf_counter()
    try:
        recommendations, run_id = await run_strategist_graph(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Strategist graph failed.")
        raise HTTPException(status_code=500, detail=f"Pricing engine error: {exc}")

    elapsed = round(_time.perf_counter() - _t0, 2)
    logger.info(
        "LangGraph pipeline complete: run_id=%s products=%d retention=%d elapsed=%.2fs",
        run_id, len(recommendations),
        sum(1 for r in recommendations if r.strategy == "retention"),
        elapsed,
    )

    # ── Step 5: Build response ────────────────────────────────────────────
    strategies_used = sorted({r.strategy for r in recommendations})
    avg_margin = (
        round(sum(r.margin_percent for r in recommendations) / len(recommendations), 1)
        if recommendations else 0.0
    )

    return StrategistResponse(
        recommendations = recommendations,
        total_products  = len(recommendations),
        flagged_count   = sum(1 for r in recommendations if r.flag),
        strategies_used = strategies_used,
        avg_margin_pct  = avg_margin,
        retention_count = sum(1 for r in recommendations if r.strategy == "retention"),
        run_id          = run_id,
        client_id       = request.client_id,
        status          = "ok",
        elapsed_seconds = elapsed,
    )


# ---------------------------------------------------------------------------
# POST /ingest-churn — receive Analyst Agent churn payload
# ---------------------------------------------------------------------------

@router.post(
    "/ingest-churn",
    summary="Validate and acknowledge Analyst Agent churn payload",
#     description="""
# Receive the Analyst Agent's `churn_scores.json` payload.

# This endpoint validates the schema and returns a summary.  
# **It does NOT persist to DB** — the churn data is already in Analyst DB  
# (written by the Analyst Agent). This endpoint is useful for:

# - Validating that your churn_scores.json is well-formed before using it
# - Checking the risk distribution before running `/recommend`
# - Getting a quick count of HIGH/MEDIUM/LOW risk customers
# """,
)
async def ingest_churn(payload: ChurnBatch) -> dict:
    if not payload.scores:
        raise HTTPException(status_code=422, detail="churn_batch.scores is empty.")

    # Compute risk distribution for the summary
    high   = sum(1 for s in payload.scores if s.risk_level == "HIGH")
    medium = sum(1 for s in payload.scores if s.risk_level == "MEDIUM")
    low    = sum(1 for s in payload.scores if s.risk_level == "LOW")

    return {
        "status":    "validated",
        "message":   "Churn payload is valid. Pass it as churn_batch in /api/strategist/recommend.",
        "total":     len(payload.scores),
        "generated_at": payload.generated_at,
        "risk_distribution": {
            "HIGH":   high,
            "MEDIUM": medium,
            "LOW":    low,
        },
        "tiers": {
            tier: sum(1 for s in payload.scores if s.customer_tier == tier)
            for tier in ("Platinum", "Gold", "Silver", "Bronze")
        },
    }


# ---------------------------------------------------------------------------
# GET /market-trend/{product_name} — single product trend lookup
# ---------------------------------------------------------------------------

@router.get(
    "/market-trend/{product_name}",
    summary="Get market trend for a specific product",
#     description="""
# Returns the current market trend for a product based on price_history data:

# - **rising**: average price in last 14 days > last 30 days by >2%
# - **falling**: average price in last 14 days < last 30 days by >2%
# - **stable**: no significant movement

# Used internally by /recommend but exposed for debugging.
# """,
)
async def market_trend(product_name: str) -> dict:
    from strategist.db.persistence import fetch_market_trends
    trends = await fetch_market_trends([product_name])
    return {
        "product_name": product_name,
        "trend":        trends.get(product_name, "stable"),
    }


# ---------------------------------------------------------------------------
# GET /costs — LangFuse cost summary
# ---------------------------------------------------------------------------

@router.get(
    "/costs",
    summary="LangFuse cost and latency summary for all Strategist runs",
)
async def costs(limit: int = 100) -> dict:
    """
    Returns per-run cost, latency, and node-level timing from LangFuse.

    Fields:
      - configured:       bool — whether LangFuse is set up
      - total_runs:       int  — number of pipeline runs tracked
      - total_cost_usd:   float — total LLM cost (0.0 for rule-based pricing)
      - avg_latency_ms:   float — average pipeline execution time
      - node_latencies:   dict  — per-node average ms (validate→persist)
      - recent_runs:      list  — last 10 runs with full breakdown
      - dashboard:        str   — link to LangFuse dashboard
    """
    return await get_cost_summary(limit=limit)


# ---------------------------------------------------------------------------
# GET /sample-request — prefill Swagger UI with real DB data
# ---------------------------------------------------------------------------

@router.get(
    "/sample-request",
    summary="Get a prefilled sample request from live DB data",
)
async def sample_request(
    client_id: str,
    limit: int = 3,
) -> dict:
    """
    Fetches real data from Scout DB + Analyst DB and returns a ready-to-paste
    request body for POST /api/strategist/recommend.

    How to use:
      1. Call GET /api/strategist/sample-request
      2. Copy the full JSON response
      3. Paste it into the request body of POST /api/strategist/recommend
      4. Adjust our_costs values to your actual COGS if needed

    What it returns:
      - scout_output: real competitor listings from entity_listings table
      - our_costs:    COGS estimated at 60% of lowest listed price per product
      - churn_batch:  real HIGH/MEDIUM risk customers from churn_scores table
    """
    from strategist.db.connection import get_scout_pool, get_analyst_pool

    scout_output: dict = {"status": "ok", "products": []}
    our_costs:    dict = {}
    churn_batch:  dict = {"scores": []}
    errors:       list = []

    # ── Pull entity_listings from Scout DB ────────────────────────────────
    # entity_listings schema: id, entity_id, platform, title, price, currency,
    #                         availability, last_seen, product_url
    # entities schema:        id (uuid), canonical_name, canonical_brand, query
    # Join entities → entity_listings to get canonical product name + listings
    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    e.canonical_name   AS product_name,
                    el.platform,
                    el.price,
                    el.currency,
                    el.availability,
                    el.product_url     AS url,
                    el.last_seen
                FROM entities e
                JOIN entity_listings el ON el.entity_id = e.id
                WHERE el.price > 0
                  AND el.availability = 'in_stock'
                ORDER BY e.canonical_name, el.last_seen DESC
                LIMIT $1
                """,
                limit,
            )

        # Group by canonical product name
        products: dict = {}
        for r in rows:
            name = r["product_name"]
            if name not in products:
                products[name] = {"name": name, "listings": []}
            products[name]["listings"].append({
                "platform":     r["platform"],
                "price":        {
                    "value":    float(r["price"]),
                    "currency": r.get("currency", "INR"),
                },
                "availability": r.get("availability", "in_stock"),
                "url":          r.get("url"),
                "source":       {"type": "db", "confidence": 0.9},
            })

        scout_output["products"] = list(products.values())

        # Estimate COGS at 60% of minimum listed price (adjust to your actual margins)
        for prod in scout_output["products"]:
            prices = [lst["price"]["value"] for lst in prod["listings"]]
            if prices:
                our_costs[prod["name"]] = round(min(prices) * 0.60, 2)

    except Exception as exc:
        errors.append(f"entity_listings fetch failed: {exc}")

    # ── Pull churn scores from Analyst DB ─────────────────────────────────
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (cs.customer_id)
                    cs.customer_id,
                    cs.client_id,
                    cs.churn_probability,
                    cs.risk_tier                                     AS risk_level,
                    COALESCE(mv.customer_tier,
                             rf.customer_tier,  'Bronze')            AS customer_tier,
                    COALESCE(mv.total_spend_usd,
                             rf.total_spend_usd,        0)           AS total_spend_usd,
                    COALESCE(mv.total_orders,
                             rf.total_orders,           0)           AS total_orders,
                    COALESCE(mv.avg_order_value_usd,
                             rf.avg_order_value_usd,    0)           AS avg_order_value_usd,
                    COALESCE(mv.avg_rating,             0)           AS avg_rating,
                    COALESCE(mv.days_since_last_order,
                             rf.days_since_last_order,  0)           AS days_since_last_order,
                    0                                                 AS is_high_value,
                    COALESCE(mv.rfm_total_score,
                             rf.rfm_total_score,        0)           AS rfm_total_score
                FROM churn_scores cs
                LEFT JOIN mv_customer_features mv
                       ON mv.customer_id = cs.customer_id
                      AND mv.client_id   = cs.client_id
                LEFT JOIN customer_rfm_features rf
                       ON rf.customer_id = cs.customer_id
                      AND rf.client_id   = cs.client_id
                WHERE cs.client_id = $1
                  AND cs.risk_tier IN ('HIGH', 'MEDIUM')
                ORDER BY cs.customer_id, cs.scored_at DESC
                LIMIT 5
                """,
                client_id,
            )
        churn_batch["scores"] = [dict(r) for r in rows]

    except Exception as exc:
        errors.append(f"churn_scores fetch failed: {exc}")

    result = {
        "client_id":           client_id,
        "scout_output":        scout_output,
        "our_costs":           our_costs,
        "churn_batch":         churn_batch,
        "target_margin_pct":   20.0,
        "min_margin_pct":       8.0,
        "undercut_pct":         2.0,
        "overhead_multiplier":  1.15,
        "min_confidence":       0.5,
        "max_discount_pct":    30.0,
        "high_ltv_threshold": 500.0,
        "client_priority":    None,
        "customer_segment":   None,
    }

    if errors:
        result["_warnings"] = errors

    return result