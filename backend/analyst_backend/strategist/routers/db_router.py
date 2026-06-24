"""
routers/db_router.py — Customer Retention Platform
===================================================

Admin/debug endpoints for inspecting DB state.
All endpoints are GET-only (read-only).
Prefix: /api/db

  GET /api/db/churn-scores           — latest churn scores from Analyst DB
  GET /api/db/price-contexts         — Strategist-written retention prices
  GET /api/db/interventions          — retention interventions written by Retention Agent
  GET /api/db/value-propositions     — current discount rules
  GET /api/db/client-config/{id}     — client guardrail config

These endpoints are useful for:
  - Debugging the pipeline end-to-end without a separate DB client
  - Verifying that data flows correctly between Strategist and Retention agents
  - Checking discount rules before running a retention batch

In production, protect these behind an admin API key or remove them entirely.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from app.auth_router import get_current_user

from strategist.db.connection import get_analyst_pool, get_scout_pool
from strategist.db.repositories import (
    ChurnScoresRepo,
    ClientConfigRepo,
    CustomerPriceContextRepo,
    RetentionRepo,
    ValuePropositionsRepo,
)

logger = logging.getLogger(__name__)
# SECURITY: require a valid session token for every route (was unauthenticated —
# allowed cross-tenant reads of any client_id). Tenant-scoping is a follow-up.
router = APIRouter(prefix="/api/db", tags=["Admin / Debug"],
                   dependencies=[Depends(get_current_user)])


# ---------------------------------------------------------------------------
# GET /churn-scores — view latest churn scores
# ---------------------------------------------------------------------------

@router.get(
    "/churn-scores",
    summary="View latest churn scores from Analyst DB",
)
async def churn_scores(
    client_id:  str = Query(default="CLT-001"),
    risk_tier:  str = Query(default="HIGH", description="HIGH | MEDIUM | LOW | ALL"),
    limit:      int = Query(default=20, le=200),
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> dict:
    cid = x_client_id or client_id

    # Determine which risk tiers to query
    if risk_tier.upper() == "ALL":
        tiers = ("HIGH", "MEDIUM", "LOW")
    else:
        tiers = (risk_tier.upper(),)

    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            scores = await ChurnScoresRepo.get_at_risk(conn, cid, tiers, limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")

    return {
        "client_id": cid,
        "count":     len(scores),
        "scores":    [s.model_dump() for s in scores],
    }


# ---------------------------------------------------------------------------
# GET /price-contexts — view Strategist-written retention prices
# ---------------------------------------------------------------------------

@router.get(
    "/price-contexts",
    summary="View customer_price_context (Strategist retention prices)",
#     description="""
# Shows prices written by the Strategist Agent for HIGH-risk customers.
# The Retention Agent reads this table to prevent double-discounting.
# If a customer appears here with strategy='retention', the Retention Agent
# will skip its own discount for that customer.
# """,
)
async def price_contexts(
    client_id:    str = Query(default="CLT-001"),
    customer_ids: str = Query(
        default="",
        description="Comma-separated customer IDs (leave blank for all recent)"
    ),
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> dict:
    cid = x_client_id or client_id

    # If no customer_ids provided, fetch the 20 most recent from a raw query
    if not customer_ids.strip():
        try:
            pool = await get_scout_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT customer_id, product_name, strategy, suggested_price,
                           pre_retention_price, discount_pct_applied,
                           churn_probability, risk_tier, run_id,
                           currency, created_at
                    FROM customer_price_context
                    WHERE client_id = $1
                    ORDER BY created_at DESC
                    LIMIT 20
                    """,
                    cid,
                )
            return {
                "client_id": cid,
                "count":     len(rows),
                "contexts":  [dict(r) for r in rows],
            }
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"DB error: {exc}")

    # Specific customer IDs provided
    ids = [i.strip() for i in customer_ids.split(",") if i.strip()]
    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            ctx_map = await CustomerPriceContextRepo.get_latest_retention_prices(
                conn, cid, ids
            )
        return {
            "client_id": cid,
            "count":     len(ctx_map),
            "contexts":  {k: v.model_dump() for k, v in ctx_map.items()},
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")


# ---------------------------------------------------------------------------
# GET /interventions — view retention interventions
# ---------------------------------------------------------------------------

@router.get(
    "/interventions",
    summary="View retention interventions from Analyst DB",
)
async def interventions(
    client_id: str = Query(default="CLT-001"),
    status:    str = Query(default="ALL", description="pending | accepted | declined | ALL"),
    limit:     int = Query(default=20, le=200),
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> dict:
    cid = x_client_id or client_id

    # Build status filter
    status_filter = "" if status.upper() == "ALL" else f"AND offer_status = '{status.lower()}'"

    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT intervention_id, customer_id, churn_probability,
                       risk_tier, offer_type, discount_pct, channel,
                       offer_status, escalated_to_human, customer_ltv_usd,
                       revenue_recovered, created_at
                FROM retention_interventions
                WHERE client_id = $1
                {status_filter}
                ORDER BY created_at DESC
                LIMIT $2
                """,
                cid,
                limit,
            )
        return {
            "client_id": cid,
            "count":     len(rows),
            "interventions": [dict(r) for r in rows],
        }
    except Exception as exc:
        msg = str(exc)
        # "Table doesn't exist yet" means nothing has run — treat as empty, not error.
        # The pipeline hasn't run in save-mode; the UI should show "no data" not "failed".
        if "does not exist" in msg.lower() or "relation" in msg.lower():
            return {
                "client_id": cid,
                "count": 0,
                "interventions": [],
                "message": "No retention interventions yet. Run the pipeline to generate.",
            }
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")


# ---------------------------------------------------------------------------
# GET /value-propositions — view discount rules
# ---------------------------------------------------------------------------

@router.get(
    "/value-propositions",
    summary="View discount rules from value_propositions table",
#     description="""
# Shows the current discount rules used by both Strategist and Retention agents.
# If this table is empty, agents fall back to the hardcoded default table
# (Platinum+HIGH=20%, Gold+HIGH=15%, etc.).
# """,
)
async def value_propositions() -> dict:
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            vps = await ValuePropositionsRepo.get_all(conn)
        return {
            "count":             len(vps),
            "value_propositions": [vp.model_dump() for vp in vps],
            "using_db_rules":    len(vps) > 0,
            "fallback_note": (
                "Both agents fall back to hardcoded defaults when this table is empty."
                if len(vps) == 0 else ""
            ),
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")


# ---------------------------------------------------------------------------
# GET /client-config/{client_id} — view client guardrail config
# ---------------------------------------------------------------------------

@router.get(
    "/client-config/{client_id}",
    summary="View client guardrail configuration",
)
async def client_config(client_id: str) -> dict:
    try:
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            cfg = await ClientConfigRepo.get(conn, client_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")

    if not cfg:
        raise HTTPException(
            status_code=404,
            detail=f"No client_config found for client_id='{client_id}'."
        )

    return cfg.model_dump()


# ---------------------------------------------------------------------------
# POST /product-costs — save COGS into product_prices.cost_price_usd
# Looks up product by name + client_id, updates cost_price_usd.
# CostFetchTool reads product_prices.cost_price_usd on every /recommend call.
# ---------------------------------------------------------------------------

class ProductCostsRequest(BaseModel):
    client_id: str
    costs: dict[str, float]


@router.post("/product-costs", summary="Save product COGS into product_prices.cost_price_usd")
async def save_product_costs(body: ProductCostsRequest) -> dict:
    if not body.costs:
        raise HTTPException(status_code=422, detail="costs dict is empty.")
    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            saved, skipped, not_found = [], [], []
            for product_name, cost_usd in body.costs.items():
                if not product_name.strip():
                    continue
                if cost_usd <= 0:
                    skipped.append(product_name)
                    continue
                product_id = await conn.fetchval(
                    "SELECT product_id FROM products WHERE product_name = $1 AND client_id = $2 LIMIT 1",
                    product_name.strip(), body.client_id,
                )
                if not product_id:
                    not_found.append(product_name.strip())
                    continue
                await conn.execute(
                    "UPDATE product_prices SET cost_price_usd = $1 WHERE product_id = $2 AND client_id = $3",
                    cost_usd, product_id, body.client_id,
                )
                saved.append(product_name.strip())
        return {"status": "ok", "client_id": body.client_id, "saved": len(saved), "skipped": len(skipped), "not_found": not_found}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")


# ---------------------------------------------------------------------------
# GET /product-costs — fetch COGS from product_prices.cost_price_usd
# ---------------------------------------------------------------------------

@router.get("/product-costs", summary="Get product COGS from product_prices.cost_price_usd")
async def get_product_costs(client_id: str = Query(default="CLT-001")) -> dict:
    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (p.product_name)
                    p.product_name,
                    pp.cost_price_usd AS cost_usd
                FROM products p
                JOIN product_prices pp ON pp.product_id = p.product_id
                                      AND pp.client_id  = $1
                WHERE p.client_id = $1
                  AND pp.cost_price_usd IS NOT NULL
                ORDER BY p.product_name, pp.price_id DESC
                """,
                client_id,
            )
        return {"client_id": client_id, "count": len(rows),
                "costs": [{"product_name": r["product_name"], "cost_usd": float(r["cost_usd"])} for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")
    
    # ---------------------------------------------------------------------------
# GET /products — list scraped products for UI autocomplete
# ---------------------------------------------------------------------------

@router.get(
    "/products",
    summary="List CLIENT'S products from their catalog (for UI autocomplete)",
)
async def products(
    client_id: str = Query(..., description="Client identifier — required."),
    q:         str = Query(default="", description="Optional search filter — matches substring of product_name"),
    limit:     int = Query(default=20, le=100),
) -> dict:
    """
    Return products from the CLIENT'S OWN CATALOG (public.products table).
    These are products the client actually sells — not competitor scraped data.

    The Pricing Engine's product-name autocomplete uses this so the user picks
    a real Walmart/Costco/Target SKU. Strategist then asks Scout if any
    competitors carry the same product (matched by name in entity_listings).

    With `q` empty, returns the 20 most-recently-added active products.
    With `q` set, returns up to `limit` products whose name contains the query.
    """
    try:
        from strategist.db.connection import get_analyst_pool
        pool = await get_analyst_pool()
        async with pool.acquire() as conn:
            if q.strip():
                rows = await conn.fetch(
                    """
                    SELECT
                        p.product_name AS name,
                        p.sku          AS sku,
                        -- Get the single-unit cost, or first non-null cost, or 0
                        COALESCE(
                            (SELECT cost_price_usd
                             FROM product_prices pp
                             WHERE pp.product_id = p.product_id
                               AND pp.client_id  = p.client_id
                               AND pp.cost_price_usd IS NOT NULL
                             ORDER BY pp.qty_min ASC
                             LIMIT 1),
                            0
                        ) AS saved_cost
                    FROM products p
                    WHERE p.client_id = $1
                      AND p.active = 1
                      AND p.product_name ILIKE '%' || $2 || '%'
                    ORDER BY p.product_name
                    LIMIT $3
                    """,
                    client_id,
                    q.strip(),
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT
                        p.product_name AS name,
                        p.sku          AS sku,
                        COALESCE(
                            (SELECT cost_price_usd
                             FROM product_prices pp
                             WHERE pp.product_id = p.product_id
                               AND pp.client_id  = p.client_id
                               AND pp.cost_price_usd IS NOT NULL
                             ORDER BY pp.qty_min ASC
                             LIMIT 1),
                            0
                        ) AS saved_cost
                    FROM products p
                    WHERE p.client_id = $1
                      AND p.active = 1
                    ORDER BY p.product_name
                    LIMIT $2
                    """,
                    client_id,
                    limit,
                )
        return {
            "client_id": client_id,
            "count":     len(rows),
            "products":  [
                {
                    "name":       r["name"],
                    "sku":        r["sku"],
                    "saved_cost": float(r["saved_cost"]) if r["saved_cost"] else 0,
                }
                for r in rows
            ],
        }
    except Exception as exc:
        logger.error("GET /products failed for client_id=%s: %s", client_id, exc)
        return {"client_id": client_id, "count": 0, "products": [], "error": str(exc)}
    

@router.get("/scout-products", summary="Search Scout entity names for Pricing Engine autocomplete")
async def scout_products(
    q:         str = Query(default="", description="Search filter"),
    limit:     int = Query(default=20, le=100),
    client_id: str = Query(default="", description="Tenant scope"),
) -> dict:
    """Search entity names from Scout DB for the Pricing Engine autocomplete."""
    try:
        from strategist.db.connection import get_scout_pool
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT e.canonical_name AS name, e.query
                FROM entities e
                JOIN entity_listings el ON el.entity_id = e.id
                WHERE el.price > 0
                  AND el.client_id = $3
                  AND e.client_id  = $3
                  AND ($1 = '' OR LOWER(e.canonical_name) LIKE '%' || LOWER($1) || '%'
                               OR LOWER(e.query)          LIKE '%' || LOWER($1) || '%')
                ORDER BY e.canonical_name
                LIMIT $2
                """,
                q.strip(), limit, client_id,
            )
        products = [
            {"name": r["name"], "sku": r["query"] or "", "saved_cost": 0}
            for r in rows
        ]

        # Fallback to price_history if no entity data for this client_id
        if not products:
            async with pool.acquire() as conn2:
                rows2 = await conn2.fetch(
                    """
                    SELECT DISTINCT product_name AS name
                    FROM price_history
                    WHERE client_id = $1
                      AND ($2 = '' OR LOWER(product_name) LIKE '%' || LOWER($2) || '%')
                    ORDER BY product_name
                    LIMIT $3
                    """,
                    client_id, q.strip(), limit,
                )
            products = [
                {"name": r["name"], "sku": r["name"], "saved_cost": 0}
                for r in rows2
            ]

        return {"count": len(products), "products": products}
    except Exception as exc:
        logger.error("GET /scout-products failed: %s", exc)
        return {"count": 0, "products": [], "error": str(exc)}
    

# ---------------------------------------------------------------------------
# GET /price-history-products — search product names in price_history
# Used by Market Trends autocomplete
# ---------------------------------------------------------------------------

@router.get("/price-history-products", summary="Search product names in price_history for Market Trends autocomplete")
async def price_history_products(
    q:     str = Query(default="", description="Search filter"),
    limit: int = Query(default=10, le=50),
) -> dict:
    """Search distinct product names from price_history table for Market Trends autocomplete."""
    try:
        pool = await get_scout_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT product_name
                FROM price_history
                WHERE $1 = '' OR LOWER(product_name) LIKE '%' || LOWER($1) || '%'
                ORDER BY product_name
                LIMIT $2
                """,
                q.strip(), limit,
            )
        return {
            "count":    len(rows),
            "products": [r["product_name"] for r in rows],
        }
    except Exception as exc:
        logger.error("GET /price-history-products failed: %s", exc)
        return {"count": 0, "products": [], "error": str(exc)}


# GET /client-products — fetch product names uploaded by a specific client
# Used by Market Trends "YOUR PRODUCTS" section to show only the client's
# own products instead of global Scout DB data.
# ---------------------------------------------------------------------------

@router.get("/client-products", summary="Get product names for a specific client from their uploaded data")
async def client_products(
    client_id: str = Query(..., description="Client ID to fetch products for"),
    limit:     int = Query(default=20, le=100),
) -> dict:
    """
    Fetch distinct product names from the `products` table scoped to this
    client_id. Returns empty list for clients who haven't uploaded data yet.
    Used by the Market Trends page to show only the client's own products
    in the 'YOUR PRODUCTS' chip section — never global/shared demo data.
    """
    from app.database import engine
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT DISTINCT product_name
                    FROM products
                    WHERE client_id = :cid
                      AND product_name IS NOT NULL
                      AND product_name <> ''
                    ORDER BY product_name
                    LIMIT :limit
                """),
                {"cid": client_id, "limit": limit},
            ).fetchall()
        names = [r[0] for r in rows]
        return {"count": len(names), "products": names}
    except Exception as exc:
        logger.error("GET /client-products failed for %s: %s", client_id, exc)
        return {"count": 0, "products": [], "error": str(exc)}