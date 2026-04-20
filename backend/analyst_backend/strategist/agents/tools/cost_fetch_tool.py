"""
agents/tools/cost_fetch_tool.py — Customer Retention Platform
=============================================================

CostFetchTool
-------------
A LangChain BaseTool that fetches COGS (cost of goods sold) for a list of
products from the product_prices table in the Scout/Strategist DB.

WHY THIS IS A TOOL:
  Previously, our_costs had to be supplied manually in every /recommend
  request body. The client had to know and re-enter COGS on every call.

  As a tool:
    1. Client saves COGS once via POST /api/db/product-costs (onboarding)
    2. Every subsequent /recommend call fetches COGS automatically
    3. Caller can still override by supplying our_costs in the request body
       (the tool only fires when our_costs is empty — caller takes priority)
    4. The DB fetch is a visible LangSmith span — slow lookups are traceable
    5. Tests inject CostFetchTool(override=my_costs) without needing a live DB

FETCH LOGIC:
    SELECT p.product_name, pp.cost_price_usd
    FROM products p
    JOIN product_prices pp ON pp.product_id = p.product_id
                           AND pp.client_id  = $2
    WHERE p.product_name  = ANY($1::text[])
      AND p.client_id     = $2
      AND pp.cost_price_usd IS NOT NULL
    ORDER BY p.product_name, pp.price_id DESC

    Returns {product_name: cost_price_usd} dict.
    Products not found in the table are absent from the dict — the pricing
    engine will flag them as no_cost_data (existing behaviour, unchanged).

FALLBACK:
    If the DB is unreachable, returns an empty dict.
    The pricing engine handles missing costs gracefully (no_cost_data flag).
    The client's request is never aborted due to a cost lookup failure.

INPUTS:
    product_names: list[str]   — product names to look up
    client_id:     str         — for future per-client cost overrides

OUTPUTS:
    dict[str, float]   — {product_name: unit_price_usd}
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class CostFetchInput(BaseModel):
    product_names: list[str] = Field(
        description="List of product names to fetch COGS for."
    )
    client_id: str = Field(
        description="Client ID — reserved for future per-client cost overrides.",
    )


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class CostFetchTool(BaseTool):
    """
    Fetches COGS (unit_price_usd) for a list of products from the
    product_prices table. Called by load_market_context when our_costs
    is not already present in the request body.

    Never raises — returns empty dict on any failure so the pricing engine
    can continue and flag missing products as no_cost_data.
    """

    name: str = "cost_fetch"
    description: str = (
        "Fetch COGS (cost of goods sold) for a list of products from the "
        "product_prices table. Returns {product_name: cost_price_usd} dict."
    )
    args_schema: Type[BaseModel] = CostFetchInput

    # Optional override for tests — inject costs directly without DB call
    override: Optional[dict[str, float]] = Field(default=None, exclude=True)

    def _run(
        self,
        product_names: list[str],
        client_id: str,
    ) -> dict[str, float]:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun(product_names=product_names, client_id=client_id)
        )

    async def _arun(
        self,
        product_names: list[str],
        client_id: str,
        **kwargs: Any,
    ) -> dict[str, float]:
        """
        Fetch COGS from Scout DB.

        If an override was injected at construction (for tests), return it
        immediately without any DB call.
        """
        # ── Test override path ─────────────────────────────────────────────
        if self.override is not None:
            logger.debug(
                "CostFetchTool: returning injected override (%d products)",
                len(self.override),
            )
            return self.override

        if not product_names:
            return {}

        # ── Normal DB path ─────────────────────────────────────────────────
        try:
            from strategist.db.connection import get_scout_pool

            pool = await get_scout_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (p.product_name)
                        p.product_name,
                        pp.cost_price_usd
                    FROM products p
                    JOIN product_prices pp ON pp.product_id = p.product_id
                                          AND pp.client_id  = $2
                    WHERE p.product_name   = ANY($1::text[])
                      AND p.client_id      = $2
                      AND pp.cost_price_usd IS NOT NULL
                    ORDER BY p.product_name, pp.price_id DESC
                    """,
                    product_names,
                    client_id,
                )

            costs = {row["product_name"]: float(row["cost_price_usd"]) for row in rows}

            found    = list(costs.keys())
            missing  = [n for n in product_names if n not in costs]

            logger.info(
                "CostFetchTool: fetched %d/%d costs from DB — found: %s%s",
                len(found),
                len(product_names),
                found[:3],
                f", missing: {missing}" if missing else "",
            )

            if missing:
                logger.warning(
                    "CostFetchTool: %d products not found in product_prices — "
                    "they will get no_cost_data flag: %s",
                    len(missing), missing,
                )

            return costs

        except Exception as exc:
            logger.error(
                "CostFetchTool: DB fetch failed (%s) — "
                "returning empty dict, products will get no_cost_data flag.",
                exc,
            )
            return {}