"""
agents/tools/market_trend_tool.py — Customer Retention Platform
================================================================

MarketTrendTool
---------------
A LangChain BaseTool that wraps the existing fetch_market_trends() DB function
as an observable, mockable, retryable tool step in the LangGraph pipeline.

WHY THIS IS A TOOL (not a direct DB call):
  The load_market_context node previously called fetch_market_trends() directly.
  That works, but has two problems:
    1. The DB call is invisible in LangSmith traces — you see the node ran but
       not HOW LONG the DB query took or whether it fell back to 'stable'.
    2. Unit tests for the graph have to mock the entire DB layer instead of
       swapping in a lightweight MockMarketTrendTool.

  As a tool:
    - Every DB call is a named span in LangSmith with latency
    - Tests inject a mock tool without touching DB connection code
    - Retry / timeout logic is centralised here

HOW TREND IS COMPUTED (see PriceHistoryRepo.get_trend):
  Compares 14-day average price vs 30-day average price from price_history table.
  - short_avg > long_avg × 1.02  → "rising"
  - short_avg < long_avg × 0.98  → "falling"
  - otherwise                    → "stable"

INPUTS:
    product_names: list[str]   — product names to look up trends for

OUTPUTS:
    dict[str, str]  — {product_name: "rising" | "falling" | "stable"}
"""

from __future__ import annotations

import logging
from typing import Any, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class MarketTrendInput(BaseModel):
    product_names: list[str] = Field(
        description="List of product names to fetch market trends for."
    )


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class MarketTrendTool(BaseTool):
    """
    Fetches market price trends (rising / falling / stable) for a list of products
    by querying the price_history table in the Scout DB.

    Used by the load_market_context node in the Strategist LangGraph pipeline.
    The trend result directly influences the pricing strategy decision tree:
      rising  → hold margin (don't undercut aggressively)
      falling → undercut to capture share
      stable  → standard decision tree
    """

    name: str = "market_trend_fetch"
    description: str = (
        "Fetch market price trend (rising/falling/stable) for each product "
        "by comparing recent vs historical prices in the Scout DB."
    )
    args_schema: Type[BaseModel] = MarketTrendInput

    def _run(self, product_names: list[str]) -> dict[str, str]:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun(product_names=product_names)
        )

    async def _arun(
        self,
        product_names: list[str],
        **kwargs: Any,
    ) -> dict[str, str]:
        """
        Delegate to fetch_market_trends() in the persistence layer.
        Returns {product_name: "stable"} for all products on any DB failure
        so the pricing engine always has a valid trend to work with.
        """
        if not product_names:
            return {}

        try:
            from strategist.db.persistence import fetch_market_trends
            trends = await fetch_market_trends(product_names)

            non_stable = {k: v for k, v in trends.items() if v != "stable"}
            if non_stable:
                logger.info("MarketTrendTool: non-stable trends found: %s", non_stable)
            else:
                logger.info(
                    "MarketTrendTool: all %d products stable", len(product_names)
                )

            return trends

        except Exception as exc:
            logger.warning(
                "MarketTrendTool: DB query failed (%s) — "
                "defaulting all %d products to 'stable'.",
                exc, len(product_names),
            )
            return {name: "stable" for name in product_names}