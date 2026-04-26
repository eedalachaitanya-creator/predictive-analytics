"""
agents/tools/scout_tool.py — Customer Retention Platform
=========================================================

ScoutPriceFetchTool
-------------------
Fetches competitor price listings for the pricing engine.

TWO data sources (tried in order):
  1. Scout Agent HTTP API  (POST /search/products)
     — used when SCOUT_AGENT_URL is set and the service is reachable
     — Scout Agent scrapes live prices from Amazon, Flipkart, etc.

  2. entity_listings DB table  (direct asyncpg query — ALWAYS available)
     — used when Scout Agent is unreachable OR SCOUT_AGENT_URL is not set
     — entity_listings is written by the Scout Agent on every scrape run
     — same data, no HTTP round-trip, always consistent with DB state

This means the Strategist pipeline NEVER blocks on Scout Agent availability.
If Scout is down, we price from the last scraped data in entity_listings.

DB query:
  SELECT e.canonical_name, el.platform, el.price, el.availability, el.product_url, el.last_seen
  FROM entity_listings el
  JOIN entities e ON e.id = el.entity_id
  WHERE e.canonical_name = ANY($1)
    AND el.availability = 'in_stock'
    AND el.price > 0
  ORDER BY e.canonical_name, el.last_seen DESC

ENVIRONMENT:
    SCOUT_AGENT_URL  — optional. If set, HTTP API is tried first.
                       default: not set (DB-only mode)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from strategist.models.schemas import (
    ScoutBulkResponse,
    ScoutListing,
    ScoutPrice,
    ScoutProduct,
    ScoutSource,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class ScoutFetchInput(BaseModel):
    product_names: list[str] = Field(
        description="List of product names to fetch competitor prices for."
    )
    client_id: str = Field(
        ...,
        description="Client identifier forwarded to Scout Agent (HTTP mode only). Required.",
    )
    currency: str = Field(
        default="INR",
        description="Filter competitor listings to this currency only (INR/USD/EUR).",
    )


# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------

class ScoutPriceFetchTool(BaseTool):
    """
    Fetches competitor price listings for the pricing engine.

    Tries Scout Agent HTTP API first (if SCOUT_AGENT_URL is set),
    then falls back to querying entity_listings directly from the Scout DB.
    Returns a ScoutBulkResponse with all in-stock listings per product.
    """

    name: str = "scout_price_fetch"
    description: str = (
        "Fetch competitor prices for a list of products. "
        "Queries Scout Agent API or entity_listings DB table directly."
    )
    args_schema: Type[BaseModel] = ScoutFetchInput

    base_url:        str   = Field(default_factory=lambda: os.getenv("SCOUT_AGENT_URL", ""))
    timeout_seconds: float = 20.0
    max_retries:     int   = 1

    def _run(self, product_names: list[str], client_id: str) -> ScoutBulkResponse:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun(product_names=product_names, client_id=client_id)
        )

    async def _arun(
        self,
        product_names: list[str],
        client_id: str,
        currency: str = "INR",
        **kwargs: Any,
    ) -> ScoutBulkResponse:

        if not product_names:
            return ScoutBulkResponse(status="ok", products=[])

        # ── Source 1: Scout Agent HTTP API (if URL configured) ─────────────
        if self.base_url:
            result = await self._fetch_from_api(product_names, client_id)
            if result is not None:
                return result
            logger.info("ScoutPriceFetchTool: API unavailable — falling back to DB")

        # ── Source 2: entity_listings DB (always available) ─────────────────
        return await self._fetch_from_db(product_names, currency=currency)

    # ── HTTP source ───────────────────────────────────────────────────────────

    async def _fetch_from_api(
        self,
        product_names: list[str],
        client_id: str,
    ) -> Optional[ScoutBulkResponse]:
        """
        POST /search/products to the Scout Agent.
        Returns None on any connection error so the caller falls back to DB.
        """
        try:
            import httpx
        except ImportError:
            return None

        url     = f"{self.base_url.rstrip('/')}/search/products"
        payload = {"product_names": product_names}
        headers = {"Content-Type": "application/json", "X-Client-Id": client_id}

        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.post(url, json=payload, headers=headers)
                    response.raise_for_status()

                data   = response.json()
                result = ScoutBulkResponse(**data)
                logger.info(
                    "ScoutPriceFetchTool [API]: %d products fetched (attempt %d)",
                    len(result.products), attempt + 1,
                )
                return result

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500:
                    break   # 4xx — don't retry
                logger.warning("ScoutPriceFetchTool [API]: HTTP %d", exc.response.status_code)

            except Exception as exc:
                logger.warning("ScoutPriceFetchTool [API]: %s", exc)
                break   # connection error → fall back to DB immediately

        return None

    # ── DB source (entity_listings) ───────────────────────────────────────────

    # async def _fetch_from_db(self, product_names: list[str]) -> ScoutBulkResponse:
    async def _fetch_from_db(
        self,
        product_names: list[str],
        currency: str = "INR",
    ) -> ScoutBulkResponse:
        """
        Query entity_listings directly from Scout DB.

        Matching strategy (user-friendly names supported):
          1. Exact match on entities.canonical_name  (e.g. "Dolo-650 - Strip of 15 Tablets")
          2. OR case-insensitive match on entities.query  (e.g. "dolo 650")

        Scout's entities table stores both the full scraped title (canonical_name)
        AND the original user-facing search term (query). By matching either,
        users can type the clean short name OR the messy full title — both resolve
        to the same product's listings.
        """
        try:
            from strategist.db.connection import get_scout_pool

            # Lowercase variants for case-insensitive query-column match
            lowered = [n.lower() for n in product_names]

            pool = await get_scout_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        e.canonical_name   AS canonical_name,
                        e.query            AS orig_query,
                        el.platform,
                        el.price,
                        el.currency,
                        el.availability,
                        el.product_url     AS url,
                        el.last_seen       AS scraped_at
                    FROM entity_listings el
                    JOIN entities e ON e.id = el.entity_id
                    WHERE (
                        e.canonical_name = ANY($1::text[])
                        OR LOWER(e.query) = ANY($2::text[])
                    )
                      AND el.availability = 'in_stock'
                      AND el.price > 0
                      AND el.currency = $3
                    ORDER BY e.canonical_name, el.last_seen DESC
                    """,
                    product_names,
                    lowered,
                    currency,
                )

            # Map each returned row back to whichever input key it matched.
            # The pricing engine expects results keyed by the USER's input,
            # not by canonical_name. Build a reverse lookup so "dolo 650"
            # (input) gets the Dolo-650 listings attached to it.
            input_set_exact = set(product_names)
            input_set_lower = {n.lower(): n for n in product_names}

            products: dict[str, list[ScoutListing]] = {n: [] for n in product_names}

            for row in rows:
                canonical = row["canonical_name"]
                orig_q    = row["orig_query"]

                # Figure out which input name this row belongs to
                matched_key = None
                if canonical in input_set_exact:
                    matched_key = canonical
                elif orig_q and orig_q.lower() in input_set_lower:
                    matched_key = input_set_lower[orig_q.lower()]

                if matched_key is None:
                    continue

                listing = ScoutListing(
                    platform     = row["platform"],
                    price        = ScoutPrice(
                        value    = float(row["price"]),
                        currency = row.get("currency", "INR"),
                    ),
                    availability = row.get("availability", "in_stock"),
                    url          = row.get("url"),
                    source       = ScoutSource(
                        type       = "db",
                        confidence = 0.9,
                    ),
                )
                if len(products[matched_key]) < 10:
                    products[matched_key].append(listing)

            scout_products = [
                ScoutProduct(name=name, listings=listings)
                for name, listings in products.items()
            ]

            found = sum(1 for p in scout_products if p.listings)
            logger.info(
                "ScoutPriceFetchTool [DB]: %d/%d products have listings "
                "(total %d rows; matched via canonical_name or query column)",
                found, len(product_names), len(rows),
            )

            return ScoutBulkResponse(status="ok", products=scout_products)

        except Exception as exc:
            logger.error(
                "ScoutPriceFetchTool [DB]: query failed: %s — "
                "returning empty response; products will get no_price_data flag.",
                exc,
            )
            return ScoutBulkResponse(
                status   = "error",
                products = [ScoutProduct(name=n, listings=[]) for n in product_names],
            )

        except Exception as exc:
            logger.error(
                "ScoutPriceFetchTool [DB]: query failed: %s — "
                "returning empty response; products will get no_price_data flag.",
                exc,
            )
            # Return products with empty listings — pricing engine handles gracefully
            return ScoutBulkResponse(
                status   = "error",
                products = [ScoutProduct(name=n, listings=[]) for n in product_names],
            )