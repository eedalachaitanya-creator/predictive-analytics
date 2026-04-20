"""
agents/tools/churn_tool.py — Customer Retention Platform
=========================================================

ChurnScoreFetchTool
-------------------
A LangChain BaseTool that fetches churn risk scores from the Analyst Agent API
(GET /churn-scores/{client_id}) or falls back to the Analyst DB directly.

WHY THIS IS A TOOL:
  Previously, churn_batch had to be included in the request body by the caller.
  If the caller forgot it, HIGH-risk customers got no retention discount silently.
  As a tool, the graph actively fetches churn data itself — the churn_batch in
  the request body becomes optional (a convenience override, not a requirement).

  Fetch priority:
    1. If churn_batch is already in the request → use it (caller knows best)
    2. Try Analyst Agent REST API (GET /churn-scores/{client_id})
    3. Fall back to Analyst DB direct query (fetch_at_risk_customers)
    4. Return empty batch → graph continues without churn fusion

INPUTS:
    client_id:    str          — used to scope churn scores to the right client
    risk_levels:  list[str]    — ["HIGH", "MEDIUM"] by default; ["HIGH"] for retention-only runs

OUTPUTS:
    ChurnBatch — same shape as churn_batch in the StrategistRequest

ENVIRONMENT:
    ANALYST_AGENT_URL — base URL of the Analyst Agent service
                        default: http://localhost:8003
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional, Type

import httpx
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from strategist.models.schemas import ChurnBatch, ChurnScore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class ChurnFetchInput(BaseModel):
    client_id: str = Field(
        description="Client ID to scope churn score lookup."
    )
    risk_levels: list[str] = Field(
        default=["HIGH", "MEDIUM"],
        description="Risk levels to include. Typically ['HIGH', 'MEDIUM'].",
    )


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class ChurnScoreFetchTool(BaseTool):
    """
    Fetches churn risk scores from the Analyst Agent API or Analyst DB.

    Called by the build_churn_lookup node when the request does not already
    contain a churn_batch. Makes the Strategist Agent self-sufficient —
    it no longer requires the caller to pre-fetch and pass churn data.
    """

    name: str = "churn_score_fetch"
    description: str = (
        "Fetch churn risk scores for all at-risk customers from the Analyst Agent. "
        "Returns a ChurnBatch with HIGH and MEDIUM risk customers for churn fusion."
    )
    args_schema: Type[BaseModel] = ChurnFetchInput

    base_url: str = Field(
        default_factory=lambda: os.getenv("ANALYST_AGENT_URL", "http://localhost:8003")
    )
    timeout_seconds: float = 20.0

    def _run(self, client_id: str, risk_levels: list[str] | None = None) -> ChurnBatch:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun(client_id=client_id, risk_levels=risk_levels or ["HIGH", "MEDIUM"])
        )

    async def _arun(
        self,
        client_id: str,
        risk_levels: list[str] | None = None,
        **kwargs: Any,
    ) -> ChurnBatch:
        """
        Fetch churn scores using two-tier strategy:
          1. Try the Analyst Agent REST API
          2. Fall back to direct DB query via persistence layer
        """
        risk_levels = risk_levels or ["HIGH", "MEDIUM"]

        # ── Tier 1: Analyst Agent REST API ────────────────────────────────
        scores = await self._fetch_from_api(client_id, risk_levels)

        # ── Tier 2: Direct DB fallback ────────────────────────────────────
        if scores is None:
            scores = await self._fetch_from_db(client_id, risk_levels)

        if not scores:
            logger.warning(
                "ChurnScoreFetchTool: no churn scores found for client=%s — "
                "churn fusion will be skipped.",
                client_id,
            )
            return ChurnBatch(scores=[])

        logger.info(
            "ChurnScoreFetchTool: fetched %d scores for client=%s (HIGH=%d, MEDIUM=%d)",
            len(scores),
            client_id,
            sum(1 for s in scores if s.risk_level == "HIGH"),
            sum(1 for s in scores if s.risk_level == "MEDIUM"),
        )

        return ChurnBatch(
            total_customers=len(scores),
            scores=scores,
        )

    async def _fetch_from_api(
        self,
        client_id: str,
        risk_levels: list[str],
    ) -> Optional[list[ChurnScore]]:
        """
        Call GET /churn-scores/{client_id}?risk_levels=HIGH,MEDIUM on the Analyst Agent.
        Returns None if the API is unreachable (triggers DB fallback).
        """
        url = f"{self.base_url.rstrip('/')}/churn-scores/{client_id}"
        params = {"risk_levels": ",".join(risk_levels)}

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()

            data = response.json()
            # Analyst Agent returns {"scores": [...]} shape
            raw_scores = data.get("scores", data if isinstance(data, list) else [])
            scores = [ChurnScore(**s) for s in raw_scores]

            logger.info(
                "ChurnScoreFetchTool: API returned %d scores for client=%s",
                len(scores), client_id,
            )
            return scores

        except httpx.ConnectError:
            logger.info(
                "ChurnScoreFetchTool: Analyst Agent unreachable at %s — "
                "falling back to DB.", self.base_url,
            )
            return None

        except Exception as exc:
            logger.warning(
                "ChurnScoreFetchTool: API fetch failed (%s) — falling back to DB.", exc
            )
            return None

    async def _fetch_from_db(
        self,
        client_id: str,
        risk_levels: list[str],
    ) -> list[ChurnScore]:
        """
        Direct DB fallback using the existing persistence layer.
        Same data source the Retention Agent uses.
        """
        try:
            from strategist.db.persistence import fetch_at_risk_customers
            scores = await fetch_at_risk_customers(
                client_id=client_id,
                risk_tiers=tuple(risk_levels),
            )
            logger.info(
                "ChurnScoreFetchTool: DB fallback returned %d scores for client=%s",
                len(scores), client_id,
            )
            return scores

        except Exception as exc:
            logger.error(
                "ChurnScoreFetchTool: DB fallback also failed (%s) — "
                "returning empty ChurnBatch.", exc,
            )
            return []