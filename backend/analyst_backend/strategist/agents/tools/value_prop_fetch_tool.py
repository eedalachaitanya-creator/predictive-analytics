"""
agents/tools/value_prop_fetch_tool.py — Customer Retention Platform
====================================================================

ValuePropFetchTool
------------------
A LangChain BaseTool that wraps fetch_value_props() as an observable,
mockable tool step in the LangGraph pipeline.

WHY THIS IS A TOOL:
  value_propositions is the discount table: (tier, risk_level) → discount_pct.
  Without this tool, Strategist was using a hardcoded _VP_DISCOUNTS dict, which
  drifted silently from Retention Agent (which reads from DB). Same customer,
  different discount depending on which agent touched them first.

  As a tool:
    1. The load is a visible span in LangSmith — you can see which discount
       source was used and why (DB hit vs fallback).
    2. Integration tests can inject ValuePropFetchTool(override=[...]) without
       needing a live DB.
    3. Retention and Strategist now share one source of truth (value_propositions
       table). Change a discount in the DB → both agents pick it up.

INPUTS:
    (none — value_propositions is a global table, not per-client today)

OUTPUTS:
    list[ValueProposition]  — rows from the DB, or empty list on failure.
                              Caller should fall back to hardcoded _VP_DISCOUNTS
                              when this returns [].
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from strategist.models.schemas import ValueProposition

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input schema (no inputs — but LangChain tools require one)
# ---------------------------------------------------------------------------

class ValuePropFetchInput(BaseModel):
    """
    No inputs — value_propositions is loaded globally.
    A future per-client override could add `client_id` here.
    """
    pass


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class ValuePropFetchTool(BaseTool):
    """
    Loads (tier, risk_level) → discount_pct rules from the value_propositions
    table in the Analyst DB. Returns empty list if the DB is unavailable —
    the caller (StrategistAgent) falls back to its hardcoded _VP_DISCOUNTS.

    Used by the load_market_context node so Strategist and Retention share
    a single source of truth for retention discount rules.
    """

    name: str = "value_prop_fetch"
    description: str = (
        "Load (tier, risk_level) → discount_pct rules from value_propositions table. "
        "Returns empty list if DB is unavailable; caller falls back to hardcoded table."
    )
    args_schema: Type[BaseModel] = ValuePropFetchInput

    # Optional override for testing — inject discount rows directly
    # without hitting the DB. Set in tests: tool = ValuePropFetchTool(override=[...])
    override: Optional[List[ValueProposition]] = Field(default=None, exclude=True)

    def _run(self) -> List[ValueProposition]:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(self._arun())

    async def _arun(self, **kwargs: Any) -> List[ValueProposition]:
        """
        Load discount rules from Analyst DB.

        If an override was injected at construction (for tests), return it
        immediately without any DB call.
        """
        # ── Test override path ─────────────────────────────────────────────
        if self.override is not None:
            logger.debug(
                "ValuePropFetchTool: returning %d injected overrides", len(self.override)
            )
            return self.override

        # ── Normal DB path ─────────────────────────────────────────────────
        try:
            from strategist.db.persistence import fetch_value_props
            rows = await fetch_value_props()

            logger.info(
                "ValuePropFetchTool: loaded %d discount rules from value_propositions", len(rows)
            )
            return rows

        except Exception as exc:
            logger.warning(
                "ValuePropFetchTool: DB fetch failed (%s) — returning empty list. "
                "StrategistAgent will fall back to hardcoded _VP_DISCOUNTS.", exc,
            )
            return []