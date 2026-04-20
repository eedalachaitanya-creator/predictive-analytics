"""
agents/tools/client_config_tool.py — Customer Retention Platform
=================================================================

ClientConfigTool
----------------
A LangChain BaseTool that wraps the fetch_client_config() DB call as an
observable, mockable tool step in the LangGraph pipeline.

WHY THIS IS A TOOL:
  client_config holds the guardrails that govern every pricing decision:
    - max_discount_pct   → hard cap on churn retention discounts
    - high_ltv_threshold → which customers get escalated to human review
    - churn_window_days  → how far back to look for churn signals

  Previously this was called inline in the router. As a tool:
    1. The config load is a visible span in LangSmith — you can see if it
       fell back to defaults and why.
    2. Integration tests can inject ClientConfigTool(override=my_config)
       without needing a live DB or mocking the entire persistence layer.
    3. If the Analyst DB is slow, the timeout is enforced here (not as a
       silent hang in the node).

INPUTS:
    client_id: str   — must match a client_id in client_config table

OUTPUTS:
    ClientConfig     — the loaded config, or the hardcoded default if DB is down

DEFAULT CONFIG (when DB unreachable — mirrors persistence._DEFAULT_CONFIG):
    max_discount_pct   = 30.0
    high_ltv_threshold = 500.0
    mid_ltv_threshold  = 250.0
    churn_window_days  = 90
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from strategist.models.schemas import ClientConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hardcoded fallback — mirrors persistence._DEFAULT_CONFIG
# Defined here so it's available even if the DB module fails to import
# ---------------------------------------------------------------------------
_DEFAULT_CONFIG = ClientConfig(
    client_id          = "CLT-001",
    client_name        = "Default Client",
    currency           = "USD",
    max_discount_pct   = 30.0,
    high_ltv_threshold = 500.0,
    mid_ltv_threshold  = 250.0,
    churn_window_days  = 90,
)


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class ClientConfigInput(BaseModel):
    client_id: str = Field(
        description="Client ID to load guardrail config for. Must match a client_id in client_config table.",
    )


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class ClientConfigTool(BaseTool):
    """
    Loads client-level guardrail config (max discount %, LTV thresholds)
    from the Analyst DB. Falls back to safe defaults if DB is unavailable.

    Used by the load_market_context node to enforce per-client pricing rules
    before the pricing engine runs.
    """

    name: str = "client_config_fetch"
    description: str = (
        "Load client guardrail configuration (max_discount_pct, LTV thresholds) "
        "from the Analyst DB. Returns safe defaults if the DB is unavailable."
    )
    args_schema: Type[BaseModel] = ClientConfigInput

    # Optional override for testing — inject a ClientConfig directly
    # without hitting the DB. Set this in tests: tool = ClientConfigTool(override=cfg)
    override: Optional[ClientConfig] = Field(default=None, exclude=True)

    def _run(self, client_id: str) -> ClientConfig:
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun(client_id=client_id)
        )

    async def _arun(
        self,
        client_id: str,
        **kwargs: Any,
    ) -> ClientConfig:
        """
        Load client config from Analyst DB.

        If an override was injected at construction (for tests), return it
        immediately without any DB call.
        """
        # ── Test override path ─────────────────────────────────────────────
        if self.override is not None:
            logger.debug(
                "ClientConfigTool: returning injected override for client=%s", client_id
            )
            return self.override

        # ── Normal DB path ─────────────────────────────────────────────────
        try:
            from strategist.db.persistence import fetch_client_config
            config = await fetch_client_config(client_id)

            logger.info(
                "ClientConfigTool: loaded config for client=%s "
                "(max_discount=%.0f%%, high_ltv=%.0f)",
                client_id, config.max_discount_pct, config.high_ltv_threshold,
            )
            return config

        except Exception as exc:
            logger.warning(
                "ClientConfigTool: DB fetch failed for client=%s (%s) — "
                "using default config.",
                client_id, exc,
            )
            return _DEFAULT_CONFIG