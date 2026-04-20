"""
agents/tools/__init__.py — Customer Retention Platform
=======================================================

Exports all LangChain tools used by the Strategist Agent LangGraph pipeline.

Tool registry — import from here in graph nodes:

    from strategist.agents.tools import (
        ScoutPriceFetchTool,
        ChurnScoreFetchTool,
        MarketTrendTool,
        ClientConfigTool,
        CostFetchTool,
        PersistRecommendationTool,
    )

Tool → Node mapping:
  ScoutPriceFetchTool       → run_pricing_engine    (fetch competitor prices)
  ChurnScoreFetchTool       → build_churn_lookup    (fetch churn risk scores)
  MarketTrendTool           → load_market_context   (DB: price trend per product)
  ClientConfigTool          → load_market_context   (DB: guardrail thresholds)
  PersistRecommendationTool → persist_results       (DB: write recommendations)
"""

from strategist.agents.tools.scout_tool import ScoutPriceFetchTool, ScoutFetchInput
from strategist.agents.tools.churn_tool import ChurnScoreFetchTool, ChurnFetchInput
from strategist.agents.tools.market_trend_tool import MarketTrendTool, MarketTrendInput
from strategist.agents.tools.client_config_tool import ClientConfigTool, ClientConfigInput
from strategist.agents.tools.persist_tool import PersistRecommendationTool, PersistResult

__all__ = [
    "ScoutPriceFetchTool",
    "ChurnScoreFetchTool",
    "MarketTrendTool",
    "ClientConfigTool",
    "PersistRecommendationTool",
    "ScoutFetchInput",
    "ChurnFetchInput",
    "MarketTrendInput",
    "ClientConfigInput",
    "PersistResult",
]