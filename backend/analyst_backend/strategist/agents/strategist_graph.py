"""
agents/strategist_graph.py — Customer Retention Platform
=========================================================

LangGraph-powered Strategist Agent
------------------------------------

This module wraps the existing StrategistAgent pricing engine in a
LangGraph StateGraph, giving the pipeline explicit, inspectable nodes
with typed state, conditional routing, and full LangSmith / LangFuse
observability.

Graph topology
--------------

                  ┌─────────────────────┐
                  │   START             │
                  └────────┬────────────┘
                           │
                  ┌────────▼────────────┐
                  │  validate_input     │  Pydantic guards, early-exit on bad data
                  └────────┬────────────┘
                           │
              ┌────────────▼────────────────┐
              │  load_market_context        │  Fetch market trends + client config from DB
              └────────────┬────────────────┘
                           │
              ┌────────────▼────────────────┐
              │  build_churn_lookup         │  Index churn batch for O(1) access per product
              └────────────┬────────────────┘
                           │
              ┌────────────▼────────────────┐
              │  run_pricing_engine         │  5-layer pricing engine (StrategistAgent.run)
              └────────────┬────────────────┘
                           │
              ┌────────────▼────────────────┐
              │  apply_charm_pricing        │  Post-process: psychological pricing pass
              └────────────┬────────────────┘
                           │
        ┌──────────────────▼─────────────────────┐
        │  route_by_churn_risk (conditional)      │
        │   HIGH   → retention_offer_node         │
        │   MEDIUM → soft_flag_node               │
        │   LOW    → persist_results              │
        └──────────────────┬─────────────────────┘
                           │
          ┌────────────────┴────────────────┐
          │                                 │
 ┌────────▼──────────┐          ┌──────────▼──────────┐
 │ retention_offer   │          │   soft_flag_node     │
 │  _node            │          │ (MEDIUM risk note)   │
 └────────┬──────────┘          └──────────┬──────────┘
          │                                 │
          └────────────┬────────────────────┘
                       │
            ┌──────────▼──────────┐
            │   persist_results   │  Write to DB (best-effort, non-blocking)
            └──────────┬──────────┘
                       │
            ┌──────────▼──────────┐
            │       END           │
            └─────────────────────┘

Node responsibilities
---------------------
validate_input        Validates request fields; sets error_message on bad input.
load_market_context   Async DB calls: fetch_client_config + fetch_market_trends.
build_churn_lookup    Indexes ChurnBatch.scores by customer_id for O(1) lookup.
run_pricing_engine    Delegates to StrategistAgent._process_product() per product.
apply_charm_pricing   Re-runs charm pricing post-processing (idempotent).
retention_offer_node  Enriches HIGH-risk recommendations with full ChurnContext.
soft_flag_node        Appends MEDIUM-risk re-engagement warnings.
persist_results       Calls persistence.persist_run() — failures never abort the graph.

State schema
------------
StrategistState is a TypedDict consumed and produced by every node.
All fields default to None / [] so nodes can be composed in any order.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Optional

from typing_extensions import TypedDict

# LangGraph
from langgraph.graph import END, START, StateGraph

# LangChain core (tracing + callbacks)
from langchain_core.runnables import RunnableConfig

# Internal modules
from strategist.agents.strategist_agent import StrategistAgent, StrategistConfig
from strategist.agents.tools import (
    ChurnScoreFetchTool,
    ClientConfigTool,
    CostFetchTool,
    MarketTrendTool,
    PersistRecommendationTool,
    ScoutPriceFetchTool,
    ValuePropFetchTool,
)
from strategist.models.schemas import (
    ChurnBatch,
    ChurnContext,
    ChurnScore,
    PricingRecommendation,
    StrategistRequest,
    StrategistResponse,
)
from strategist.services.langfuse_service import get_langfuse_safe

logger = logging.getLogger(__name__)


# ===========================================================================
# Typed State — the single object passed between every node
# ===========================================================================

class StrategistState(TypedDict, total=False):
    """
    Shared state for the Strategist LangGraph pipeline.

    Every node reads from and writes to this dict.
    Fields are optional (total=False) so nodes only touch what they own.
    """

    # ── Input (set at graph entry) ──────────────────────────────────────────
    request:        StrategistRequest           # The original API request
    run_id:         str                         # UUID for this run

    # ── Intermediate: context fetched from DB ──────────────────────────────
    market_trends:  dict[str, str]              # {product_name: "rising"|"falling"|"stable"}
    client_config:  Optional[Any]               # ClientConfig or None if DB unavailable
    value_props:    list                        # [ValueProposition] — discount rules from DB

    # ── Intermediate: churn data ───────────────────────────────────────────
    churn_lookup:   dict[str, ChurnScore]       # {customer_id: ChurnScore}
    highest_risk:   Optional[str]               # "HIGH" | "MEDIUM" | "LOW" | None

    # ── Pricing engine output ──────────────────────────────────────────────
    recommendations: list[PricingRecommendation]

    # ── Final response ─────────────────────────────────────────────────────
    response:       Optional[StrategistResponse]

    # ── Error handling ─────────────────────────────────────────────────────
    error_message:  Optional[str]               # Set by validate_input on failure

    # ── Timing ─────────────────────────────────────────────────────────────
    started_at:     float                       # perf_counter at graph entry


# ===========================================================================
# Node: validate_input
# ===========================================================================

def validate_input(state: StrategistState) -> StrategistState:
    """
    Guard node — runs first.
    Validates the StrategistRequest and short-circuits on bad data.
    If error_message is set, downstream nodes should check it and skip.
    """
    request = state.get("request")

    if not request:
        return {**state, "error_message": "request is required"}

    if not request.scout_output or not request.scout_output.products:
        return {**state, "error_message": "scout_output.products is empty"}

    if not request.our_costs:
        logger.warning(
            "validate_input: our_costs is empty — all products will get no_cost_data flag"
        )

    logger.info(
        "validate_input OK: %d products, client=%s",
        len(request.scout_output.products),
        request.client_id,
    )

    return {
        **state,
        "run_id":      state.get("run_id", str(uuid.uuid4())),
        "started_at":  time.perf_counter(),
        "error_message": None,
    }


# ===========================================================================
# Node: load_market_context  (async)
# ===========================================================================

async def load_market_context(state: StrategistState) -> StrategistState:
    """
    Async DB node — fetches via LangChain tools:
      1. ClientConfigTool  → guardrails (max_discount_pct, LTV thresholds)
      2. MarketTrendTool   → price trend per product (rising/falling/stable)

    Using tools instead of direct DB calls means:
      - Each call is an observable LangSmith span with its own latency
      - Tests inject mock tools without touching DB connection code
      - Timeout + fallback logic lives in the tool, not the node
    """
    if state.get("error_message"):
        return state

    request       = state["request"]
    client_id     = request.client_id
    product_names = [p.name for p in request.scout_output.products]

    # ── Tool 1: ClientConfigTool — load guardrail thresholds ──────────────
    config_tool   = ClientConfigTool()
    client_config = await config_tool.ainvoke({"client_id": client_id})

    request.max_discount_pct   = client_config.max_discount_pct
    request.high_ltv_threshold = client_config.high_ltv_threshold
    logger.info(
        "load_market_context: ClientConfigTool → client=%s, max_discount=%.0f%%",
        client_id, client_config.max_discount_pct,
    )

    # ── Tool 2: MarketTrendTool — compute trend from price_history ─────────
    trend_tool    = MarketTrendTool()
    market_trends = await trend_tool.ainvoke({"product_names": product_names})
    logger.info(
        "load_market_context: MarketTrendTool → %d trends fetched", len(market_trends)
    )

    # ── Tool 3: CostFetchTool — fetch COGS from product_prices.cost_price_usd ──
    # Only fires when our_costs is NOT already in the request body.
    # product_prices.cost_price_usd is updated via the Upload Agent or UI.
    if not request.our_costs:
        cost_tool     = CostFetchTool()
        fetched_costs = await cost_tool.ainvoke({
            "product_names": product_names,
            "client_id":     client_id,
        })
        if fetched_costs:
            request.our_costs = fetched_costs
            logger.info(
                "load_market_context: CostFetchTool → %d costs fetched from product_prices",
                len(fetched_costs),
            )
        else:
            logger.warning(
                "load_market_context: CostFetchTool returned empty — "
                "products will get no_cost_data flag. "
                "Client should save costs via /api/db/product-costs first."
            )
    else:
        logger.info(
            "load_market_context: our_costs supplied by caller (%d products) — "
            "skipping CostFetchTool",
            len(request.our_costs),
        )

    # ── Tool 4: ValuePropFetchTool — load discount rules from DB ───────────
    vp_tool     = ValuePropFetchTool()
    value_props = await vp_tool.ainvoke({"client_id": request.client_id})
    logger.info(
        "load_market_context: ValuePropFetchTool → %d discount rules loaded",
        len(value_props),
    )

    # ── Resolve effective currency (request override OR client default) ────
    # Request-level currency wins (UI dropdown override). If omitted, fall
    # back to the client's configured currency. Final fallback: INR.
    effective_currency = (
        request.currency
        or (client_config.currency if client_config else None)
        or "INR"
    ).upper()
    # Stash back on the request so downstream nodes (run_pricing_engine)
    # have it without needing to re-derive
    request.currency = effective_currency
    logger.info(
        "load_market_context: effective currency = %s "
        "(request=%s, client_config=%s)",
        effective_currency,
        request.currency,
        getattr(client_config, "currency", None),
    )

    return {
        **state,
        "request":       request,
        "client_config": client_config,
        "market_trends": market_trends,
        "value_props":   value_props,
    }


# ===========================================================================
# Node: build_churn_lookup
# ===========================================================================

async def build_churn_lookup(state: StrategistState) -> StrategistState:
    """
    Indexes churn scores by customer_id for O(1) access in the pricing engine.
    Also derives highest_risk so the conditional router can branch without
    re-scanning recommendations.

    Uses ChurnScoreFetchTool with two-tier strategy:
      1. If churn_batch already in request body → use it directly (caller priority)
      2. Otherwise → ChurnScoreFetchTool fetches from Analyst Agent API or DB

    This means the caller no longer NEEDS to supply churn_batch — the graph
    will fetch it automatically when omitted.
    """
    if state.get("error_message"):
        return state

    request = state["request"]

    # ── skip_churn flag: caller explicitly disabled retention pricing ──────
    if request.skip_churn:
        logger.info("build_churn_lookup: skip_churn=True — skipping churn fetch")
        return {**state, "request": request, "churn_lookup": {}, "highest_risk": None}

    # ── Priority 1: use inline churn_batch if caller supplied it ──────────
    if request.churn_batch and request.churn_batch.scores:
        churn_batch = request.churn_batch
        logger.info(
            "build_churn_lookup: using inline churn_batch (%d scores)",
            len(churn_batch.scores),
        )
    else:
        # ── Priority 2: fetch via ChurnScoreFetchTool ─────────────────────
        # This is the key upgrade: the graph now fetches its own churn data
        # instead of requiring the caller to pre-fetch and pass it in
        churn_tool  = ChurnScoreFetchTool()
        churn_batch = await churn_tool.ainvoke({
            "client_id":   request.client_id,
            "risk_levels": ["HIGH", "MEDIUM"],
        })
        # Write back so the pricing engine (StrategistAgent.run) sees it
        request.churn_batch = churn_batch
        logger.info(
            "build_churn_lookup: ChurnScoreFetchTool fetched %d scores",
            len(churn_batch.scores),
        )

    # ── Index by customer_id for O(1) lookup ─────────────────────────────
    churn_lookup: dict[str, ChurnScore] = {
        s.customer_id: s for s in churn_batch.scores
    }

    highest_risk: Optional[str] = None
    if churn_lookup:
        risk_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        best = max(churn_lookup.values(), key=lambda s: risk_rank.get(s.risk_level, 0))
        highest_risk = best.risk_level
        logger.info(
            "build_churn_lookup: %d customers indexed, highest_risk=%s",
            len(churn_lookup), highest_risk,
        )

    return {**state, "request": request, "churn_lookup": churn_lookup, "highest_risk": highest_risk}


# ===========================================================================
# Node: run_pricing_engine
# ===========================================================================

async def run_pricing_engine(state: StrategistState) -> StrategistState:
    """
    Runs the 5-layer pricing engine (StrategistAgent.run).

    Also owns the ScoutPriceFetchTool call: if the request arrived without
    scout_output products (caller omitted them), this node fetches live
    competitor prices itself before running the engine.

    ScoutPriceFetchTool fetch priority:
      1. scout_output.products already populated → use as-is (standard path)
      2. scout_output empty → call ScoutPriceFetchTool with our_costs keys
         as the product names to search for

    The pricing engine itself is CPU-bound (pure Python math), so the async
    overhead here is only for the optional Scout fetch.
    """
    if state.get("error_message"):
        return state

    request       = state["request"]
    market_trends = state.get("market_trends", {})

    # ── ScoutPriceFetchTool — fire when any product has no listings ─────────
    # Covers both "scout_output empty" and "products provided but listings empty"
    # (e.g. UI sends product name only, expects backend to fetch competitor data).
    products_missing_listings = [
        p for p in request.scout_output.products if not p.listings
    ]
    should_fetch = (not request.scout_output.products) or bool(products_missing_listings)

    if should_fetch:
        # Build the set of product names we need competitor data for:
        #   (products with empty listings)  ∪  (keys of our_costs)
        names_to_fetch = {p.name for p in products_missing_listings}
        if not request.scout_output.products and request.our_costs:
            names_to_fetch.update(request.our_costs.keys())

        if names_to_fetch:
            logger.info(
                "run_pricing_engine: fetching competitor data for %d products "
                "in currency=%s via ScoutPriceFetchTool",
                len(names_to_fetch),
                request.currency,
            )
            scout_tool = ScoutPriceFetchTool()
            # Resolve currency: per-request override wins; else client_config default; else INR
            effective_currency = (
                request.currency
                or (state.get("client_config").currency if state.get("client_config") else None)
                or "INR"
            ).upper()

            fetched = await scout_tool.ainvoke({
                "product_names": list(names_to_fetch),
                "client_id":     request.client_id,
                "currency":      effective_currency,
            })
            # Merge fetched listings back into the request:
            # - products that existed in request with empty listings → fill from fetch
            # - new products → append
            fetched_by_name = {p.name: p for p in fetched.products}
            for p in request.scout_output.products:
                if not p.listings and p.name in fetched_by_name:
                    p.listings = fetched_by_name[p.name].listings

            # If the request had no products at all, replace scout_output entirely
            if not request.scout_output.products:
                request.scout_output = fetched

            logger.info(
                "run_pricing_engine: ScoutPriceFetchTool returned %d products "
                "(listings merged into request)",
                len(fetched.products),
            )
        else:
            logger.warning(
                "run_pricing_engine: no product names available for Scout fetch "
                "(scout_output empty AND our_costs empty)"
            )

    # ── 5-layer pricing engine ─────────────────────────────────────────────
    try:
        agent = StrategistAgent()
        recommendations, run_id = agent.run(
            request,
            market_trends=market_trends,
            value_props=state.get("value_props", []),
        )
        logger.info(
            "run_pricing_engine: %d recommendations produced (run_id=%s)",
            len(recommendations), run_id,
        )
        return {**state, "recommendations": recommendations, "run_id": run_id}

    except Exception as exc:
        logger.exception("run_pricing_engine: pricing engine failed")
        return {**state, "error_message": f"Pricing engine error: {exc}"}


# ===========================================================================
# Node: apply_charm_pricing  (post-processing pass — idempotent)
# ===========================================================================

def apply_charm_pricing(state: StrategistState) -> StrategistState:
    """
    Explicit charm-pricing post-processing node.

    The StrategistAgent already applies charm pricing internally, but this node
    makes it a visible, auditable step in the graph so it can be toggled off
    (e.g. for B2B clients) by removing this node from the graph without touching
    the core engine.

    Currently a no-op (charm already applied) — kept as an explicit seam
    for future A/B testing or config-driven disabling.
    """
    if state.get("error_message"):
        return state

    recs = state.get("recommendations", [])
    logger.debug("apply_charm_pricing: %d recommendations (charm already applied by engine)", len(recs))
    return state


# ===========================================================================
# Conditional router: route_by_churn_risk
# ===========================================================================

def route_by_churn_risk(state: StrategistState) -> str:
    """
    Conditional edge function — called by LangGraph to choose the next node.

    Returns one of:
      "retention_offer_node"  — at least one HIGH-risk customer in the batch
      "soft_flag_node"        — highest risk is MEDIUM (no HIGH)
      "persist_results"       — no churn data or all LOW risk
    """
    if state.get("error_message"):
        return "persist_results"

    risk = state.get("highest_risk")
    if risk == "HIGH":
        logger.info("route_by_churn_risk → retention_offer_node (HIGH risk detected)")
        return "retention_offer_node"
    elif risk == "MEDIUM":
        logger.info("route_by_churn_risk → soft_flag_node (MEDIUM risk detected)")
        return "soft_flag_node"
    else:
        logger.info("route_by_churn_risk → persist_results (no churn risk / LOW only)")
        return "persist_results"


# ===========================================================================
# Node: retention_offer_node
# ===========================================================================

def retention_offer_node(state: StrategistState) -> StrategistState:
    """
    Enrichment node for HIGH-risk churn customers.

    The StrategistAgent already applies the churn discount inside run_pricing_engine.
    This node's job is to:
      1. Log a structured audit line per retention recommendation
      2. Validate that no recommendation is below floor_price after discount
      3. Prepare any additional context needed for Retention Agent handoff

    This is the seam where you would inject LLM-generated personalised
    offer copy in a future iteration.
    """
    if state.get("error_message"):
        return state

    recs         = state.get("recommendations", [])
    churn_lookup = state.get("churn_lookup", {})

    retention_recs = [r for r in recs if r.strategy == "retention"]

    for rec in retention_recs:
        if rec.churn_context:
            logger.info(
                "retention_offer_node: %s → ₹%.2f (was ₹%.2f, discount=%.0f%%, tier=%s)",
                rec.product_name,
                rec.suggested_price,
                rec.pre_retention_price,
                rec.churn_context.discount_applied,
                rec.churn_context.customer_tier,
            )
        # Guardrail: retention price must never go below floor
        if rec.suggested_price < rec.floor_price:
            logger.warning(
                "retention_offer_node: GUARDRAIL HIT — %s retention price ₹%.2f < floor ₹%.2f, "
                "clamping to floor.",
                rec.product_name, rec.suggested_price, rec.floor_price,
            )
            rec.suggested_price = rec.floor_price

    logger.info(
        "retention_offer_node: %d retention recommendations audited", len(retention_recs)
    )
    return {**state, "recommendations": recs}


# ===========================================================================
# Node: soft_flag_node
# ===========================================================================

def soft_flag_node(state: StrategistState) -> StrategistState:
    """
    MEDIUM-risk churn handling.

    Does not modify pricing. Appends a re-engagement note to each recommendation
    where a MEDIUM-risk customer is in the churn batch. This note surfaces in
    the Retention Agent's campaign trigger.
    """
    if state.get("error_message"):
        return state

    recs         = state.get("recommendations", [])
    churn_lookup = state.get("churn_lookup", {})

    medium_customers = [
        s for s in churn_lookup.values() if s.risk_level == "MEDIUM"
    ]

    if medium_customers:
        note = (
            f"{len(medium_customers)} MEDIUM-risk customer(s) detected "
            f"(IDs: {', '.join(s.customer_id for s in medium_customers[:3])}). "
            "Consider re-engagement campaign — no discount applied."
        )
        if recs and note not in recs[0].warnings:
            recs[0].warnings.append(note)

        logger.info(
            "soft_flag_node: re-engagement note added as batch-level signal "
            "(%d MEDIUM customers)", len(medium_customers)
        )

    return {**state, "recommendations": recs}


# ===========================================================================
# Node: persist_results  (async)
# ===========================================================================

async def persist_results(state: StrategistState) -> StrategistState:
    """
    DB persistence node — writes:
      - pricing_recommendations table
      - customer_price_context  table (for Retention Agent handoff)

    Failures are logged but NEVER abort the graph.
    The client always gets recommendations even if DB is down.
    """
    if state.get("error_message"):
        # Build an error response so the router doesn't return None
        return {
            **state,
            "response": StrategistResponse(
                recommendations = [],
                total_products  = 0,
                flagged_count   = 0,
                strategies_used = [],
                avg_margin_pct  = 0.0,
                retention_count = 0,
                run_id          = state.get("run_id", "error"),
            ),
        }

    request         = state["request"]
    recommendations = state.get("recommendations", [])
    run_id          = state.get("run_id", str(uuid.uuid4()))

    # ── PersistRecommendationTool — retry-aware, structured result ────────
    # Never raises — returns PersistResult(success=False) on failure so the
    # client always receives their recommendations even if the DB is down.
    persist_tool = PersistRecommendationTool()
    persist_result = await persist_tool._arun_with_data(
        run_id          = run_id,
        request         = request,
        recommendations = recommendations,
    )
    if persist_result.success:
        logger.info(
            "persist_results: PersistRecommendationTool OK — "
            "%d recs + %d price contexts (run_id=%s)",
            persist_result.recommendations_written,
            persist_result.contexts_written,
            run_id,
        )
    else:
        logger.error(
            "persist_results: PersistRecommendationTool FAILED (run_id=%s): %s",
            run_id, persist_result.error,
        )

    # ── Assemble final StrategistResponse ──────────────────────────────────
    strategies_used = sorted({r.strategy for r in recommendations})
    avg_margin = (
        round(sum(r.margin_percent for r in recommendations) / len(recommendations), 1)
        if recommendations else 0.0
    )
    latency_ms = round((time.perf_counter() - state.get("started_at", time.perf_counter())) * 1000, 1)

    response = StrategistResponse(
        recommendations = recommendations,
        total_products  = len(recommendations),
        flagged_count   = sum(1 for r in recommendations if r.flag),
        strategies_used = strategies_used,
        avg_margin_pct  = avg_margin,
        retention_count = sum(1 for r in recommendations if r.strategy == "retention"),
        run_id          = run_id,
    )

    logger.info(
        "persist_results: pipeline complete — %d recs, avg_margin=%.1f%%, "
        "retention=%d, latency=%.1fms",
        len(recommendations), avg_margin,
        response.retention_count, latency_ms,
    )

    return {**state, "response": response}


# ===========================================================================
# Graph builder
# ===========================================================================

def build_strategist_graph() -> StateGraph:
    """
    Constructs and compiles the Strategist LangGraph StateGraph.

    Node async status after tool wiring:
      validate_input        sync   (pure Pydantic validation)
      load_market_context   async  (ClientConfigTool + MarketTrendTool)
      build_churn_lookup    async  (ChurnScoreFetchTool)
      run_pricing_engine    async  (ScoutPriceFetchTool + StrategistAgent.run)
      apply_charm_pricing   sync   (no I/O)
      retention_offer_node  sync   (audit + guardrail checks)
      soft_flag_node        sync   (append warnings)
      persist_results       async  (PersistRecommendationTool)

    Returns a compiled graph ready for .ainvoke() calls.
    Compile once at module load; reuse across requests (thread-safe).
    """
    graph = StateGraph(StrategistState)

    # ── Register nodes ──────────────────────────────────────────────────────
    graph.add_node("validate_input",       validate_input)
    graph.add_node("load_market_context",  load_market_context)
    graph.add_node("build_churn_lookup",   build_churn_lookup)
    graph.add_node("run_pricing_engine",   run_pricing_engine)
    graph.add_node("apply_charm_pricing",  apply_charm_pricing)
    graph.add_node("retention_offer_node", retention_offer_node)
    graph.add_node("soft_flag_node",       soft_flag_node)
    graph.add_node("persist_results",      persist_results)

    # ── Linear edges ────────────────────────────────────────────────────────
    graph.add_edge(START,                  "validate_input")
    graph.add_edge("validate_input",       "load_market_context")
    graph.add_edge("load_market_context",  "build_churn_lookup")
    graph.add_edge("build_churn_lookup",   "run_pricing_engine")
    graph.add_edge("run_pricing_engine",   "apply_charm_pricing")

    # ── Conditional edge after charm pricing ─────────────────────────────
    graph.add_conditional_edges(
        "apply_charm_pricing",
        route_by_churn_risk,
        {
            "retention_offer_node": "retention_offer_node",
            "soft_flag_node":       "soft_flag_node",
            "persist_results":      "persist_results",
        },
    )

    # ── Retention / soft-flag both converge to persist ───────────────────
    graph.add_edge("retention_offer_node", "persist_results")
    graph.add_edge("soft_flag_node",       "persist_results")
    graph.add_edge("persist_results",      END)

    return graph.compile()


# Module-level compiled graph — build once, reuse across requests
strategist_graph = build_strategist_graph()


# ===========================================================================
# Public entry point
# ===========================================================================

async def run_strategist_graph(
    request: StrategistRequest,
    config: RunnableConfig | None = None,
) -> tuple[list[PricingRecommendation], str]:
    """
    Entry point for the LangGraph-powered Strategist pipeline.

    Replaces the direct StrategistAgent.run() call in strategist_router.py.
    Compatible with the existing (recommendations, run_id) return signature
    so the router needs zero changes.

    Args:
        request: Full StrategistRequest (Scout data + costs + optional churn batch).
        config:  Optional LangChain RunnableConfig (for LangSmith tracing, tags, etc.)

    Returns:
        (recommendations, run_id) — same shape as StrategistAgent.run()
    """
    initial_state: StrategistState = {
        "request":        request,
        "run_id":         str(uuid.uuid4()),
        "started_at":     time.perf_counter(),
        "recommendations": [],
        "market_trends":  {},
        "churn_lookup":   {},
        "highest_risk":   None,
        "client_config":  None,
        "value_props":    [],
        "response":       None,
        "error_message":  None,
    }

    final_state: StrategistState = await strategist_graph.ainvoke(
        initial_state,
        config=config or {},
    )

    error = final_state.get("error_message")
    if error:
        raise ValueError(f"Strategist graph failed: {error}")

    response = final_state.get("response")
    recommendations = response.recommendations if response else final_state.get("recommendations", [])
    run_id = final_state.get("run_id", "unknown")

    return recommendations, run_id