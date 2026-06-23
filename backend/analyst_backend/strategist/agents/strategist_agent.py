"""
agents/strategist_agent.py — Customer Retention Platform
=========================================================

5-Layer Pricing Engine
----------------------

Layer 1 — Cost Math (guarantees we never sell below cost)
  true_cost    = COGS × overhead_multiplier   (covers logistics, ops, tax)
  floor_price  = true_cost × (1 + min_margin_pct / 100)
  target_price = true_cost × (1 + target_margin_pct / 100)

Layer 2 — Competitor Anchoring (Scout DB entity_listings prices)
  comp_min    = cheapest in-stock, high-confidence competitor price
  comp_median = median (more robust than avg — ignores outliers)
  comp_avg    = mean (used for premium positioning)
  comp_max    = most expensive
  spread_pct  = (comp_max - comp_min) / comp_median × 100 (price war signal)

Layer 3 — Market Trend (Scout DB price_history, 14-day vs 30-day avg)
  rising  → hold margin, don't undercut aggressively
  falling → match fast, capture share while competitors are expensive
  stable  → standard decision tree

Layer 4 — Strategy Decision Tree
  floor_price > comp_min              → floor_only  (flag: low_margin_warning)
  priority=brand OR segment=premium   → premium     (comp_avg × 1.05)
  target ≤ comp_min AND rising+margin → match
  target ≤ comp_min                  → undercut    (comp_min × (1 - undercut_pct%))
  else                                → match       (min of target, comp_min)

Layer 5 — Churn-Signal Fusion (Analyst Agent churn_scores.json)
  HIGH risk  → retention_price = suggested × (1 - discount_pct / 100)
               discount_pct from value_propositions table (tier-specific)
               capped at client_config.max_discount_pct
               strategy → "retention"
               pre_retention_price saved for Retention Agent handoff
  MEDIUM risk → soft flag, standard price, re-engagement note
  LOW risk    → no change

Post-Processing — Charm Pricing
  ₹247 → ₹249   ₹1823 → ₹1799   ₹312 → ₹299
  Applied AFTER all margin and churn logic.
  Never applied to floor_price (precise cost boundary, must stay exact).
"""

from __future__ import annotations

import logging
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

from strategist.models.schemas import (
    ChurnContext,
    ChurnScore,
    MarginBand,
    PlatformPrice,
    PricingRecommendation,
    ScoutListing,
    ScoutProduct,
    StrategistRequest,
)
from strategist.services.langfuse_service import get_langfuse_safe


# ---------------------------------------------------------------------------
# Fallback discount table — mirrors value_propositions seed data
# Used when Analyst DB is not queryable (always the case if DB is down)
# ---------------------------------------------------------------------------
# ── Currency symbol map ───────────────────────────────────────────────────
_CURRENCY_SYMBOLS: dict[str, str] = {
    "INR": "₹",
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "AUD": "A$",
    "CAD": "C$",
}

# ── VP discount fallback ──────────────────────────────────────────────────
_VP_DISCOUNTS: dict[tuple[str, str], float] = {
    ("Platinum", "HIGH"):   20.0,   # 20% off for Platinum customers at HIGH risk
    ("Platinum", "MEDIUM"): 10.0,
    ("Gold",     "HIGH"):   15.0,
    ("Gold",     "MEDIUM"):  8.0,
    ("Silver",   "HIGH"):   10.0,
    ("Silver",   "MEDIUM"):  5.0,
    ("Bronze",   "HIGH"):    5.0,
    ("Bronze",   "MEDIUM"):  0.0,   # Bronze + MEDIUM: re-engagement message only
}


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------
@dataclass
class StrategistConfig:
    """Runtime config for the pricing engine. Synced from StrategistRequest at run time."""
    target_margin_pct:         float = 20.0
    min_margin_pct:            float = 8.0
    undercut_pct:              float = 2.0
    overhead_multiplier:       float = 1.15
    min_confidence:            float = 0.5
    max_discount_pct:          float = 30.0   # hard cap from client_config
    high_ltv_threshold:        float = 500.0
    premium_markup_pct:        float = 5.0    # % above comp_avg for premium strategy
    falling_market_undercut_pct: float = 1.0  # % undercut when market trend is falling
    charm_high_threshold:      float = 1000.0 # prices >= this snap to xx99 pattern
    charm_low_threshold:       float = 50.0   # prices < this skip charm pricing entirely


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
class StrategistAgent:
    """
    Stateless pricing engine. Create one per request (lightweight — no DB connections).
    All state is in self.config which is synced from the request at the start of run().
    """

    def __init__(self, config: StrategistConfig | None = None):
        self.config = config or StrategistConfig()
        self.lf     = get_langfuse_safe()   # LangFuse client (None if not configured)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        request: StrategistRequest,
        market_trends: dict[str, str] | None = None,
        value_props: list | None = None,
    ) -> tuple[list[PricingRecommendation], str]:
        """
        Process all products in the Scout output and generate price recommendations.

        Args:
            request:       Full StrategistRequest including Scout data, COGS, and churn batch.
            market_trends: {product_name: "rising"|"falling"|"stable"} from price_history.
                           Computed by persistence.py before calling run().
                           If None, all products default to "stable".
            value_props:   [ValueProposition] — discount rules loaded by the graph from
                           value_propositions DB table. If None or empty, the agent
                           falls back to hardcoded _VP_DISCOUNTS.
                           Shared with Retention Agent so both agents apply the same
                           (tier, risk) → discount_pct rules.

        Returns:
            (recommendations list, run_id UUID string)
        """
        run_id = str(uuid.uuid4())
        market_trends = market_trends or {}

        # Sync config from the request (request values override dataclass defaults)
        self.config.target_margin_pct   = request.target_margin_pct
        self.config.min_margin_pct      = request.min_margin_pct
        self.config.undercut_pct        = request.undercut_pct
        self.config.overhead_multiplier = request.overhead_multiplier
        logger.debug(
            "PricingConfig: target=%.1f%% min=%.1f%% undercut=%.1f%% overhead=%.2f",
            self.config.target_margin_pct, self.config.min_margin_pct,
            self.config.undercut_pct, self.config.overhead_multiplier,
        )
        self.config.min_confidence      = request.min_confidence
        self.config.max_discount_pct    = request.max_discount_pct
        self.config.high_ltv_threshold  = request.high_ltv_threshold

        # Build discount lookup — DB rows beat hardcoded fallback
        discount_lookup = _build_discount_lookup(value_props)

        # churn_lookup: built from request.churn_batch which was already
        # populated by the graph's build_churn_lookup node.
        churn_lookup: dict[str, ChurnScore] = {}
        if request.churn_batch and not request.skip_churn:
            for score in request.churn_batch.scores:
                churn_lookup[score.customer_id] = score

        # Start LangFuse trace (best-effort — agent doesn't crash if LF is down)
        trace = self._lf_trace(run_id, request)

        t0 = time.perf_counter()
        recommendations: list[PricingRecommendation] = []

        for product in request.scout_output.products:
            # Look up this product's market trend (default stable)
            trend = market_trends.get(product.name, "stable")
            rec = self._process_product(
                    product          = product,
                    our_costs        = request.our_costs,
                    market_trend     = trend,
                    client_priority  = request.client_priority,
                    customer_segment = request.customer_segment,
                    churn_lookup     = churn_lookup,
                    discount_lookup  = discount_lookup,
                    trace            = trace,
                    currency         = request.currency or "INR",
                )
            recommendations.append(rec)

        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        self._lf_close(trace, recommendations, latency_ms)

        return recommendations, run_id

    # ------------------------------------------------------------------
    # Per-product processing (all 5 layers)
    # ------------------------------------------------------------------

    def _process_product(
        self,
        product:         ScoutProduct,
        our_costs:       dict[str, float],
        market_trend:    str,
        client_priority: Optional[str],
        customer_segment:Optional[str],
        churn_lookup:    dict[str, ChurnScore],
        discount_lookup: dict[tuple[str, str], float],
        trace,
        currency:        str = "INR",
    ) -> PricingRecommendation:
        currency_symbol = _CURRENCY_SYMBOLS.get(currency.upper(), currency)

        span = self._lf_span(trace, f"product:{product.name[:50]}", len(product.listings))

        # ── Layer 1: Filter valid listings ─────────────────────────────────
        # Only use in-stock listings with confidence above threshold AND
        # matching the requested currency so prices are never mixed across
        # currencies (e.g. INR listings must not pollute a USD pricing run).
        valid: list[ScoutListing] = [
            lst for lst in product.listings
            if lst.availability == "in_stock"
            and lst.price.value > 0
            and lst.source.confidence >= self.config.min_confidence
            and lst.price.currency.upper() == currency.upper()
        ]

        # Build platform breakdown for the response (all valid listings, not just cheapest)
        platform_breakdown = [
            PlatformPrice(
                platform     = lst.platform,
                price        = round(lst.price.value, 2),
                availability = lst.availability,
                confidence   = lst.source.confidence,
                url          = lst.url,
            )
            for lst in valid
        ]

        prices = [lst.price.value for lst in valid]

        # ── Guard: no valid listings ────────────────────────────────────────
        if not prices:
            self._lf_end_span(span, {"skipped": "no_valid_listings"})
            return self._no_data(
                product.name, platform_breakdown, "no_price_data",
                f"No in-stock listings found with confidence ≥ {self.config.min_confidence}.",
            )

        # ── Guard: no COGS or suspiciously low COGS ─────────────────────────
        raw_cogs = our_costs.get(product.name)
        if raw_cogs is None or raw_cogs <= 0:
            self._lf_end_span(span, {"skipped": "no_cost_data"})
            return self._no_data(
                product.name, platform_breakdown, "no_cost_data",
                f"COGS not provided for '{product.name}'. Add to our_costs.",
                comp_min    = round(min(prices), 2),
                comp_avg    = round(statistics.mean(prices), 2),
                comp_max    = round(max(prices), 2),
                comp_median = round(statistics.median(prices), 2),
            )
        # COGS must be at least ₹1 — anything less produces absurd margins
        # (₹0.50 cost + ₹100 price = 19,900% margin). Almost certainly a data-entry error.
        if raw_cogs < 1.0:
            self._lf_end_span(span, {"skipped": "cogs_too_low"})
            return self._no_data(
                product.name, platform_breakdown, "invalid_cost_data",
                f"COGS for '{product.name}' is suspiciously low ({currency_symbol}{raw_cogs}). "
                f"Minimum {currency_symbol}1 required. Check your data.",
                comp_min    = round(min(prices), 2),
                comp_avg    = round(statistics.mean(prices), 2),
                comp_max    = round(max(prices), 2),
                comp_median = round(statistics.median(prices), 2),
            )

        # ── Layer 1: Cost math ──────────────────────────────────────────────
        # overhead_multiplier covers logistics, ops overhead, tax
        true_cost    = round(raw_cogs * self.config.overhead_multiplier, 2)
        floor_price  = round(true_cost * (1 + self.config.min_margin_pct    / 100), 2)
        target_price = round(true_cost * (1 + self.config.target_margin_pct / 100), 2)
        logger.debug(
            "%s: cogs=%s true_cost=%s floor=%s target=%s comp_min=%s undercut=%.1f%%",
            product.name, raw_cogs, true_cost, floor_price, target_price,
            min(prices) if prices else 0, self.config.undercut_pct,
        )

        # ── Layer 2: Competitor stats ───────────────────────────────────────
        comp_min    = round(min(prices), 2)
        comp_max    = round(max(prices), 2)
        comp_avg    = round(statistics.mean(prices), 2)
        comp_median = round(statistics.median(prices), 2)

        # spread_pct detects price war conditions (wide spread = unstable market)
        spread_pct = ((comp_max - comp_min) / comp_median * 100) if comp_median else 0.0

        # ── Layer 3 + 4: Strategy decision tree ────────────────────────────
        warnings: list[str] = []
        flag:     str | None = None

        if spread_pct > 20:
            # Wide spread suggests a price war or data quality issue
            warnings.append(
                f"Price war risk: competitor spread is {spread_pct:.1f}% — "
                "verify listings before publishing."
            )

        if floor_price > comp_min:
            # Our cheapest viable price is already above the market floor
            # → sell at floor, flag for margin review
            suggested = floor_price
            strategy  = "floor_only"
            flag      = "low_margin_warning"
            warnings.append(
                f"Our floor ({currency_symbol}{floor_price}) exceeds market min ({currency_symbol}{comp_min}). "
                "Competitor may be selling at a loss or have lower COGS."
            )

        elif client_priority == "brand" or customer_segment == "premium":
            # Premium positioning: price 5% above avg to signal quality
            premium_price = round(comp_avg * (1 + self.config.premium_markup_pct / 100), 2)
            if premium_price >= floor_price:
                suggested = premium_price
                strategy  = "premium"
                warnings.append(
                    "Premium positioning: 5% above market avg. "
                    "Ensure product quality and brand story justify this."
                )
            else:
                # Even premium price is below our floor → floor_only
                suggested = floor_price
                strategy  = "floor_only"
                flag      = "low_margin_warning"

        elif target_price <= comp_min:
            # Our target price is cheaper than the cheapest competitor
            # → great margin position, decide whether to undercut or hold
            if market_trend == "rising" and client_priority == "margin":
                # Market is rising AND client wants margin → don't give away margin
                suggested = round(min(target_price, comp_min), 2)
                strategy  = "match"
            else:
                # Undercut to capture volume (most common case)
                raw_undercut = round(comp_min * (1 - self.config.undercut_pct / 100), 2)
                suggested    = max(raw_undercut, floor_price)   # NEVER below floor
                strategy     = "undercut"

        else:
            # Our target is above comp_min → match near market floor
            suggested = round(min(target_price, comp_min), 2)
            strategy  = "match"

        # ── Layer 3 continuation: trend adjustments ─────────────────────────
        if market_trend == "falling" and strategy == "match":
            # Market falling → be more aggressive to capture share before competitors catch up
            aggressive = round(comp_min * (1 - self.config.falling_market_undercut_pct / 100), 2)
            if aggressive >= floor_price:
                suggested = aggressive
                strategy  = "undercut"
                warnings.append("Market is falling — undercutting slightly to capture share.")

        elif market_trend == "rising" and strategy == "undercut":
            # Market rising → hold our price instead of undercutting
            suggested = round(min(target_price, comp_min), 2)
            strategy  = "match"
            warnings.append("Market is rising — holding price instead of undercutting.")

        # ── Layer 5: Churn-signal fusion ────────────────────────────────────
        # Find the worst-churn customer from the batch (in single-customer mode: one entry)
        churn_score  = _best_churn_for_retention(churn_lookup)
        churn_context: Optional[ChurnContext] = None
        pre_retention_price = 0.0   # 0.0 = no churn discount applied

        if churn_score and churn_score.risk_level == "HIGH":
            # Look up the appropriate discount for this tier + risk combination.
            # discount_lookup comes from value_propositions DB (via graph) with
            # hardcoded _VP_DISCOUNTS as the fallback. Shared with Retention Agent.
            lookup_key   = (churn_score.customer_tier, churn_score.risk_level)
            raw_discount = discount_lookup.get(lookup_key, None)

            if raw_discount is None:
                logger.warning(
                    "_process_product: no discount rule for tier=%s risk=%s — "
                    "skipping retention pricing for customer %s. "
                    "Add this tier to value_propositions table or _VP_DISCOUNTS.",
                    churn_score.customer_tier, churn_score.risk_level,
                    churn_score.customer_id,
                )
                raw_discount = 0.0

            capped_discount = min(raw_discount, self.config.max_discount_pct)

            if capped_discount > 0:
                pre_retention_price = suggested    # save original for audit trail
                suggested           = round(suggested * (1 - capped_discount / 100), 2)
                suggested           = max(suggested, floor_price)   # guardrail: never below floor

                strategy = "retention"

                # ChurnContext is stored in the recommendation for the Retention Agent handoff
                churn_context = ChurnContext(
                    customer_id       = churn_score.customer_id,
                    churn_probability = churn_score.churn_probability,
                    risk_level        = churn_score.risk_level,
                    customer_tier     = churn_score.customer_tier,
                    discount_applied  = capped_discount,
                    discount_reason   = (
                        f"{capped_discount}% churn retention discount "
                        f"(tier: {churn_score.customer_tier}, "
                        f"churn_prob: {churn_score.churn_probability:.0%})"
                    ),
                )

        elif churn_score and churn_score.risk_level == "MEDIUM":
            # MEDIUM risk: keep standard price, add a note for the team
            warnings.append(
                f"Customer {churn_score.customer_id} is MEDIUM churn risk "
                f"({churn_score.churn_probability:.0%}) — consider re-engagement campaign."
            )

        
        # ── Layer 5 (post): Charm pricing ──────────────────────────────────
        # Applied AFTER all margin/churn logic. The final floor guardrail runs
        # LAST (below) so charm can never push the price below the cost floor.
        if strategy not in ("floor_only", "no_data"):
            charmed = _charm_price(suggested, self.config.charm_high_threshold, self.config.charm_low_threshold)
            # Guard: charm must not push price UP to or above comp_min on undercut/match
            # e.g. match at ₹130 → charm ₹139 = above market floor → defeats the match
            # e.g. undercut at ₹293 → charm ₹299 = same as Amazon → defeats the undercut
            if strategy in ("undercut", "match") and charmed >= comp_min:
                pass   # skip charm, keep raw price
            else:
                suggested = charmed

        # Final floor guardrail — runs LAST so nothing (charm, retention, trend)
        # can ever leave the final price below cost floor.
        suggested = max(suggested, floor_price)

        # ── Calculate final margin ──────────────────────────────────────────
        margin_pct = _margin(suggested, true_cost)

        margin_band = MarginBand(
            at_floor     = _margin(floor_price,  true_cost),
            at_target    = _margin(target_price, true_cost),
            at_suggested = margin_pct,
            at_comp_min  = _margin(comp_min,     true_cost),
        )

        # Confidence: high = 3+ competitors, medium = 1-2.
        # (Zero competitors is guarded upstream at line ~218, so "low" is unreachable here.)
        confidence = "high" if len(valid) >= 3 else "medium"

        # ── Build final recommendation ──────────────────────────────────────
        rec = PricingRecommendation(
            product_name         = product.name,
            suggested_price      = suggested,
            pre_retention_price  = pre_retention_price,
            floor_price          = floor_price,
            target_price         = target_price,
            our_cost             = true_cost,
            raw_cogs             = raw_cogs,
            competitor_min       = comp_min,
            competitor_avg       = comp_avg,
            competitor_max       = comp_max,
            competitor_median    = comp_median,
            platform_breakdown   = platform_breakdown,
            market_trend         = market_trend,
            margin_percent       = margin_pct,
            margin_band          = margin_band,
            strategy             = strategy,
            confidence           = confidence,
            reasoning            = self._build_reasoning(
                strategy, suggested, pre_retention_price, target_price,
                floor_price, comp_min, comp_avg, comp_median, true_cost,
                raw_cogs, market_trend, margin_pct, client_priority,
                customer_segment, flag, warnings, platform_breakdown,
                currency_symbol,
            ),
            client_note          = self._client_note(strategy, suggested, margin_pct, flag, currency_symbol),
            customer_note        = self._customer_note(
                strategy, suggested, comp_min, comp_avg, customer_segment, currency_symbol
            ),
            flag                 = flag,
            warnings             = warnings,
            churn_context        = churn_context,
        )

        self._lf_end_span(span, {
            "strategy":   strategy,
            "price":      suggested,
            "margin_pct": margin_pct,
        })
        return rec

    # ------------------------------------------------------------------
    # Reasoning text builders (detailed explanations for strategists)
    # ------------------------------------------------------------------

    def _build_reasoning(
        self, strategy, suggested, pre_retention, target, floor,
        comp_min, comp_avg, comp_median, true_cost, raw_cogs,
        market_trend, margin_pct, client_priority, customer_segment,
        flag, warnings, platform_breakdown, currency_symbol="₹",
    ) -> str:
        """Build a detailed, multi-part reasoning string for internal use."""

        # Cost breakdown block (helps strategists understand margin math)
        cost_block = (
            f"COGS: {currency_symbol}{raw_cogs} × {self.config.overhead_multiplier}x overhead = "
            f"{currency_symbol}{true_cost} true cost. "
            f"Floor (min {self.config.min_margin_pct}% margin): {currency_symbol}{floor}. "
            f"Target ({self.config.target_margin_pct}% margin): {currency_symbol}{target}."
        )

        # Competitor market block
        platform_str = ", ".join(p.platform for p in platform_breakdown[:5])
        market_block = (
            f"Market ({len(platform_breakdown)} platforms: {platform_str}): "
            f"min={currency_symbol}{comp_min}, median={currency_symbol}{comp_median}, avg={currency_symbol}{comp_avg}. "
            f"Trend: {market_trend}."
        )

        # Strategy-specific explanation
        strategy_text = {
            "undercut":   (f"UNDERCUT — target ({currency_symbol}{target}) < comp_min ({currency_symbol}{comp_min}). "
                           f"Undercutting by {self.config.undercut_pct}% → {currency_symbol}{suggested}."),
            "match":      (f"MATCH — pricing at {currency_symbol}{suggested} (min of target/comp_min)."),
            "floor_only": (f"FLOOR ONLY — our floor ({currency_symbol}{floor}) > comp_min ({currency_symbol}{comp_min}). "
                           "Cannot profitably undercut. Selling at cost floor."),
            "premium":    (f"PREMIUM — brand positioning 5% above avg ({currency_symbol}{comp_avg}) → {currency_symbol}{suggested}."),
            "retention":  (f"RETENTION — churn discount applied. "
                           f"Standard price was {currency_symbol}{pre_retention} → {currency_symbol}{suggested}."),
            "no_data":    "INSUFFICIENT DATA.",
        }.get(strategy, "")

        parts = [
            strategy_text,
            cost_block,
            market_block,
            f"Final margin: {margin_pct}%.",
        ]
        if client_priority:  parts.append(f"Client priority: {client_priority}.")
        if customer_segment: parts.append(f"Customer segment: {customer_segment}.")
        if flag:             parts.append(f"Flag: {flag}.")
        parts.extend([f"Warning: {w}" for w in warnings])

        return " | ".join(parts)

    def _client_note(self, strategy: str, suggested: float, margin_pct: float, flag, currency_symbol: str = "₹") -> str:
        """Short note for internal teams / dashboards."""
        notes = {
            "undercut":   f"{currency_symbol}{suggested} — undercutting market, {margin_pct}% margin.",
            "match":      f"{currency_symbol}{suggested} — matching market floor, {margin_pct}% margin.",
            "floor_only": f"⚠ {currency_symbol}{suggested} (floor only) — market is cheaper; review COGS.",
            "premium":    f"{currency_symbol}{suggested} — premium positioning, {margin_pct}% margin.",
            "retention":  f"{currency_symbol}{suggested} — retention price (churn discount applied).",
            "no_data":    "⚠ Cannot price — missing cost or competitor data.",
        }
        return notes.get(strategy, "")

    def _customer_note(
        self,
        strategy: str,
        suggested: float,
        comp_min: float,
        comp_avg: float,
        customer_segment: str | None,
        currency_symbol: str = "₹",
    ) -> str:
        """Customer-facing price justification text."""
        if strategy == "no_data":
            return "Price unavailable."
        savings_vs_avg = round(comp_avg - suggested, 2)

        if strategy == "premium":
            return (
                f"Priced at {currency_symbol}{suggested} — a quality-first choice. "
                "You get premium formulation, authenticity guarantee, and priority support."
            )
        elif strategy == "retention":
            return (
                f"Special offer: {currency_symbol}{suggested}. "
                "Exclusive price for valued customers — limited time."
            )
        elif strategy == "undercut":
            return (
                f"Best price available: {currency_symbol}{suggested} — "
                f"{currency_symbol}{abs(savings_vs_avg)} below market average ({currency_symbol}{comp_avg}). "
                "Quality product, lowest price."
            )
        elif savings_vs_avg > 0:
            return (
                f"Competitive price: {currency_symbol}{suggested} — "
                f"{currency_symbol}{savings_vs_avg} below the market average."
            )
        else:
            return (
                f"Market-aligned price: {currency_symbol}{suggested}. "
                "Inline with the best available rate from trusted sellers."
            )

    # ------------------------------------------------------------------
    # No-data helper (guards for missing COGS or listings)
    # ------------------------------------------------------------------

    def _no_data(
        self,
        product_name:     str,
        platform_breakdown: list,
        flag:             str,
        reason:           str,
        comp_min:         float = 0.0,
        comp_avg:         float = 0.0,
        comp_max:         float = 0.0,
        comp_median:      float = 0.0,
    ) -> PricingRecommendation:
        """Return a placeholder recommendation when data is insufficient."""
        return PricingRecommendation(
            product_name         = product_name,
            suggested_price      = 0.0,
            pre_retention_price  = 0.0,
            floor_price          = 0.0,
            target_price         = 0.0,
            our_cost             = 0.0,
            raw_cogs             = 0.0,
            competitor_min       = comp_min,
            competitor_avg       = comp_avg,
            competitor_max       = comp_max,
            competitor_median    = comp_median,
            platform_breakdown   = platform_breakdown,
            market_trend         = "stable",
            margin_percent       = 0.0,
            margin_band          = MarginBand(at_floor=0.0, at_target=0.0, at_suggested=0.0, at_comp_min=0.0),
            strategy     = "no_data",
            confidence   = "low",
            reasoning    = reason,
            client_note  = "⚠ Insufficient data — review product setup.",
            customer_note= "Price not currently available.",
            flag         = flag,
            warnings     = [reason],
        )

    # ------------------------------------------------------------------
    # LangFuse helpers (all best-effort — never crash the agent)
    # ------------------------------------------------------------------

    def _lf_trace(self, run_id: str, request: StrategistRequest):
        try:
            if not self.lf: return None
            return self.lf.trace(
                id=run_id, name="strategist_agent",
                input={
                    "product_count": len(request.scout_output.products),
                    "products":      [p.name for p in request.scout_output.products],
                    "churn_count":   len(request.churn_batch.scores) if request.churn_batch else 0,
                    "client_id":     request.client_id,
                },
            )
        except Exception:
            return None

    def _lf_span(self, trace, name: str, listings: int):
        try:
            return trace.span(name=name, input={"listings": listings}) if trace else None
        except Exception:
            return None

    def _lf_end_span(self, span, output: dict):
        try:
            if span: span.end(output=output, usage={"total_cost": 0.0})
        except Exception:
            pass

    def _lf_close(self, trace, recommendations: list, latency_ms: float):
        try:
            if not trace: return
            # Skip margin_health score for empty runs — a score of 0.0 would
            # falsely conflate "no data" with "nothing healthy."
            if recommendations:
                viable = sum(1 for r in recommendations if r.margin_percent >= self.config.min_margin_pct)
                trace.score(
                    name="margin_health",
                    value=viable / len(recommendations),
                    comment=f"{viable}/{len(recommendations)} above min margin",
                )
            trace.update(output={
                "count":      len(recommendations),
                "flagged":    sum(1 for r in recommendations if r.flag),
                "retention":  sum(1 for r in recommendations if r.strategy == "retention"),
                "latency_ms": latency_ms,
            })
        except Exception:
            pass


# ===========================================================================
# Module-level pure functions
# ===========================================================================

def _margin(price: float, cost: float) -> float:
    """Calculate profit margin % on cost. Returns 0.0 if cost is zero."""
    if cost <= 0:
        return 0.0
    return round(((price - cost) / cost) * 100, 1)


def _charm_price(price: float, high_threshold: float = 1000.0, low_threshold: float = 50.0) -> float:
    """
    Psychological pricing — proven 1-3% conversion lift in e-commerce.

    Rules:
      Prices >= high_threshold: snap to nearest xx99  (1823 → 1799, not 1899)
      Prices  < high_threshold: snap to nearest x9    (250  → 249,  not 259)
      Prices  < low_threshold:  no adjustment         (charm looks odd on small amounts)

    Always picks the NEAREST charm number — never silently rounds up and
    inflates the price past the uncharmed value.
    """
    if price < low_threshold:
        return price

    if price >= high_threshold:
        # Two candidates: (hundreds_floor - 1) and (hundreds_floor + 99)
        base = int(price / 100) * 100
        low  = base - 1        # e.g. 1799 for 1823
        high = base + 99       # e.g. 1899 for 1823
        return float(low) if abs(low - price) <= abs(high - price) else float(high)
    else:
        # Two candidates: (tens_floor - 1) and (tens_floor + 9)
        base = int(price / 10) * 10
        low  = base - 1        # e.g. 249 for 250
        high = base + 9        # e.g. 259 for 250
        # Don't go below 39 — charm below this looks odd
        if low < 39:
            return float(high)
        return float(low) if abs(low - price) <= abs(high - price) else float(high)


def _best_churn_for_retention(churn_lookup: dict[str, ChurnScore]) -> ChurnScore | None:
    """
    Return the most urgent at-risk customer for retention pricing.

    Priority:
      1. Highest churn_probability among HIGH-risk customers
      2. If no HIGH exists, highest churn_probability among MEDIUM-risk customers

    Returns None if the lookup is empty.
    """
    if not churn_lookup:
        return None

    high_scores = [s for s in churn_lookup.values() if s.risk_level == "HIGH"]
    if high_scores:
        return max(high_scores, key=lambda s: s.churn_probability)

    medium_scores = [s for s in churn_lookup.values() if s.risk_level == "MEDIUM"]
    if medium_scores:
        return max(medium_scores, key=lambda s: s.churn_probability)

    return None

def _build_discount_lookup(
    value_props: list | None,
) -> dict[tuple[str, str], float]:
    """
    Build {(tier_name, risk_level): discount_pct} lookup.

    Merge semantics — DB rows override hardcoded defaults for matching keys,
    but missing keys (e.g. no MEDIUM rows in the DB today) fall back to the
    hardcoded table. Mirrors RetentionAgent._build_discount_lookup so both
    agents use the same precedence logic.
    """
    merged = dict(_VP_DISCOUNTS)   # start with hardcoded safety net
    if value_props:
        for vp in value_props:
            if vp.discount_pct is not None:
                merged[(vp.tier_name, vp.risk_level)] = vp.discount_pct
    return merged