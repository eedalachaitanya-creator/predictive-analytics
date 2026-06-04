"""
agents/retention_agent.py — Customer Retention Platform
========================================================

Retention Pipeline
------------------

Step 1 — Sync guardrails from client_config
           (max_discount_pct, LTV thresholds)

Step 2 — Build discount lookup from value_propositions table
           (falls back to hardcoded _VP_DISCOUNTS if table is empty)

Step 3 — For each at-risk customer (HIGH or MEDIUM risk, never LOW):

  a. Check Strategist DB (customer_price_context):
       → If Strategist already applied a retention_price for this customer,
         SKIP the discount step entirely (double-discount prevention).
         Retention Agent only sends a message referencing the Strategist price.

  b. If no Strategist price exists:
       → Look up discount % for (tier, risk_level) pair
       → Cap at client_config.max_discount_pct
       → Determine offer_type based on result

  c. Route to the right channel:
       Platinum/Gold → email
       Silver        → sms
       Bronze        → push notification

  d. Check LTV and escalation:
       churn_prob >= 0.90 AND (Platinum|Gold OR is_high_value)
       → escalated_to_human = True (human outreach queue)

  e. Craft personalised message for the channel

Step 4 — Emit LangFuse trace per customer (best-effort)

Step 5 — Return RetentionBatch with all interventions

Guardrails (never violated):
  - Never discount more than client_config.max_discount_pct (e.g. 30%)
  - Never double-discount (check customer_price_context first)
  - Bronze + MEDIUM → re-engagement only, no discount
  - MEDIUM scores below min_probability_medium floor are skipped entirely
  - LOW risk customers are always skipped
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from strategist.models.schemas import (
    ChurnScore,
    ClientConfig,
    CustomerPriceContext,
    RetentionBatch,
    RetentionIntervention,
    ValueProposition,
)
from strategist.services.langfuse_service import get_langfuse_safe


# ---------------------------------------------------------------------------
# Fallback discount table (mirrors value_propositions seed / strategist agent)
# Used when Analyst DB value_propositions table is empty or unreachable.
# ---------------------------------------------------------------------------
_VP_DISCOUNTS: dict[tuple[str, str], float] = {
    ("Platinum", "HIGH"):   20.0,
    ("Platinum", "MEDIUM"): 10.0,
    ("Gold",     "HIGH"):   15.0,
    ("Gold",     "MEDIUM"):  8.0,
    ("Silver",   "HIGH"):   10.0,
    ("Silver",   "MEDIUM"):  5.0,
    ("Bronze",   "HIGH"):    5.0,
    ("Bronze",   "MEDIUM"):  0.0,   # Bronze + MEDIUM → re-engagement only
}

# Channel routing — hardcoded fallback. Used ONLY when a (tier, risk) row in
# value_propositions has no `channel` column value. Otherwise the DB wins.
# Change the DB, not this dict, to customize channels per client.
_CHANNEL_ROUTING: dict[str, str] = {
    "Platinum": "email",
    "Gold":     "email",
    "Silver":   "email",
    "Bronze":   "email",
}

# Human escalation thresholds
ESCALATION_THRESHOLD = 0.90   # churn_probability must be at or above this
ESCALATION_TIERS     = {"Platinum", "Gold"}   # only escalate valuable customers


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------
@dataclass
class RetentionConfig:
    """
    Runtime config for the Retention Agent.
    Values from client_config override these defaults at run time.
    """
    max_discount_pct:       float = 30.0    # hard cap — never exceed this
    high_ltv_threshold:     float = 500.0   # USD — above this = escalate
    mid_ltv_threshold:      float = 250.0   # USD — above this = heavier discount
    min_probability_medium: float = 0.40    # skip MEDIUM scores below this
    escalation_threshold:   float = ESCALATION_THRESHOLD
    dry_run:                bool  = False   # if True, skip DB write


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
class RetentionAgent:
    """
    Stateless retention offer generator. One instance per request.
    Receives data from both Analyst Agent (churn scores) and Strategist Agent
    (customer_price_context) and generates personalised retention interventions.
    """

    def __init__(self, config: RetentionConfig | None = None):
        self.config = config or RetentionConfig()
        self.lf     = get_langfuse_safe()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        churn_scores:   list[ChurnScore],
        client_config:  ClientConfig,
        value_props:    list[ValueProposition] | None = None,
        price_contexts: dict[str, CustomerPriceContext] | None = None,
    ) -> RetentionBatch:
        """
        Process a batch of churn scores and generate retention interventions.

        Args:
            churn_scores:    HIGH+MEDIUM risk customers from Analyst Agent.
            client_config:   Guardrail config from Analyst DB.
            value_props:     Optional discount table from value_propositions.
                             Falls back to _VP_DISCOUNTS if None/empty.
            price_contexts:  {customer_id: CustomerPriceContext} from Strategist DB.
                             Customers with a 'retention' strategy here skip discounting.

        Returns:
            RetentionBatch containing all interventions and a run summary.
        """
        run_id = str(uuid.uuid4())
        t0     = time.perf_counter()

        # Sync guardrails from DB (DB values are authoritative)
        self.config.max_discount_pct   = client_config.max_discount_pct
        self.config.high_ltv_threshold = client_config.high_ltv_threshold
        self.config.mid_ltv_threshold  = client_config.mid_ltv_threshold

        # Build lookup tables (DB rows beat hardcoded fallbacks)
        discount_lookup = self._build_discount_lookup(value_props)
        channel_lookup  = self._build_channel_lookup(value_props)

        price_contexts = price_contexts or {}

        # Start LangFuse trace for this batch run
        trace = self._lf_trace(run_id, client_config.client_id, len(churn_scores))

        interventions: list[RetentionIntervention] = []

        for score in churn_scores:
            # LOW risk: no action needed — the customer is unlikely to churn
            if score.risk_level == "LOW":
                continue

            # MEDIUM risk below floor: borderline signal, not worth acting on
            if (score.risk_level == "MEDIUM"
                    and score.churn_probability < self.config.min_probability_medium):
                continue

            intervention = self._process_customer(
                score           = score,
                discount_lookup = discount_lookup,
                channel_lookup  = channel_lookup,
                price_ctx       = price_contexts.get(score.customer_id),
                trace           = trace,
            )
            interventions.append(intervention)

        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        self._lf_close(trace, interventions, latency_ms)

        return RetentionBatch(
            run_id          = run_id,
            client_id       = client_config.client_id,
            generated_at    = datetime.now(timezone.utc),
            total_processed = len(churn_scores),
            interventions   = interventions,
            summary         = self._summarise(interventions),
        )

    # ------------------------------------------------------------------
    # Per-customer logic
    # ------------------------------------------------------------------

    def _process_customer(
        self,
        score:           ChurnScore,
        discount_lookup: dict[tuple[str, str], float],
        channel_lookup:  dict[tuple[str, str], str],
        price_ctx:       CustomerPriceContext | None,
        trace,
    ) -> RetentionIntervention:
        """
        Generate one retention intervention for one at-risk customer.

        Core decision: did the Strategist already apply a retention price?
          YES → send message referencing that price, no additional discount
          NO  → calculate discount from value_propositions table
        """
        span = self._lf_span(trace, score.customer_id, score.risk_level)

        tier       = score.customer_tier
        risk       = score.risk_level
        churn_prob = score.churn_probability

        # ── Double-discount prevention ──────────────────────────────────────
        # Check if the Strategist Agent already issued a retention price.
        # strategy='retention' in customer_price_context = Strategist discounted.
        strategist_already_discounted = (
            price_ctx is not None
            and price_ctx.strategy == "retention"
            and price_ctx.pre_retention_price is not None
        )

        if strategist_already_discounted:
            # Strategist already handled the price — only send message + route channel
            discount_pct     = 0.0
            offer_type       = "strategist_retention_price"
            guardrail_passed = True   # no discount to check against guardrail
        else:
            # Calculate our own discount from the lookup table
            raw_discount     = discount_lookup.get((tier, risk), 0.0)
            # Apply guardrail: cap at max_discount_pct from client_config
            discount_pct     = min(raw_discount, self.config.max_discount_pct)
            guardrail_passed = raw_discount <= self.config.max_discount_pct
            offer_type       = self._offer_type(tier, risk, discount_pct)

        # ── Channel routing ─────────────────────────────────────────────────
        # DB-first: if value_propositions specifies a channel for this (tier, risk),
        # use it. Otherwise fall back to the hardcoded tier-based routing.
        channel = channel_lookup.get((tier, risk)) or _CHANNEL_ROUTING.get(tier, "email")

        # ── LTV and high-value flag ─────────────────────────────────────────
        ltv_usd       = score.total_spend_usd
        is_high_value = ltv_usd >= self.config.high_ltv_threshold or bool(score.is_high_value)

        # ── Escalation check ────────────────────────────────────────────────
        # Escalate when churn_prob is very high AND the customer is valuable
        escalate = (
            churn_prob >= self.config.escalation_threshold
            and (tier in ESCALATION_TIERS or is_high_value)
        )

        # ── Personalised message ────────────────────────────────────────────
        offer_message = self._craft_message(
            score                      = score,
            discount_pct               = discount_pct,
            offer_type                 = offer_type,
            channel                    = channel,
            strategist_already_handled = strategist_already_discounted,
            price_ctx                  = price_ctx,
        )

        intervention = RetentionIntervention(
            client_id            = score.client_id,
            customer_id          = score.customer_id,
            churn_probability    = churn_prob,
            risk_tier            = risk,
            offer_type           = offer_type,
            discount_pct         = discount_pct,
            offer_message        = offer_message,
            channel              = channel,
            customer_ltv_usd     = ltv_usd,
            max_allowed_discount = self.config.max_discount_pct,
            guardrail_passed     = guardrail_passed,
            escalated_to_human   = escalate,
            offer_status         = "pending",
            avg_order_value_usd  = score.avg_order_value_usd,
        )

        self._lf_end_span(span, intervention)
        return intervention

    # ------------------------------------------------------------------
    # Message crafting
    # ------------------------------------------------------------------

    def _craft_message(
        self,
        score:                      ChurnScore,
        discount_pct:               float,
        offer_type:                 str,
        channel:                    str,
        strategist_already_handled: bool,
        price_ctx:                  CustomerPriceContext | None,
    ) -> str:
        """
        Craft a personalised retention message.

        Message varies based on:
          - Whether Strategist already issued a retention price
          - Risk level (HIGH vs MEDIUM)
          - Tier (Platinum/Gold get warmer language)
          - Discount amount (or re-engagement if no discount)
          - Days since last order (urgency signal)
        """
        tier = score.customer_tier
        risk = score.risk_level
        days = score.days_since_last_order
        # Customer name is not in ChurnScore schema — use a friendly fallback.
        # TODO: join against customer_master to get real name.
        name = f"valued {tier} member"

        # Case 1: Strategist already applied a retention price
        # → reference that special price, don't mention our own discount
        if strategist_already_handled and price_ctx:
            return (
                f"Hi {name}, we've been thinking about you. "
                f"Your personalised price is waiting — we've set aside a special rate "
                f"just for you as a valued {tier} member. "
                f"It's been {days} days since your last order. "
                f"Come back and see what we have for you today."
            )

        # Case 2: Re-engagement only (no discount — e.g. Bronze + MEDIUM)
        if offer_type == "re_engagement":
            return (
                f"Hi {name}, we miss you! It's been {days} days since your last visit. "
                f"As a {tier} member you have exclusive access to new arrivals this week. "
                f"No strings attached — just great products curated for you."
            )

        # Case 3: HIGH-risk customers with a discount
        if risk == "HIGH":
            # Platinum/Gold get more personal, empathetic language
            urgency = "We'd hate to lose you."
            if tier in ("Platinum", "Gold"):
                urgency = "As one of our most valued customers, your satisfaction matters deeply to us."
            return (
                f"Hi {name}, {urgency} "
                f"It's been {days} days and we want you back. "
                f"We're offering you an exclusive {discount_pct:.0f}% discount on your next order — "
                f"our way of saying thank you for being a {tier} member. "
                f"This offer is reserved for you and expires in 7 days."
            )

        # Case 4: MEDIUM-risk customers with a small discount
        if discount_pct > 0:
            return (
                f"Hi {name}, we noticed you haven't shopped with us in a while. "
                f"Here's a {discount_pct:.0f}% discount as a thank-you for being a {tier} member. "
                f"Come back and explore what's new for you."
            )
        else:
            # MEDIUM + no discount (shouldn't normally reach here given our discount table)
            return (
                f"Hi {name}, we've been thinking about you! "
                f"Your {tier} benefits are still active and waiting. "
                f"Discover this week's picks tailored just for you."
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _offer_type(tier: str, risk: str, discount_pct: float) -> str:
        """
        Derive a machine-readable offer type string.
        Used for CRM segmentation and reporting.
        """
        if discount_pct == 0:
            return "re_engagement"
        if risk == "HIGH":
            return f"retention_discount_{int(discount_pct)}pct"
        return f"winback_discount_{int(discount_pct)}pct"

    @staticmethod
    def _build_discount_lookup(
        value_props: list[ValueProposition] | None,
    ) -> dict[tuple[str, str], float]:
        """
        Build {(tier_name, risk_level): discount_pct} lookup.

        Merge semantics — DB rows override hardcoded defaults for matching keys,
        but any (tier, risk) combo that doesn't appear in the DB falls back to
        the hardcoded table. This matters because the Analyst DB's
        value_propositions table today only covers (At-Risk/Reactivated/New),
        which map to (HIGH/LOW/LOW) — there are no MEDIUM rows. Without this
        merge, MEDIUM-risk customers would silently get 0% discount.
        """
        merged = dict(_VP_DISCOUNTS)   # start with hardcoded safety net
        if value_props:
            for vp in value_props:
                if vp.discount_pct is not None:
                    merged[(vp.tier_name, vp.risk_level)] = vp.discount_pct
        return merged

    @staticmethod
    def _build_channel_lookup(
        value_props: list[ValueProposition] | None,
    ) -> dict[tuple[str, str], str]:
        """
        Build {(tier_name, risk_level): channel} lookup.
        DB rows (value_propositions.channel) take precedence. Rows with null channel
        are skipped — the caller falls back to the tier-based _CHANNEL_ROUTING dict.
        """
        if not value_props:
            return {}
        return {
            (vp.tier_name, vp.risk_level): vp.channel
            for vp in value_props
            if vp.channel is not None and vp.channel.strip()
        }

    @staticmethod
    def _summarise(interventions: list[RetentionIntervention]) -> dict:
        """Build a summary dict for the RetentionBatch response."""
        total     = len(interventions)
        high      = sum(1 for i in interventions if i.risk_tier == "HIGH")
        medium    = sum(1 for i in interventions if i.risk_tier == "MEDIUM")
        escalated = sum(1 for i in interventions if i.escalated_to_human)
        discounted= sum(1 for i in interventions if i.discount_pct > 0)

        # Discount exposure = revenue given up on ONE order per customer
        total_discount_exposure = sum(
            i.avg_order_value_usd * i.discount_pct / 100
            for i in interventions
            if i.discount_pct > 0
        )

        # Channel breakdown for CRM routing
        by_channel: dict[str, int] = {}
        for i in interventions:
            by_channel[i.channel] = by_channel.get(i.channel, 0) + 1

        return {
            "total_interventions":   total,
            "high_risk":             high,
            "medium_risk":           medium,
            "escalated_to_human":    escalated,
            "with_discount":         discounted,
            "discount_exposure_usd": round(total_discount_exposure, 2),
            "by_channel":            by_channel,
        }

    # ------------------------------------------------------------------
    # LangFuse helpers (all best-effort — never crash the agent)
    # ------------------------------------------------------------------

    def _lf_trace(self, run_id: str, client_id: str, count: int):
        try:
            if not self.lf: return None
            return self.lf.trace(
                id=run_id, name="retention_agent",
                input={"client_id": client_id, "total_customers": count},
            )
        except Exception:
            return None

    def _lf_span(self, trace, customer_id: str, risk: str):
        try:
            return (
                trace.span(name=f"customer:{customer_id}", input={"risk": risk})
                if trace else None
            )
        except Exception:
            return None

    def _lf_end_span(self, span, intervention: RetentionIntervention):
        try:
            if span:
                span.end(output={
                    "offer_type": intervention.offer_type,
                    "discount":   intervention.discount_pct,
                    "channel":    intervention.channel,
                    "escalated":  intervention.escalated_to_human,
                }, usage={"total_cost": 0.0})
        except Exception:
            pass

    def _lf_close(self, trace, interventions: list, latency_ms: float):
        try:
            if not trace: return
            escalated = sum(1 for i in interventions if i.escalated_to_human)
            trace.score(
                name="escalation_rate",
                value=escalated / max(len(interventions), 1),
                comment=f"{escalated}/{len(interventions)} escalated to human",
            )
            trace.update(output={
                "total":      len(interventions),
                "escalated":  escalated,
                "latency_ms": latency_ms,
            })
        except Exception:
            pass