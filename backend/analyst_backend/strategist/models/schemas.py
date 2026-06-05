"""
models/schemas.py — Customer Retention Platform
================================================

All Pydantic models for both agents in one place.
Split into logical sections:

  Section 1  — Scout Agent input models (competitor price data)
  Section 2  — Analyst Agent churn input models
  Section 3  — Strategist Agent request / response models
  Section 4  — Shared config models (client guardrails, value propositions)
  Section 5  — Retention Agent request / response models
  Section 6  — Cross-agent models (CustomerPriceContext links both agents)

DB / JSON naming note:
  - Analyst DB.churn_scores uses    risk_tier   (VARCHAR 10 column name)
  - Analyst JSON uses               risk_level  (JSON key name)
  Both hold identical values: HIGH / MEDIUM / LOW.
  All Pydantic models here use risk_level to match the JSON shape.
  DB queries internally alias risk_tier → risk_level in SELECT statements.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ===========================================================================
# Section 1 — Scout Agent Input Models
# ===========================================================================

class ScoutPrice(BaseModel):
    """Single price point from a competitor listing."""
    value:    float
    currency: str           = "INR"
    raw:      Optional[str] = None  # e.g. "₹183" — stored for debugging

    @field_validator("value")
    @classmethod
    def must_be_non_negative(cls, v: float) -> float:
        """Price can be 0 (out-of-stock marker) but never negative."""
        if v < 0:
            raise ValueError("price.value cannot be negative")
        return v


class ScoutSource(BaseModel):
    """Metadata about how this listing was scraped."""
    type:       str   = "scraper"
    # confidence: 0.0 = completely uncertain, 1.0 = verified API data
    confidence: float = Field(default=0.85, ge=0.0, le=1.0)


class ScoutListing(BaseModel):
    """
    One competitor listing for a product on one platform.
    Shape matches Scout Agent /search/products response.
    """
    platform:     str
    price:        ScoutPrice
    url:          Optional[str] = None
    ingredients:  Optional[str] = None  # supplement-specific fields
    supplement:   Optional[str] = None
    nutrition:    Optional[str] = None
    manufacturer: Optional[str] = None
    marketed_by:  Optional[str] = None
    availability: str           = "in_stock"
    source:       ScoutSource   = Field(default_factory=ScoutSource)
    last_updated: Optional[datetime] = None

    @property
    def is_usable(self) -> bool:
        """A listing is usable for pricing only if in stock with a non-zero price."""
        return (
            self.availability == "in_stock"
            and self.price.value > 0
        )


class ScoutProduct(BaseModel):
    """One product with all its competitor listings."""
    name:     str
    listings: list[ScoutListing] = Field(default_factory=list)


class ScoutBulkResponse(BaseModel):
    """
    Full response from Scout Agent POST /search/products.
    Paste this directly into StrategistRequest.scout_output.
    """
    status:   str
    products: list[ScoutProduct] = Field(default_factory=list)


# ===========================================================================
# Section 2 — Analyst Agent Churn Input Models
# ===========================================================================

class ChurnScore(BaseModel):
    """
    One customer's churn prediction from the Analyst Agent.
    Matches the exact shape of churn_scores.json.
    """
    client_id:             str
    customer_id:           str
    churn_probability:     float = Field(ge=0.0, le=1.0)   # 0.0 = won't churn, 1.0 = will churn
    risk_level:            Literal["HIGH", "MEDIUM", "LOW"]
    customer_tier:         Literal["Platinum", "Gold", "Silver", "Bronze"]
    total_spend_usd:       float = 0.0     # lifetime spend
    total_orders:          int   = 0
    avg_order_value_usd:   float = 0.0
    avg_rating:            float = 0.0     # customer's average rating given
    days_since_last_order: int   = 0       # recency — key churn signal
    is_high_value:         int   = 0       # 0 or 1 flag from Analyst model
    rfm_total_score:       int   = 0       # RFM composite (higher = better customer)
    actual_churn_label:    Optional[int] = None  # 1 = churned (for model eval)

    @property
    def ltv_estimate_usd(self) -> float:
        """
        Simple LTV proxy when Analyst DB LTV is not available.
        Formula: avg_order_value × projected orders per year.
        """
        if self.total_orders > 0 and self.days_since_last_order > 0:
            orders_per_year = self.total_orders / max(self.days_since_last_order / 365, 0.1)
            return round(self.avg_order_value_usd * orders_per_year, 2)
        return self.total_spend_usd


class ChurnBatch(BaseModel):
    """Full Analyst Agent churn_scores.json payload."""
    generated_at:    Optional[datetime] = None
    total_customers: Optional[int]      = None
    scores:          list[ChurnScore]   = Field(default_factory=list)


# ===========================================================================
# Section 3 — Strategist Agent Models
# ===========================================================================

class StrategistRequest(BaseModel):
    """
    Input to POST /api/strategist/recommend.

    Minimum required:
      - scout_output: competitor prices from Scout Agent
      - our_costs: COGS per product (otherwise pricing is skipped with flag)

    Optional but recommended:
      - churn_batch: Analyst Agent churn scores for churn-price fusion
    """
    # Required: Scout Agent output
    scout_output: ScoutBulkResponse

    # COGS per product (INR): {"Organic India Ashwagandha 60 Capsules": 120.0}
    # Missing entries → strategy = "no_cost_data"
    our_costs: dict[str, float] = Field(default_factory=dict)

    # Optional: Analyst Agent churn batch for churn-signal fusion
    churn_batch: Optional[ChurnBatch] = None

    skip_churn: bool = False

    # Pricing configuration — can be overridden per-request
    target_margin_pct:   float = Field(default=20.0, ge=0, le=200,
        description="Desired profit margin % on true cost")
    min_margin_pct:      float = Field(default=8.0,  ge=0, le=200,
        description="Absolute floor — never sell below this margin")
    undercut_pct:        float = Field(default=2.0,  ge=0, le=50,
        description="% to undercut the cheapest competitor")
    overhead_multiplier: float = Field(default=1.15, ge=1.0, le=5.0,
        description="COGS × this = true cost (covers logistics, ops, tax)")
    min_confidence:      float = Field(default=0.5, ge=0.0, le=1.0,
        description="Ignore Scout listings below this confidence score")

    # Business context for strategy selection
    client_id:        str  # required — must be supplied by caller
    client_priority:  Optional[Literal["volume", "margin", "brand"]] = None
    customer_segment: Optional[Literal["budget", "mid", "premium"]]  = None

    # Market currency — filters competitor listings to this currency only.
    # If None, falls back to client_config.currency. Lets the UI override the
    # client's default per-request (e.g. switch between INR and USD market views).

    # Guardrails from Analyst DB.client_config (overridden at runtime)
    max_discount_pct:   float = Field(default=30.0, ge=0, le=100,
        description="Absolute max churn discount % (from client_config)")
    high_ltv_threshold: float = Field(default=500.0,
        description="USD spend above which customer is 'high LTV'")

    # Market currency selector — determines which competitor listings are
    # considered. If omitted, falls back to client_config.currency.
    # Listings in other currencies are filtered out (no cross-currency math).
    currency: Optional[str] = Field(default=None,
        description="ISO currency code. Only listings in this currency are used. "
                    "If None, uses client_config.currency.")

    @model_validator(mode="after")
    def min_lte_target(self) -> "StrategistRequest":
        """Sanity check: min margin cannot exceed target margin."""
        if self.min_margin_pct > self.target_margin_pct:
            raise ValueError(
                f"min_margin_pct ({self.min_margin_pct}) "
                f"cannot exceed target_margin_pct ({self.target_margin_pct})"
            )
        return self


class PlatformPrice(BaseModel):
    """Competitor price summary for one platform — included in recommendations."""
    platform:     str
    price:        float
    availability: str   = "in_stock"
    confidence:   float = 0.85
    url:          Optional[str] = None


class MarginBand(BaseModel):
    """Margin % at each key price point — for financial analysis."""
    at_floor:     float  # margin if we sell at floor price
    at_target:    float  # margin if we sell at target price
    at_suggested: float  # margin at final suggested price
    at_comp_min:  float  # margin if we match cheapest competitor


class ChurnContext(BaseModel):
    """Churn fusion metadata attached to a recommendation."""
    customer_id:       str
    churn_probability: float
    risk_level:        str
    customer_tier:     str
    discount_applied:  float = 0.0    # % discount given
    discount_reason:   str  = ""      # explanation for auditors


class PricingRecommendation(BaseModel):
    """
    Full pricing recommendation for one product.
    Returned by Strategist Agent and persisted to pricing_recommendations table.
    """
    product_name:        str

    # Core price outputs
    suggested_price:     float    # final price (after churn discount + charm pricing)
    pre_retention_price: float    # price before churn discount (0.0 if no churn discount)
    floor_price:         float    # minimum viable price (never go below this)
    target_price:        float    # ideal price given margin targets

    # Cost breakdown
    our_cost:   float    # COGS × overhead_multiplier
    raw_cogs:   float    # raw COGS as provided

    # Competitor market data
    competitor_min:    float
    competitor_avg:    float
    competitor_max:    float
    competitor_median: float
    platform_breakdown: list[PlatformPrice]

    # Market analysis
    market_trend:   str    = "stable"   # "rising" | "falling" | "stable"
    margin_percent: float  = 0.0        # margin at suggested price
    margin_band:    MarginBand

    # Strategy metadata
    strategy:   str    # undercut | match | premium | floor_only | retention | no_data
    confidence: str    # high | medium | low
    reasoning:  str    # detailed explanation for strategists
    client_note: str   # short note for internal teams
    customer_note: str # customer-facing price justification

    # Flags and warnings
    flag:     Optional[str]  = None   # low_margin_warning | no_price_data | etc.
    warnings: list[str]      = Field(default_factory=list)

    # Churn fusion data (populated when strategy == "retention")
    churn_context: Optional[ChurnContext] = None


class StrategistResponse(BaseModel):
    recommendations: list[PricingRecommendation]
    total_products:  int
    flagged_count:   int
    strategies_used: list[str]
    avg_margin_pct:  float
    retention_count: int
    run_id:          str
    client_id:       Optional[str] = None
    status:          str           = "ok"
    elapsed_seconds: float         = 0.0


# ---------------------------------------------------------------------------
# Cost tracking (LangFuse)
# ---------------------------------------------------------------------------

class AgentCostBreakdown(BaseModel):
    """Per-model LLM cost for one run (from LangFuse)."""
    model:       str
    input_tokens:  int   = 0
    output_tokens: int   = 0
    cost_usd:    float   = 0.0


class CostSummaryResponse(BaseModel):
    """Response from GET /api/strategist/costs."""
    total_runs:        int
    total_cost_usd:    float
    avg_cost_per_run:  float
    by_model:          list[AgentCostBreakdown]


# ===========================================================================
# Section 4 — Shared Config Models
# ===========================================================================

class ClientConfig(BaseModel):
    """
    Client-level guardrails loaded from Analyst DB.client_config.
    These values are authoritative — they override any request-level defaults.
    """
    client_id:          str
    client_name:        str   = ""
    currency:           str   = "USD"
    max_discount_pct:   float = 30.0   # hard cap on any churn discount
    high_ltv_threshold: float = 500.0  # USD — above this = escalate to human
    mid_ltv_threshold:  float = 250.0  # USD — above this = heavier discount
    churn_window_days:  int   = 90     # how far back churn scores look


class ValueProposition(BaseModel):
    """
    Discount rule per (tier, risk_level) from value_propositions table.
    If this table is empty, agents fall back to the hardcoded _VP_DISCOUNTS dict.
    """
    tier_name:        str
    risk_level:       str
    action_type:      Optional[str]  = None   # discount | re_engagement | escalate
    message_template: Optional[str]  = None   # optional message template
    discount_pct:     float          = 0.0
    channel:          Optional[str]  = None   # email | sms | push
    priority:         int            = 5       # lower = higher priority


# ===========================================================================
# Section 5 — Retention Agent Models
# ===========================================================================

class RetentionIntervention(BaseModel):
    """
    One personalised retention action for one at-risk customer.
    Persisted to retention_interventions table in Analyst DB.
    """
    intervention_id:      Optional[int]   = None   # set after DB insert
    client_id:            str
    customer_id:          str
    created_at:           datetime        = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Churn signal from Analyst
    churn_probability:    float
    risk_tier:            str             # HIGH | MEDIUM

    # Offer details
    offer_type:           str             # retention_discount_Xpct | re_engagement | strategist_retention_price
    discount_pct:         float           = 0.0
    offer_message:        str             # personalised message to send

    # Delivery
    channel:              str             = "email"  # email | sms | push

    # Guardrail tracking (for audit)
    customer_ltv_usd:     float           = 0.0
    avg_order_value_usd:  float           = 0.0   
    max_allowed_discount: float           = 30.0
    guardrail_passed:     bool            = True   # False if discount was capped

    # Escalation flag — set True for Platinum/Gold with churn_prob >= 0.90
    escalated_to_human:   bool            = False

    # CRM outcome (updated by PATCH endpoint after offer is sent)
    offer_status:         str             = "pending"   # pending | accepted | declined | no_response | bounced
    revenue_recovered:    Optional[float] = None        # set when status = accepted

    # Tracing
    langfuse_trace_id:    Optional[str]   = None
    agent_cost_usd:       Optional[float] = None


class RetentionBatch(BaseModel):
    """Internal result returned by RetentionAgent.run() before API serialisation."""
    run_id:          str
    client_id:       str
    generated_at:    datetime
    total_processed: int                    # all scores evaluated (incl. LOW skipped)
    interventions:   list[RetentionIntervention]
    summary:         dict                   # totals + by_channel + discount_exposure


class RetentionRequest(BaseModel):
    """
    POST /api/retention/run request body.

    churn_batch  — paste Analyst Agent churn_scores.json directly, OR omit
                   to pull from Analyst DB automatically.
    client_id    — required. Must match a client_id in client_config table.
    dry_run      — build offers but skip DB write (safe for testing).
    min_risk     — floor for risk tier processed (HIGH = skip MEDIUM customers).
    """
    churn_batch:            Optional[ChurnBatch] = None
    client_id:              str  # required — must be supplied by caller
    dry_run:                bool = False
    min_risk:               Literal["HIGH", "MEDIUM"] = "MEDIUM"   # LOW always skipped
    min_probability_medium: float = Field(default=0.40, ge=0.0, le=1.0,
        description="Skip MEDIUM risk customers below this churn probability (default 0.40)")


class RetentionResponse(BaseModel):
    """Response from POST /api/retention/run."""
    run_id:        str
    client_id:     str
    generated_at:  datetime
    dry_run:       bool
    summary:       dict
    interventions: list[RetentionIntervention]


class OutcomeUpdate(BaseModel):
    """
    PATCH /api/retention/{intervention_id}/outcome request body.
    Called by CRM/marketing automation when customer responds.
    """
    intervention_id:   int
    offer_status:      Literal["accepted", "declined", "no_response", "bounced"]
    revenue_recovered: Optional[float] = None   # fill when status = accepted


# ===========================================================================
# Section 6 — Cross-Agent: CustomerPriceContext
# ===========================================================================

class CustomerPriceContext(BaseModel):
    """
    Written by Strategist Agent, read by Retention Agent.

    Purpose: when the Strategist applies a retention-discount price for a
    HIGH-risk customer, that discount is stored here. The Retention Agent
    checks this table BEFORE issuing its own discount — if a strategist
    retention price already exists, the Retention Agent skips its price
    step and only sends the message + channel. This prevents double-discounting.

    Persisted to: customer_price_context table (Scout/Strategist DB)
    """
    customer_id:          str
    product_name:         str
    strategy:             str           # "retention" | "undercut" | "match" | "premium" | etc.
    suggested_price:      float         # final price after all adjustments
    pre_retention_price:  Optional[float] = None   # price BEFORE churn discount (audit trail)
    discount_pct_applied: Optional[float] = None   # % discount given
    churn_probability:    Optional[float] = None
    risk_tier:            Optional[str]   = None
    run_id:               Optional[str]   = None
    created_at:           Optional[datetime] = None