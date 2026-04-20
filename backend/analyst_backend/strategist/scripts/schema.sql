-- =============================================================================
-- Customer Retention Platform — Database Schema
-- =============================================================================
-- Run this on BOTH databases (Scout DB and Analyst DB) unless they share
-- a single Postgres instance, in which case run once.
--
-- Execution order matters — foreign key references must exist first.
-- This file is idempotent: safe to re-run (uses CREATE TABLE IF NOT EXISTS).
--
-- Tables:
--   Scout/Strategist DB:
--     entity_listings         — competitor price listings (written by Scout Agent)
--     price_history           — historical price snapshots (written by Scout Agent)
--     pricing_recommendations — Strategist Agent output
--     customer_price_context  — cross-agent bridge (Strategist → Retention)
--
--   Analyst DB:
--     customers               — customer master data
--     customer_rfm_features   — RFM signals (written by Analyst Agent)
--     churn_scores            — churn predictions (written by Analyst Agent)
--     client_config           — guardrail configuration per client
--     value_propositions      — discount rules per tier+risk
--     retention_interventions — Retention Agent output
-- =============================================================================


-- =============================================================================
-- SCOUT / STRATEGIST DB TABLES
-- =============================================================================

-- Competitor price listings (Scout Agent writes these)
CREATE TABLE IF NOT EXISTS entity_listings (
    listing_id      BIGSERIAL PRIMARY KEY,
    product_name    TEXT          NOT NULL,
    platform        TEXT          NOT NULL,
    price           NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    currency        VARCHAR(3)    NOT NULL DEFAULT 'INR',
    availability    VARCHAR(20)   NOT NULL DEFAULT 'in_stock',
    confidence      NUMERIC(4,3)  NOT NULL DEFAULT 0.85,
    url             TEXT,
    scraped_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT entity_listings_confidence_range CHECK (confidence BETWEEN 0 AND 1)
);

-- Index for the most common query: latest prices for a product
CREATE INDEX IF NOT EXISTS idx_entity_listings_product_scraped
    ON entity_listings (product_name, scraped_at DESC);


-- Historical price snapshots for market trend calculation
CREATE TABLE IF NOT EXISTS price_history (
    id              BIGSERIAL PRIMARY KEY,
    product_name    TEXT          NOT NULL,
    platform        TEXT          NOT NULL,
    price           NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    currency        VARCHAR(3)    NOT NULL DEFAULT 'INR',
    recorded_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Index for the 14-day vs 30-day trend query in PriceHistoryRepo
CREATE INDEX IF NOT EXISTS idx_price_history_product_recorded
    ON price_history (product_name, recorded_at DESC);


-- Strategist Agent pricing recommendations output
CREATE TABLE IF NOT EXISTS pricing_recommendations (
    recommendation_id    BIGSERIAL PRIMARY KEY,
    run_id               UUID          NOT NULL,
    client_id            VARCHAR(50)   NOT NULL,
    product_name         TEXT          NOT NULL,
    suggested_price      NUMERIC(12,2),
    pre_retention_price  NUMERIC(12,2) DEFAULT 0,    -- original price before churn discount
    floor_price          NUMERIC(12,2),
    target_price         NUMERIC(12,2),
    our_cost             NUMERIC(12,2),
    raw_cogs             NUMERIC(12,2),
    competitor_min       NUMERIC(12,2),
    competitor_avg       NUMERIC(12,2),
    competitor_max       NUMERIC(12,2),
    competitor_median    NUMERIC(12,2),
    strategy             VARCHAR(30),                -- undercut|match|premium|floor_only|retention|no_data
    confidence           VARCHAR(10),                -- high|medium|low
    margin_percent       NUMERIC(6,2),
    market_trend         VARCHAR(10)   DEFAULT 'stable',
    flag                 VARCHAR(50),
    reasoning            TEXT,
    platform_breakdown   JSONB,                      -- list of {platform, price, ...}
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pricing_recommendations_run
    ON pricing_recommendations (run_id, client_id);

CREATE INDEX IF NOT EXISTS idx_pricing_recommendations_product
    ON pricing_recommendations (product_name, created_at DESC);


-- Cross-agent bridge: Strategist writes, Retention reads
-- Prevents the Retention Agent from double-discounting customers
-- that the Strategist already discounted via churn-signal fusion.
CREATE TABLE IF NOT EXISTS customer_price_context (
    context_id           BIGSERIAL PRIMARY KEY,
    customer_id          VARCHAR(100)  NOT NULL,
    client_id            VARCHAR(50)   NOT NULL,
    product_name         TEXT          NOT NULL,
    strategy             VARCHAR(30)   NOT NULL,     -- 'retention' is the signal for Retention Agent
    suggested_price      NUMERIC(12,2) NOT NULL,
    pre_retention_price  NUMERIC(12,2),              -- price before churn discount
    discount_pct_applied NUMERIC(6,2),              -- % discount given
    churn_probability    NUMERIC(5,4),
    risk_tier            VARCHAR(10),
    run_id               UUID,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    -- One active retention price per customer per product
    CONSTRAINT uq_customer_price_context
        UNIQUE (customer_id, product_name)
);

CREATE INDEX IF NOT EXISTS idx_customer_price_context_lookup
    ON customer_price_context (client_id, customer_id, strategy, created_at DESC);


-- =============================================================================
-- ANALYST DB TABLES
-- =============================================================================

-- Customer master data
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(100)  NOT NULL,
    client_id       VARCHAR(50)   NOT NULL,
    tier_name       VARCHAR(20)   NOT NULL DEFAULT 'Bronze',  -- Platinum|Gold|Silver|Bronze
    email           TEXT,
    phone           TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    PRIMARY KEY (customer_id, client_id),
    CONSTRAINT customers_tier_check
        CHECK (tier_name IN ('Platinum', 'Gold', 'Silver', 'Bronze'))
);


-- RFM features computed by Analyst Agent
CREATE TABLE IF NOT EXISTS customer_rfm_features (
    feature_id             BIGSERIAL PRIMARY KEY,
    customer_id            VARCHAR(100)  NOT NULL,
    client_id              VARCHAR(50)   NOT NULL,
    total_spend_usd        NUMERIC(12,2) DEFAULT 0,
    total_orders           INT           DEFAULT 0,
    avg_order_value_usd    NUMERIC(10,2) DEFAULT 0,
    avg_rating             NUMERIC(4,2)  DEFAULT 0,
    days_since_last_order  INT           DEFAULT 0,
    is_high_value          SMALLINT      DEFAULT 0 CHECK (is_high_value IN (0, 1)),
    rfm_total_score        INT           DEFAULT 0,
    computed_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customer_rfm_lookup
    ON customer_rfm_features (customer_id, client_id, computed_at DESC);


-- Churn predictions from Analyst Agent (ML model output)
CREATE TABLE IF NOT EXISTS churn_scores (
    score_id            BIGSERIAL PRIMARY KEY,
    client_id           VARCHAR(50)  NOT NULL,
    customer_id         VARCHAR(100) NOT NULL,
    churn_probability   NUMERIC(5,4) NOT NULL CHECK (churn_probability BETWEEN 0 AND 1),
    risk_tier           VARCHAR(10)  NOT NULL CHECK (risk_tier IN ('HIGH', 'MEDIUM', 'LOW')),
    model_version       VARCHAR(50),
    scored_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actual_churn_label  SMALLINT     CHECK (actual_churn_label IN (0, 1))   -- filled post-hoc
);

-- This index is used by the DISTINCT ON query in ChurnScoresRepo.get_at_risk()
CREATE INDEX IF NOT EXISTS idx_churn_scores_at_risk
    ON churn_scores (client_id, risk_tier, customer_id, scored_at DESC);


-- Client-level guardrail configuration
CREATE TABLE IF NOT EXISTS client_config (
    client_id           VARCHAR(50)   PRIMARY KEY,
    client_name         TEXT          NOT NULL,
    currency            VARCHAR(3)    NOT NULL DEFAULT 'USD',
    max_discount_pct    NUMERIC(5,2)  NOT NULL DEFAULT 30.0,  -- absolute cap on any discount
    high_ltv_threshold  NUMERIC(10,2) NOT NULL DEFAULT 500.0,
    mid_ltv_threshold   NUMERIC(10,2) NOT NULL DEFAULT 250.0,
    churn_window_days   INT           NOT NULL DEFAULT 90,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);


-- Discount rules per (tier, risk_level) — editable via admin UI
-- When empty, both agents fall back to the hardcoded _VP_DISCOUNTS dict.
CREATE TABLE IF NOT EXISTS value_propositions (
    vp_id             BIGSERIAL PRIMARY KEY,
    tier_name         VARCHAR(20)  NOT NULL,
    risk_level        VARCHAR(10)  NOT NULL,
    action_type       VARCHAR(30),               -- discount|re_engagement|escalate
    message_template  TEXT,
    discount_pct      NUMERIC(5,2) NOT NULL DEFAULT 0.0,
    channel           VARCHAR(10),               -- email|sms|push
    priority          INT          NOT NULL DEFAULT 5,

    CONSTRAINT uq_vp_tier_risk UNIQUE (tier_name, risk_level),
    CONSTRAINT vp_tier_check CHECK (tier_name IN ('Platinum', 'Gold', 'Silver', 'Bronze')),
    CONSTRAINT vp_risk_check  CHECK (risk_level IN ('HIGH', 'MEDIUM', 'LOW'))
);


-- Retention Agent interventions (one row per customer per run)
CREATE TABLE IF NOT EXISTS retention_interventions (
    intervention_id      BIGSERIAL PRIMARY KEY,
    client_id            VARCHAR(50)   NOT NULL,
    customer_id          VARCHAR(100)  NOT NULL,
    churn_probability    NUMERIC(5,4)  NOT NULL,
    risk_tier            VARCHAR(10)   NOT NULL,
    offer_type           VARCHAR(50)   NOT NULL,    -- retention_discount_Xpct|re_engagement|...
    discount_pct         NUMERIC(6,2)  NOT NULL DEFAULT 0,
    offer_message        TEXT          NOT NULL,
    channel              VARCHAR(10)   NOT NULL,    -- email|sms|push
    customer_ltv_usd     NUMERIC(12,2) DEFAULT 0,
    max_allowed_discount NUMERIC(6,2)  DEFAULT 30,
    guardrail_passed     BOOLEAN       NOT NULL DEFAULT TRUE,
    escalated_to_human   BOOLEAN       NOT NULL DEFAULT FALSE,
    offer_status         VARCHAR(20)   NOT NULL DEFAULT 'pending',
    revenue_recovered    NUMERIC(12,2),             -- set when offer_status=accepted
    outcome_recorded_at  TIMESTAMPTZ,               -- set when outcome is recorded
    langfuse_trace_id    TEXT,                      -- run_id for cross-agent tracing
    agent_cost_usd       NUMERIC(10,6),
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT ri_offer_status_check
        CHECK (offer_status IN ('pending', 'accepted', 'declined', 'no_response', 'bounced')),
    CONSTRAINT ri_risk_tier_check
        CHECK (risk_tier IN ('HIGH', 'MEDIUM'))
);

CREATE INDEX IF NOT EXISTS idx_ri_client_status
    ON retention_interventions (client_id, offer_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ri_escalations
    ON retention_interventions (client_id, escalated_to_human, offer_status)
    WHERE escalated_to_human = TRUE;


-- =============================================================================
-- SEED DATA
-- =============================================================================

-- Default client config (Walmart-like client for development)
INSERT INTO client_config (
    client_id, client_name, currency,
    max_discount_pct, high_ltv_threshold, mid_ltv_threshold, churn_window_days
) VALUES (
    'CLT-001', 'Default Client', 'USD',
    30.0, 500.0, 250.0, 90
) ON CONFLICT (client_id) DO NOTHING;


-- Default value propositions (mirrors _VP_DISCOUNTS in both agents)
-- These are used when the agents query the DB for discount rules.
INSERT INTO value_propositions
    (tier_name, risk_level, action_type, discount_pct, channel, priority)
VALUES
    ('Platinum', 'HIGH',   'discount',      20.0, 'email', 1),
    ('Platinum', 'MEDIUM', 'discount',      10.0, 'email', 2),
    ('Gold',     'HIGH',   'discount',      15.0, 'email', 3),
    ('Gold',     'MEDIUM', 'discount',       8.0, 'email', 4),
    ('Silver',   'HIGH',   'discount',      10.0, 'sms',   5),
    ('Silver',   'MEDIUM', 'discount',       5.0, 'sms',   6),
    ('Bronze',   'HIGH',   'discount',       5.0, 'push',  7),
    ('Bronze',   'MEDIUM', 're_engagement',  0.0, 'push',  8)
ON CONFLICT (tier_name, risk_level) DO NOTHING;
