-- ============================================================
-- CUSTOMER RETENTION PLATFORM — PostgreSQL Schema
-- Analyst Agent | Churn Prediction Pipeline
-- Generated: 2026-03-24
-- Client: Walmart Inc. (CLT-001)
-- ============================================================

-- Enable extension for UUID generation (optional)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- SECTION 1: REFERENCE / MASTER TABLES
-- ============================================================

-- 1. Client Config
CREATE TABLE IF NOT EXISTS client_config (
    config_id              SERIAL PRIMARY KEY,
    client_id              VARCHAR(20)   NOT NULL UNIQUE,
    client_name            VARCHAR(100)  NOT NULL,
    client_code            VARCHAR(10)   NOT NULL,
    currency               VARCHAR(10)   DEFAULT 'USD',
    timezone               VARCHAR(50)   DEFAULT 'America/Chicago',
    fiscal_year_start      DATE,
    churn_window_days      INT           DEFAULT 90,
    high_ltv_threshold     NUMERIC(10,2) DEFAULT 1000.00,
    mid_ltv_threshold      NUMERIC(10,2) DEFAULT 200.00,
    max_discount_pct       NUMERIC(5,2)  DEFAULT 30.00,
    created_at             TIMESTAMPTZ   DEFAULT NOW()
);

-- 2. Categories (multi-tenant: each client has own categories)
CREATE TABLE IF NOT EXISTS categories (
    client_id     VARCHAR(20)  NOT NULL,
    category_id   INT          NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (client_id, category_id)
);
CREATE INDEX IF NOT EXISTS idx_categories_client ON categories(client_id);

-- 3. Sub-Categories (no FK to categories — upload order must not matter)
CREATE TABLE IF NOT EXISTS sub_categories (
    client_id         VARCHAR(20)  NOT NULL,
    sub_category_id   INT          NOT NULL,
    sub_category_name VARCHAR(100) NOT NULL,
    category_id       INT          NOT NULL,
    PRIMARY KEY (client_id, sub_category_id)
);
CREATE INDEX IF NOT EXISTS idx_sub_categories_client ON sub_categories(client_id);

-- 4. Sub-Sub-Categories (no FKs — upload order must not matter)
CREATE TABLE IF NOT EXISTS sub_sub_categories (
    client_id             VARCHAR(20)  NOT NULL,
    sub_sub_category_id   INT          NOT NULL,
    sub_sub_category_name VARCHAR(150) NOT NULL,
    sub_category_id       INT          NOT NULL,
    category_id           INT          NOT NULL,
    PRIMARY KEY (client_id, sub_sub_category_id)
);
CREATE INDEX IF NOT EXISTS idx_sub_sub_categories_client ON sub_sub_categories(client_id);

-- 5. Vendors
CREATE TABLE IF NOT EXISTS vendors (
    client_id          VARCHAR(20)  NOT NULL,
    vendor_id          INT          NOT NULL,
    vendor_name        VARCHAR(150) NOT NULL,
    vendor_description TEXT,
    vendor_contact_no  VARCHAR(30),
    vendor_address     TEXT,
    vendor_email       VARCHAR(150),
    PRIMARY KEY (client_id, vendor_id)
);
CREATE INDEX IF NOT EXISTS idx_vendors_client ON vendors(client_id);

-- 6. Brands
CREATE TABLE IF NOT EXISTS brands (
    client_id         VARCHAR(20)  NOT NULL,
    brand_id          INT          NOT NULL,
    brand_name        VARCHAR(100) NOT NULL,
    brand_description TEXT,
    vendor_id         INT,
    active            SMALLINT DEFAULT 1,
    not_available     SMALLINT DEFAULT 0,
    category_hint     VARCHAR(100),
    PRIMARY KEY (client_id, brand_id)
);
CREATE INDEX IF NOT EXISTS idx_brands_client ON brands(client_id);

-- 7. Products (product_price_id FK added after product_prices is created)
CREATE TABLE IF NOT EXISTS products (
    client_id           VARCHAR(20)  NOT NULL,
    product_id          INT          NOT NULL,
    sku                 VARCHAR(50)  NOT NULL,
    product_name        VARCHAR(200) NOT NULL,
    category_id         INT,
    sub_category_id     INT,
    sub_sub_category_id INT,
    brand_id            INT,
    product_price_id    INT,
    rating              NUMERIC(3,1),
    active              SMALLINT DEFAULT 1,
    not_available       SMALLINT DEFAULT 0,
    PRIMARY KEY (client_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_products_client ON products(client_id);

-- 8. Product Price Master
CREATE TABLE IF NOT EXISTS product_prices (
    client_id       VARCHAR(20)   NOT NULL,
    price_id        INT           NOT NULL,
    product_id      INT           NOT NULL,
    qty_range_label VARCHAR(50),
    qty_min         INT           NOT NULL,
    qty_max         INT,
    unit_price_usd  NUMERIC(10,2) NOT NULL,
    cost_price_usd  NUMERIC(10,2),          -- supplier cost; enables margin-safe discounts
    PRIMARY KEY (client_id, price_id)
);
CREATE INDEX IF NOT EXISTS idx_product_prices_client ON product_prices(client_id);

-- 9. Product-Vendor Mapping
CREATE TABLE IF NOT EXISTS product_vendor_mapping (
    client_id   VARCHAR(20) NOT NULL,
    pv_id       INT         NOT NULL,
    product_id  INT         NOT NULL,
    brand_id    INT,
    vendor_id   INT,
    PRIMARY KEY (client_id, pv_id)
);
CREATE INDEX IF NOT EXISTS idx_pvm_client ON product_vendor_mapping(client_id);

-- ============================================================
-- SECTION 2: CUSTOMER & TRANSACTION TABLES
-- ============================================================

-- 10. Customers
CREATE TABLE IF NOT EXISTS customers (
    client_id             VARCHAR(20)  NOT NULL,
    customer_id           VARCHAR(30)  NOT NULL,
    customer_email        VARCHAR(150),
    customer_name         VARCHAR(100),
    customer_phone        VARCHAR(30),
    account_created_date  DATE,
    registration_channel  VARCHAR(100),
    country_code          VARCHAR(5)   DEFAULT 'US',
    state                 VARCHAR(5),
    city                  VARCHAR(100),
    zip_code              VARCHAR(20),
    shipping_address      TEXT,
    preferred_device      VARCHAR(50),
    email_opt_in          BOOLEAN DEFAULT TRUE,
    sms_opt_in            BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customers_email  ON customers(customer_email);
CREATE INDEX IF NOT EXISTS idx_customers_client ON customers(client_id);

-- 11. Orders
CREATE TABLE IF NOT EXISTS orders (
    client_id        VARCHAR(20)   NOT NULL,
    order_id         VARCHAR(50)   NOT NULL,
    customer_id      VARCHAR(30)   NOT NULL,
    order_date       TIMESTAMPTZ,
    order_status     VARCHAR(30),
    order_value_usd  NUMERIC(10,2),
    discount_usd     NUMERIC(10,2) DEFAULT 0,
    coupon_code      VARCHAR(50),
    payment_method   VARCHAR(50),
    order_item_count INT,
    PRIMARY KEY (client_id, order_id),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date     ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders(order_status);

-- 12. Line Items
CREATE TABLE IF NOT EXISTS line_items (
    client_id              VARCHAR(20)   NOT NULL,
    line_item_id           VARCHAR(30)   NOT NULL,
    order_id               VARCHAR(50)   NOT NULL,
    customer_id            VARCHAR(30)   NOT NULL,
    product_id             INT           NOT NULL,
    quantity               INT           NOT NULL DEFAULT 1,
    unit_price_usd         NUMERIC(10,2),
    final_line_total_usd   NUMERIC(10,2),
    item_discount_usd      NUMERIC(10,2) DEFAULT 0,
    item_status            VARCHAR(30),
    PRIMARY KEY (client_id, line_item_id),
    FOREIGN KEY (client_id, order_id) REFERENCES orders(client_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_line_items_order    ON line_items(client_id, order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_customer ON line_items(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_line_items_product  ON line_items(product_id);

-- ============================================================
-- SECTION 3: CONFIGURATION & STRATEGY TABLES
-- ============================================================

-- 13. Value-Tier Master
CREATE TABLE IF NOT EXISTS value_tiers (
    tier_id          VARCHAR(10) PRIMARY KEY,
    tier_name        VARCHAR(50) NOT NULL,
    threshold_type   VARCHAR(20),
    threshold_value  NUMERIC(10,2),
    description      TEXT,
    benefits         TEXT
);

-- 14. Business Segment Master
CREATE TABLE IF NOT EXISTS business_segments (
    segment_id        VARCHAR(15) PRIMARY KEY,
    segment_name      VARCHAR(50) NOT NULL,
    description       TEXT,
    criteria          VARCHAR(200),
    recommended_focus TEXT
);

-- 15. Value Proposition Master
CREATE TABLE IF NOT EXISTS value_propositions (
    vp_id            SERIAL PRIMARY KEY,
    tier_name        VARCHAR(50)  NOT NULL,
    risk_level       VARCHAR(30)  NOT NULL,
    action_type      VARCHAR(100),
    message_template TEXT,
    discount_pct     NUMERIC(5,2) DEFAULT 0,
    channel          VARCHAR(50),
    priority         INT          DEFAULT 5
);

CREATE INDEX IF NOT EXISTS idx_vp_tier_risk ON value_propositions(tier_name, risk_level);

-- ============================================================
-- SECTION 4: ANALYST AGENT TABLES (Churn Pipeline)
-- ============================================================

-- 16. Customer RFM Features
CREATE TABLE IF NOT EXISTS customer_rfm_features (
    client_id               VARCHAR(20) NOT NULL,
    customer_id             VARCHAR(30) NOT NULL,
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    days_since_last_order   INT,
    last_order_date         DATE,
    last_order_status       VARCHAR(30),
    total_orders            INT DEFAULT 0,
    orders_last_30d         INT DEFAULT 0,
    orders_last_90d         INT DEFAULT 0,
    orders_last_180d        INT DEFAULT 0,
    avg_orders_per_month    NUMERIC(6,2),
    order_frequency_trend   VARCHAR(20),
    total_spend_usd         NUMERIC(12,2) DEFAULT 0,
    avg_order_value_usd     NUMERIC(10,2),
    spend_last_90d_usd      NUMERIC(12,2) DEFAULT 0,
    spend_last_180d_usd     NUMERIC(12,2) DEFAULT 0,
    ltv_usd                 NUMERIC(12,2),
    spend_trend             VARCHAR(20),
    recency_score           SMALLINT,
    frequency_score         SMALLINT,
    monetary_score          SMALLINT,
    rfm_total_score         SMALLINT,
    rfm_segment             VARCHAR(50),
    total_items_purchased   INT DEFAULT 0,
    unique_products_bought  INT DEFAULT 0,
    top_category            VARCHAR(100),
    return_rate_pct         NUMERIC(5,2),
    total_discounts_used    INT DEFAULT 0,
    total_discount_usd      NUMERIC(10,2) DEFAULT 0,
    discount_dependency_pct NUMERIC(5,2),
    account_age_days        INT,
    customer_tier           VARCHAR(20),
    PRIMARY KEY (client_id, customer_id)
);

-- 17. Churn Scores
-- One score per (client, customer). The UNIQUE constraint is load-bearing:
-- without it the DB silently accepted duplicates when two pipeline runs
-- overlapped, since ml/predict.py save_scores_to_db does DELETE + INSERT
-- and only the constraint prevents both INSERTs from winning. If you ever
-- need to keep score history, move this to a new table (churn_scores_history)
-- and leave this one as the latest-per-customer view.
CREATE TABLE IF NOT EXISTS churn_scores (
    score_id               SERIAL PRIMARY KEY,
    client_id              VARCHAR(20)  NOT NULL,
    customer_id            VARCHAR(30)  NOT NULL,
    scored_at              TIMESTAMPTZ  DEFAULT NOW(),
    churn_probability      NUMERIC(5,4),
    risk_tier              VARCHAR(10),
    churn_label_simulated  BOOLEAN      DEFAULT FALSE,
    driver_1               VARCHAR(100),
    driver_2               VARCHAR(100),
    driver_3               VARCHAR(100),
    -- Widened 2026-04-29: predict.py auto-generates model_version
    -- strings like "xgboost_2026-04-28_auc0.880" (~30 chars). The
    -- prior VARCHAR(20) caused StringDataRightTruncation on insert
    -- and rolled back the entire scoring transaction.
    model_version          VARCHAR(80),
    batch_run_id           VARCHAR(80),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id),
    CONSTRAINT churn_scores_client_customer_unique UNIQUE (client_id, customer_id)
);
CREATE INDEX IF NOT EXISTS idx_churn_scores_tier     ON churn_scores(risk_tier);
CREATE INDEX IF NOT EXISTS idx_churn_scores_scored   ON churn_scores(scored_at DESC);

-- 18. Retention Interventions Log
CREATE TABLE IF NOT EXISTS retention_interventions (
    intervention_id      SERIAL PRIMARY KEY,
    client_id            VARCHAR(20)  NOT NULL,
    customer_id          VARCHAR(30)  NOT NULL,
    created_at           TIMESTAMPTZ  DEFAULT NOW(),
    churn_score_id       INT REFERENCES churn_scores(score_id),
    churn_probability    NUMERIC(5,4),
    risk_tier            VARCHAR(10),
    offer_type           VARCHAR(100),
    discount_pct         NUMERIC(5,2),
    offer_message        TEXT,
    channel              VARCHAR(50),
    customer_ltv_usd     NUMERIC(12,2),
    max_allowed_discount NUMERIC(5,2),
    guardrail_passed     BOOLEAN DEFAULT TRUE,
    escalated_to_human   BOOLEAN DEFAULT FALSE,
    offer_status         VARCHAR(20)  DEFAULT 'pending',
    outcome_recorded_at  TIMESTAMPTZ,
    revenue_recovered    NUMERIC(10,2),
    langfuse_trace_id    VARCHAR(100),
    agent_cost_usd       NUMERIC(8,6)
);

CREATE INDEX IF NOT EXISTS idx_interventions_customer ON retention_interventions(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_interventions_status   ON retention_interventions(offer_status);

-- ============================================================
-- SECTION 4b: CUSTOMER FEEDBACK TABLES (added in v6)
-- ============================================================

-- 19. Customer Reviews
CREATE TABLE IF NOT EXISTS customer_reviews (
    client_id    VARCHAR(20)   NOT NULL,
    review_id    VARCHAR(30)   NOT NULL,
    customer_id  VARCHAR(30)   NOT NULL,
    product_id   INT,
    order_id     VARCHAR(50),
    rating       SMALLINT      CHECK (rating BETWEEN 1 AND 5),
    review_text  TEXT,
    review_date  DATE,
    sentiment    VARCHAR(20),
    PRIMARY KEY (client_id, review_id),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_reviews_customer   ON customer_reviews(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_reviews_product    ON customer_reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating     ON customer_reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment  ON customer_reviews(sentiment);

-- 20. Support Tickets
CREATE TABLE IF NOT EXISTS support_tickets (
    client_id            VARCHAR(20)   NOT NULL,
    ticket_id            VARCHAR(30)   NOT NULL,
    customer_id          VARCHAR(30)   NOT NULL,
    ticket_type          VARCHAR(100),
    priority             VARCHAR(20),
    status               VARCHAR(30),
    channel              VARCHAR(50),
    opened_date          TIMESTAMPTZ,
    resolved_date        TIMESTAMPTZ,
    resolution_time_hrs  NUMERIC(8,2),
    PRIMARY KEY (client_id, ticket_id),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_tickets_customer  ON support_tickets(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status    ON support_tickets(status);
CREATE INDEX IF NOT EXISTS idx_tickets_priority  ON support_tickets(priority);
CREATE INDEX IF NOT EXISTS idx_tickets_type      ON support_tickets(ticket_type);

-- ============================================================
-- SECTION 5: HELPER VIEWS
-- ============================================================

CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT
    c.client_id,
    c.customer_id,
    c.customer_name,
    c.customer_email,
    c.customer_phone,
    c.account_created_date,
    c.registration_channel,
    c.state,
    c.city,
    c.preferred_device,
    c.email_opt_in,
    c.sms_opt_in,
    r.days_since_last_order,
    r.last_order_date,
    r.total_orders,
    r.orders_last_90d,
    r.avg_order_value_usd,
    r.total_spend_usd,
    r.ltv_usd,
    r.rfm_total_score,
    r.rfm_segment,
    r.customer_tier,
    r.return_rate_pct,
    r.account_age_days,
    cs.churn_probability,
    cs.risk_tier,
    cs.driver_1,
    cs.driver_2,
    cs.driver_3,
    cs.scored_at AS last_scored_at
FROM customers c
LEFT JOIN customer_rfm_features r
    ON c.client_id = r.client_id AND c.customer_id = r.customer_id
LEFT JOIN LATERAL (
    SELECT * FROM churn_scores s
    WHERE s.client_id = c.client_id AND s.customer_id = c.customer_id
    ORDER BY s.scored_at DESC LIMIT 1
) cs ON TRUE;

CREATE OR REPLACE VIEW vw_at_risk_customers AS
SELECT * FROM vw_customer_360
WHERE risk_tier IN ('HIGH', 'MEDIUM')
ORDER BY churn_probability DESC;

CREATE OR REPLACE VIEW vw_customer_order_summary AS
SELECT
    o.client_id,
    o.customer_id,
    COUNT(o.order_id)                                                          AS total_orders,
    SUM(o.order_value_usd)                                                     AS total_spend_usd,
    AVG(o.order_value_usd)                                                     AS avg_order_value_usd,
    MIN(o.order_date)                                                          AS first_order_date,
    MAX(o.order_date)                                                          AS last_order_date,
    EXTRACT(DAY FROM NOW() - MAX(o.order_date))                                AS days_since_last_order,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'  THEN 1 ELSE 0 END) AS orders_last_30d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN 1 ELSE 0 END) AS orders_last_90d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN o.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
    SUM(o.discount_usd)                                                        AS total_discounts_usd,
    COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                            AS orders_with_discount
FROM orders o
WHERE o.order_status NOT IN ('Cancelled')
GROUP BY o.client_id, o.customer_id;

-- ============================================================
-- SECTION 6: STRATEGIST AGENT + COST TRACKING (merged from teammate's
--            predictive_analysis_final dump on 2026-04-20)
-- ============================================================

-- 21. Pricing Recommendations — one row per (run_id, product_name).
-- Holds the price the Strategist pricing engine suggests plus the
-- competitor / cost context it used to decide.
CREATE TABLE IF NOT EXISTS pricing_recommendations (
    recommendation_id   BIGSERIAL PRIMARY KEY,
    run_id              UUID            NOT NULL,
    client_id           VARCHAR(50)     NOT NULL,
    product_name        TEXT            NOT NULL,
    suggested_price     NUMERIC(12,2),
    pre_retention_price NUMERIC(12,2),
    floor_price         NUMERIC(12,2),
    target_price        NUMERIC(12,2),
    our_cost            NUMERIC(12,2),
    raw_cogs            NUMERIC(12,2),
    competitor_min      NUMERIC(12,2),
    competitor_avg      NUMERIC(12,2),
    competitor_max      NUMERIC(12,2),
    competitor_median   NUMERIC(12,2),
    strategy            VARCHAR(30),
    confidence          VARCHAR(10),
    margin_percent      NUMERIC(6,2),
    market_trend        VARCHAR(10),
    flag                VARCHAR(50),
    reasoning           TEXT,
    platform_breakdown  JSONB,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pricing_rec_client_run ON pricing_recommendations (client_id, run_id);
CREATE INDEX IF NOT EXISTS idx_pricing_rec_product    ON pricing_recommendations (client_id, product_name);

-- 22. Customer Price Context — joins a customer to the price the engine
-- would offer them for a given product, tagged with churn context.
-- UNIQUE (customer_id, product_name) keeps one live suggestion per
-- customer per product — the newest run overwrites the previous.
CREATE TABLE IF NOT EXISTS customer_price_context (
    context_id           BIGSERIAL PRIMARY KEY,
    customer_id          VARCHAR(100)  NOT NULL,
    client_id            VARCHAR(50)   NOT NULL,
    product_name         TEXT          NOT NULL,
    strategy             VARCHAR(30),
    suggested_price      NUMERIC(12,2),
    pre_retention_price  NUMERIC(12,2),
    discount_pct_applied NUMERIC(6,2),
    churn_probability    NUMERIC(5,4),
    risk_tier            VARCHAR(10),
    run_id               UUID,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_customer_price_context UNIQUE (customer_id, product_name)
);

CREATE INDEX IF NOT EXISTS idx_cpc_client_customers ON customer_price_context (client_id, customer_id);

-- 23. LLM Cost Log — append-only log of every LLM call (Groq, OpenAI, …)
-- with token counts and cost. The Cost Tracking page reads from this;
-- the over_budget flag lets us highlight runs that exceeded the
-- per-client budget from client_config.
CREATE TABLE IF NOT EXISTS llm_cost_log (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(20)    NOT NULL,
    call_type       VARCHAR(50)    NOT NULL,
    model           VARCHAR(100)   NOT NULL,
    input_tokens    INTEGER        NOT NULL DEFAULT 0,
    output_tokens   INTEGER        NOT NULL DEFAULT 0,
    total_tokens    INTEGER        NOT NULL DEFAULT 0,
    input_cost_usd  NUMERIC(12,8)  NOT NULL DEFAULT 0,
    output_cost_usd NUMERIC(12,8)  NOT NULL DEFAULT 0,
    total_cost_usd  NUMERIC(12,8)  NOT NULL DEFAULT 0,
    over_budget     BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_cost_log_client_created ON llm_cost_log (client_id, created_at DESC);

COMMENT ON TABLE client_config             IS 'Per-tenant client configuration';
COMMENT ON TABLE customers                 IS 'Customer master — one row per unique customer per client';
COMMENT ON TABLE orders                    IS 'Order header — one row per order';
COMMENT ON TABLE line_items                IS 'Order line items — one row per product per order';
COMMENT ON TABLE customer_rfm_features     IS 'Computed RFM + engagement features — refreshed nightly';
COMMENT ON TABLE churn_scores              IS 'ML model churn risk scores — refreshed nightly';
COMMENT ON TABLE retention_interventions   IS 'Log of all retention offers sent by the AI agent';
COMMENT ON TABLE customer_reviews          IS 'Customer product ratings and review text — added in v6';
COMMENT ON TABLE support_tickets           IS 'Customer support ticket log — added in v6';
COMMENT ON TABLE pricing_recommendations   IS 'Strategist Agent — pricing engine output (one row per run per product)';
COMMENT ON TABLE customer_price_context    IS 'Strategist Agent — per-customer price suggestion from latest run';
COMMENT ON TABLE llm_cost_log              IS 'Cost Tracking — per-call LLM usage + dollar cost';
