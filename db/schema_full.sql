-- ============================================================
-- schema_full.sql
-- Customer Retention Platform — Complete PostgreSQL Schema
-- ============================================================
-- Single-file database setup. Creates ALL tables, views, and
-- the materialized feature view in one run. No migrations needed.
--
-- Usage:  Open in pgAdmin4 → Query Tool → F5
-- ============================================================

BEGIN;

-- Enable extension for UUID generation (optional)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- SECTION 1: REFERENCE / MASTER TABLES
-- ============================================================

-- 1. Client Config (with all UI-driven dynamic parameters)
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
    -- Dynamic config (maps to Settings UI)
    min_repeat_orders      INT           DEFAULT 2,
    high_value_percentile  INT           DEFAULT 75,
    recent_order_gap_window INT          DEFAULT 3,
    tier_method            VARCHAR(20)   DEFAULT 'quartile',
    custom_platinum_min    NUMERIC(10,2) DEFAULT 500.00,
    custom_gold_min        NUMERIC(10,2) DEFAULT 250.00,
    custom_silver_min      NUMERIC(10,2) DEFAULT 100.00,
    custom_bronze_min      NUMERIC(10,2) DEFAULT 0.00,
    reference_date_mode    VARCHAR(10)   DEFAULT 'auto',
    reference_date         DATE          DEFAULT NULL,
    prediction_mode        VARCHAR(20)   DEFAULT 'churn',
    created_at             TIMESTAMPTZ   DEFAULT NOW()
);

-- 2. Categories
CREATE TABLE IF NOT EXISTS categories (
    category_id   INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

-- 3. Sub-Categories
CREATE TABLE IF NOT EXISTS sub_categories (
    sub_category_id   INT PRIMARY KEY,
    sub_category_name VARCHAR(100) NOT NULL,
    category_id       INT NOT NULL REFERENCES categories(category_id)
);

-- 4. Sub-Sub-Categories
CREATE TABLE IF NOT EXISTS sub_sub_categories (
    sub_sub_category_id   INT PRIMARY KEY,
    sub_sub_category_name VARCHAR(150) NOT NULL,
    sub_category_id       INT NOT NULL REFERENCES sub_categories(sub_category_id),
    category_id           INT NOT NULL REFERENCES categories(category_id)
);

-- 5. Vendors
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id          INT PRIMARY KEY,
    vendor_name        VARCHAR(150) NOT NULL,
    vendor_description TEXT,
    vendor_contact_no  VARCHAR(30),
    vendor_address     TEXT,
    vendor_email       VARCHAR(150)
);

-- 6. Brands
CREATE TABLE IF NOT EXISTS brands (
    brand_id          INT PRIMARY KEY,
    brand_name        VARCHAR(100) NOT NULL,
    brand_description TEXT,
    vendor_id         INT REFERENCES vendors(vendor_id),
    active            SMALLINT DEFAULT 1,
    not_available     SMALLINT DEFAULT 0,
    category_hint     VARCHAR(100)
);

-- 7. Products
CREATE TABLE IF NOT EXISTS products (
    product_id          INT PRIMARY KEY,
    sku                 VARCHAR(50)  NOT NULL,
    product_name        VARCHAR(200) NOT NULL,
    category_id         INT REFERENCES categories(category_id),
    sub_category_id     INT REFERENCES sub_categories(sub_category_id),
    sub_sub_category_id INT REFERENCES sub_sub_categories(sub_sub_category_id),
    brand_id            INT REFERENCES brands(brand_id),
    product_price_id    INT,
    rating              NUMERIC(3,1),
    active              SMALLINT DEFAULT 1,
    not_available       SMALLINT DEFAULT 0
);

-- 8. Product Price Master
CREATE TABLE IF NOT EXISTS product_prices (
    price_id        INT PRIMARY KEY,
    product_id      INT NOT NULL REFERENCES products(product_id),
    qty_range_label VARCHAR(50),
    qty_min         INT NOT NULL,
    qty_max         INT,
    unit_price_usd  NUMERIC(10,2) NOT NULL
);

-- Add deferred FK from products → product_prices (avoids circular load issue)
DO $$ BEGIN
    ALTER TABLE products
        ADD CONSTRAINT fk_products_price
        FOREIGN KEY (product_price_id) REFERENCES product_prices(price_id)
        DEFERRABLE INITIALLY DEFERRED;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 9. Product-Vendor Mapping
CREATE TABLE IF NOT EXISTS product_vendor_mapping (
    pv_id       INT PRIMARY KEY,
    product_id  INT NOT NULL REFERENCES products(product_id),
    brand_id    INT REFERENCES brands(brand_id),
    vendor_id   INT REFERENCES vendors(vendor_id)
);

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
    product_id             INT           NOT NULL REFERENCES products(product_id),
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
    model_version          VARCHAR(20)  DEFAULT 'v1.0',
    batch_run_id           VARCHAR(50),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_churn_scores_customer ON churn_scores(client_id, customer_id);
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
-- SECTION 5: CUSTOMER FEEDBACK TABLES
-- ============================================================

-- 19. Customer Reviews
CREATE TABLE IF NOT EXISTS customer_reviews (
    client_id    VARCHAR(20)   NOT NULL,
    review_id    VARCHAR(30)   NOT NULL,
    customer_id  VARCHAR(30)   NOT NULL,
    product_id   INT           REFERENCES products(product_id),
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
-- SECTION 6: SUBSCRIPTION & OUTREACH TABLES
-- ============================================================

-- 21. Customer Purchase Cycles
CREATE TABLE IF NOT EXISTS customer_purchase_cycles (
    cycle_id              SERIAL PRIMARY KEY,
    client_id             VARCHAR(20)   NOT NULL,
    customer_id           VARCHAR(30)   NOT NULL,
    product_id            INT           NOT NULL REFERENCES products(product_id),
    purchase_count        INT           DEFAULT 0,
    first_purchase_date   DATE,
    last_purchase_date    DATE,
    avg_refill_days       NUMERIC(8,1),
    expected_next_date    DATE,
    days_overdue          INT,
    missed_refill_count   INT           DEFAULT 0,
    is_active_subscriber  BOOLEAN       DEFAULT TRUE,
    computed_at           TIMESTAMPTZ   DEFAULT NOW(),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id),
    UNIQUE (client_id, customer_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_cycles_customer
    ON customer_purchase_cycles(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_cycles_overdue
    ON customer_purchase_cycles(days_overdue DESC);
CREATE INDEX IF NOT EXISTS idx_cycles_expected
    ON customer_purchase_cycles(expected_next_date);

-- 22. Outreach Messages
CREATE TABLE IF NOT EXISTS outreach_messages (
    message_id        SERIAL PRIMARY KEY,
    client_id         VARCHAR(20)   NOT NULL,
    customer_id       VARCHAR(30)   NOT NULL,
    product_id        INT           REFERENCES products(product_id),
    message_type      VARCHAR(50)   NOT NULL,
    trigger_reason    VARCHAR(200),
    message_text      TEXT          NOT NULL,
    channel           VARCHAR(30)   NOT NULL,
    days_overdue      INT,
    discount_offered  NUMERIC(5,2),
    sent_at           TIMESTAMPTZ   DEFAULT NOW(),
    responded_at      TIMESTAMPTZ,
    responded         BOOLEAN       DEFAULT FALSE,
    outcome           VARCHAR(50),
    revenue_recovered NUMERIC(10,2),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_outreach_customer
    ON outreach_messages(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_outreach_type
    ON outreach_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_outreach_outcome
    ON outreach_messages(outcome);

-- ============================================================
-- SECTION 7: VIEWS
-- ============================================================

-- 7a. Subscription Product Detection View
CREATE OR REPLACE VIEW vw_subscription_products AS
WITH

keyword_flag AS (
    SELECT
        product_id,
        product_name,
        CASE WHEN LOWER(product_name) LIKE ANY (ARRAY[
            '%refill%', '%subscription%', '%monthly%', '%daily%',
            '%vitamin%', '%supplement%', '%tablet%', '%capsule%',
            '%mg %', '% mg%', '%dose%', '%pill%', '%softgel%',
            '%gummy%', '%probiotic%', '%omega%', '%protein%',
            '%insulin%', '%inhaler%', '%drops%', '%syrup%',
            '%pack of%', '%count)%', '%supply%'
        ]) THEN TRUE ELSE FALSE END AS is_subscription_by_name
    FROM products
),

repeat_counts AS (
    SELECT customer_id, product_id, COUNT(*) AS purchase_count
    FROM line_items
    GROUP BY customer_id, product_id
),

purchase_gaps AS (
    SELECT customer_id, product_id,
        EXTRACT(DAY FROM order_date - LAG(order_date) OVER (
            PARTITION BY customer_id, product_id ORDER BY order_date
        )) AS gap_days
    FROM (
        SELECT li.customer_id, li.product_id, o.order_date
        FROM line_items li
        JOIN orders o ON li.order_id = o.order_id
    ) ordered_purchases
),

avg_gaps AS (
    SELECT customer_id, product_id,
        AVG(gap_days) AS avg_gap
    FROM purchase_gaps
    WHERE gap_days IS NOT NULL
    GROUP BY customer_id, product_id
),

behaviour_flag AS (
    SELECT
        li.product_id,
        COUNT(DISTINCT li.customer_id)                                  AS total_buyers,
        COUNT(DISTINCT CASE
            WHEN rc.purchase_count >= 3 THEN li.customer_id END)        AS repeat_buyers,
        ROUND(AVG(ag.avg_gap)::NUMERIC, 1)                              AS avg_refill_days,
        ROUND(STDDEV(ag.avg_gap)::NUMERIC, 1)                           AS stddev_refill_days
    FROM line_items li
    LEFT JOIN repeat_counts rc
           ON li.customer_id = rc.customer_id
          AND li.product_id  = rc.product_id
    LEFT JOIN avg_gaps ag
           ON li.customer_id = ag.customer_id
          AND li.product_id  = ag.product_id
    GROUP BY li.product_id
),

combined AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category_id,
        kf.is_subscription_by_name,
        COALESCE(bf.repeat_buyers, 0)       AS repeat_buyers,
        COALESCE(bf.total_buyers, 0)        AS total_buyers,
        COALESCE(bf.avg_refill_days, 0)     AS avg_refill_days,
        COALESCE(bf.stddev_refill_days, 0)  AS stddev_refill_days,
        CASE WHEN bf.total_buyers > 0
              AND (bf.repeat_buyers * 1.0 / bf.total_buyers) >= 0.30
              AND COALESCE(bf.stddev_refill_days, 999) < 15
             THEN TRUE ELSE FALSE END       AS is_subscription_by_behaviour
    FROM products p
    LEFT JOIN keyword_flag   kf ON p.product_id = kf.product_id
    LEFT JOIN behaviour_flag bf ON p.product_id = bf.product_id
)

SELECT
    product_id,
    product_name,
    category_id,
    is_subscription_by_name,
    is_subscription_by_behaviour,
    avg_refill_days,
    repeat_buyers,
    total_buyers,
    (is_subscription_by_name OR is_subscription_by_behaviour) AS is_subscription_product,
    CASE
        WHEN is_subscription_by_name AND is_subscription_by_behaviour THEN 'both'
        WHEN is_subscription_by_name                                   THEN 'keyword'
        WHEN is_subscription_by_behaviour                              THEN 'behaviour'
        ELSE 'none'
    END AS detection_source
FROM combined;

-- 7b. Customer 360 View
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

-- 7c. At-Risk Customers View
CREATE OR REPLACE VIEW vw_at_risk_customers AS
SELECT * FROM vw_customer_360
WHERE risk_tier IN ('HIGH', 'MEDIUM')
ORDER BY churn_probability DESC;

-- 7d. Customer Order Summary View
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
-- SECTION 8: MATERIALIZED VIEW — 52-Column Feature Matrix
-- ============================================================
-- All time-based and threshold features read dynamically from
-- client_config so the pipeline respects the Settings UI.
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mv_customer_features;

CREATE MATERIALIZED VIEW mv_customer_features AS

WITH

-- ── Config: resolve reference_date per client ────────────────────────────────
client_ref AS (
    SELECT
        client_id,
        churn_window_days,
        min_repeat_orders,
        high_value_percentile,
        recent_order_gap_window,
        tier_method,
        custom_platinum_min,
        custom_gold_min,
        custom_silver_min,
        custom_bronze_min,
        CASE WHEN reference_date_mode = 'fixed' AND reference_date IS NOT NULL
             THEN reference_date::TIMESTAMPTZ
             ELSE NOW()
        END AS ref_date
    FROM client_config
),

-- ── Order-level aggregations ─────────────────────────────────────────────────
order_agg AS (
    SELECT
        o.client_id,
        o.customer_id,
        COUNT(*)                                                                        AS total_orders,
        MIN(o.order_date)                                                               AS first_order_date,
        MAX(o.order_date)                                                               AS last_order_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(o.order_date))::INT                          AS days_since_last_order,
        SUM(o.order_value_usd)                                                          AS total_spend_usd,
        ROUND(AVG(o.order_value_usd)::NUMERIC, 2)                                       AS avg_order_value_usd,
        MAX(o.order_value_usd)                                                          AS max_order_value_usd,
        COALESCE(SUM(o.discount_usd), 0)                                                AS total_discount_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '30 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_30d_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_90d_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '180 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_180d_usd,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '30 days'  THEN 1 END)   AS orders_last_30d,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'  THEN 1 END)   AS orders_last_90d,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '180 days' THEN 1 END)   AS orders_last_180d,
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                                 AS orders_with_discount
    FROM orders o
    JOIN client_ref cr ON o.client_id = cr.client_id
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY o.client_id, o.customer_id, cr.ref_date
),

-- ── Order gap statistics (mean + median) ─────────────────────────────────────
order_gaps AS (
    SELECT client_id, customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1)                                    AS avg_days_between_orders,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap_days)::NUMERIC, 1)
                                                                            AS median_days_between_orders
    FROM (
        SELECT client_id, customer_id,
            EXTRACT(DAY FROM order_date - LAG(order_date) OVER (
                PARTITION BY client_id, customer_id ORDER BY order_date
            ))::NUMERIC AS gap_days
        FROM orders WHERE order_status NOT IN ('Cancelled')
    ) gaps
    WHERE gap_days IS NOT NULL
    GROUP BY client_id, customer_id
),

-- ── Line-item aggregations ───────────────────────────────────────────────────
line_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT li.product_id)                                                   AS unique_products_purchased,
        ROUND(AVG(li.quantity)::NUMERIC, 2)                                             AS avg_items_per_order,
        ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS return_rate_pct
    FROM line_items li
    GROUP BY li.client_id, li.customer_id
),

-- ── Category breadth ─────────────────────────────────────────────────────────
cat_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT p.category_id)  AS unique_categories_purchased
    FROM line_items li
    JOIN products p ON li.product_id = p.product_id
    GROUP BY li.client_id, li.customer_id
),

-- ── Customer review signals ──────────────────────────────────────────────────
review_agg AS (
    SELECT r.client_id, r.customer_id,
        COUNT(*)                                                                        AS total_reviews,
        ROUND(AVG(r.rating)::NUMERIC, 2)                                                AS avg_rating,
        ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_positive_reviews,
        ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_negative_reviews,
        MAX(r.review_date)                                                              AS last_review_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(r.review_date::TIMESTAMPTZ))::INT            AS days_since_last_review
    FROM customer_reviews r
    JOIN client_ref cr ON r.client_id = cr.client_id
    GROUP BY r.client_id, r.customer_id, cr.ref_date
),

-- ── Support ticket signals ───────────────────────────────────────────────────
ticket_agg AS (
    SELECT t.client_id, t.customer_id,
        COUNT(*)                                                                        AS total_tickets,
        COUNT(CASE WHEN LOWER(t.status)   = 'open'     THEN 1 END)                     AS open_tickets,
        COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END)                     AS critical_tickets,
        ROUND(AVG(t.resolution_time_hrs)::NUMERIC, 1)                                  AS avg_resolution_time_hrs,
        ROUND(COUNT(CASE WHEN LOWER(t.status) = 'resolved' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_tickets_resolved
    FROM support_tickets t
    GROUP BY t.client_id, t.customer_id
),

-- ── RFM scores via NTILE(5) ─────────────────────────────────────────────────
rfm_scored AS (
    SELECT client_id, customer_id,
        6 - NTILE(5) OVER (PARTITION BY client_id ORDER BY days_since_last_order ASC)  AS rfm_recency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_orders ASC)               AS rfm_frequency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_spend_usd ASC)            AS rfm_monetary_score
    FROM order_agg
),

-- ── Pre-aggregate last purchase per product (for subscription signals) ───────
last_purchase_per_product AS (
    SELECT
        li.client_id,
        li.customer_id,
        li.product_id,
        MAX(o.order_date) AS last_purchase_date
    FROM line_items li
    JOIN orders o ON li.order_id = o.order_id
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY li.client_id, li.customer_id, li.product_id
),

-- ── Subscription signals per customer ────────────────────────────────────────
subscription_agg AS (
    SELECT
        lp.client_id,
        lp.customer_id,
        COUNT(DISTINCT lp.product_id)                                                   AS subscription_product_count,
        ROUND(AVG(sp.avg_refill_days)::NUMERIC, 1)                                      AS avg_refill_cycle_days,
        MAX(
            EXTRACT(DAY FROM
                cr.ref_date - (lp.last_purchase_date::TIMESTAMPTZ
                         + (sp.avg_refill_days::TEXT || ' days')::INTERVAL)
            )
        )::INT                                                                          AS days_overdue_for_refill,
        SUM(
            CASE WHEN EXTRACT(DAY FROM cr.ref_date - lp.last_purchase_date::TIMESTAMPTZ)
                      > sp.avg_refill_days * 1.5
                 THEN 1 ELSE 0 END
        )                                                                               AS missed_refill_count
    FROM last_purchase_per_product lp
    JOIN vw_subscription_products sp
         ON lp.product_id = sp.product_id
        AND sp.is_subscription_product = TRUE
    JOIN client_ref cr ON lp.client_id = cr.client_id
    GROUP BY lp.client_id, lp.customer_id, cr.ref_date
),

-- ── Repeat customer flag (dynamic threshold from config) ─────────────────────
repeat_flag AS (
    SELECT
        oa.client_id,
        oa.customer_id,
        CASE WHEN oa.total_orders >= cr.min_repeat_orders THEN 1 ELSE 0 END            AS is_repeat_customer
    FROM order_agg oa
    JOIN client_ref cr ON oa.client_id = cr.client_id
),

-- ── Recent order gaps (configurable window for rhythm detection) ──────────────
recent_gaps AS (
    SELECT client_id, customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1) AS recent_avg_gap_days
    FROM (
        SELECT g.client_id, g.customer_id, g.gap_days,
            ROW_NUMBER() OVER (
                PARTITION BY g.client_id, g.customer_id ORDER BY g.order_date DESC
            ) AS rn,
            cr.recent_order_gap_window
        FROM (
            SELECT client_id, customer_id, order_date,
                EXTRACT(DAY FROM order_date - LAG(order_date) OVER (
                    PARTITION BY client_id, customer_id ORDER BY order_date
                ))::NUMERIC AS gap_days
            FROM orders WHERE order_status NOT IN ('Cancelled')
        ) g
        JOIN client_ref cr ON g.client_id = cr.client_id
        WHERE g.gap_days IS NOT NULL
    ) ranked
    WHERE rn <= recent_order_gap_window
    GROUP BY client_id, customer_id
),

-- ── Tier assignment (quartile or custom thresholds from config) ──────────────
spend_percentiles AS (
    SELECT
        oa.client_id,
        oa.customer_id,
        oa.total_spend_usd,
        PERCENT_RANK() OVER (
            PARTITION BY oa.client_id ORDER BY oa.total_spend_usd ASC
        ) * 100 AS spend_pct_rank
    FROM order_agg oa
),

tier_assignment AS (
    SELECT
        sp.client_id,
        sp.customer_id,
        CASE WHEN cr.tier_method = 'quartile' THEN
            CASE WHEN sp.spend_pct_rank >= cr.high_value_percentile THEN 'Platinum'
                 WHEN sp.spend_pct_rank >= 50                        THEN 'Gold'
                 WHEN sp.spend_pct_rank >= 25                        THEN 'Silver'
                 ELSE 'Bronze'
            END
        ELSE
            CASE WHEN sp.total_spend_usd >= cr.custom_platinum_min THEN 'Platinum'
                 WHEN sp.total_spend_usd >= cr.custom_gold_min     THEN 'Gold'
                 WHEN sp.total_spend_usd >= cr.custom_silver_min   THEN 'Silver'
                 ELSE 'Bronze'
            END
        END AS customer_tier,
        CASE WHEN cr.tier_method = 'quartile'
                  AND sp.spend_pct_rank >= cr.high_value_percentile
             THEN 1
             WHEN cr.tier_method != 'quartile'
                  AND sp.total_spend_usd >= cr.custom_platinum_min
             THEN 1
             ELSE 0
        END AS is_high_value
    FROM spend_percentiles sp
    JOIN client_ref cr ON sp.client_id = cr.client_id
)

-- ── Final SELECT: 52 columns ─────────────────────────────────────────────────
SELECT
    c.client_id,
    c.customer_id,

    -- Account
    EXTRACT(DAY FROM cr.ref_date - c.account_created_date::TIMESTAMPTZ)::INT AS account_age_days,

    -- Recency
    oa.first_order_date,
    oa.last_order_date,
    oa.days_since_last_order,

    -- Frequency
    oa.total_orders,
    oa.orders_last_30d,
    oa.orders_last_90d,
    oa.orders_last_180d,
    COALESCE(og.avg_days_between_orders, 0)                                 AS avg_days_between_orders,
    COALESCE(og.median_days_between_orders, 0)                              AS median_days_between_orders,
    ROUND(ABS(COALESCE(og.avg_days_between_orders, 0)
              - COALESCE(og.median_days_between_orders, 0))::NUMERIC, 1)   AS order_gap_mean_median_diff,
    COALESCE(rg.recent_avg_gap_days, 0)                                     AS recent_avg_gap_days,

    -- Monetary
    oa.total_spend_usd,
    oa.avg_order_value_usd,
    oa.max_order_value_usd,
    oa.spend_last_30d_usd,
    oa.spend_last_90d_usd,
    oa.spend_last_180d_usd,
    oa.total_discount_usd,

    -- Discount behaviour
    ROUND(oa.total_discount_usd * 100.0
          / NULLIF(oa.total_spend_usd + oa.total_discount_usd, 0)::NUMERIC, 2)
                                                                            AS discount_rate_pct,
    oa.orders_with_discount,

    -- Basket behaviour
    COALESCE(la.unique_products_purchased, 0)                               AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, 0)                             AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, 0)                                     AS avg_items_per_order,
    COALESCE(la.return_rate_pct, 0)                                         AS return_rate_pct,

    -- Review signals
    COALESCE(ra.total_reviews, 0)                                           AS total_reviews,
    COALESCE(ra.avg_rating, 0)                                              AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0)                                    AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0)                                    AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999)                               AS days_since_last_review,

    -- Support ticket signals
    COALESCE(ta.total_tickets, 0)                                           AS total_tickets,
    COALESCE(ta.open_tickets, 0)                                            AS open_tickets,
    COALESCE(ta.critical_tickets, 0)                                        AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0)                                 AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0)                                    AS pct_tickets_resolved,

    -- RFM scores
    oa.total_spend_usd                                                      AS ltv_usd,
    rf.rfm_recency_score,
    rf.rfm_frequency_score,
    rf.rfm_monetary_score,
    (rf.rfm_recency_score + rf.rfm_frequency_score + rf.rfm_monetary_score) AS rfm_total_score,

    -- Dynamic config-driven features
    COALESCE(rpf.is_repeat_customer, 0)                                     AS is_repeat_customer,
    COALESCE(ta2.customer_tier, 'Bronze')                                   AS customer_tier,
    COALESCE(ta2.is_high_value, 0)                                          AS is_high_value,

    -- Subscription signals
    COALESCE(sa.subscription_product_count, 0)                              AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, 0)                                   AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0)                                 AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, 0)                                     AS missed_refill_count,

    -- Churn label (dynamic from client_config.churn_window_days)
    CASE WHEN oa.days_since_last_order >= cr.churn_window_days THEN 1 ELSE 0 END AS churn_label,

    cr.ref_date                                                             AS computed_at

FROM customers c
JOIN  client_ref   cr  ON c.client_id = cr.client_id
JOIN  order_agg    oa  ON c.client_id = oa.client_id  AND c.customer_id = oa.customer_id
JOIN  rfm_scored   rf  ON c.client_id = rf.client_id  AND c.customer_id = rf.customer_id
LEFT JOIN order_gaps       og  ON c.client_id = og.client_id  AND c.customer_id = og.customer_id
LEFT JOIN recent_gaps      rg  ON c.client_id = rg.client_id  AND c.customer_id = rg.customer_id
LEFT JOIN line_agg         la  ON c.client_id = la.client_id  AND c.customer_id = la.customer_id
LEFT JOIN cat_agg          ca  ON c.client_id = ca.client_id  AND c.customer_id = ca.customer_id
LEFT JOIN review_agg       ra  ON c.client_id = ra.client_id  AND c.customer_id = ra.customer_id
LEFT JOIN ticket_agg       ta  ON c.client_id = ta.client_id  AND c.customer_id = ta.customer_id
LEFT JOIN repeat_flag      rpf ON c.client_id = rpf.client_id AND c.customer_id = rpf.customer_id
LEFT JOIN tier_assignment   ta2 ON c.client_id = ta2.client_id AND c.customer_id = ta2.customer_id
LEFT JOIN subscription_agg sa  ON c.client_id = sa.client_id  AND c.customer_id = sa.customer_id;

-- ── Indexes on materialized view ─────────────────────────────────────────────
CREATE UNIQUE INDEX idx_mv_cf_pk
    ON mv_customer_features (client_id, customer_id);
CREATE INDEX idx_mv_cf_churn
    ON mv_customer_features (churn_label, rfm_total_score DESC);
CREATE INDEX idx_mv_cf_recency
    ON mv_customer_features (days_since_last_order DESC);
CREATE INDEX idx_mv_cf_overdue
    ON mv_customer_features (days_overdue_for_refill DESC);
CREATE INDEX idx_mv_cf_tier
    ON mv_customer_features (customer_tier, is_high_value);

-- ============================================================
-- SECTION 9: SCOUT AGENT TABLES
-- ============================================================

-- 23. Websites (tracked e-commerce platforms)
CREATE TABLE IF NOT EXISTS websites (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    base_url        TEXT NOT NULL DEFAULT '',
    search_url      TEXT NOT NULL DEFAULT '',
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    encoding        TEXT NOT NULL DEFAULT 'plus'
);

-- 24. Entities (canonical products to track)
CREATE TABLE IF NOT EXISTS entities (
    id                UUID DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    canonical_name    TEXT NOT NULL,
    canonical_brand   TEXT,
    canonical_variant TEXT,
    query             TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 25. Entity Listings (product listings per platform)
CREATE TABLE IF NOT EXISTS entity_listings (
    id              SERIAL PRIMARY KEY,
    entity_id       UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    platform        TEXT NOT NULL,
    product_url     TEXT NOT NULL,
    title           TEXT NOT NULL,
    price           NUMERIC(10,2),
    currency        TEXT NOT NULL DEFAULT 'INR',
    ingredients     TEXT,
    manufacturer    TEXT,
    marketed_by     TEXT,
    availability    TEXT DEFAULT 'unknown',
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, platform)
);

-- 26. Product Results (scraped search results)
CREATE TABLE IF NOT EXISTS product_results (
    id              SERIAL PRIMARY KEY,
    product_name    TEXT NOT NULL,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    product_url     TEXT,
    price           DOUBLE PRECISION,
    platform        TEXT,
    product_details JSONB,
    title           TEXT,
    UNIQUE (product_name, platform)
);

-- 27. Price History (historical price tracking)
CREATE TABLE IF NOT EXISTS price_history (
    id              SERIAL PRIMARY KEY,
    product_name    TEXT NOT NULL,
    platform        TEXT NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'INR',
    url             TEXT,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 28. Price Alerts (price change notifications)
CREATE TABLE IF NOT EXISTS price_alerts (
    id              SERIAL PRIMARY KEY,
    product_name    TEXT NOT NULL,
    platform        TEXT NOT NULL,
    old_price       NUMERIC(10,2),
    new_price       NUMERIC(10,2) NOT NULL,
    change_amount   NUMERIC(10,2),
    change_percent  NUMERIC(6,2),
    direction       TEXT NOT NULL,
    url             TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged    BOOLEAN NOT NULL DEFAULT FALSE
);

-- 29. Product Features (extracted product attributes)
CREATE TABLE IF NOT EXISTS product_features (
    id              SERIAL PRIMARY KEY,
    product_name    TEXT NOT NULL,
    platform        TEXT NOT NULL,
    category        TEXT,
    product_feats   JSONB,
    platform_feats  JSONB,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (product_name, platform)
);

CREATE INDEX IF NOT EXISTS idx_price_history_product   ON price_history(product_name, platform);
CREATE INDEX IF NOT EXISTS idx_price_history_scraped   ON price_history(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_alerts_product    ON price_alerts(product_name, platform);
CREATE INDEX IF NOT EXISTS idx_price_alerts_detected   ON price_alerts(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_product_results_product ON product_results(product_name);
CREATE INDEX IF NOT EXISTS idx_entity_listings_entity  ON entity_listings(entity_id);

COMMIT;

-- ── Populate the materialized view ───────────────────────────────────────────
REFRESH MATERIALIZED VIEW mv_customer_features;

-- ── Table comments ───────────────────────────────────────────────────────────
COMMENT ON TABLE client_config           IS 'Per-tenant client configuration with UI-driven dynamic parameters';
COMMENT ON TABLE customers               IS 'Customer master — one row per unique customer per client';
COMMENT ON TABLE orders                  IS 'Order header — one row per order';
COMMENT ON TABLE line_items              IS 'Order line items — one row per product per order';
COMMENT ON TABLE customer_rfm_features   IS 'Computed RFM + engagement features — refreshed nightly';
COMMENT ON TABLE churn_scores            IS 'ML model churn risk scores — refreshed nightly';
COMMENT ON TABLE retention_interventions IS 'Log of all retention offers sent by the AI agent';
COMMENT ON TABLE customer_reviews        IS 'Customer product ratings and review text';
COMMENT ON TABLE support_tickets         IS 'Customer support ticket log';
COMMENT ON TABLE customer_purchase_cycles IS 'Per-customer per-product refill pattern tracking';
COMMENT ON TABLE outreach_messages       IS 'Personalised messages sent on churn/refill triggers';
COMMENT ON TABLE websites                IS 'E-commerce platforms tracked by Scout Agent';
COMMENT ON TABLE entities                IS 'Canonical products tracked for competitive intelligence';
COMMENT ON TABLE entity_listings         IS 'Per-platform product listings for tracked entities';
COMMENT ON TABLE product_results         IS 'Raw scraped search results from tracked platforms';
COMMENT ON TABLE price_history           IS 'Historical price snapshots for trend analysis';
COMMENT ON TABLE price_alerts            IS 'Automated price change notifications';
COMMENT ON TABLE product_features        IS 'Extracted product attributes and platform features';

-- ── Verify: should list all 52 columns ───────────────────────────────────────
SELECT column_name, ordinal_position AS "#"
FROM information_schema.columns
WHERE table_name   = 'mv_customer_features'
  AND table_schema = 'public'
ORDER BY ordinal_position;
