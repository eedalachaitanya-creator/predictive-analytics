-- ============================================================
-- schema_sqlite.sql — SQLite schema for local development
-- Customer Retention Platform | Analyst Agent
-- ============================================================
PRAGMA foreign_keys = ON;

CREATE TABLE brands (
    brand_id          INTEGER PRIMARY KEY,
    brand_name        TEXT NOT NULL,
    brand_description TEXT,
    vendor_id         INTEGER REFERENCES vendors(vendor_id),
    active            INTEGER DEFAULT 1,
    not_available     INTEGER DEFAULT 0,
    category_hint     TEXT
);

CREATE TABLE business_segments (
    segment_id        TEXT PRIMARY KEY,
    segment_name      TEXT NOT NULL,
    description       TEXT,
    criteria          TEXT,
    recommended_focus TEXT
);

CREATE TABLE categories (
    category_id   INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE churn_scores (
    score_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id             TEXT NOT NULL,
    customer_id           TEXT NOT NULL,
    scored_at             TEXT DEFAULT (datetime('now')),
    churn_probability     REAL,
    risk_tier             TEXT,
    churn_label_simulated INTEGER DEFAULT 0,
    driver_1              TEXT,
    driver_2              TEXT,
    driver_3              TEXT,
    model_version         TEXT DEFAULT 'v1.0-simulated',
    batch_run_id          TEXT
);

CREATE TABLE client_config (
    config_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id    TEXT NOT NULL UNIQUE,
    client_name  TEXT NOT NULL,
    client_code  TEXT NOT NULL,
    currency     TEXT DEFAULT 'USD',
    timezone     TEXT DEFAULT 'America/Chicago',
    churn_window_days      INTEGER DEFAULT 90,
    high_ltv_threshold     REAL DEFAULT 1000.00,
    mid_ltv_threshold      REAL DEFAULT 200.00,
    max_discount_pct       REAL DEFAULT 30.00,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE customer_rfm_features (
    client_id              TEXT NOT NULL,
    customer_id            TEXT NOT NULL,
    computed_at            TEXT DEFAULT (datetime('now')),
    days_since_last_order  INTEGER,
    last_order_date        TEXT,
    last_order_status      TEXT,
    total_orders           INTEGER DEFAULT 0,
    orders_last_30d        INTEGER DEFAULT 0,
    orders_last_90d        INTEGER DEFAULT 0,
    orders_last_180d       INTEGER DEFAULT 0,
    avg_orders_per_month   REAL,
    order_frequency_trend  TEXT,
    total_spend_usd        REAL DEFAULT 0,
    avg_order_value_usd    REAL,
    spend_last_90d_usd     REAL DEFAULT 0,
    spend_last_180d_usd    REAL DEFAULT 0,
    ltv_usd                REAL,
    spend_trend            TEXT,
    recency_score          INTEGER,
    frequency_score        INTEGER,
    monetary_score         INTEGER,
    rfm_total_score        INTEGER,
    rfm_segment            TEXT,
    total_items_purchased  INTEGER DEFAULT 0,
    unique_products_bought INTEGER DEFAULT 0,
    top_category           TEXT,
    return_rate_pct        REAL,
    total_discounts_used   INTEGER DEFAULT 0,
    total_discount_usd     REAL DEFAULT 0,
    discount_dependency_pct REAL,
    account_age_days       INTEGER,
    customer_tier          TEXT,
    PRIMARY KEY (client_id, customer_id)
);

CREATE TABLE customers (
    client_id             TEXT NOT NULL,
    customer_id           TEXT NOT NULL,
    customer_email        TEXT,
    customer_name         TEXT,
    customer_phone        TEXT,
    account_created_date  TEXT,
    registration_channel  TEXT,
    country_code          TEXT DEFAULT 'US',
    state                 TEXT,
    city                  TEXT,
    zip_code              TEXT,
    shipping_address      TEXT,
    preferred_device      TEXT,
    email_opt_in          INTEGER DEFAULT 1,
    sms_opt_in            INTEGER DEFAULT 0,
    PRIMARY KEY (client_id, customer_id)
);

CREATE TABLE line_items (
    client_id            TEXT NOT NULL,
    line_item_id         TEXT NOT NULL,
    order_id             TEXT NOT NULL,
    customer_id          TEXT NOT NULL,
    product_id           INTEGER NOT NULL REFERENCES products(product_id),
    quantity             INTEGER NOT NULL DEFAULT 1,
    unit_price_usd       REAL,
    final_line_total_usd REAL,
    item_discount_usd    REAL DEFAULT 0,
    item_status          TEXT,
    PRIMARY KEY (client_id, line_item_id)
);

CREATE TABLE orders (
    client_id        TEXT NOT NULL,
    order_id         TEXT NOT NULL,
    customer_id      TEXT NOT NULL,
    order_date       TEXT,
    order_status     TEXT,
    order_value_usd  REAL,
    discount_usd     REAL DEFAULT 0,
    coupon_code      TEXT,
    payment_method   TEXT,
    order_item_count INTEGER,
    PRIMARY KEY (client_id, order_id)
);

CREATE TABLE product_prices (
    price_id        INTEGER PRIMARY KEY,
    product_id      INTEGER NOT NULL,
    qty_range_label TEXT,
    qty_min         INTEGER NOT NULL,
    qty_max         INTEGER,
    unit_price_usd  REAL NOT NULL
);

CREATE TABLE product_vendor_mapping (
    pv_id      INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    brand_id   INTEGER REFERENCES brands(brand_id),
    vendor_id  INTEGER REFERENCES vendors(vendor_id)
);

CREATE TABLE products (
    product_id          INTEGER PRIMARY KEY,
    sku                 TEXT NOT NULL,
    product_name        TEXT NOT NULL,
    category_id         INTEGER REFERENCES categories(category_id),
    sub_category_id     INTEGER REFERENCES sub_categories(sub_category_id),
    sub_sub_category_id INTEGER REFERENCES sub_sub_categories(sub_sub_category_id),
    brand_id            INTEGER REFERENCES brands(brand_id),
    product_price_id    INTEGER REFERENCES product_prices(price_id) DEFERRABLE INITIALLY DEFERRED,
    rating              REAL,
    active              INTEGER DEFAULT 1,
    not_available       INTEGER DEFAULT 0
);

CREATE TABLE retention_interventions (
    intervention_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id            TEXT NOT NULL,
    customer_id          TEXT NOT NULL,
    created_at           TEXT DEFAULT (datetime('now')),
    churn_score_id       INTEGER REFERENCES churn_scores(score_id),
    churn_probability    REAL,
    risk_tier            TEXT,
    offer_type           TEXT,
    discount_pct         REAL,
    offer_message        TEXT,
    channel              TEXT,
    customer_ltv_usd     REAL,
    max_allowed_discount REAL,
    guardrail_passed     INTEGER DEFAULT 1,
    escalated_to_human   INTEGER DEFAULT 0,
    offer_status         TEXT DEFAULT 'pending',
    outcome_recorded_at  TEXT,
    revenue_recovered    REAL,
    langfuse_trace_id    TEXT,
    agent_cost_usd       REAL
);

CREATE TABLE sqlite_sequence(name,seq);

CREATE TABLE sub_categories (
    sub_category_id   INTEGER PRIMARY KEY,
    sub_category_name TEXT NOT NULL,
    category_id       INTEGER NOT NULL REFERENCES categories(category_id)
);

CREATE TABLE sub_sub_categories (
    sub_sub_category_id   INTEGER PRIMARY KEY,
    sub_sub_category_name TEXT NOT NULL,
    sub_category_id       INTEGER NOT NULL REFERENCES sub_categories(sub_category_id),
    category_id           INTEGER NOT NULL REFERENCES categories(category_id)
);

CREATE TABLE value_propositions (
    vp_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tier_name        TEXT NOT NULL,
    risk_level       TEXT NOT NULL,
    action_type      TEXT,
    message_template TEXT,
    discount_pct     REAL DEFAULT 0,
    channel          TEXT,
    priority         INTEGER DEFAULT 5
);

CREATE TABLE value_tiers (
    tier_id         TEXT PRIMARY KEY,
    tier_name       TEXT NOT NULL,
    threshold_type  TEXT,
    threshold_value REAL,
    description     TEXT,
    benefits        TEXT
);

CREATE TABLE vendors (
    vendor_id          INTEGER PRIMARY KEY,
    vendor_name        TEXT NOT NULL,
    vendor_description TEXT,
    vendor_contact_no  TEXT,
    vendor_address     TEXT,
    vendor_email       TEXT
);

CREATE INDEX idx_churn_scores_customer ON churn_scores(client_id, customer_id);

CREATE INDEX idx_churn_scores_tier     ON churn_scores(risk_tier);

CREATE INDEX idx_customers_email ON customers(customer_email);

CREATE INDEX idx_line_items_customer ON line_items(client_id, customer_id);

CREATE INDEX idx_line_items_order    ON line_items(client_id, order_id);

CREATE INDEX idx_line_items_product  ON line_items(product_id);

CREATE INDEX idx_orders_customer ON orders(client_id, customer_id);

CREATE INDEX idx_orders_date     ON orders(order_date);
