-- ============================================================
-- schema_migration_v3.sql
-- Analyst Agent — Subscription Churn Detection
-- ============================================================
-- Adds subscription product detection (from product names +
-- purchase behaviour) and personalised outreach tracking.
--
-- Run AFTER schema_migration_v2.sql
-- ============================================================

BEGIN;

-- ============================================================
-- STEP 1: NEW TABLES
-- ============================================================

-- 21. Customer Purchase Cycles
-- Tracks each customer's personal refill pattern per product.
-- One row per customer per subscription product.
-- ─────────────────────────────────────────────────────────────
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
-- Stores every personalised message sent to a customer
-- when they miss a refill window or show churn signals.
-- ─────────────────────────────────────────────────────────────
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
-- STEP 2: SUBSCRIPTION PRODUCT DETECTION VIEW
-- Automatically tags products as subscription-type using
-- two signals: keyword matching + repeat purchase behaviour.
-- ============================================================

CREATE OR REPLACE VIEW vw_subscription_products AS
WITH

-- Signal 1: Keyword match on product name
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

-- Signal 2a: Count repeat buyers per product
repeat_counts AS (
    SELECT customer_id, product_id, COUNT(*) AS purchase_count
    FROM line_items
    GROUP BY customer_id, product_id
),

-- Signal 2b: Compute gap between consecutive purchases
--            FIX Bug 1 — LAG must live in its own subquery;
--            AVG cannot wrap a window function directly
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

-- Signal 2c: Average gap per customer per product (now safe to aggregate)
avg_gaps AS (
    SELECT customer_id, product_id,
        AVG(gap_days) AS avg_gap
    FROM purchase_gaps
    WHERE gap_days IS NOT NULL
    GROUP BY customer_id, product_id
),

-- Signal 2: Combine repeat buyer counts and gap averages per product
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

-- Combine both signals
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

-- ============================================================
-- STEP 3: REBUILD mv_customer_features WITH SUBSCRIPTION SIGNALS
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mv_customer_features;

CREATE MATERIALIZED VIEW mv_customer_features AS

WITH

-- ── Existing CTEs ─────────────────────────────────────────────────────────────
order_agg AS (
    SELECT
        o.client_id,
        o.customer_id,
        COUNT(*)                                                                        AS total_orders,
        MIN(o.order_date)                                                               AS first_order_date,
        MAX(o.order_date)                                                               AS last_order_date,
        EXTRACT(DAY FROM NOW() - MAX(o.order_date))::INT                                AS days_since_last_order,
        SUM(o.order_value_usd)                                                          AS total_spend_usd,
        ROUND(AVG(o.order_value_usd)::NUMERIC, 2)                                       AS avg_order_value_usd,
        MAX(o.order_value_usd)                                                          AS max_order_value_usd,
        COALESCE(SUM(o.discount_usd), 0)                                                AS total_discount_usd,
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_30d_usd,
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_90d_usd,
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '180 days'
                 THEN o.order_value_usd ELSE 0 END)                                     AS spend_last_180d_usd,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'  THEN 1 END)         AS orders_last_30d,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN 1 END)         AS orders_last_90d,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '180 days' THEN 1 END)         AS orders_last_180d,
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                                 AS orders_with_discount
    FROM orders o
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY o.client_id, o.customer_id
),

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

line_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT li.product_id)                                                   AS unique_products_purchased,
        ROUND(AVG(li.quantity)::NUMERIC, 2)                                             AS avg_items_per_order,
        ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS return_rate_pct
    FROM line_items li
    GROUP BY li.client_id, li.customer_id
),

cat_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT p.category_id)  AS unique_categories_purchased
    FROM line_items li
    JOIN products p ON li.product_id = p.product_id
    GROUP BY li.client_id, li.customer_id
),

review_agg AS (
    SELECT r.client_id, r.customer_id,
        COUNT(*)                                                                        AS total_reviews,
        ROUND(AVG(r.rating)::NUMERIC, 2)                                                AS avg_rating,
        ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_positive_reviews,
        ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_negative_reviews,
        MAX(r.review_date)                                                              AS last_review_date,
        EXTRACT(DAY FROM NOW() - MAX(r.review_date::TIMESTAMPTZ))::INT                 AS days_since_last_review
    FROM customer_reviews r
    GROUP BY r.client_id, r.customer_id
),

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

rfm_scored AS (
    SELECT client_id, customer_id,
        6 - NTILE(5) OVER (PARTITION BY client_id ORDER BY days_since_last_order ASC)  AS rfm_recency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_orders ASC)               AS rfm_frequency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_spend_usd ASC)            AS rfm_monetary_score
    FROM order_agg
),

-- ── FIX Bug 2 — Pre-aggregate MAX(order_date) per customer per product FIRST,
--    then aggregate across products. Nested aggregates are not allowed.
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
        -- FIX Bug 3 — cast NUMERIC to TEXT before || for interval string construction
        MAX(
            EXTRACT(DAY FROM
                NOW() - (lp.last_purchase_date::TIMESTAMPTZ
                         + (sp.avg_refill_days::TEXT || ' days')::INTERVAL)
            )
        )::INT                                                                          AS days_overdue_for_refill,
        SUM(
            CASE WHEN EXTRACT(DAY FROM NOW() - lp.last_purchase_date::TIMESTAMPTZ)
                      > sp.avg_refill_days * 1.5
                 THEN 1 ELSE 0 END
        )                                                                               AS missed_refill_count
    FROM last_purchase_per_product lp
    JOIN vw_subscription_products sp
         ON lp.product_id = sp.product_id
        AND sp.is_subscription_product = TRUE
    GROUP BY lp.client_id, lp.customer_id
)

-- ── Final SELECT ──────────────────────────────────────────────────────────────
SELECT
    c.client_id,
    c.customer_id,

    -- Account
    EXTRACT(DAY FROM NOW() - c.account_created_date::TIMESTAMPTZ)::INT     AS account_age_days,

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

    -- Subscription signals
    COALESCE(sa.subscription_product_count, 0)                              AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, 0)                                   AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0)                                 AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, 0)                                     AS missed_refill_count,

    -- Churn label
    CASE WHEN oa.days_since_last_order >= 90 THEN 1 ELSE 0 END              AS churn_label,

    NOW()                                                                   AS computed_at

FROM customers c
JOIN  order_agg    oa  ON c.client_id = oa.client_id  AND c.customer_id = oa.customer_id
JOIN  rfm_scored   rf  ON c.client_id = rf.client_id  AND c.customer_id = rf.customer_id
LEFT JOIN order_gaps    og ON c.client_id = og.client_id AND c.customer_id = og.customer_id
LEFT JOIN line_agg      la ON c.client_id = la.client_id AND c.customer_id = la.customer_id
LEFT JOIN cat_agg       ca ON c.client_id = ca.client_id AND c.customer_id = ca.customer_id
LEFT JOIN review_agg    ra ON c.client_id = ra.client_id AND c.customer_id = ra.customer_id
LEFT JOIN ticket_agg    ta ON c.client_id = ta.client_id AND c.customer_id = ta.customer_id
LEFT JOIN subscription_agg sa ON c.client_id = sa.client_id AND c.customer_id = sa.customer_id;

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE UNIQUE INDEX idx_mv_cf_pk
    ON mv_customer_features (client_id, customer_id);
CREATE INDEX idx_mv_cf_churn
    ON mv_customer_features (churn_label, rfm_total_score DESC);
CREATE INDEX idx_mv_cf_recency
    ON mv_customer_features (days_since_last_order DESC);
CREATE INDEX idx_mv_cf_overdue
    ON mv_customer_features (days_overdue_for_refill DESC);

COMMIT;

-- ============================================================
-- STEP 4: Populate the view
-- FIX Bug 4 — Use plain REFRESH (not CONCURRENTLY) on first run.
-- CONCURRENTLY requires existing data + unique index already populated.
-- After first run, future refreshes can use CONCURRENTLY.
-- ============================================================
REFRESH MATERIALIZED VIEW mv_customer_features;

-- ============================================================
-- STEP 5: Verify — should list all 48 columns
-- ============================================================
SELECT column_name, ordinal_position AS "#"
FROM information_schema.columns
WHERE table_name   = 'mv_customer_features'
  AND table_schema = 'public'
ORDER BY ordinal_position;
