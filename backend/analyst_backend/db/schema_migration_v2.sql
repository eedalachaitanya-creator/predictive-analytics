-- ============================================================
-- schema_migration_v2.sql
-- Analyst Agent — Sprint 0 | Task 1.1
-- ============================================================
-- Adds the 38-feature materialized view (mv_customer_features)
-- that powers the churn prediction model and analyst agent.
--
-- Depends on: all tables from schema_postgresql.sql +
--             customer_reviews and support_tickets (add_new_tables.sql)
--
-- Run in pgAdmin4 Query Tool → F5
-- Then run Task 1.2: REFRESH MATERIALIZED VIEW mv_customer_features;
-- ============================================================

BEGIN;

-- ============================================================
-- STEP 1: Drop and recreate the materialized view
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mv_customer_features;

CREATE MATERIALIZED VIEW mv_customer_features AS

WITH

-- ── Order-level aggregations ──────────────────────────────────────────────────
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
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'  THEN o.order_value_usd ELSE 0 END) AS spend_last_30d_usd,
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN o.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
        SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '180 days' THEN o.order_value_usd ELSE 0 END) AS spend_last_180d_usd,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'  THEN 1 END)         AS orders_last_30d,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN 1 END)         AS orders_last_90d,
        COUNT(CASE WHEN o.order_date >= NOW() - INTERVAL '180 days' THEN 1 END)         AS orders_last_180d,
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                                 AS orders_with_discount
    FROM orders o
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY o.client_id, o.customer_id
),

-- ── Average gap between consecutive orders (order cadence) ────────────────────
order_gaps AS (
    SELECT
        client_id,
        customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1)  AS avg_days_between_orders
    FROM (
        SELECT
            client_id,
            customer_id,
            EXTRACT(DAY FROM order_date - LAG(order_date) OVER (
                PARTITION BY client_id, customer_id ORDER BY order_date
            ))::NUMERIC AS gap_days
        FROM orders
        WHERE order_status NOT IN ('Cancelled')
    ) gaps
    WHERE gap_days IS NOT NULL
    GROUP BY client_id, customer_id
),

-- ── Line-item aggregations ────────────────────────────────────────────────────
line_agg AS (
    SELECT
        li.client_id,
        li.customer_id,
        COUNT(DISTINCT li.product_id)                                                   AS unique_products_purchased,
        ROUND(AVG(li.quantity)::NUMERIC, 2)                                             AS avg_items_per_order,
        ROUND(
            COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
            / NULLIF(COUNT(*), 0), 1
        )                                                                               AS return_rate_pct
    FROM line_items li
    GROUP BY li.client_id, li.customer_id
),

-- ── Category breadth ──────────────────────────────────────────────────────────
cat_agg AS (
    SELECT
        li.client_id,
        li.customer_id,
        COUNT(DISTINCT p.category_id)  AS unique_categories_purchased
    FROM line_items li
    JOIN products p ON li.product_id = p.product_id
    GROUP BY li.client_id, li.customer_id
),

-- ── Customer review signals ───────────────────────────────────────────────────
review_agg AS (
    SELECT
        r.client_id,
        r.customer_id,
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

-- ── Support ticket signals ────────────────────────────────────────────────────
ticket_agg AS (
    SELECT
        t.client_id,
        t.customer_id,
        COUNT(*)                                                                        AS total_tickets,
        COUNT(CASE WHEN LOWER(t.status) = 'open'     THEN 1 END)                       AS open_tickets,
        COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END)                     AS critical_tickets,
        ROUND(AVG(t.resolution_time_hrs)::NUMERIC, 1)                                  AS avg_resolution_time_hrs,
        ROUND(COUNT(CASE WHEN LOWER(t.status) = 'resolved' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_tickets_resolved
    FROM support_tickets t
    GROUP BY t.client_id, t.customer_id
),

-- ── RFM scores via NTILE(5) ───────────────────────────────────────────────────
-- Higher score = better customer on each dimension
rfm_scored AS (
    SELECT
        client_id,
        customer_id,
        -- Recency: fewer days = better = higher score → order ASC, NTILE reverses
        6 - NTILE(5) OVER (PARTITION BY client_id ORDER BY days_since_last_order ASC)  AS rfm_recency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_orders ASC)               AS rfm_frequency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_spend_usd ASC)            AS rfm_monetary_score
    FROM order_agg
)

-- ── Final SELECT: 38 feature columns + 2 keys + 1 timestamp ──────────────────
SELECT
    -- Keys
    c.client_id,
    c.customer_id,

    -- [1]  Account
    EXTRACT(DAY FROM NOW() - c.account_created_date::TIMESTAMPTZ)::INT     AS account_age_days,

    -- [2-4] Recency
    oa.first_order_date,
    oa.last_order_date,
    oa.days_since_last_order,

    -- [5-10] Frequency
    oa.total_orders,
    oa.orders_last_30d,
    oa.orders_last_90d,
    oa.orders_last_180d,
    COALESCE(og.avg_days_between_orders, 0)                                 AS avg_days_between_orders,

    -- [11-17] Monetary
    oa.total_spend_usd,
    oa.avg_order_value_usd,
    oa.max_order_value_usd,
    oa.spend_last_30d_usd,
    oa.spend_last_90d_usd,
    oa.spend_last_180d_usd,
    oa.total_discount_usd,

    -- [18-19] Discount behaviour
    ROUND(oa.total_discount_usd * 100.0
          / NULLIF(oa.total_spend_usd + oa.total_discount_usd, 0)::NUMERIC, 2)
                                                                            AS discount_rate_pct,
    oa.orders_with_discount,

    -- [20-23] Basket behaviour
    COALESCE(la.unique_products_purchased, 0)                               AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, 0)                             AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, 0)                                     AS avg_items_per_order,
    COALESCE(la.return_rate_pct, 0)                                         AS return_rate_pct,

    -- [24-29] Review signals
    COALESCE(ra.total_reviews, 0)                                           AS total_reviews,
    COALESCE(ra.avg_rating, 0)                                              AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0)                                    AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0)                                    AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999)                               AS days_since_last_review,

    -- [30-34] Support ticket signals
    COALESCE(ta.total_tickets, 0)                                           AS total_tickets,
    COALESCE(ta.open_tickets, 0)                                            AS open_tickets,
    COALESCE(ta.critical_tickets, 0)                                        AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0)                                 AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0)                                    AS pct_tickets_resolved,

    -- [35-38] RFM scores
    oa.total_spend_usd                                                      AS ltv_usd,
    rf.rfm_recency_score,
    rf.rfm_frequency_score,
    rf.rfm_monetary_score,
    (rf.rfm_recency_score + rf.rfm_frequency_score + rf.rfm_monetary_score) AS rfm_total_score,

    -- Churn label: 1 = churned (90+ days inactive), 0 = active
    CASE WHEN oa.days_since_last_order >= 90 THEN 1 ELSE 0 END              AS churn_label,

    -- Metadata
    NOW()                                                                   AS computed_at

FROM customers c
JOIN  order_agg   oa  ON c.client_id = oa.client_id  AND c.customer_id = oa.customer_id
JOIN  rfm_scored  rf  ON c.client_id = rf.client_id  AND c.customer_id = rf.customer_id
LEFT JOIN order_gaps og ON c.client_id = og.client_id AND c.customer_id = og.customer_id
LEFT JOIN line_agg  la  ON c.client_id = la.client_id AND c.customer_id = la.customer_id
LEFT JOIN cat_agg   ca  ON c.client_id = ca.client_id AND c.customer_id = ca.customer_id
LEFT JOIN review_agg ra ON c.client_id = ra.client_id AND c.customer_id = ra.customer_id
LEFT JOIN ticket_agg ta ON c.client_id = ta.client_id AND c.customer_id = ta.customer_id;

-- ── Indexes for fast agent queries ────────────────────────────────────────────
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cf_pk
    ON mv_customer_features (client_id, customer_id);

CREATE INDEX IF NOT EXISTS idx_mv_cf_churn
    ON mv_customer_features (churn_label, rfm_total_score DESC);

CREATE INDEX IF NOT EXISTS idx_mv_cf_recency
    ON mv_customer_features (days_since_last_order DESC);

CREATE INDEX IF NOT EXISTS idx_mv_cf_spend
    ON mv_customer_features (total_spend_usd DESC);

COMMIT;

-- ============================================================
-- STEP 2: Populate the view (Task 1.2)
-- Run this line separately after the above succeeds:
-- ============================================================
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_features;

-- ============================================================
-- STEP 3: Verify — should show 38 feature columns + 2 keys
-- ============================================================
SELECT
    column_name,
    ordinal_position AS "#"
FROM information_schema.columns
WHERE table_name = 'mv_customer_features'
  AND table_schema = 'public'
ORDER BY ordinal_position;
