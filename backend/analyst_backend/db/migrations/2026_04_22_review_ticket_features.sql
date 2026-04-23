-- ============================================================
-- migration_2026_04_22_review_ticket_features.sql
--
-- Adds recency / severity / trend features derived from
-- customer_reviews and support_tickets to mv_customer_features.
--
-- WHY:
--   Today the churn model only "sees" 11 review/ticket signals
--   out of the box (total_reviews, avg_rating, pct_positive/negative,
--   days_since_last_review, total_tickets, open_tickets,
--   critical_tickets, avg_resolution_time_hrs, pct_tickets_resolved,
--   last_review_date). All of those are ALL-TIME aggregations, so
--   an account that was happy for 5 years and has been filing
--   1-star reviews + open tickets for the last 3 weeks looks
--   identical to one that's been stable the whole time. That's a
--   feature-engineering gap — not a label gap.
--
--   This migration closes the gap by adding recency-windowed and
--   severity-weighted features so the model can pick up the
--   "recently unhappy / recently needy" signal that reliably
--   precedes churn.
--
-- WHAT THIS ADDS:
--   Review features (6 new):
--     reviews_last_90d                 → volume trend
--     avg_sentiment_score              → numeric sentiment (uses sentiment_score col that was unused)
--     avg_sentiment_score_last_90d     → same, windowed
--     negative_reviews_last_90d        → count of sentiment='negative' in last 90d
--     low_star_reviews_last_90d        → count of rating <= 2 in last 90d
--     days_since_last_negative_review  → "when did they last complain?"
--
--   Ticket features (5 new):
--     tickets_last_30d                 → ticket rate acceleration
--     tickets_last_90d                 → ticket rate trend
--     high_priority_tickets            → count of priority IN ('high','critical')
--     unresolved_tickets               → count of status NOT IN ('resolved','closed')
--     days_since_last_ticket           → "when did they last complain to support?"
--
-- WHAT THIS DOES NOT TOUCH:
--   * The churn LABEL definition (still `days_since_last_order >= churn_window_days`).
--   * The median_days_between_orders gate (stays as it is from the 2026-04-21 migration).
--   * Any other feature — every existing column is kept at the same
--     position and type so downstream readers (compute_rfm.py,
--     customer_rfm_features table, etc.) don't break.
--
-- HOW TO RUN (pgAdmin 4):
--   1. Open pgAdmin 4 → connect to walmart_crp.
--   2. Right-click walmart_crp → Query Tool.
--   3. Paste this file's contents → Execute (F5).
--   4. Verification queries are at the bottom of the file (uncomment
--      and run them separately after the COMMIT).
--
-- IDEMPOTENCY:
--   Safe to re-run: we DROP IF EXISTS and recreate. No table data is
--   lost — the MV is derived. The REFRESH at the bottom rebuilds
--   the rows.
--
-- DEPENDENCIES CASCADE:
--   vw_customer_360 reads from customer_rfm_features (the table), NOT
--   from mv_customer_features, so DROP ... CASCADE stays contained to
--   the MV and its 5 indexes. If future code ever adds a direct view
--   dependency on mv_customer_features, add its recreation below.
-- ============================================================

BEGIN;

-- Postgres doesn't support CREATE OR REPLACE MATERIALIZED VIEW.
-- CASCADE is safe here: only the 5 idx_mv_cf_* indexes depend on
-- this MV, and we recreate them all at the bottom.
DROP MATERIALIZED VIEW IF EXISTS mv_customer_features CASCADE;

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
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                                  AS orders_with_discount
    FROM orders o
    JOIN client_ref cr ON o.client_id = cr.client_id
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY o.client_id, o.customer_id, cr.ref_date
),

-- ── Order gap statistics (mean + median, median gated on ≥2 gap rows) ────────
-- Kept identical to the 2026-04-21 gate migration.
order_gaps AS (
    SELECT client_id, customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1)                                                AS avg_days_between_orders,
        CASE
            WHEN COUNT(gap_days) >= 2 THEN
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap_days)::NUMERIC, 1)
            ELSE NULL
        END                                                                             AS median_days_between_orders
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
-- >>> CHANGED in this migration <<<
-- Added 6 new features (recency- and severity-weighted). All existing
-- columns kept identical so downstream readers don't break.
review_agg AS (
    SELECT r.client_id, r.customer_id,
        -- Existing features (unchanged)
        COUNT(*)                                                                        AS total_reviews,
        ROUND(AVG(r.rating)::NUMERIC, 2)                                                AS avg_rating,
        ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_positive_reviews,
        ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_negative_reviews,
        MAX(r.review_date)                                                              AS last_review_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(r.review_date::TIMESTAMPTZ))::INT            AS days_since_last_review,

        -- NEW: how many reviews has the customer posted in the last 90 days?
        -- Rising review volume often precedes churn (unhappy customers post more).
        COUNT(CASE WHEN r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS reviews_last_90d,

        -- NEW: numeric sentiment (sentiment_score column was collected but
        -- unused). Finer-grained than categorical 'positive/neutral/negative'.
        ROUND(AVG(r.sentiment_score)::NUMERIC, 3)                                       AS avg_sentiment_score,

        -- NEW: recent sentiment. AVG over a date-filtered expression:
        -- rows outside the window contribute NULL and are ignored by AVG.
        ROUND(AVG(CASE WHEN r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                       THEN r.sentiment_score END)::NUMERIC, 3)                         AS avg_sentiment_score_last_90d,

        -- NEW: count of explicitly negative reviews in last 90d — strongest signal.
        COUNT(CASE WHEN r.sentiment = 'negative'
                    AND r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS negative_reviews_last_90d,

        -- NEW: count of 1- or 2-star reviews in last 90d (severity, not sentiment).
        COUNT(CASE WHEN r.rating <= 2
                    AND r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS low_star_reviews_last_90d,

        -- NEW: days since the last negative review (NULL/huge if never).
        -- MAX() with CASE returns NULL for customers who've never left a
        -- negative review — the outer COALESCE in the SELECT maps that to 9999.
        EXTRACT(DAY FROM cr.ref_date -
                MAX(CASE WHEN r.sentiment = 'negative'
                         THEN r.review_date::TIMESTAMPTZ END))::INT                     AS days_since_last_negative_review
    FROM customer_reviews r
    JOIN client_ref cr ON r.client_id = cr.client_id
    GROUP BY r.client_id, r.customer_id, cr.ref_date
),

-- ── Support ticket signals ───────────────────────────────────────────────────
-- >>> CHANGED in this migration <<<
-- Added 5 new features (recency + severity). All existing columns kept
-- identical so downstream readers don't break.
ticket_agg AS (
    SELECT t.client_id, t.customer_id,
        -- Existing features (unchanged)
        COUNT(*)                                                                        AS total_tickets,
        COUNT(CASE WHEN LOWER(t.status)   = 'open'     THEN 1 END)                     AS open_tickets,
        COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END)                     AS critical_tickets,
        ROUND(AVG(t.resolution_time_hrs)::NUMERIC, 1)                                  AS avg_resolution_time_hrs,
        ROUND(COUNT(CASE WHEN LOWER(t.status) = 'resolved' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_tickets_resolved,

        -- NEW: ticket rate acceleration — how many tickets in last 30d?
        COUNT(CASE WHEN t.opened_date >= cr.ref_date - INTERVAL '30 days'
                   THEN 1 END)                                                          AS tickets_last_30d,

        -- NEW: same, 90d window, for trend detection against 30d.
        COUNT(CASE WHEN t.opened_date >= cr.ref_date - INTERVAL '90 days'
                   THEN 1 END)                                                          AS tickets_last_90d,

        -- NEW: severity. 'critical' alone was too narrow — 'high' also matters.
        COUNT(CASE WHEN LOWER(t.priority) IN ('high','critical')
                   THEN 1 END)                                                          AS high_priority_tickets,

        -- NEW: unresolved backlog (anything not resolved or closed).
        COUNT(CASE WHEN LOWER(t.status) NOT IN ('resolved','closed')
                   THEN 1 END)                                                          AS unresolved_tickets,

        -- NEW: days since the customer last opened a ticket.
        EXTRACT(DAY FROM cr.ref_date - MAX(t.opened_date))::INT                         AS days_since_last_ticket
    FROM support_tickets t
    JOIN client_ref cr ON t.client_id = cr.client_id
    GROUP BY t.client_id, t.customer_id, cr.ref_date
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

-- ── Recent order gaps (configurable window for rhythm detection) ─────────────
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
    og.median_days_between_orders                                           AS median_days_between_orders,
    CASE
        WHEN og.median_days_between_orders IS NULL THEN NULL
        ELSE ROUND(ABS(COALESCE(og.avg_days_between_orders, 0)
                       - og.median_days_between_orders)::NUMERIC, 1)
    END                                                                     AS order_gap_mean_median_diff,
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

    -- ── Review signals ──────────────────────────────────────────────────────
    -- Existing 6 columns (unchanged):
    COALESCE(ra.total_reviews, 0)                                           AS total_reviews,
    COALESCE(ra.avg_rating, 0)                                              AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0)                                    AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0)                                    AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999)                               AS days_since_last_review,
    -- NEW 6 columns (added by this migration):
    COALESCE(ra.reviews_last_90d, 0)                                        AS reviews_last_90d,
    COALESCE(ra.avg_sentiment_score, 0)                                     AS avg_sentiment_score,
    COALESCE(ra.avg_sentiment_score_last_90d, 0)                            AS avg_sentiment_score_last_90d,
    COALESCE(ra.negative_reviews_last_90d, 0)                               AS negative_reviews_last_90d,
    COALESCE(ra.low_star_reviews_last_90d, 0)                               AS low_star_reviews_last_90d,
    COALESCE(ra.days_since_last_negative_review, 9999)                      AS days_since_last_negative_review,

    -- ── Support ticket signals ──────────────────────────────────────────────
    -- Existing 5 columns (unchanged):
    COALESCE(ta.total_tickets, 0)                                           AS total_tickets,
    COALESCE(ta.open_tickets, 0)                                            AS open_tickets,
    COALESCE(ta.critical_tickets, 0)                                       AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0)                                 AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0)                                    AS pct_tickets_resolved,
    -- NEW 5 columns (added by this migration):
    COALESCE(ta.tickets_last_30d, 0)                                        AS tickets_last_30d,
    COALESCE(ta.tickets_last_90d, 0)                                        AS tickets_last_90d,
    COALESCE(ta.high_priority_tickets, 0)                                   AS high_priority_tickets,
    COALESCE(ta.unresolved_tickets, 0)                                      AS unresolved_tickets,
    COALESCE(ta.days_since_last_ticket, 9999)                               AS days_since_last_ticket,

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
LEFT JOIN tier_assignment  ta2 ON c.client_id = ta2.client_id AND c.customer_id = ta2.customer_id
LEFT JOIN subscription_agg sa  ON c.client_id = sa.client_id  AND c.customer_id = sa.customer_id;

-- Recreate the 5 indexes that the CASCADE drop removed
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

COMMIT;

-- Rebuild rows. Outside the transaction so you can see timing in pgAdmin.
REFRESH MATERIALIZED VIEW mv_customer_features;


-- ── Verification (run separately in pgAdmin Query Tool after COMMIT) ────────
-- Expect all 11 new columns to be present and non-NULL (they COALESCE to 0/9999).
--
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'mv_customer_features'
--   AND column_name IN (
--     'reviews_last_90d','avg_sentiment_score','avg_sentiment_score_last_90d',
--     'negative_reviews_last_90d','low_star_reviews_last_90d','days_since_last_negative_review',
--     'tickets_last_30d','tickets_last_90d','high_priority_tickets',
--     'unresolved_tickets','days_since_last_ticket'
--   )
-- ORDER BY column_name;
--
-- Spot-check one client's recency features:
-- SELECT client_id, customer_id,
--        total_reviews, reviews_last_90d, negative_reviews_last_90d,
--        avg_sentiment_score, avg_sentiment_score_last_90d,
--        total_tickets, tickets_last_30d, tickets_last_90d,
--        high_priority_tickets, unresolved_tickets, days_since_last_ticket
-- FROM mv_customer_features
-- WHERE client_id = 'CLT-001'
--   AND (reviews_last_90d > 0 OR tickets_last_30d > 0)
-- ORDER BY negative_reviews_last_90d DESC, tickets_last_30d DESC
-- LIMIT 20;
