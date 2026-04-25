-- ============================================================
-- migration_2026_04_22_behavioral_trend_features.sql
--
-- STEP 2 of the churn-feature expansion plan.
-- Adds behavioral-trend features derived from orders + line_items
-- to mv_customer_features.
--
-- WHY:
--   Step 1 (2026_04_22_review_ticket_features.sql) added recency-
--   windowed review & ticket signals. Step 2 closes the remaining
--   gap on the *transaction* side. Today the model can see totals
--   and windowed sums (orders_last_90d, spend_last_90d_usd, etc.)
--   but cannot see *trends* — i.e. "is this customer's basket
--   shrinking?" or "is their last 30 days below their 90-day rate?"
--   Tree models don't compute ratios/divisions on the fly; they
--   split on thresholds. Pre-computing these ratios gives the
--   model clean 1-feature splits for directional change.
--
-- WHAT THIS ADDS (9 new features):
--
--   Order-value behavior (2):
--     avg_order_value_last_90d_usd  → windowed AOV
--     aov_trend_pct                 → (last_90d_AOV / all_time_AOV − 1) × 100
--                                     Negative = basket value shrinking.
--
--   Basket-size behavior (2):
--     avg_items_per_order_last_90d  → windowed basket
--     basket_size_trend_pct         → same trend math on items-per-order
--
--   Discount dependency (3):
--     orders_with_discount_last_90d → raw count
--     pct_orders_discounted_last_90d→ % of recent orders that used a discount
--     discount_rate_last_90d_pct    → % of recent spend that was discount
--
--   Pace deceleration (2):
--     order_gap_inflation_pct       → (recent_gap − longterm_gap)/longterm_gap × 100
--                                     > 50% = clearly slowing down
--     spend_velocity_ratio          → spend_last_30d × 3 / spend_last_90d
--                                     1.0 = flat, <1 = decelerating, >1 = accelerating
--
-- WHAT THIS DOES NOT TOUCH:
--   * The churn LABEL definition.
--   * Any base table — every column needed already exists on orders
--     and line_items. No ALTER TABLE, no CREATE TABLE.
--   * Any Step-1 feature. Step-1 additions are preserved verbatim.
--   * Any existing column anywhere. Strict additive change.
--
-- PREREQUISITE:
--   Run 2026_04_22_review_ticket_features.sql FIRST. This file
--   assumes the Step-1 feature set is already present in the MV
--   definition (we re-declare all of it here because Postgres
--   requires DROP + full CREATE on materialized views). If Step 1
--   has not been applied, this file still produces a correct MV —
--   it just means you get Step 1 + Step 2 in one shot.
--
-- HOW TO RUN (pgAdmin 4):
--   1. Open pgAdmin 4 → connect to walmart_crp.
--   2. Right-click walmart_crp → Query Tool.
--   3. Paste this file's contents → Execute (F5).
--   4. Verification queries are at the bottom (uncomment and run
--      them separately after the COMMIT).
--
-- IDEMPOTENCY:
--   Safe to re-run: we DROP IF EXISTS and recreate. No table data
--   is lost. The REFRESH at the bottom rebuilds the rows.
--
-- DEPENDENCIES CASCADE:
--   vw_customer_360 reads from customer_rfm_features (the table),
--   not from mv_customer_features. Only 5 idx_mv_cf_* indexes
--   depend on this MV, and we recreate them all below.
-- ============================================================

BEGIN;

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
-- >>> CHANGED in Step 2 <<<
-- Added 2 new windowed sums so we can compute discount dependency and
-- AOV trend downstream. All existing columns kept identical.
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
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                                  AS orders_with_discount,

        -- NEW (Step 2): total discount $ in last 90 days. Paired with
        -- spend_last_90d_usd to produce discount_rate_last_90d_pct.
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'
                 THEN COALESCE(o.discount_usd, 0) ELSE 0 END)                           AS discount_last_90d_usd,

        -- NEW (Step 2): count of recent orders that actually used a discount.
        -- Paired with orders_last_90d to produce pct_orders_discounted_last_90d.
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'
                    AND o.discount_usd > 0 THEN 1 END)                                  AS orders_with_discount_last_90d
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
-- >>> CHANGED in Step 2 <<<
-- Added 1 new feature (avg_items_per_order_last_90d) which requires
-- joining orders (for order_date) and client_ref (for ref_date).
-- The JOINs are LEFT to preserve the row-set of the original line_agg
-- — no line_item is filtered out, so unique_products_purchased,
-- avg_items_per_order, and return_rate_pct continue to produce the
-- same values as before.
line_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT li.product_id)                                                   AS unique_products_purchased,
        ROUND(AVG(li.quantity)::NUMERIC, 2)                                             AS avg_items_per_order,
        ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS return_rate_pct,

        -- NEW (Step 2): basket size in the last 90 days only.
        -- AVG(CASE ... THEN li.quantity END) → rows outside the window
        -- return NULL, which AVG ignores. Customers with no recent
        -- line items get NULL → COALESCE to 0 in the final SELECT.
        ROUND(AVG(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'
                       THEN li.quantity END)::NUMERIC, 2)                               AS avg_items_per_order_last_90d
    FROM line_items li
    LEFT JOIN orders o  ON li.order_id = o.order_id
    LEFT JOIN client_ref cr ON li.client_id = cr.client_id
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

-- ── Customer review signals (Step 1 additions preserved verbatim) ────────────
review_agg AS (
    SELECT r.client_id, r.customer_id,
        COUNT(*)                                                                        AS total_reviews,
        ROUND(AVG(r.rating)::NUMERIC, 2)                                                AS avg_rating,
        ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_positive_reviews,
        ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_negative_reviews,
        MAX(r.review_date)                                                              AS last_review_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(r.review_date::TIMESTAMPTZ))::INT            AS days_since_last_review,
        COUNT(CASE WHEN r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS reviews_last_90d,
        ROUND(AVG(r.sentiment_score)::NUMERIC, 3)                                       AS avg_sentiment_score,
        ROUND(AVG(CASE WHEN r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                       THEN r.sentiment_score END)::NUMERIC, 3)                         AS avg_sentiment_score_last_90d,
        COUNT(CASE WHEN r.sentiment = 'negative'
                    AND r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS negative_reviews_last_90d,
        COUNT(CASE WHEN r.rating <= 2
                    AND r.review_date >= (cr.ref_date - INTERVAL '90 days')::DATE
                   THEN 1 END)                                                          AS low_star_reviews_last_90d,
        EXTRACT(DAY FROM cr.ref_date -
                MAX(CASE WHEN r.sentiment = 'negative'
                         THEN r.review_date::TIMESTAMPTZ END))::INT                     AS days_since_last_negative_review
    FROM customer_reviews r
    JOIN client_ref cr ON r.client_id = cr.client_id
    GROUP BY r.client_id, r.customer_id, cr.ref_date
),

-- ── Support ticket signals (Step 1 additions preserved verbatim) ─────────────
ticket_agg AS (
    SELECT t.client_id, t.customer_id,
        COUNT(*)                                                                        AS total_tickets,
        COUNT(CASE WHEN LOWER(t.status)   = 'open'     THEN 1 END)                     AS open_tickets,
        COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END)                     AS critical_tickets,
        ROUND(AVG(t.resolution_time_hrs)::NUMERIC, 1)                                  AS avg_resolution_time_hrs,
        ROUND(COUNT(CASE WHEN LOWER(t.status) = 'resolved' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1)                                                 AS pct_tickets_resolved,
        COUNT(CASE WHEN t.opened_date >= cr.ref_date - INTERVAL '30 days'
                   THEN 1 END)                                                          AS tickets_last_30d,
        COUNT(CASE WHEN t.opened_date >= cr.ref_date - INTERVAL '90 days'
                   THEN 1 END)                                                          AS tickets_last_90d,
        COUNT(CASE WHEN LOWER(t.priority) IN ('high','critical')
                   THEN 1 END)                                                          AS high_priority_tickets,
        COUNT(CASE WHEN LOWER(t.status) NOT IN ('resolved','closed')
                   THEN 1 END)                                                          AS unresolved_tickets,
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
            -- True quartile bands: top 25% = Platinum, next 25% = Gold, etc.
            -- high_value_percentile governs is_high_value (below), not tier cutoffs.
            CASE WHEN sp.spend_pct_rank >= 75 THEN 'Platinum'
                 WHEN sp.spend_pct_rank >= 50 THEN 'Gold'
                 WHEN sp.spend_pct_rank >= 25 THEN 'Silver'
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

    -- ── Review signals (Step 1) ─────────────────────────────────────────────
    COALESCE(ra.total_reviews, 0)                                           AS total_reviews,
    COALESCE(ra.avg_rating, 0)                                              AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0)                                    AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0)                                    AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999)                               AS days_since_last_review,
    COALESCE(ra.reviews_last_90d, 0)                                        AS reviews_last_90d,
    COALESCE(ra.avg_sentiment_score, 0)                                     AS avg_sentiment_score,
    COALESCE(ra.avg_sentiment_score_last_90d, 0)                            AS avg_sentiment_score_last_90d,
    COALESCE(ra.negative_reviews_last_90d, 0)                               AS negative_reviews_last_90d,
    COALESCE(ra.low_star_reviews_last_90d, 0)                               AS low_star_reviews_last_90d,
    COALESCE(ra.days_since_last_negative_review, 9999)                      AS days_since_last_negative_review,

    -- ── Support ticket signals (Step 1) ─────────────────────────────────────
    COALESCE(ta.total_tickets, 0)                                           AS total_tickets,
    COALESCE(ta.open_tickets, 0)                                            AS open_tickets,
    COALESCE(ta.critical_tickets, 0)                                        AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0)                                 AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0)                                    AS pct_tickets_resolved,
    COALESCE(ta.tickets_last_30d, 0)                                        AS tickets_last_30d,
    COALESCE(ta.tickets_last_90d, 0)                                        AS tickets_last_90d,
    COALESCE(ta.high_priority_tickets, 0)                                   AS high_priority_tickets,
    COALESCE(ta.unresolved_tickets, 0)                                      AS unresolved_tickets,
    COALESCE(ta.days_since_last_ticket, 9999)                               AS days_since_last_ticket,

    -- ── NEW: Behavioral trend features (Step 2 — 9 columns) ─────────────────

    -- Order value: windowed AOV + trend vs all-time
    -- avg_order_value_last_90d_usd = spend_last_90d / orders_last_90d
    -- Zero orders in window → NULL → COALESCE(0).
    ROUND(
        (oa.spend_last_90d_usd / NULLIF(oa.orders_last_90d, 0))::NUMERIC, 2
    )                                                                        AS avg_order_value_last_90d_usd,

    -- (last_90d_AOV / all_time_AOV − 1) × 100.
    -- If all-time AOV is 0 (zero non-cancelled spend), return 0 — no trend to report.
    -- If no orders in last 90d, numerator is 0, ratio = -100 (strongly negative signal).
    CASE
        WHEN COALESCE(oa.avg_order_value_usd, 0) = 0 THEN 0
        ELSE ROUND((
            (COALESCE(oa.spend_last_90d_usd / NULLIF(oa.orders_last_90d, 0), 0)
             / oa.avg_order_value_usd - 1) * 100
        )::NUMERIC, 2)
    END                                                                      AS aov_trend_pct,

    -- Basket size: windowed items-per-order + trend vs all-time
    COALESCE(la.avg_items_per_order_last_90d, 0)                             AS avg_items_per_order_last_90d,

    CASE
        WHEN COALESCE(la.avg_items_per_order, 0) = 0 THEN 0
        ELSE ROUND((
            (COALESCE(la.avg_items_per_order_last_90d, 0) / la.avg_items_per_order - 1) * 100
        )::NUMERIC, 2)
    END                                                                      AS basket_size_trend_pct,

    -- Discount dependency
    COALESCE(oa.orders_with_discount_last_90d, 0)                            AS orders_with_discount_last_90d,

    -- % of recent orders that used any discount
    CASE
        WHEN COALESCE(oa.orders_last_90d, 0) = 0 THEN 0
        ELSE ROUND(
            (oa.orders_with_discount_last_90d * 100.0 / oa.orders_last_90d)::NUMERIC, 2
        )
    END                                                                      AS pct_orders_discounted_last_90d,

    -- % of recent GMV that came off the top (discount share of pre-discount sell)
    CASE
        WHEN COALESCE(oa.spend_last_90d_usd, 0) + COALESCE(oa.discount_last_90d_usd, 0) = 0 THEN 0
        ELSE ROUND(
            (oa.discount_last_90d_usd * 100.0
             / (oa.spend_last_90d_usd + oa.discount_last_90d_usd))::NUMERIC, 2
        )
    END                                                                      AS discount_rate_last_90d_pct,

    -- Pace deceleration: recent gap vs long-term gap, as % change.
    -- Requires both recent_avg_gap_days and avg_days_between_orders to exist
    -- (i.e. customer has ≥ 2 non-cancelled orders). Otherwise 0.
    CASE
        WHEN COALESCE(og.avg_days_between_orders, 0) = 0 THEN 0
        WHEN COALESCE(rg.recent_avg_gap_days, 0)    = 0 THEN 0
        ELSE ROUND((
            (rg.recent_avg_gap_days - og.avg_days_between_orders)
            / og.avg_days_between_orders * 100
        )::NUMERIC, 1)
    END                                                                      AS order_gap_inflation_pct,

    -- Spend velocity: spend_last_30d normalized to a 30-day share of spend_last_90d.
    -- Interpretation: 1.0 = spending evenly across the 90d, <1 = slowing, >1 = picking up.
    -- spend_last_90d_usd = 0 → COALESCE to 0 (zero recent activity).
    COALESCE(
        ROUND((oa.spend_last_30d_usd * 3.0 / NULLIF(oa.spend_last_90d_usd, 0))::NUMERIC, 3),
        0
    )                                                                        AS spend_velocity_ratio,

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
-- Expect all 9 new Step-2 columns plus all 11 Step-1 columns to exist.
--
-- 1) Confirm the 9 new columns exist:
--
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'mv_customer_features'
--   AND column_name IN (
--     'avg_order_value_last_90d_usd','aov_trend_pct',
--     'avg_items_per_order_last_90d','basket_size_trend_pct',
--     'orders_with_discount_last_90d','pct_orders_discounted_last_90d',
--     'discount_rate_last_90d_pct','order_gap_inflation_pct',
--     'spend_velocity_ratio'
--   )
-- ORDER BY column_name;
--
-- 2) Spot-check behavioral-trend features on real customers:
--
-- SELECT client_id, customer_id,
--        avg_order_value_usd, avg_order_value_last_90d_usd, aov_trend_pct,
--        avg_items_per_order, avg_items_per_order_last_90d, basket_size_trend_pct,
--        orders_with_discount_last_90d, pct_orders_discounted_last_90d,
--        discount_rate_last_90d_pct,
--        avg_days_between_orders, recent_avg_gap_days, order_gap_inflation_pct,
--        spend_last_30d_usd, spend_last_90d_usd, spend_velocity_ratio,
--        churn_label
-- FROM mv_customer_features
-- WHERE client_id = 'CLT-001'
--   AND total_orders >= 3
-- ORDER BY aov_trend_pct ASC NULLS LAST
-- LIMIT 20;
--
-- 3) Sanity: customers with the biggest pace deceleration should skew churn=1.
--
-- SELECT
--   CASE WHEN order_gap_inflation_pct >= 50 THEN 'decelerating ≥50%' ELSE 'normal' END AS bucket,
--   churn_label,
--   COUNT(*) AS customers
-- FROM mv_customer_features
-- WHERE client_id = 'CLT-001' AND avg_days_between_orders > 0
-- GROUP BY 1, 2
-- ORDER BY 1, 2;
