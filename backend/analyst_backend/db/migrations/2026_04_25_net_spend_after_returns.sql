-- ═══════════════════════════════════════════════════════════════════════════
-- 2026-04-25  Net spend after returns  (Phase 3 of churn-accuracy series)
-- ═══════════════════════════════════════════════════════════════════════════
-- Bug being fixed
-- ─────────────
-- Until today, mv_customer_features computed every spend metric (total,
-- avg, max, last-30/90/180-day) directly from `orders.order_value_usd`.
-- That column is the gross order amount at purchase time and is not
-- adjusted when a customer returns items. So:
--
--   • A customer who placed a $500 order and fully returned it (the
--     order_status='Returned' case we documented yesterday) was credited
--     with $500 of spend even though they kept nothing.
--
--   • A customer who returned ONE $200 item from a $500 order
--     (line_item.item_status='Returned' on a Completed order) was
--     credited with $500 even though they only kept $300 of merchandise.
--
-- Both cases inflate total_spend_usd, avg_order_value_usd, and the 30/90/
-- 180-day spend windows. That ripples into:
--   - rfm_monetary_score (NTILE on total_spend_usd) → wrong RFM segment
--   - customer_tier (Platinum/Gold/Silver/Bronze split by spend rank)
--   - is_high_value flag
--   - ltv_usd shown to the Retention Agent
--   - spend_velocity_ratio, aov_trend_pct (derived from these spends)
--
-- A customer who returns most of their orders is now scored as if they
-- kept everything. The model trains on inflated LTV signals, gets the
-- wrong tier assignment, and the Retention Agent picks the wrong
-- discount tier when generating outreach.
--
-- Fix
-- ───
-- 1. New CTE `order_returns` that sums the value of every line item
--    where item_status = 'Returned', grouped by (client_id, order_id).
--
-- 2. order_agg LEFT JOINs order_returns and computes a per-order
--    net value:
--        net_order_value =
--            GREATEST(0,
--                     order_value_usd
--                   - COALESCE(order_returns.returned_value, 0))
--    The GREATEST(0, …) clamps negative results that could arise if
--    line-item totals over-state the returned amount due to data
--    inconsistencies (e.g. discount rounding, partial refunds with
--    different unit prices on the line).
--
-- 3. All six spend calculations in order_agg switch from
--    `o.order_value_usd` → `(net_order_value)`:
--        - total_spend_usd
--        - avg_order_value_usd
--        - max_order_value_usd
--        - spend_last_30d_usd
--        - spend_last_90d_usd
--        - spend_last_180d_usd
--    Order count, first/last_order_date, and discount calculations are
--    intentionally NOT touched — those reflect engagement / order
--    placement, which DID happen even when items were later returned.
--
-- What this migration deliberately does NOT change
-- ─────────────────────────────────────────────────
-- • orders.order_status filter — still excludes only 'Cancelled'. A
--   fully-returned order naturally nets to $0 spend through the new
--   subtraction (return value == order value), so we don't need a
--   separate exclusion. Leaving 'Returned' orders in keeps the order
--   count and gap calculations honest about engagement.
--
-- • last_purchase_per_product — still uses the old order_status filter.
--   A separate cleanup if/when subscription refill alerts need to also
--   ignore returned line items.
--
-- • return_rate_pct in line_agg — still computed across all line items.
--   That metric is supposed to surface return behaviour, so leaving it
--   wide is correct.
--
-- Run order: open pgAdmin → Query Tool → walmart_crp database → paste
-- this whole file → execute. Wraps in a transaction so a partial
-- failure leaves the original MV untouched.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. Drop the current MV (also drops the 5 indexes via CASCADE) ──────────
DROP MATERIALIZED VIEW IF EXISTS mv_customer_features CASCADE;


-- ── 2. Recreate, with order_returns CTE + net-spend usage in order_agg ─────
CREATE MATERIALIZED VIEW mv_customer_features AS
 WITH client_ref AS (
         SELECT client_config.client_id,
            client_config.churn_window_days,
            client_config.login_window_days,
            client_config.min_repeat_orders,
            client_config.high_value_percentile,
            client_config.recent_order_gap_window,
            client_config.tier_method,
            client_config.custom_platinum_min,
            client_config.custom_gold_min,
            client_config.custom_silver_min,
            client_config.custom_bronze_min,
                CASE
                    WHEN client_config.reference_date_mode::text = 'fixed'::text AND client_config.reference_date IS NOT NULL THEN client_config.reference_date::timestamp with time zone
                    ELSE now()
                END AS ref_date
           FROM client_config
        ), order_returns AS (
         -- NEW (Phase 3 — net spend after returns)
         -- Sum the value of every returned line item per order. Used by
         -- order_agg below to compute net spend. We prefer
         -- final_line_total_usd when it's populated (already accounts for
         -- line-level discounts), and fall back to unit_price × quantity
         -- otherwise. Orders with no returned line items don't appear
         -- here, so the LEFT JOIN below produces NULL for them and the
         -- COALESCE turns it into 0 (= no return adjustment).
         SELECT li.client_id,
                li.order_id,
                SUM(COALESCE(li.final_line_total_usd, li.unit_price_usd * li.quantity, 0)) AS returned_value
           FROM line_items li
          WHERE li.item_status::text = 'Returned'::text
          GROUP BY li.client_id, li.order_id
        ), order_agg AS (
         SELECT o.client_id,
            o.customer_id,
            count(*) AS total_orders,
            min(o.order_date) AS first_order_date,
            max(o.order_date) AS last_order_date,
            EXTRACT(day FROM cr_1.ref_date - max(o.order_date))::integer AS days_since_last_order,
            -- ── Spend metrics now use NET value (gross − returned line items) ──
            -- Phase 3 (2026-04-25): clamp at 0 to handle data quirks where
            -- the line-item return total over-states the order_value_usd.
            sum(GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))) AS total_spend_usd,
            round(avg(GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))), 2) AS avg_order_value_usd,
            max(GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))) AS max_order_value_usd,
            COALESCE(sum(o.discount_usd), 0::numeric) AS total_discount_usd,
            sum(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '30 days'::interval) THEN GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))
                    ELSE 0::numeric
                END) AS spend_last_30d_usd,
            sum(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '90 days'::interval) THEN GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))
                    ELSE 0::numeric
                END) AS spend_last_90d_usd,
            sum(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '180 days'::interval) THEN GREATEST(0::numeric, o.order_value_usd - COALESCE(oret.returned_value, 0::numeric))
                    ELSE 0::numeric
                END) AS spend_last_180d_usd,
            count(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '30 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_30d,
            count(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '90 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_90d,
            count(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '180 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_180d,
            count(
                CASE
                    WHEN o.discount_usd > 0::numeric THEN 1
                    ELSE NULL::integer
                END) AS orders_with_discount,
            sum(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '90 days'::interval) THEN COALESCE(o.discount_usd, 0::numeric)
                    ELSE 0::numeric
                END) AS discount_last_90d_usd,
            count(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '90 days'::interval) AND o.discount_usd > 0::numeric THEN 1
                    ELSE NULL::integer
                END) AS orders_with_discount_last_90d
           FROM orders o
             JOIN client_ref cr_1 ON o.client_id::text = cr_1.client_id::text
             LEFT JOIN order_returns oret ON o.client_id::text = oret.client_id::text AND o.order_id::text = oret.order_id::text
          WHERE o.order_status::text <> 'Cancelled'::text
          GROUP BY o.client_id, o.customer_id, cr_1.ref_date
        ), order_gaps AS (
         SELECT gaps.client_id,
            gaps.customer_id,
            round(avg(gaps.gap_days), 1) AS avg_days_between_orders,
                CASE
                    WHEN count(gaps.gap_days) >= 2 THEN round(percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (gaps.gap_days::double precision))::numeric, 1)
                    ELSE NULL::numeric
                END AS median_days_between_orders
           FROM ( SELECT orders.client_id,
                    orders.customer_id,
                    EXTRACT(day FROM orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date)) AS gap_days
                   FROM orders
                  WHERE orders.order_status::text <> 'Cancelled'::text) gaps
          WHERE gaps.gap_days IS NOT NULL
          GROUP BY gaps.client_id, gaps.customer_id
        ), line_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT li.product_id) AS unique_products_purchased,
            round(avg(li.quantity), 2) AS avg_items_per_order,
            round(count(
                CASE
                    WHEN li.item_status::text = 'Returned'::text THEN 1
                    ELSE NULL::integer
                END)::numeric * 100.0 / NULLIF(count(*), 0)::numeric, 1) AS return_rate_pct,
            round(avg(
                CASE
                    WHEN o.order_date >= (cr_1.ref_date - '90 days'::interval) THEN li.quantity
                    ELSE NULL::integer
                END), 2) AS avg_items_per_order_last_90d
           FROM line_items li
             LEFT JOIN orders o ON li.order_id::text = o.order_id::text
             LEFT JOIN client_ref cr_1 ON li.client_id::text = cr_1.client_id::text
          GROUP BY li.client_id, li.customer_id
        ), cat_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT p.category_id) AS unique_categories_purchased
           FROM line_items li
             JOIN products p ON li.product_id = p.product_id
          GROUP BY li.client_id, li.customer_id
        ), review_agg AS (
         SELECT r.client_id,
            r.customer_id,
            count(*) AS total_reviews,
            round(avg(r.rating), 2) AS avg_rating,
            round(count(
                CASE
                    WHEN r.sentiment::text = 'positive'::text THEN 1
                    ELSE NULL::integer
                END)::numeric * 100.0 / NULLIF(count(*), 0)::numeric, 1) AS pct_positive_reviews,
            round(count(
                CASE
                    WHEN r.sentiment::text = 'negative'::text THEN 1
                    ELSE NULL::integer
                END)::numeric * 100.0 / NULLIF(count(*), 0)::numeric, 1) AS pct_negative_reviews,
            max(r.review_date) AS last_review_date,
            EXTRACT(day FROM cr_1.ref_date - max(r.review_date::timestamp with time zone))::integer AS days_since_last_review,
            count(
                CASE
                    WHEN r.review_date >= (cr_1.ref_date - '90 days'::interval)::date THEN 1
                    ELSE NULL::integer
                END) AS reviews_last_90d,
            round(avg(r.sentiment_score), 3) AS avg_sentiment_score,
            round(avg(
                CASE
                    WHEN r.review_date >= (cr_1.ref_date - '90 days'::interval)::date THEN r.sentiment_score
                    ELSE NULL::numeric
                END), 3) AS avg_sentiment_score_last_90d,
            count(
                CASE
                    WHEN r.sentiment::text = 'negative'::text AND r.review_date >= (cr_1.ref_date - '90 days'::interval)::date THEN 1
                    ELSE NULL::integer
                END) AS negative_reviews_last_90d,
            count(
                CASE
                    WHEN r.rating <= 2 AND r.review_date >= (cr_1.ref_date - '90 days'::interval)::date THEN 1
                    ELSE NULL::integer
                END) AS low_star_reviews_last_90d,
            EXTRACT(day FROM cr_1.ref_date - max(
                CASE
                    WHEN r.sentiment::text = 'negative'::text THEN r.review_date::timestamp with time zone
                    ELSE NULL::timestamp with time zone
                END))::integer AS days_since_last_negative_review
           FROM customer_reviews r
             JOIN client_ref cr_1 ON r.client_id::text = cr_1.client_id::text
          GROUP BY r.client_id, r.customer_id, cr_1.ref_date
        ), ticket_agg AS (
         SELECT t.client_id,
            t.customer_id,
            count(*) AS total_tickets,
            count(
                CASE
                    WHEN lower(t.status::text) = 'open'::text THEN 1
                    ELSE NULL::integer
                END) AS open_tickets,
            count(
                CASE
                    WHEN lower(t.priority::text) = 'critical'::text THEN 1
                    ELSE NULL::integer
                END) AS critical_tickets,
            round(avg(t.resolution_time_hrs), 1) AS avg_resolution_time_hrs,
            round(count(
                CASE
                    WHEN lower(t.status::text) = 'resolved'::text THEN 1
                    ELSE NULL::integer
                END)::numeric * 100.0 / NULLIF(count(*), 0)::numeric, 1) AS pct_tickets_resolved,
            count(
                CASE
                    WHEN t.opened_date >= (cr_1.ref_date - '30 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS tickets_last_30d,
            count(
                CASE
                    WHEN t.opened_date >= (cr_1.ref_date - '90 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS tickets_last_90d,
            count(
                CASE
                    WHEN lower(t.priority::text) = ANY (ARRAY['high'::text, 'critical'::text]) THEN 1
                    ELSE NULL::integer
                END) AS high_priority_tickets,
            count(
                CASE
                    WHEN lower(t.status::text) <> ALL (ARRAY['resolved'::text, 'closed'::text]) THEN 1
                    ELSE NULL::integer
                END) AS unresolved_tickets,
            EXTRACT(day FROM cr_1.ref_date - max(t.opened_date))::integer AS days_since_last_ticket
           FROM support_tickets t
             JOIN client_ref cr_1 ON t.client_id::text = cr_1.client_id::text
          GROUP BY t.client_id, t.customer_id, cr_1.ref_date
        ), rfm_scored AS (
         SELECT order_agg.client_id,
            order_agg.customer_id,
            6 - ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.days_since_last_order) AS rfm_recency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_orders) AS rfm_frequency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_spend_usd) AS rfm_monetary_score
           FROM order_agg
        ), last_purchase_per_product AS (
         SELECT li.client_id,
            li.customer_id,
            li.product_id,
            max(o.order_date) AS last_purchase_date
           FROM line_items li
             JOIN orders o ON li.order_id::text = o.order_id::text
          WHERE o.order_status::text <> 'Cancelled'::text
          GROUP BY li.client_id, li.customer_id, li.product_id
        ), subscription_agg AS (
         SELECT lp.client_id,
            lp.customer_id,
            count(DISTINCT lp.product_id) AS subscription_product_count,
            round(avg(sp.avg_refill_days), 1) AS avg_refill_cycle_days,
            max(EXTRACT(day FROM cr_1.ref_date - (lp.last_purchase_date + ((sp.avg_refill_days::text || ' days'::text)::interval))))::integer AS days_overdue_for_refill,
            sum(
                CASE
                    WHEN EXTRACT(day FROM cr_1.ref_date - lp.last_purchase_date) > (sp.avg_refill_days * 1.5) THEN 1
                    ELSE 0
                END) AS missed_refill_count
           FROM last_purchase_per_product lp
             JOIN vw_subscription_products sp ON lp.product_id = sp.product_id AND sp.is_subscription_product = true
             JOIN client_ref cr_1 ON lp.client_id::text = cr_1.client_id::text
          GROUP BY lp.client_id, lp.customer_id, cr_1.ref_date
        ), repeat_flag AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
                CASE
                    WHEN oa_1.total_orders >= cr_1.min_repeat_orders THEN 1
                    ELSE 0
                END AS is_repeat_customer
           FROM order_agg oa_1
             JOIN client_ref cr_1 ON oa_1.client_id::text = cr_1.client_id::text
        ), recent_gaps AS (
         SELECT ranked.client_id,
            ranked.customer_id,
            round(avg(ranked.gap_days), 1) AS recent_avg_gap_days
           FROM ( SELECT g.client_id,
                    g.customer_id,
                    g.gap_days,
                    row_number() OVER (PARTITION BY g.client_id, g.customer_id ORDER BY g.order_date DESC) AS rn,
                    cr_1.recent_order_gap_window
                   FROM ( SELECT orders.client_id,
                            orders.customer_id,
                            orders.order_date,
                            EXTRACT(day FROM orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date)) AS gap_days
                           FROM orders
                          WHERE orders.order_status::text <> 'Cancelled'::text) g
                     JOIN client_ref cr_1 ON g.client_id::text = cr_1.client_id::text
                  WHERE g.gap_days IS NOT NULL) ranked
          WHERE ranked.rn <= ranked.recent_order_gap_window
          GROUP BY ranked.client_id, ranked.customer_id
        ), spend_percentiles AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
            oa_1.total_spend_usd,
            percent_rank() OVER (PARTITION BY oa_1.client_id ORDER BY oa_1.total_spend_usd) * 100::double precision AS spend_pct_rank
           FROM order_agg oa_1
        ), tier_assignment AS (
         SELECT sp.client_id,
            sp.customer_id,
                CASE
                    WHEN cr_1.tier_method::text = 'quartile'::text THEN
                    CASE
                        WHEN sp.spend_pct_rank >= 75::double precision THEN 'Platinum'::text
                        WHEN sp.spend_pct_rank >= 50::double precision THEN 'Gold'::text
                        WHEN sp.spend_pct_rank >= 25::double precision THEN 'Silver'::text
                        ELSE 'Bronze'::text
                    END
                    ELSE
                    CASE
                        WHEN sp.total_spend_usd >= cr_1.custom_platinum_min THEN 'Platinum'::text
                        WHEN sp.total_spend_usd >= cr_1.custom_gold_min THEN 'Gold'::text
                        WHEN sp.total_spend_usd >= cr_1.custom_silver_min THEN 'Silver'::text
                        ELSE 'Bronze'::text
                    END
                END AS customer_tier,
                CASE
                    WHEN cr_1.tier_method::text = 'quartile'::text AND sp.spend_pct_rank >= cr_1.high_value_percentile::double precision THEN 1
                    WHEN cr_1.tier_method::text <> 'quartile'::text AND sp.total_spend_usd >= cr_1.custom_platinum_min THEN 1
                    ELSE 0
                END AS is_high_value
           FROM spend_percentiles sp
             JOIN client_ref cr_1 ON sp.client_id::text = cr_1.client_id::text
        )
 SELECT c.client_id,
    c.customer_id,
    EXTRACT(day FROM cr.ref_date - c.account_created_date::timestamp with time zone)::integer AS account_age_days,
    c.last_login_date,
    EXTRACT(day FROM cr.ref_date - c.last_login_date::timestamp with time zone)::integer
        AS days_since_last_login,
    oa.first_order_date,
    oa.last_order_date,
    oa.days_since_last_order,
    oa.total_orders,
    oa.orders_last_30d,
    oa.orders_last_90d,
    oa.orders_last_180d,
    COALESCE(og.avg_days_between_orders, 0::numeric) AS avg_days_between_orders,
    og.median_days_between_orders,
        CASE
            WHEN og.median_days_between_orders IS NULL THEN NULL::numeric
            ELSE round(abs(COALESCE(og.avg_days_between_orders, 0::numeric) - og.median_days_between_orders), 1)
        END AS order_gap_mean_median_diff,
    COALESCE(rg.recent_avg_gap_days, 0::numeric) AS recent_avg_gap_days,
    oa.total_spend_usd,
    oa.avg_order_value_usd,
    oa.max_order_value_usd,
    oa.spend_last_30d_usd,
    oa.spend_last_90d_usd,
    oa.spend_last_180d_usd,
    oa.total_discount_usd,
    round(oa.total_discount_usd * 100.0 / NULLIF(oa.total_spend_usd + oa.total_discount_usd, 0::numeric), 2) AS discount_rate_pct,
    oa.orders_with_discount,
    COALESCE(la.unique_products_purchased, 0::bigint) AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, 0::bigint) AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, 0::numeric) AS avg_items_per_order,
    COALESCE(la.return_rate_pct, 0::numeric) AS return_rate_pct,
    COALESCE(ra.total_reviews, 0::bigint) AS total_reviews,
    COALESCE(ra.avg_rating, 0::numeric) AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0::numeric) AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0::numeric) AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999) AS days_since_last_review,
    COALESCE(ra.reviews_last_90d, 0::bigint) AS reviews_last_90d,
    COALESCE(ra.avg_sentiment_score, 0::numeric) AS avg_sentiment_score,
    COALESCE(ra.avg_sentiment_score_last_90d, 0::numeric) AS avg_sentiment_score_last_90d,
    COALESCE(ra.negative_reviews_last_90d, 0::bigint) AS negative_reviews_last_90d,
    COALESCE(ra.low_star_reviews_last_90d, 0::bigint) AS low_star_reviews_last_90d,
    COALESCE(ra.days_since_last_negative_review, 9999) AS days_since_last_negative_review,
    COALESCE(ta.total_tickets, 0::bigint) AS total_tickets,
    COALESCE(ta.open_tickets, 0::bigint) AS open_tickets,
    COALESCE(ta.critical_tickets, 0::bigint) AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0::numeric) AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0::numeric) AS pct_tickets_resolved,
    COALESCE(ta.tickets_last_30d, 0::bigint) AS tickets_last_30d,
    COALESCE(ta.tickets_last_90d, 0::bigint) AS tickets_last_90d,
    COALESCE(ta.high_priority_tickets, 0::bigint) AS high_priority_tickets,
    COALESCE(ta.unresolved_tickets, 0::bigint) AS unresolved_tickets,
    COALESCE(ta.days_since_last_ticket, 9999) AS days_since_last_ticket,
    round(oa.spend_last_90d_usd / NULLIF(oa.orders_last_90d, 0)::numeric, 2) AS avg_order_value_last_90d_usd,
        CASE
            WHEN COALESCE(oa.avg_order_value_usd, 0::numeric) = 0::numeric THEN 0::numeric
            ELSE round((COALESCE(oa.spend_last_90d_usd / NULLIF(oa.orders_last_90d, 0)::numeric, 0::numeric) / oa.avg_order_value_usd - 1::numeric) * 100::numeric, 2)
        END AS aov_trend_pct,
    COALESCE(la.avg_items_per_order_last_90d, 0::numeric) AS avg_items_per_order_last_90d,
        CASE
            WHEN COALESCE(la.avg_items_per_order, 0::numeric) = 0::numeric THEN 0::numeric
            ELSE round((COALESCE(la.avg_items_per_order_last_90d, 0::numeric) / la.avg_items_per_order - 1::numeric) * 100::numeric, 2)
        END AS basket_size_trend_pct,
    COALESCE(oa.orders_with_discount_last_90d, 0::bigint) AS orders_with_discount_last_90d,
        CASE
            WHEN COALESCE(oa.orders_last_90d, 0::bigint) = 0 THEN 0::numeric
            ELSE round(oa.orders_with_discount_last_90d::numeric * 100.0 / oa.orders_last_90d::numeric, 2)
        END AS pct_orders_discounted_last_90d,
        CASE
            WHEN (COALESCE(oa.spend_last_90d_usd, 0::numeric) + COALESCE(oa.discount_last_90d_usd, 0::numeric)) = 0::numeric THEN 0::numeric
            ELSE round(oa.discount_last_90d_usd * 100.0 / (oa.spend_last_90d_usd + oa.discount_last_90d_usd), 2)
        END AS discount_rate_last_90d_pct,
        CASE
            WHEN COALESCE(og.avg_days_between_orders, 0::numeric) = 0::numeric THEN 0::numeric
            WHEN COALESCE(rg.recent_avg_gap_days, 0::numeric) = 0::numeric THEN 0::numeric
            ELSE round((rg.recent_avg_gap_days - og.avg_days_between_orders) / og.avg_days_between_orders * 100::numeric, 1)
        END AS order_gap_inflation_pct,
    COALESCE(round(oa.spend_last_30d_usd * 3.0 / NULLIF(oa.spend_last_90d_usd, 0::numeric), 3), 0::numeric) AS spend_velocity_ratio,
    oa.total_spend_usd AS ltv_usd,
    rf.rfm_recency_score,
    rf.rfm_frequency_score,
    rf.rfm_monetary_score,
    rf.rfm_recency_score + rf.rfm_frequency_score + rf.rfm_monetary_score AS rfm_total_score,
    COALESCE(rpf.is_repeat_customer, 0) AS is_repeat_customer,
    COALESCE(ta2.customer_tier, 'Bronze'::text) AS customer_tier,
    COALESCE(ta2.is_high_value, 0) AS is_high_value,
    COALESCE(sa.subscription_product_count, 0::bigint) AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, 0::numeric) AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0) AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, 0::bigint) AS missed_refill_count,
        CASE
            WHEN oa.days_since_last_order >= cr.churn_window_days
             AND COALESCE(
                   EXTRACT(day FROM cr.ref_date - c.last_login_date::timestamp with time zone)::integer,
                   9999
                 ) > cr.login_window_days
            THEN 1
            ELSE 0
        END AS churn_label,
    cr.ref_date AS computed_at
   FROM customers c
     JOIN client_ref cr ON c.client_id::text = cr.client_id::text
     JOIN order_agg oa ON c.client_id::text = oa.client_id::text AND c.customer_id::text = oa.customer_id::text
     JOIN rfm_scored rf ON c.client_id::text = rf.client_id::text AND c.customer_id::text = rf.customer_id::text
     LEFT JOIN order_gaps og ON c.client_id::text = og.client_id::text AND c.customer_id::text = og.customer_id::text
     LEFT JOIN recent_gaps rg ON c.client_id::text = rg.client_id::text AND c.customer_id::text = rg.customer_id::text
     LEFT JOIN line_agg la ON c.client_id::text = la.client_id::text AND c.customer_id::text = la.customer_id::text
     LEFT JOIN cat_agg ca ON c.client_id::text = ca.client_id::text AND c.customer_id::text = ca.customer_id::text
     LEFT JOIN review_agg ra ON c.client_id::text = ra.client_id::text AND c.customer_id::text = ra.customer_id::text
     LEFT JOIN ticket_agg ta ON c.client_id::text = ta.client_id::text AND c.customer_id::text = ta.customer_id::text
     LEFT JOIN repeat_flag rpf ON c.client_id::text = rpf.client_id::text AND c.customer_id::text = rpf.customer_id::text
     LEFT JOIN tier_assignment ta2 ON c.client_id::text = ta2.client_id::text AND c.customer_id::text = ta2.customer_id::text
     LEFT JOIN subscription_agg sa ON c.client_id::text = sa.client_id::text AND c.customer_id::text = sa.customer_id::text;


-- ── 3. Recreate the 5 indexes that CASCADE dropped ─────────────────────────
CREATE UNIQUE INDEX idx_mv_cf_pk      ON mv_customer_features (client_id, customer_id);
CREATE INDEX        idx_mv_cf_churn   ON mv_customer_features (churn_label, rfm_total_score DESC);
CREATE INDEX        idx_mv_cf_recency ON mv_customer_features (days_since_last_order DESC);
CREATE INDEX        idx_mv_cf_overdue ON mv_customer_features (days_overdue_for_refill DESC);
CREATE INDEX        idx_mv_cf_tier    ON mv_customer_features (customer_tier, is_high_value);


-- ── 4. Populate the MV so subsequent SELECTs return data immediately ───────
REFRESH MATERIALIZED VIEW mv_customer_features;

COMMIT;


-- ── Verification queries (run AFTER the COMMIT) ────────────────────────────
-- 1. Spot a customer who should have a return-adjusted spend — find the
--    biggest gap between their gross order_value_usd total and their net
--    total_spend_usd in the MV:
--      WITH gross AS (
--        SELECT customer_id, SUM(order_value_usd) AS gross_total
--        FROM orders
--        WHERE client_id = 'CLT-001' AND order_status <> 'Cancelled'
--        GROUP BY customer_id
--      )
--      SELECT g.customer_id,
--             g.gross_total,
--             mv.total_spend_usd  AS net_total,
--             g.gross_total - mv.total_spend_usd AS adjusted_off,
--             ROUND((g.gross_total - mv.total_spend_usd) * 100.0 / NULLIF(g.gross_total, 0), 1) AS pct_returned
--      FROM gross g
--      JOIN mv_customer_features mv USING (customer_id)
--      WHERE mv.client_id = 'CLT-001'
--      ORDER BY adjusted_off DESC
--      LIMIT 10;
--
-- 2. Sanity: for any customer with NO returned line items, gross == net:
--      SELECT customer_id, total_spend_usd
--      FROM mv_customer_features
--      WHERE client_id = 'CLT-001'
--        AND customer_id NOT IN (
--          SELECT DISTINCT li.customer_id
--          FROM line_items li
--          WHERE li.client_id = 'CLT-001' AND li.item_status = 'Returned'
--        )
--      LIMIT 5;
--    These should match the SUM(order_value_usd) in the orders table.
--
-- 3. Tier shift — see if any customer dropped a tier because their net
--    spend now puts them in a lower percentile:
--      SELECT customer_tier, COUNT(*)
--      FROM mv_customer_features
--      WHERE client_id = 'CLT-001'
--      GROUP BY customer_tier
--      ORDER BY customer_tier;
--    Compare against the pre-migration tier counts to see the redistribution.
