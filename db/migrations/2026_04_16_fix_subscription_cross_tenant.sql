-- =============================================================================
-- Migration: fix cross-tenant leak in vw_subscription_products and
--            mv_customer_features.subscription_agg
-- Date:       2026-04-16
--
-- PROBLEM
--   After the schema was refactored to composite PK (client_id, product_id),
--   the view vw_subscription_products still aggregated and joined only on
--   product_id. That meant:
--     - behaviour_flag counted buyers of the same product_id across ALL tenants
--     - mv_customer_features.subscription_agg joined last_purchase_per_product
--       to vw_subscription_products on product_id only
--   → Subscription features in mv_customer_features could leak across clients
--     whenever two tenants happened to share a product_id string.
--
-- FIX
--   1. Rebuild vw_subscription_products so every internal CTE carries client_id
--      and every JOIN uses (client_id, product_id).
--   2. Rebuild mv_customer_features with the subscription_agg JOIN tightened
--      to include lp.client_id = sp.client_id.
--
-- SAFE TO RE-RUN: wrapped in a transaction. The MV is recreated WITHOUT the
-- WITH NO DATA clause, so it is populated immediately inside the transaction.
-- =============================================================================

BEGIN;

-- ── 1. Drop the MV (and its dependent indexes come with it) ─────────────────
DROP MATERIALIZED VIEW IF EXISTS public.mv_customer_features;

-- ── 2. Drop the old view so we can change its column list ───────────────────
DROP VIEW IF EXISTS public.vw_subscription_products;

-- ── 3. Recreate vw_subscription_products with client_id threaded through ────
CREATE VIEW public.vw_subscription_products AS
 WITH keyword_flag AS (
     SELECT p.client_id,
            p.product_id,
            p.product_name,
            CASE
                WHEN lower(p.product_name::text) ~~ ANY (ARRAY[
                    '%refill%'::text, '%subscription%'::text, '%monthly%'::text,
                    '%daily%'::text, '%vitamin%'::text, '%supplement%'::text,
                    '%tablet%'::text, '%capsule%'::text, '%mg %'::text,
                    '% mg%'::text, '%dose%'::text, '%pill%'::text,
                    '%softgel%'::text, '%gummy%'::text, '%probiotic%'::text,
                    '%omega%'::text, '%protein%'::text, '%insulin%'::text,
                    '%inhaler%'::text, '%drops%'::text, '%syrup%'::text,
                    '%pack of%'::text, '%count)%'::text, '%supply%'::text
                ]) THEN true
                ELSE false
            END AS is_subscription_by_name
       FROM public.products p
 ), repeat_counts AS (
     SELECT li.client_id,
            li.customer_id,
            li.product_id,
            count(*) AS purchase_count
       FROM public.line_items li
      GROUP BY li.client_id, li.customer_id, li.product_id
 ), purchase_gaps AS (
     SELECT li.client_id,
            li.customer_id,
            li.product_id,
            o.order_date,
            EXTRACT(day FROM (o.order_date - lag(o.order_date) OVER (
                PARTITION BY li.client_id, li.customer_id, li.product_id
                ORDER BY o.order_date))) AS gap_days
       FROM public.line_items li
       JOIN public.orders o
         ON li.client_id::text = o.client_id::text
        AND li.order_id::text = o.order_id::text
 ), avg_gaps AS (
     SELECT client_id, customer_id, product_id,
            avg(gap_days) AS avg_gap
       FROM purchase_gaps
      WHERE gap_days IS NOT NULL
      GROUP BY client_id, customer_id, product_id
 ), behaviour_flag AS (
     SELECT li.client_id,
            li.product_id,
            count(DISTINCT li.customer_id) AS total_buyers,
            count(DISTINCT
                CASE WHEN rc.purchase_count >= 3 THEN li.customer_id END
            ) AS repeat_buyers,
            round(avg(ag.avg_gap), 1) AS avg_refill_days,
            round(stddev(ag.avg_gap), 1) AS stddev_refill_days
       FROM public.line_items li
  LEFT JOIN repeat_counts rc
         ON li.client_id::text = rc.client_id::text
        AND li.customer_id::text = rc.customer_id::text
        AND li.product_id = rc.product_id
  LEFT JOIN avg_gaps ag
         ON li.client_id::text = ag.client_id::text
        AND li.customer_id::text = ag.customer_id::text
        AND li.product_id = ag.product_id
      GROUP BY li.client_id, li.product_id
 ), combined AS (
     SELECT p.client_id,
            p.product_id,
            p.product_name,
            p.category_id,
            kf.is_subscription_by_name,
            COALESCE(bf.repeat_buyers, 0::bigint)     AS repeat_buyers,
            COALESCE(bf.total_buyers, 0::bigint)      AS total_buyers,
            COALESCE(bf.avg_refill_days, 0::numeric)  AS avg_refill_days,
            COALESCE(bf.stddev_refill_days, 0::numeric) AS stddev_refill_days,
            CASE
                WHEN bf.total_buyers > 0
                 AND (bf.repeat_buyers::numeric * 1.0 / bf.total_buyers::numeric) >= 0.30
                 AND COALESCE(bf.stddev_refill_days, 999::numeric) < 15::numeric
                THEN true
                ELSE false
            END AS is_subscription_by_behaviour
       FROM public.products p
  LEFT JOIN keyword_flag kf
         ON p.client_id::text = kf.client_id::text
        AND p.product_id = kf.product_id
  LEFT JOIN behaviour_flag bf
         ON p.client_id::text = bf.client_id::text
        AND p.product_id = bf.product_id
 )
 SELECT client_id,
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
            WHEN is_subscription_by_name AND is_subscription_by_behaviour THEN 'both'::text
            WHEN is_subscription_by_name THEN 'keyword'::text
            WHEN is_subscription_by_behaviour THEN 'behaviour'::text
            ELSE 'none'::text
        END AS detection_source
   FROM combined;


-- ── 4. Recreate mv_customer_features with the composite-key JOIN fix ────────
CREATE MATERIALIZED VIEW public.mv_customer_features AS
 WITH client_ref AS (
         SELECT client_config.client_id,
            client_config.churn_window_days,
            client_config.min_repeat_orders,
            client_config.high_value_percentile,
            client_config.recent_order_gap_window,
            client_config.tier_method,
            client_config.custom_platinum_min,
            client_config.custom_gold_min,
            client_config.custom_silver_min,
            client_config.custom_bronze_min,
                CASE
                    WHEN (((client_config.reference_date_mode)::text = 'fixed'::text) AND (client_config.reference_date IS NOT NULL)) THEN (client_config.reference_date)::timestamp with time zone
                    ELSE now()
                END AS ref_date
           FROM public.client_config
        ), order_agg AS (
         SELECT o.client_id,
            o.customer_id,
            count(*) AS total_orders,
            min(o.order_date) AS first_order_date,
            max(o.order_date) AS last_order_date,
            (EXTRACT(day FROM (cr_1.ref_date - max(o.order_date))))::integer AS days_since_last_order,
            sum(o.order_value_usd) AS total_spend_usd,
            round(avg(o.order_value_usd), 2) AS avg_order_value_usd,
            max(o.order_value_usd) AS max_order_value_usd,
            COALESCE(sum(o.discount_usd), (0)::numeric) AS total_discount_usd,
            sum(CASE WHEN (o.order_date >= (cr_1.ref_date - '30 days'::interval))  THEN o.order_value_usd ELSE (0)::numeric END) AS spend_last_30d_usd,
            sum(CASE WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval))  THEN o.order_value_usd ELSE (0)::numeric END) AS spend_last_90d_usd,
            sum(CASE WHEN (o.order_date >= (cr_1.ref_date - '180 days'::interval)) THEN o.order_value_usd ELSE (0)::numeric END) AS spend_last_180d_usd,
            count(CASE WHEN (o.order_date >= (cr_1.ref_date - '30 days'::interval))  THEN 1 END) AS orders_last_30d,
            count(CASE WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval))  THEN 1 END) AS orders_last_90d,
            count(CASE WHEN (o.order_date >= (cr_1.ref_date - '180 days'::interval)) THEN 1 END) AS orders_last_180d,
            count(CASE WHEN (o.discount_usd > (0)::numeric) THEN 1 END) AS orders_with_discount
           FROM (public.orders o
             JOIN client_ref cr_1 ON (((o.client_id)::text = (cr_1.client_id)::text)))
          WHERE ((o.order_status)::text <> 'Cancelled'::text)
          GROUP BY o.client_id, o.customer_id, cr_1.ref_date
        ), order_gaps AS (
         SELECT gaps.client_id,
            gaps.customer_id,
            round(avg(gaps.gap_days), 1) AS avg_days_between_orders,
            round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((gaps.gap_days)::double precision)))::numeric, 1) AS median_days_between_orders
           FROM ( SELECT orders.client_id,
                    orders.customer_id,
                    EXTRACT(day FROM (orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date))) AS gap_days
                   FROM public.orders
                  WHERE ((orders.order_status)::text <> 'Cancelled'::text)) gaps
          WHERE (gaps.gap_days IS NOT NULL)
          GROUP BY gaps.client_id, gaps.customer_id
        ), line_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT li.product_id) AS unique_products_purchased,
            round(avg(li.quantity), 2) AS avg_items_per_order,
            round((((count(CASE WHEN ((li.item_status)::text = 'Returned'::text) THEN 1 END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS return_rate_pct
           FROM public.line_items li
          GROUP BY li.client_id, li.customer_id
        ), cat_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT p.category_id) AS unique_categories_purchased
           FROM (public.line_items li
             JOIN public.products p ON ((((li.client_id)::text = (p.client_id)::text) AND (li.product_id = p.product_id))))
          GROUP BY li.client_id, li.customer_id
        ), review_agg AS (
         SELECT r.client_id,
            r.customer_id,
            count(*) AS total_reviews,
            round(avg(r.rating), 2) AS avg_rating,
            round((((count(CASE WHEN ((r.sentiment)::text = 'positive'::text) THEN 1 END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_positive_reviews,
            round((((count(CASE WHEN ((r.sentiment)::text = 'negative'::text) THEN 1 END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_negative_reviews,
            max(r.review_date) AS last_review_date,
            (EXTRACT(day FROM (cr_1.ref_date - max((r.review_date)::timestamp with time zone))))::integer AS days_since_last_review
           FROM (public.customer_reviews r
             JOIN client_ref cr_1 ON (((r.client_id)::text = (cr_1.client_id)::text)))
          GROUP BY r.client_id, r.customer_id, cr_1.ref_date
        ), ticket_agg AS (
         SELECT t.client_id,
            t.customer_id,
            count(*) AS total_tickets,
            count(CASE WHEN (lower((t.status)::text) = 'open'::text) THEN 1 END) AS open_tickets,
            count(CASE WHEN (lower((t.priority)::text) = 'critical'::text) THEN 1 END) AS critical_tickets,
            round(avg(t.resolution_time_hrs), 1) AS avg_resolution_time_hrs,
            round((((count(CASE WHEN (lower((t.status)::text) = 'resolved'::text) THEN 1 END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_tickets_resolved
           FROM public.support_tickets t
          GROUP BY t.client_id, t.customer_id
        ), rfm_scored AS (
         SELECT order_agg.client_id,
            order_agg.customer_id,
            (6 - ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.days_since_last_order)) AS rfm_recency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_orders) AS rfm_frequency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_spend_usd) AS rfm_monetary_score
           FROM order_agg
        ), last_purchase_per_product AS (
         SELECT li.client_id,
            li.customer_id,
            li.product_id,
            max(o.order_date) AS last_purchase_date
           FROM (public.line_items li
             JOIN public.orders o ON ((((li.client_id)::text = (o.client_id)::text) AND ((li.order_id)::text = (o.order_id)::text))))
          WHERE ((o.order_status)::text <> 'Cancelled'::text)
          GROUP BY li.client_id, li.customer_id, li.product_id
        ), subscription_agg AS (
         SELECT lp.client_id,
            lp.customer_id,
            count(DISTINCT lp.product_id) AS subscription_product_count,
            round(avg(sp.avg_refill_days), 1) AS avg_refill_cycle_days,
            (max(EXTRACT(day FROM (cr_1.ref_date - (lp.last_purchase_date + (((sp.avg_refill_days)::text || ' days'::text))::interval)))))::integer AS days_overdue_for_refill,
            sum(CASE WHEN (EXTRACT(day FROM (cr_1.ref_date - lp.last_purchase_date)) > (sp.avg_refill_days * 1.5)) THEN 1 ELSE 0 END) AS missed_refill_count
           FROM ((last_purchase_per_product lp
             JOIN public.vw_subscription_products sp
               ON (((lp.client_id)::text = (sp.client_id)::text)     -- ★ FIX: composite join
                   AND (lp.product_id = sp.product_id)
                   AND (sp.is_subscription_product = true)))
             JOIN client_ref cr_1 ON (((lp.client_id)::text = (cr_1.client_id)::text)))
          GROUP BY lp.client_id, lp.customer_id, cr_1.ref_date
        ), repeat_flag AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
                CASE WHEN (oa_1.total_orders >= cr_1.min_repeat_orders) THEN 1 ELSE 0 END AS is_repeat_customer
           FROM (order_agg oa_1
             JOIN client_ref cr_1 ON (((oa_1.client_id)::text = (cr_1.client_id)::text)))
        ), recent_gaps AS (
         SELECT ranked.client_id,
            ranked.customer_id,
            round(avg(ranked.gap_days), 1) AS recent_avg_gap_days
           FROM ( SELECT g.client_id,
                    g.customer_id,
                    g.gap_days,
                    row_number() OVER (PARTITION BY g.client_id, g.customer_id ORDER BY g.order_date DESC) AS rn,
                    cr_1.recent_order_gap_window
                   FROM (( SELECT orders.client_id,
                            orders.customer_id,
                            orders.order_date,
                            EXTRACT(day FROM (orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date))) AS gap_days
                           FROM public.orders
                          WHERE ((orders.order_status)::text <> 'Cancelled'::text)) g
                     JOIN client_ref cr_1 ON (((g.client_id)::text = (cr_1.client_id)::text)))
                  WHERE (g.gap_days IS NOT NULL)) ranked
          WHERE (ranked.rn <= ranked.recent_order_gap_window)
          GROUP BY ranked.client_id, ranked.customer_id
        ), spend_percentiles AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
            oa_1.total_spend_usd,
            (percent_rank() OVER (PARTITION BY oa_1.client_id ORDER BY oa_1.total_spend_usd) * (100)::double precision) AS spend_pct_rank
           FROM order_agg oa_1
        ), tier_assignment AS (
         SELECT sp.client_id,
            sp.customer_id,
                CASE
                    WHEN ((cr_1.tier_method)::text = 'quartile'::text) THEN
                        CASE
                            WHEN (sp.spend_pct_rank >= (cr_1.high_value_percentile)::double precision) THEN 'Platinum'::text
                            WHEN (sp.spend_pct_rank >= (50)::double precision) THEN 'Gold'::text
                            WHEN (sp.spend_pct_rank >= (25)::double precision) THEN 'Silver'::text
                            ELSE 'Bronze'::text
                        END
                    ELSE
                        CASE
                            WHEN (sp.total_spend_usd >= cr_1.custom_platinum_min) THEN 'Platinum'::text
                            WHEN (sp.total_spend_usd >= cr_1.custom_gold_min)     THEN 'Gold'::text
                            WHEN (sp.total_spend_usd >= cr_1.custom_silver_min)   THEN 'Silver'::text
                            ELSE 'Bronze'::text
                        END
                END AS customer_tier,
                CASE
                    WHEN (((cr_1.tier_method)::text = 'quartile'::text) AND (sp.spend_pct_rank >= (cr_1.high_value_percentile)::double precision)) THEN 1
                    WHEN (((cr_1.tier_method)::text <> 'quartile'::text) AND (sp.total_spend_usd >= cr_1.custom_platinum_min)) THEN 1
                    ELSE 0
                END AS is_high_value
           FROM (spend_percentiles sp
             JOIN client_ref cr_1 ON (((sp.client_id)::text = (cr_1.client_id)::text)))
        )
 SELECT c.client_id,
    c.customer_id,
    (EXTRACT(day FROM (cr.ref_date - (c.account_created_date)::timestamp with time zone)))::integer AS account_age_days,
    oa.first_order_date,
    oa.last_order_date,
    oa.days_since_last_order,
    oa.total_orders,
    oa.orders_last_30d,
    oa.orders_last_90d,
    oa.orders_last_180d,
    COALESCE(og.avg_days_between_orders, (0)::numeric) AS avg_days_between_orders,
    COALESCE(og.median_days_between_orders, (0)::numeric) AS median_days_between_orders,
    round(abs((COALESCE(og.avg_days_between_orders, (0)::numeric) - COALESCE(og.median_days_between_orders, (0)::numeric))), 1) AS order_gap_mean_median_diff,
    COALESCE(rg.recent_avg_gap_days, (0)::numeric) AS recent_avg_gap_days,
    oa.total_spend_usd,
    oa.avg_order_value_usd,
    oa.max_order_value_usd,
    oa.spend_last_30d_usd,
    oa.spend_last_90d_usd,
    oa.spend_last_180d_usd,
    oa.total_discount_usd,
    round(((oa.total_discount_usd * 100.0) / NULLIF((oa.total_spend_usd + oa.total_discount_usd), (0)::numeric)), 2) AS discount_rate_pct,
    oa.orders_with_discount,
    COALESCE(la.unique_products_purchased, (0)::bigint) AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, (0)::bigint) AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, (0)::numeric) AS avg_items_per_order,
    COALESCE(la.return_rate_pct, (0)::numeric) AS return_rate_pct,
    COALESCE(ra.total_reviews, (0)::bigint) AS total_reviews,
    COALESCE(ra.avg_rating, (0)::numeric) AS avg_rating,
    COALESCE(ra.pct_positive_reviews, (0)::numeric) AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, (0)::numeric) AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999) AS days_since_last_review,
    COALESCE(ta.total_tickets, (0)::bigint) AS total_tickets,
    COALESCE(ta.open_tickets, (0)::bigint) AS open_tickets,
    COALESCE(ta.critical_tickets, (0)::bigint) AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, (0)::numeric) AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, (0)::numeric) AS pct_tickets_resolved,
    oa.total_spend_usd AS ltv_usd,
    rf.rfm_recency_score,
    rf.rfm_frequency_score,
    rf.rfm_monetary_score,
    ((rf.rfm_recency_score + rf.rfm_frequency_score) + rf.rfm_monetary_score) AS rfm_total_score,
    COALESCE(rpf.is_repeat_customer, 0) AS is_repeat_customer,
    COALESCE(ta2.customer_tier, 'Bronze'::text) AS customer_tier,
    COALESCE(ta2.is_high_value, 0) AS is_high_value,
    COALESCE(sa.subscription_product_count, (0)::bigint) AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, (0)::numeric) AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0) AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, (0)::bigint) AS missed_refill_count,
    CASE WHEN (oa.days_since_last_order >= cr.churn_window_days) THEN 1 ELSE 0 END AS churn_label,
    cr.ref_date AS computed_at
   FROM ((((((((((((public.customers c
     JOIN client_ref cr ON (((c.client_id)::text = (cr.client_id)::text)))
     JOIN order_agg oa ON ((((c.client_id)::text = (oa.client_id)::text) AND ((c.customer_id)::text = (oa.customer_id)::text))))
     JOIN rfm_scored rf ON ((((c.client_id)::text = (rf.client_id)::text) AND ((c.customer_id)::text = (rf.customer_id)::text))))
     LEFT JOIN order_gaps og ON ((((c.client_id)::text = (og.client_id)::text) AND ((c.customer_id)::text = (og.customer_id)::text))))
     LEFT JOIN recent_gaps rg ON ((((c.client_id)::text = (rg.client_id)::text) AND ((c.customer_id)::text = (rg.customer_id)::text))))
     LEFT JOIN line_agg la ON ((((c.client_id)::text = (la.client_id)::text) AND ((c.customer_id)::text = (la.customer_id)::text))))
     LEFT JOIN cat_agg ca ON ((((c.client_id)::text = (ca.client_id)::text) AND ((c.customer_id)::text = (ca.customer_id)::text))))
     LEFT JOIN review_agg ra ON ((((c.client_id)::text = (ra.client_id)::text) AND ((c.customer_id)::text = (ra.customer_id)::text))))
     LEFT JOIN ticket_agg ta ON ((((c.client_id)::text = (ta.client_id)::text) AND ((c.customer_id)::text = (ta.customer_id)::text))))
     LEFT JOIN repeat_flag rpf ON ((((c.client_id)::text = (rpf.client_id)::text) AND ((c.customer_id)::text = (rpf.customer_id)::text))))
     LEFT JOIN tier_assignment ta2 ON ((((c.client_id)::text = (ta2.client_id)::text) AND ((c.customer_id)::text = (ta2.customer_id)::text))))
     LEFT JOIN subscription_agg sa ON ((((c.client_id)::text = (sa.client_id)::text) AND ((c.customer_id)::text = (sa.customer_id)::text))));


-- ── 5. Recreate the MV's indexes ────────────────────────────────────────────
CREATE UNIQUE INDEX idx_mv_cf_pk      ON public.mv_customer_features USING btree (client_id, customer_id);
CREATE INDEX        idx_mv_cf_churn   ON public.mv_customer_features USING btree (churn_label, rfm_total_score DESC);
CREATE INDEX        idx_mv_cf_overdue ON public.mv_customer_features USING btree (days_overdue_for_refill DESC);
CREATE INDEX        idx_mv_cf_recency ON public.mv_customer_features USING btree (days_since_last_order DESC);
CREATE INDEX        idx_mv_cf_tier    ON public.mv_customer_features USING btree (customer_tier, is_high_value);

COMMIT;

-- =============================================================================
-- Sanity checks — run these AFTER the COMMIT to confirm the fix worked
-- =============================================================================

-- Should return rows with a client_id column present
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_name = 'vw_subscription_products'
 ORDER BY ordinal_position;

-- Should return at least one row per client; no NULL client_ids
SELECT client_id, COUNT(*) AS customer_rows
  FROM public.mv_customer_features
 GROUP BY client_id
 ORDER BY client_id;
