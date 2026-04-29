-- ====================================================================
-- 2026_04_29_widen_model_version.sql
-- Widens churn_scores.model_version and batch_run_id from varchar(20)
-- to varchar(80) so the audit-trail strings produced by predict.py
-- (e.g. "xgboost_2026-04-28_auc0.880") fit without truncation errors.
--
-- Why this migration is needed:
--   The audit fix that derives model_version from training metadata
--   (predict.py:run_scoring_pipeline, audit issue #4) generates
--   strings up to ~30 characters. The DB column was 20. Insertions
--   from the pipeline subprocess fail with StringDataRightTruncation
--   and silently roll back, leaving churn_scores empty.
--
-- vw_customer_360 (and the at-risk view that depends on it) references
-- churn_scores.model_version, so we have to drop+recreate the view
-- around the column type change.
-- ====================================================================

BEGIN;

-- Drop the dependent views (will recreate at the bottom).
DROP VIEW IF EXISTS public.vw_at_risk_customers;
DROP VIEW IF EXISTS public.vw_customer_360;

-- Widen the columns.
ALTER TABLE churn_scores ALTER COLUMN model_version TYPE varchar(80);
ALTER TABLE churn_scores ALTER COLUMN batch_run_id  TYPE varchar(80);

-- Recreate vw_customer_360 (verbatim from pg_get_viewdef snapshot).
CREATE VIEW public.vw_customer_360 AS
 SELECT c.client_id,
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
   FROM ((public.customers c
     LEFT JOIN public.customer_rfm_features r
       ON ((((c.client_id)::text = (r.client_id)::text)
            AND ((c.customer_id)::text = (r.customer_id)::text))))
     LEFT JOIN LATERAL (
         SELECT s.score_id,
                s.client_id,
                s.customer_id,
                s.scored_at,
                s.churn_probability,
                s.risk_tier,
                s.churn_label_simulated,
                s.driver_1,
                s.driver_2,
                s.driver_3,
                s.model_version,
                s.batch_run_id
           FROM public.churn_scores s
          WHERE (((s.client_id)::text = (c.client_id)::text)
                 AND ((s.customer_id)::text = (c.customer_id)::text))
          ORDER BY s.scored_at DESC
          LIMIT 1
     ) cs ON (true));

-- Recreate vw_at_risk_customers (depends on vw_customer_360).
CREATE VIEW public.vw_at_risk_customers AS
 SELECT client_id,
    customer_id,
    customer_name,
    customer_email,
    customer_phone,
    account_created_date,
    registration_channel,
    state,
    city,
    preferred_device,
    email_opt_in,
    sms_opt_in,
    days_since_last_order,
    last_order_date,
    total_orders,
    orders_last_90d,
    avg_order_value_usd,
    total_spend_usd,
    ltv_usd,
    rfm_total_score,
    rfm_segment,
    customer_tier,
    return_rate_pct,
    account_age_days,
    churn_probability,
    risk_tier,
    driver_1,
    driver_2,
    driver_3,
    last_scored_at
   FROM public.vw_customer_360
  WHERE ((risk_tier)::text = ANY ((ARRAY['HIGH'::character varying, 'MEDIUM'::character varying])::text[]))
  ORDER BY churn_probability DESC;

COMMIT;
