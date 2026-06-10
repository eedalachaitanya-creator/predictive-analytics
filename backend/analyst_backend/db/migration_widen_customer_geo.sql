-- migration_widen_customer_geo.sql
-- Root fix for the customer-master upload failure ("A value is too large or too
-- long for its column"). The geographic columns were sized for US-style codes and
-- truncated legitimate international data:
--   * state        VARCHAR(5) -> VARCHAR(100)  ('Karnataka', 'Andhra Pradesh', …)
--   * country_code VARCHAR(5) -> VARCHAR(100)  (full names like 'United Arab Emirates')
-- (city is already VARCHAR(100) — adequate, left unchanged.)
--
-- `state` is referenced by two views, so they are dropped + recreated verbatim
-- around the ALTER. `country_code` is not used by any view.
-- Safe to run once per deployment; run on the live DB after deploying this code.

BEGIN;

DROP VIEW IF EXISTS vw_at_risk_customers;
DROP VIEW IF EXISTS vw_customer_360;

ALTER TABLE customers         ALTER COLUMN state        TYPE VARCHAR(100);
ALTER TABLE staging_customers ALTER COLUMN state        TYPE VARCHAR(100);
ALTER TABLE customers         ALTER COLUMN country_code TYPE VARCHAR(100);
ALTER TABLE staging_customers ALTER COLUMN country_code TYPE VARCHAR(100);

CREATE VIEW vw_customer_360 AS
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
   FROM customers c
     LEFT JOIN customer_rfm_features r
       ON c.client_id::text = r.client_id::text
      AND c.customer_id::text = r.customer_id::text
     LEFT JOIN LATERAL (
         SELECT s.score_id, s.client_id, s.customer_id, s.scored_at,
                s.churn_probability, s.risk_tier, s.churn_label_simulated,
                s.driver_1, s.driver_2, s.driver_3, s.model_version, s.batch_run_id
           FROM churn_scores s
          WHERE s.client_id::text = c.client_id::text
            AND s.customer_id::text = c.customer_id::text
          ORDER BY s.scored_at DESC
         LIMIT 1) cs ON true;

CREATE VIEW vw_at_risk_customers AS
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
   FROM vw_customer_360
  WHERE risk_tier::text = ANY (ARRAY['HIGH'::character varying::text,
                                     'MEDIUM'::character varying::text])
  ORDER BY churn_probability DESC;

COMMIT;
