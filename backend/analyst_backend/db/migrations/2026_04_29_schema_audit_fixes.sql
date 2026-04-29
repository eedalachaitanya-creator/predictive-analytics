-- ====================================================================
-- 2026_04_29_schema_audit_fixes.sql
-- Closes the schema audit findings from 2026-04-28.
--
-- Atomic: the whole file runs in one BEGIN/COMMIT. Any failure rolls
-- back every prior section so the DB is never left half-migrated.
--
-- Pre-flight verified on 2026-04-28 against walmart_crp:
--   * 0 churn_probability values out of [0,1]
--   * 0 risk_tier values outside {HIGH,MEDIUM,LOW}
--   * 0 discount_pct values out of [0,100]
--   * 0 client_id orphans across customers/orders/churn_scores
-- so every CHECK and FK below validates without failing on existing data.
--
-- Sections:
--   A. FK to client_config from every multi-tenant table        (audit #1, #8)
--   B. Drop DEFAULT 'CLT-001' on chat_messages, pipeline_outputs (audit #2)
--   C. Fix customer_price_context UNIQUE                        (audit #3)
--   D. Add retention_interventions → customers FK               (audit #4)
--   E. Strengthen retention_interventions → churn_scores FK     (audit #5)
--   F. CHECK constraints on probabilities / percentages / tier  (audit #6)
--   G. Rewrite vw_customer_order_summary to honor ref_date_mode (audit #7)
--   H. Standardize audit_log.client_id to varchar(20)           (audit #9)
--
-- Skipped intentionally:
--   * audit #10 (FK audit_log.user_id → users): audit logs must outlive
--     user deletions for compliance. 36 legitimate orphans exist.
--   * Staging tables: TRUNCATE-and-reload cycle, FK adds upload friction.
-- ====================================================================

BEGIN;


-- ============================================================
-- A. FK to client_config from every multi-tenant table
-- ============================================================
-- ON DELETE RESTRICT prevents accidental cascade-wipe of tenant data
-- when a client_config row is removed. Caller must explicitly clean
-- up dependent rows first.

ALTER TABLE audit_log                  ADD CONSTRAINT fk_audit_log_client                  FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE brands                     ADD CONSTRAINT fk_brands_client                     FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE categories                 ADD CONSTRAINT fk_categories_client                 FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE chat_messages              ADD CONSTRAINT fk_chat_messages_client              FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE churn_scores               ADD CONSTRAINT fk_churn_scores_client               FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE customer_price_context     ADD CONSTRAINT fk_customer_price_context_client     FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE customer_purchase_cycles   ADD CONSTRAINT fk_customer_purchase_cycles_client   FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE customer_reviews           ADD CONSTRAINT fk_customer_reviews_client           FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE customer_rfm_features      ADD CONSTRAINT fk_customer_rfm_features_client      FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE customers                  ADD CONSTRAINT fk_customers_client                  FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE line_items                 ADD CONSTRAINT fk_line_items_client                 FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE llm_cost_log               ADD CONSTRAINT fk_llm_cost_log_client               FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE message_templates          ADD CONSTRAINT fk_message_templates_client          FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE orders                     ADD CONSTRAINT fk_orders_client                     FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE outreach_messages          ADD CONSTRAINT fk_outreach_messages_client          FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE pipeline_outputs           ADD CONSTRAINT fk_pipeline_outputs_client           FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE pricing_recommendations    ADD CONSTRAINT fk_pricing_recommendations_client    FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE product_prices             ADD CONSTRAINT fk_product_prices_client             FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE product_vendor_mapping     ADD CONSTRAINT fk_product_vendor_mapping_client     FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE products                   ADD CONSTRAINT fk_products_client                   FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE retention_interventions    ADD CONSTRAINT fk_retention_interventions_client    FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE sub_categories             ADD CONSTRAINT fk_sub_categories_client             FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE sub_sub_categories         ADD CONSTRAINT fk_sub_sub_categories_client         FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE support_tickets            ADD CONSTRAINT fk_support_tickets_client            FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE upload_batches             ADD CONSTRAINT fk_upload_batches_client             FOREIGN KEY (client_id) REFERENCES client_config(client_id);
ALTER TABLE vendors                    ADD CONSTRAINT fk_vendors_client                    FOREIGN KEY (client_id) REFERENCES client_config(client_id);


-- ============================================================
-- B. Drop hardcoded DEFAULT 'CLT-001' for client_id
-- ============================================================
-- Forces every INSERT to provide client_id explicitly; eliminates the
-- "forgot to set tenant" silent-cross-contamination class of bugs.
-- Verified safe: chat_router.py:99 and pipeline_outputs_store.py:214
-- both already include client_id in their VALUES list.

ALTER TABLE chat_messages    ALTER COLUMN client_id DROP DEFAULT;
ALTER TABLE pipeline_outputs ALTER COLUMN client_id DROP DEFAULT;


-- ============================================================
-- C. Fix customer_price_context UNIQUE constraint
-- ============================================================
-- Old: UNIQUE (customer_id, product_name) — tenant-blind.
-- New: UNIQUE (client_id, customer_id, product_name).

ALTER TABLE customer_price_context DROP CONSTRAINT uq_customer_price_context;
ALTER TABLE customer_price_context ADD  CONSTRAINT uq_customer_price_context
  UNIQUE (client_id, customer_id, product_name);


-- ============================================================
-- D. Add retention_interventions → customers FK
-- ============================================================
-- Table carries client_id + customer_id NOT NULL but never enforced
-- referential integrity to customers. Orphan rows possible.

ALTER TABLE retention_interventions
  ADD CONSTRAINT fk_retention_interventions_customer
  FOREIGN KEY (client_id, customer_id)
  REFERENCES customers(client_id, customer_id);


-- ============================================================
-- E. Strengthen retention_interventions → churn_scores FK
-- ============================================================
-- Old single-column FK (FOREIGN KEY (churn_score_id) → score_id) preserves
-- referential integrity but doesn't enforce that the intervention's
-- client_id matches the score's client_id. Replace with compound FK.
--
-- Step 1: add UNIQUE (client_id, score_id) on churn_scores so the
--         compound FK has a unique target. score_id is already PK
--         (globally unique), so this UNIQUE is redundant for integrity
--         but required by Postgres for compound-FK targets.

ALTER TABLE churn_scores
  ADD CONSTRAINT churn_scores_client_score_uq UNIQUE (client_id, score_id);

-- Step 2: drop the old single-column FK and add the compound one.

ALTER TABLE retention_interventions
  DROP CONSTRAINT retention_interventions_churn_score_id_fkey;

ALTER TABLE retention_interventions
  ADD CONSTRAINT fk_retention_interventions_churn_score
  FOREIGN KEY (client_id, churn_score_id)
  REFERENCES churn_scores(client_id, score_id);


-- ============================================================
-- F. CHECK constraints on numeric columns
-- ============================================================
-- Valid ranges enforced at the DB layer so application bugs / bad
-- imports raise instead of silently storing garbage.

-- Probabilities: [0, 1]
-- (customer_rfm_features has no churn_probability/risk_tier columns —
-- those live only on churn_scores.)
ALTER TABLE churn_scores
  ADD CONSTRAINT ck_churn_scores_probability
  CHECK (churn_probability IS NULL OR (churn_probability >= 0 AND churn_probability <= 1));

ALTER TABLE retention_interventions
  ADD CONSTRAINT ck_retention_interventions_probability
  CHECK (churn_probability IS NULL OR (churn_probability >= 0 AND churn_probability <= 1));

-- Risk tiers: {HIGH, MEDIUM, LOW}
ALTER TABLE churn_scores
  ADD CONSTRAINT ck_churn_scores_risk_tier
  CHECK (risk_tier IS NULL OR risk_tier IN ('HIGH', 'MEDIUM', 'LOW'));

ALTER TABLE retention_interventions
  ADD CONSTRAINT ck_retention_interventions_risk_tier
  CHECK (risk_tier IS NULL OR risk_tier IN ('HIGH', 'MEDIUM', 'LOW'));

-- customer_rfm_features percentage columns
ALTER TABLE customer_rfm_features
  ADD CONSTRAINT ck_customer_rfm_features_return_rate
  CHECK (return_rate_pct IS NULL OR (return_rate_pct >= 0 AND return_rate_pct <= 100));

ALTER TABLE customer_rfm_features
  ADD CONSTRAINT ck_customer_rfm_features_discount_dependency
  CHECK (discount_dependency_pct IS NULL OR (discount_dependency_pct >= 0 AND discount_dependency_pct <= 100));

-- Percentages: [0, 100]
ALTER TABLE client_config
  ADD CONSTRAINT ck_client_config_max_discount_pct
  CHECK (max_discount_pct IS NULL OR (max_discount_pct >= 0 AND max_discount_pct <= 100));

ALTER TABLE message_templates
  ADD CONSTRAINT ck_message_templates_discount_pct
  CHECK (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100));

ALTER TABLE retention_interventions
  ADD CONSTRAINT ck_retention_interventions_discount_pct
  CHECK (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100));

ALTER TABLE retention_interventions
  ADD CONSTRAINT ck_retention_interventions_max_allowed_discount
  CHECK (max_allowed_discount IS NULL OR (max_allowed_discount >= 0 AND max_allowed_discount <= 100));

ALTER TABLE value_propositions
  ADD CONSTRAINT ck_value_propositions_discount_pct
  CHECK (discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100));

-- Money columns: revenue_recovered shouldn't go negative
ALTER TABLE retention_interventions
  ADD CONSTRAINT ck_retention_interventions_revenue_recovered
  CHECK (revenue_recovered IS NULL OR revenue_recovered >= 0);


-- ============================================================
-- G. Rewrite vw_customer_order_summary to honor reference_date_mode
-- ============================================================
-- Old view used now() directly, which gave wrong day counts for
-- tenants with reference_date_mode='fixed'. Match the MV pattern.

DROP VIEW IF EXISTS public.vw_customer_order_summary;

CREATE VIEW public.vw_customer_order_summary AS
WITH client_ref AS (
    SELECT
        client_id,
        CASE WHEN reference_date_mode = 'fixed' AND reference_date IS NOT NULL
             THEN reference_date::TIMESTAMPTZ
             ELSE NOW()
        END AS ref_date
    FROM client_config
)
SELECT
    o.client_id,
    o.customer_id,
    count(o.order_id)                                                  AS total_orders,
    sum(o.order_value_usd)                                             AS total_spend_usd,
    avg(o.order_value_usd)                                             AS avg_order_value_usd,
    min(o.order_date)                                                  AS first_order_date,
    max(o.order_date)                                                  AS last_order_date,
    EXTRACT(DAY FROM (cr.ref_date - max(o.order_date)))                AS days_since_last_order,
    sum(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '30 days' THEN 1 ELSE 0 END) AS orders_last_30d,
    sum(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days' THEN 1 ELSE 0 END) AS orders_last_90d,
    sum(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days'
             THEN o.order_value_usd ELSE 0 END)                        AS spend_last_90d_usd,
    sum(o.discount_usd)                                                AS total_discounts_usd,
    count(CASE WHEN o.discount_usd > 0 THEN 1 END)                    AS orders_with_discount
FROM public.orders o
JOIN client_ref cr ON o.client_id = cr.client_id
WHERE o.order_status <> 'Cancelled'
GROUP BY o.client_id, o.customer_id, cr.ref_date;


-- ============================================================
-- H. Standardize audit_log.client_id to varchar(20)
-- ============================================================
-- Every other table uses varchar(20). Verified 0 rows exceed 20 chars.

ALTER TABLE audit_log ALTER COLUMN client_id TYPE character varying(20);


COMMIT;
