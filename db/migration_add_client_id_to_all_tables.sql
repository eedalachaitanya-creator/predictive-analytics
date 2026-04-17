-- ============================================================
-- MIGRATION: Add client_id to ALL reference/catalog tables
-- ============================================================
-- This version dynamically finds and drops constraints by
-- querying pg_constraint, so it works regardless of what
-- PostgreSQL named them.
--
-- RUN WITH:
--   /Applications/Postgres.app/Contents/Versions/18/bin/psql -d walmart_crp -f db/migration_add_client_id_to_all_tables.sql
-- ============================================================

-- ============================================================
-- STEP 0: Drop dependent views/materialized views
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mv_customer_features CASCADE;
DROP VIEW IF EXISTS vw_subscription_products CASCADE;
DROP VIEW IF EXISTS vw_customer_360 CASCADE;
DROP VIEW IF EXISTS vw_at_risk_customers CASCADE;
DROP VIEW IF EXISTS vw_customer_order_summary CASCADE;


-- ============================================================
-- STEP 1: Dynamically drop ALL foreign keys that reference
--         the tables we're about to modify
-- ============================================================

DO $$
DECLARE
    r RECORD;
BEGIN
    -- Drop all FKs that REFERENCE any of these tables (i.e. other tables pointing TO them)
    FOR r IN
        SELECT
            tc.table_name AS source_table,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_name IN (
              'categories', 'sub_categories', 'sub_sub_categories',
              'vendors', 'brands', 'products', 'product_prices',
              'product_vendor_mapping'
          )
          AND tc.constraint_schema = 'public'
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                       r.source_table, r.constraint_name);
        RAISE NOTICE 'Dropped FK: %.%', r.source_table, r.constraint_name;
    END LOOP;

    -- Also drop FKs ON these tables themselves (e.g. brands → vendors)
    FOR r IN
        SELECT
            tc.table_name AS source_table,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name IN (
              'categories', 'sub_categories', 'sub_sub_categories',
              'vendors', 'brands', 'products', 'product_prices',
              'product_vendor_mapping'
          )
          AND tc.constraint_schema = 'public'
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                       r.source_table, r.constraint_name);
        RAISE NOTICE 'Dropped FK: %.%', r.source_table, r.constraint_name;
    END LOOP;
END $$;


-- Extra: Drop FKs from tables the dynamic finder missed
ALTER TABLE customer_purchase_cycles DROP CONSTRAINT IF EXISTS customer_purchase_cycles_product_id_fkey;
ALTER TABLE outreach_messages        DROP CONSTRAINT IF EXISTS outreach_messages_product_id_fkey;

-- ============================================================
-- STEP 2: Drop old primary keys dynamically
-- ============================================================

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT
            tc.table_name,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name IN (
              'categories', 'sub_categories', 'sub_sub_categories',
              'vendors', 'brands', 'products', 'product_prices',
              'product_vendor_mapping'
          )
          AND tc.constraint_schema = 'public'
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I CASCADE',
                       r.table_name, r.constraint_name);
        RAISE NOTICE 'Dropped PK: %.%', r.table_name, r.constraint_name;
    END LOOP;
END $$;


-- ============================================================
-- STEP 3: Add client_id column + backfill with CLT-001
-- ============================================================

ALTER TABLE categories             ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE sub_categories         ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE sub_sub_categories     ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE vendors                ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE brands                 ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE products               ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE product_prices         ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
ALTER TABLE product_vendor_mapping ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);

UPDATE categories             SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE sub_categories         SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE sub_sub_categories     SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE vendors                SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE brands                 SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE products               SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE product_prices         SET client_id = 'CLT-001' WHERE client_id IS NULL;
UPDATE product_vendor_mapping SET client_id = 'CLT-001' WHERE client_id IS NULL;

ALTER TABLE categories             ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE sub_categories         ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE sub_sub_categories     ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE vendors                ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE brands                 ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE products               ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE product_prices         ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE product_vendor_mapping ALTER COLUMN client_id SET NOT NULL;


-- ============================================================
-- STEP 4: Create new composite primary keys
-- ============================================================

ALTER TABLE categories             ADD PRIMARY KEY (client_id, category_id);
ALTER TABLE sub_categories         ADD PRIMARY KEY (client_id, sub_category_id);
ALTER TABLE sub_sub_categories     ADD PRIMARY KEY (client_id, sub_sub_category_id);
ALTER TABLE vendors                ADD PRIMARY KEY (client_id, vendor_id);
ALTER TABLE brands                 ADD PRIMARY KEY (client_id, brand_id);
ALTER TABLE products               ADD PRIMARY KEY (client_id, product_id);
ALTER TABLE product_prices         ADD PRIMARY KEY (client_id, price_id);
ALTER TABLE product_vendor_mapping ADD PRIMARY KEY (client_id, pv_id);


-- ============================================================
-- STEP 5: Re-create ALL foreign keys as composite
-- ============================================================

-- sub_categories → categories
ALTER TABLE sub_categories
    ADD CONSTRAINT sub_categories_category_fk
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

-- sub_sub_categories → sub_categories
ALTER TABLE sub_sub_categories
    ADD CONSTRAINT sub_sub_categories_sub_category_fk
    FOREIGN KEY (client_id, sub_category_id)
    REFERENCES sub_categories(client_id, sub_category_id);

-- sub_sub_categories → categories
ALTER TABLE sub_sub_categories
    ADD CONSTRAINT sub_sub_categories_category_fk
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

-- brands → vendors
ALTER TABLE brands
    ADD CONSTRAINT brands_vendor_fk
    FOREIGN KEY (client_id, vendor_id)
    REFERENCES vendors(client_id, vendor_id);

-- products → categories
ALTER TABLE products
    ADD CONSTRAINT products_category_fk
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

-- products → sub_categories
ALTER TABLE products
    ADD CONSTRAINT products_sub_category_fk
    FOREIGN KEY (client_id, sub_category_id)
    REFERENCES sub_categories(client_id, sub_category_id);

-- products → sub_sub_categories
ALTER TABLE products
    ADD CONSTRAINT products_sub_sub_category_fk
    FOREIGN KEY (client_id, sub_sub_category_id)
    REFERENCES sub_sub_categories(client_id, sub_sub_category_id);

-- products → brands
ALTER TABLE products
    ADD CONSTRAINT products_brand_fk
    FOREIGN KEY (client_id, brand_id)
    REFERENCES brands(client_id, brand_id);

-- products → product_prices (deferred — avoids circular load issue)
ALTER TABLE products
    ADD CONSTRAINT fk_products_price
    FOREIGN KEY (client_id, product_price_id)
    REFERENCES product_prices(client_id, price_id)
    DEFERRABLE INITIALLY DEFERRED;

-- product_prices → products
ALTER TABLE product_prices
    ADD CONSTRAINT product_prices_product_fk
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- product_vendor_mapping → products
ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT pvm_product_fk
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- product_vendor_mapping → brands
ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT pvm_brand_fk
    FOREIGN KEY (client_id, brand_id)
    REFERENCES brands(client_id, brand_id);

-- product_vendor_mapping → vendors
ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT pvm_vendor_fk
    FOREIGN KEY (client_id, vendor_id)
    REFERENCES vendors(client_id, vendor_id);

-- line_items → products (composite — line_items already has client_id)
ALTER TABLE line_items
    ADD CONSTRAINT line_items_product_fk
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- customer_reviews → products (composite — reviews already has client_id)
ALTER TABLE customer_reviews
    ADD CONSTRAINT customer_reviews_product_fk
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);


-- ============================================================
-- STEP 6: Indexes for faster client-scoped queries
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_categories_client         ON categories(client_id);
CREATE INDEX IF NOT EXISTS idx_sub_categories_client     ON sub_categories(client_id);
CREATE INDEX IF NOT EXISTS idx_sub_sub_categories_client ON sub_sub_categories(client_id);
CREATE INDEX IF NOT EXISTS idx_vendors_client            ON vendors(client_id);
CREATE INDEX IF NOT EXISTS idx_brands_client             ON brands(client_id);
CREATE INDEX IF NOT EXISTS idx_products_client           ON products(client_id);
CREATE INDEX IF NOT EXISTS idx_product_prices_client     ON product_prices(client_id);
CREATE INDEX IF NOT EXISTS idx_pvm_client                ON product_vendor_mapping(client_id);


-- ============================================================
-- STEP 7: Re-create helper views
-- ============================================================

CREATE OR REPLACE VIEW vw_customer_order_summary AS
SELECT
    o.client_id,
    o.customer_id,
    COUNT(o.order_id)                                                          AS total_orders,
    SUM(o.order_value_usd)                                                     AS total_spend_usd,
    AVG(o.order_value_usd)                                                     AS avg_order_value_usd,
    MIN(o.order_date)                                                          AS first_order_date,
    MAX(o.order_date)                                                          AS last_order_date,
    EXTRACT(DAY FROM NOW() - MAX(o.order_date))                                AS days_since_last_order,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days'  THEN 1 ELSE 0 END) AS orders_last_30d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN 1 ELSE 0 END) AS orders_last_90d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days'  THEN o.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
    SUM(o.discount_usd)                                                        AS total_discounts_usd,
    COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END)                            AS orders_with_discount
FROM orders o
WHERE o.order_status NOT IN ('Cancelled')
GROUP BY o.client_id, o.customer_id;

CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT
    c.client_id,
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
    ON c.client_id = r.client_id AND c.customer_id = r.customer_id
LEFT JOIN LATERAL (
    SELECT * FROM churn_scores s
    WHERE s.client_id = c.client_id AND s.customer_id = c.customer_id
    ORDER BY s.scored_at DESC LIMIT 1
) cs ON TRUE;

CREATE OR REPLACE VIEW vw_at_risk_customers AS
SELECT * FROM vw_customer_360
WHERE risk_tier IN ('HIGH', 'MEDIUM')
ORDER BY churn_probability DESC;


-- ============================================================
-- STEP 8: Verification
-- ============================================================

SELECT 'categories'             AS tbl, count(*) AS rows, count(DISTINCT client_id) AS clients FROM categories
UNION ALL
SELECT 'sub_categories',              count(*), count(DISTINCT client_id) FROM sub_categories
UNION ALL
SELECT 'sub_sub_categories',          count(*), count(DISTINCT client_id) FROM sub_sub_categories
UNION ALL
SELECT 'vendors',                     count(*), count(DISTINCT client_id) FROM vendors
UNION ALL
SELECT 'brands',                      count(*), count(DISTINCT client_id) FROM brands
UNION ALL
SELECT 'products',                    count(*), count(DISTINCT client_id) FROM products
UNION ALL
SELECT 'product_prices',             count(*), count(DISTINCT client_id) FROM product_prices
UNION ALL
SELECT 'product_vendor_mapping',     count(*), count(DISTINCT client_id) FROM product_vendor_mapping
ORDER BY tbl;

-- ============================================================
-- DONE! All tables are now fully multi-tenant.
-- Every table uses (client_id, <original_pk>) as its primary key.
-- The upload router auto-injects client_id from the logged-in user.
--
-- NEXT: Re-create materialized view + subscription view:
--   /Applications/Postgres.app/Contents/Versions/18/bin/psql -d walmart_crp -f db/schema_full.sql
-- ============================================================
