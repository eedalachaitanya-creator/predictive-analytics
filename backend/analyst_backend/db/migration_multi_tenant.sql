-- ============================================================
-- migration_multi_tenant.sql
-- Adds client_id to ALL shared/reference tables so each
-- client (Walmart, Costco, etc.) has its own isolated data.
-- ============================================================
-- WHAT THIS CHANGES:
--   11 tables gain a client_id column:
--     categories, sub_categories, sub_sub_categories,
--     vendors, brands, products, product_prices,
--     product_vendor_mapping, value_tiers,
--     business_segments, value_propositions
--
--   All foreign keys referencing these tables are updated
--   to composite (client_id, original_pk).
--
--   The materialized view and regular views are rebuilt.
--
-- HOW TO RUN:
--   Open in pgAdmin4 → Query Tool → F5
--   Then reload data for all clients.
-- ============================================================

BEGIN;

-- ── Step 0: Drop dependent objects ──────────────────────────
DROP MATERIALIZED VIEW IF EXISTS mv_customer_features CASCADE;
DROP VIEW IF EXISTS vw_subscription_products CASCADE;
DROP VIEW IF EXISTS vw_customer_360 CASCADE;
DROP VIEW IF EXISTS vw_at_risk_customers CASCADE;
DROP VIEW IF EXISTS vw_customer_order_summary CASCADE;

-- ── Step 1: Drop ALL foreign key constraints ────────────────
-- (We'll re-add them after modifying PKs)

-- sub_categories → categories
ALTER TABLE sub_categories DROP CONSTRAINT IF EXISTS sub_categories_category_id_fkey;

-- sub_sub_categories → sub_categories, categories
ALTER TABLE sub_sub_categories DROP CONSTRAINT IF EXISTS sub_sub_categories_sub_category_id_fkey;
ALTER TABLE sub_sub_categories DROP CONSTRAINT IF EXISTS sub_sub_categories_category_id_fkey;

-- brands → vendors
ALTER TABLE brands DROP CONSTRAINT IF EXISTS brands_vendor_id_fkey;

-- products → categories, sub_categories, sub_sub_categories, brands
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_category_id_fkey;
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_sub_category_id_fkey;
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_sub_sub_category_id_fkey;
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_brand_id_fkey;
ALTER TABLE products DROP CONSTRAINT IF EXISTS fk_products_price;

-- product_prices → products
ALTER TABLE product_prices DROP CONSTRAINT IF EXISTS product_prices_product_id_fkey;

-- product_vendor_mapping → products, brands, vendors
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS product_vendor_mapping_product_id_fkey;
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS product_vendor_mapping_brand_id_fkey;
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS product_vendor_mapping_vendor_id_fkey;

-- line_items → products
ALTER TABLE line_items DROP CONSTRAINT IF EXISTS line_items_product_id_fkey;

-- customer_reviews → products
ALTER TABLE customer_reviews DROP CONSTRAINT IF EXISTS customer_reviews_product_id_fkey;

-- customer_purchase_cycles → products
ALTER TABLE customer_purchase_cycles DROP CONSTRAINT IF EXISTS customer_purchase_cycles_product_id_fkey;

-- outreach_messages → products
ALTER TABLE outreach_messages DROP CONSTRAINT IF EXISTS outreach_messages_product_id_fkey;


-- ── Step 2: Add client_id to shared tables ──────────────────

-- Categories
ALTER TABLE categories ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE categories SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE categories ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_pkey;
ALTER TABLE categories ADD PRIMARY KEY (client_id, category_id);

-- Sub-Categories
ALTER TABLE sub_categories ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE sub_categories SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE sub_categories ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE sub_categories DROP CONSTRAINT IF EXISTS sub_categories_pkey;
ALTER TABLE sub_categories ADD PRIMARY KEY (client_id, sub_category_id);

-- Sub-Sub-Categories
ALTER TABLE sub_sub_categories ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE sub_sub_categories SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE sub_sub_categories ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE sub_sub_categories DROP CONSTRAINT IF EXISTS sub_sub_categories_pkey;
ALTER TABLE sub_sub_categories ADD PRIMARY KEY (client_id, sub_sub_category_id);

-- Vendors
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE vendors SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE vendors ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE vendors DROP CONSTRAINT IF EXISTS vendors_pkey;
ALTER TABLE vendors ADD PRIMARY KEY (client_id, vendor_id);

-- Brands
ALTER TABLE brands ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE brands SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE brands ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE brands DROP CONSTRAINT IF EXISTS brands_pkey;
ALTER TABLE brands ADD PRIMARY KEY (client_id, brand_id);

-- Products
ALTER TABLE products ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE products SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE products ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE products DROP CONSTRAINT IF EXISTS products_pkey;
ALTER TABLE products ADD PRIMARY KEY (client_id, product_id);

-- Product Prices
ALTER TABLE product_prices ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE product_prices SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE product_prices ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE product_prices DROP CONSTRAINT IF EXISTS product_prices_pkey;
ALTER TABLE product_prices ADD PRIMARY KEY (client_id, price_id);

-- Product-Vendor Mapping
ALTER TABLE product_vendor_mapping ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE product_vendor_mapping SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE product_vendor_mapping ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS product_vendor_mapping_pkey;
ALTER TABLE product_vendor_mapping ADD PRIMARY KEY (client_id, pv_id);

-- Value Tiers
ALTER TABLE value_tiers ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE value_tiers SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE value_tiers ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE value_tiers DROP CONSTRAINT IF EXISTS value_tiers_pkey;
ALTER TABLE value_tiers ADD PRIMARY KEY (client_id, tier_id);

-- Business Segments
ALTER TABLE business_segments ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE business_segments SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE business_segments ALTER COLUMN client_id SET NOT NULL;
ALTER TABLE business_segments DROP CONSTRAINT IF EXISTS business_segments_pkey;
ALTER TABLE business_segments ADD PRIMARY KEY (client_id, segment_id);

-- Value Propositions
ALTER TABLE value_propositions ADD COLUMN IF NOT EXISTS client_id VARCHAR(20);
UPDATE value_propositions SET client_id = 'CLT-001' WHERE client_id IS NULL;
ALTER TABLE value_propositions ALTER COLUMN client_id SET NOT NULL;


-- ── Step 3: Re-add foreign keys (now composite) ────────────

-- sub_categories → categories
ALTER TABLE sub_categories
    ADD CONSTRAINT fk_subcat_category
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

-- sub_sub_categories → sub_categories, categories
ALTER TABLE sub_sub_categories
    ADD CONSTRAINT fk_subsubcat_subcat
    FOREIGN KEY (client_id, sub_category_id)
    REFERENCES sub_categories(client_id, sub_category_id);

ALTER TABLE sub_sub_categories
    ADD CONSTRAINT fk_subsubcat_category
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

-- brands → vendors
ALTER TABLE brands
    ADD CONSTRAINT fk_brands_vendor
    FOREIGN KEY (client_id, vendor_id)
    REFERENCES vendors(client_id, vendor_id);

-- products → categories, sub_categories, sub_sub_categories, brands
ALTER TABLE products
    ADD CONSTRAINT fk_products_category
    FOREIGN KEY (client_id, category_id)
    REFERENCES categories(client_id, category_id);

ALTER TABLE products
    ADD CONSTRAINT fk_products_subcat
    FOREIGN KEY (client_id, sub_category_id)
    REFERENCES sub_categories(client_id, sub_category_id);

ALTER TABLE products
    ADD CONSTRAINT fk_products_subsubcat
    FOREIGN KEY (client_id, sub_sub_category_id)
    REFERENCES sub_sub_categories(client_id, sub_sub_category_id);

ALTER TABLE products
    ADD CONSTRAINT fk_products_brand
    FOREIGN KEY (client_id, brand_id)
    REFERENCES brands(client_id, brand_id);

-- product_prices → products
ALTER TABLE product_prices
    ADD CONSTRAINT fk_prices_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- products → product_prices (deferred circular FK)
ALTER TABLE products
    ADD CONSTRAINT fk_products_price
    FOREIGN KEY (client_id, product_price_id)
    REFERENCES product_prices(client_id, price_id)
    DEFERRABLE INITIALLY DEFERRED;

-- product_vendor_mapping → products, brands, vendors
ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT fk_pvm_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT fk_pvm_brand
    FOREIGN KEY (client_id, brand_id)
    REFERENCES brands(client_id, brand_id);

ALTER TABLE product_vendor_mapping
    ADD CONSTRAINT fk_pvm_vendor
    FOREIGN KEY (client_id, vendor_id)
    REFERENCES vendors(client_id, vendor_id);

-- line_items → products
ALTER TABLE line_items
    ADD CONSTRAINT fk_lineitems_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- customer_reviews → products
ALTER TABLE customer_reviews
    ADD CONSTRAINT fk_reviews_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- customer_purchase_cycles → products
ALTER TABLE customer_purchase_cycles
    ADD CONSTRAINT fk_cycles_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);

-- outreach_messages → products
ALTER TABLE outreach_messages
    ADD CONSTRAINT fk_outreach_product
    FOREIGN KEY (client_id, product_id)
    REFERENCES products(client_id, product_id);


-- ── Step 4: Add indexes on new client_id columns ────────────
CREATE INDEX IF NOT EXISTS idx_categories_client      ON categories(client_id);
CREATE INDEX IF NOT EXISTS idx_sub_categories_client   ON sub_categories(client_id);
CREATE INDEX IF NOT EXISTS idx_sub_sub_categories_client ON sub_sub_categories(client_id);
CREATE INDEX IF NOT EXISTS idx_vendors_client          ON vendors(client_id);
CREATE INDEX IF NOT EXISTS idx_brands_client           ON brands(client_id);
CREATE INDEX IF NOT EXISTS idx_products_client         ON products(client_id);
CREATE INDEX IF NOT EXISTS idx_product_prices_client   ON product_prices(client_id);
CREATE INDEX IF NOT EXISTS idx_pvm_client              ON product_vendor_mapping(client_id);
CREATE INDEX IF NOT EXISTS idx_value_tiers_client      ON value_tiers(client_id);
CREATE INDEX IF NOT EXISTS idx_business_segments_client ON business_segments(client_id);
CREATE INDEX IF NOT EXISTS idx_value_propositions_client ON value_propositions(client_id);

-- Also update the line_items product index to be composite
DROP INDEX IF EXISTS idx_line_items_product;
CREATE INDEX IF NOT EXISTS idx_line_items_product ON line_items(client_id, product_id);


-- ── Step 5: Recreate views ──────────────────────────────────

-- 5a. Subscription Product Detection View (now client-aware)
CREATE OR REPLACE VIEW vw_subscription_products AS
WITH
keyword_flag AS (
    SELECT
        client_id, product_id, product_name,
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
repeat_counts AS (
    SELECT client_id, customer_id, product_id, COUNT(*) AS purchase_count
    FROM line_items
    GROUP BY client_id, customer_id, product_id
),
purchase_gaps AS (
    SELECT li.client_id, li.customer_id, li.product_id,
        EXTRACT(DAY FROM o.order_date - LAG(o.order_date) OVER (
            PARTITION BY li.client_id, li.customer_id, li.product_id ORDER BY o.order_date
        )) AS gap_days
    FROM line_items li
    JOIN orders o ON li.client_id = o.client_id AND li.order_id = o.order_id
),
avg_gaps AS (
    SELECT client_id, customer_id, product_id, AVG(gap_days) AS avg_gap
    FROM purchase_gaps WHERE gap_days IS NOT NULL
    GROUP BY client_id, customer_id, product_id
),
behaviour_flag AS (
    SELECT
        li.client_id, li.product_id,
        COUNT(DISTINCT li.customer_id) AS total_buyers,
        COUNT(DISTINCT CASE WHEN rc.purchase_count >= 3 THEN li.customer_id END) AS repeat_buyers,
        ROUND(AVG(ag.avg_gap)::NUMERIC, 1) AS avg_refill_days,
        ROUND(STDDEV(ag.avg_gap)::NUMERIC, 1) AS stddev_refill_days
    FROM line_items li
    LEFT JOIN repeat_counts rc ON li.client_id = rc.client_id AND li.customer_id = rc.customer_id AND li.product_id = rc.product_id
    LEFT JOIN avg_gaps ag ON li.client_id = ag.client_id AND li.customer_id = ag.customer_id AND li.product_id = ag.product_id
    GROUP BY li.client_id, li.product_id
),
combined AS (
    SELECT
        p.client_id, p.product_id, p.product_name, p.category_id,
        kf.is_subscription_by_name,
        COALESCE(bf.repeat_buyers, 0) AS repeat_buyers,
        COALESCE(bf.total_buyers, 0) AS total_buyers,
        COALESCE(bf.avg_refill_days, 0) AS avg_refill_days,
        COALESCE(bf.stddev_refill_days, 0) AS stddev_refill_days,
        CASE WHEN bf.total_buyers > 0
              AND (bf.repeat_buyers * 1.0 / bf.total_buyers) >= 0.30
              AND COALESCE(bf.stddev_refill_days, 999) < 15
             THEN TRUE ELSE FALSE END AS is_subscription_by_behaviour
    FROM products p
    LEFT JOIN keyword_flag kf ON p.client_id = kf.client_id AND p.product_id = kf.product_id
    LEFT JOIN behaviour_flag bf ON p.client_id = bf.client_id AND p.product_id = bf.product_id
)
SELECT
    client_id, product_id, product_name, category_id,
    is_subscription_by_name, is_subscription_by_behaviour,
    avg_refill_days, repeat_buyers, total_buyers,
    (is_subscription_by_name OR is_subscription_by_behaviour) AS is_subscription_product,
    CASE
        WHEN is_subscription_by_name AND is_subscription_by_behaviour THEN 'both'
        WHEN is_subscription_by_name THEN 'keyword'
        WHEN is_subscription_by_behaviour THEN 'behaviour'
        ELSE 'none'
    END AS detection_source
FROM combined;


-- 5b. Customer Order Summary View (already client-aware, no change needed)
CREATE OR REPLACE VIEW vw_customer_order_summary AS
SELECT
    o.client_id, o.customer_id,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_value_usd) AS total_spend_usd,
    AVG(o.order_value_usd) AS avg_order_value_usd,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    EXTRACT(DAY FROM NOW() - MAX(o.order_date)) AS days_since_last_order,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) AS orders_last_30d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days' THEN 1 ELSE 0 END) AS orders_last_90d,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '90 days' THEN o.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
    SUM(o.discount_usd) AS total_discounts_usd,
    COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END) AS orders_with_discount
FROM orders o
WHERE o.order_status NOT IN ('Cancelled')
GROUP BY o.client_id, o.customer_id;


-- ── Step 6: Recreate Materialized View ──────────────────────
-- (cat_agg CTE now joins on client_id)

CREATE MATERIALIZED VIEW mv_customer_features AS
WITH
client_ref AS (
    SELECT client_id, churn_window_days, min_repeat_orders, high_value_percentile,
        recent_order_gap_window, tier_method, custom_platinum_min, custom_gold_min,
        custom_silver_min, custom_bronze_min,
        CASE WHEN reference_date_mode = 'fixed' AND reference_date IS NOT NULL
             THEN reference_date::TIMESTAMPTZ ELSE NOW() END AS ref_date
    FROM client_config
),
order_agg AS (
    SELECT o.client_id, o.customer_id,
        COUNT(*) AS total_orders,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(o.order_date))::INT AS days_since_last_order,
        SUM(o.order_value_usd) AS total_spend_usd,
        ROUND(AVG(o.order_value_usd)::NUMERIC, 2) AS avg_order_value_usd,
        MAX(o.order_value_usd) AS max_order_value_usd,
        COALESCE(SUM(o.discount_usd), 0) AS total_discount_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '30 days' THEN o.order_value_usd ELSE 0 END) AS spend_last_30d_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days' THEN o.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
        SUM(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '180 days' THEN o.order_value_usd ELSE 0 END) AS spend_last_180d_usd,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '30 days' THEN 1 END) AS orders_last_30d,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '90 days' THEN 1 END) AS orders_last_90d,
        COUNT(CASE WHEN o.order_date >= cr.ref_date - INTERVAL '180 days' THEN 1 END) AS orders_last_180d,
        COUNT(CASE WHEN o.discount_usd > 0 THEN 1 END) AS orders_with_discount
    FROM orders o
    JOIN client_ref cr ON o.client_id = cr.client_id
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY o.client_id, o.customer_id, cr.ref_date
),
order_gaps AS (
    SELECT client_id, customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1) AS avg_days_between_orders,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap_days)::NUMERIC, 1) AS median_days_between_orders
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
        COUNT(DISTINCT li.product_id) AS unique_products_purchased,
        ROUND(AVG(li.quantity)::NUMERIC, 2) AS avg_items_per_order,
        ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1) AS return_rate_pct
    FROM line_items li
    GROUP BY li.client_id, li.customer_id
),
cat_agg AS (
    SELECT li.client_id, li.customer_id,
        COUNT(DISTINCT p.category_id) AS unique_categories_purchased
    FROM line_items li
    JOIN products p ON li.client_id = p.client_id AND li.product_id = p.product_id
    GROUP BY li.client_id, li.customer_id
),
review_agg AS (
    SELECT r.client_id, r.customer_id,
        COUNT(*) AS total_reviews,
        ROUND(AVG(r.rating)::NUMERIC, 2) AS avg_rating,
        ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1) AS pct_positive_reviews,
        ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1) AS pct_negative_reviews,
        MAX(r.review_date) AS last_review_date,
        EXTRACT(DAY FROM cr.ref_date - MAX(r.review_date::TIMESTAMPTZ))::INT AS days_since_last_review
    FROM customer_reviews r
    JOIN client_ref cr ON r.client_id = cr.client_id
    GROUP BY r.client_id, r.customer_id, cr.ref_date
),
ticket_agg AS (
    SELECT t.client_id, t.customer_id,
        COUNT(*) AS total_tickets,
        COUNT(CASE WHEN LOWER(t.status) = 'open' THEN 1 END) AS open_tickets,
        COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END) AS critical_tickets,
        ROUND(AVG(t.resolution_time_hrs)::NUMERIC, 1) AS avg_resolution_time_hrs,
        ROUND(COUNT(CASE WHEN LOWER(t.status) = 'resolved' THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0), 1) AS pct_tickets_resolved
    FROM support_tickets t
    GROUP BY t.client_id, t.customer_id
),
rfm_scored AS (
    SELECT client_id, customer_id,
        6 - NTILE(5) OVER (PARTITION BY client_id ORDER BY days_since_last_order ASC) AS rfm_recency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_orders ASC) AS rfm_frequency_score,
        NTILE(5) OVER (PARTITION BY client_id ORDER BY total_spend_usd ASC) AS rfm_monetary_score
    FROM order_agg
),
last_purchase_per_product AS (
    SELECT li.client_id, li.customer_id, li.product_id,
        MAX(o.order_date) AS last_purchase_date
    FROM line_items li
    JOIN orders o ON li.client_id = o.client_id AND li.order_id = o.order_id
    WHERE o.order_status NOT IN ('Cancelled')
    GROUP BY li.client_id, li.customer_id, li.product_id
),
subscription_agg AS (
    SELECT lp.client_id, lp.customer_id,
        COUNT(DISTINCT lp.product_id) AS subscription_product_count,
        ROUND(AVG(sp.avg_refill_days)::NUMERIC, 1) AS avg_refill_cycle_days,
        MAX(EXTRACT(DAY FROM cr.ref_date - (lp.last_purchase_date::TIMESTAMPTZ
                     + (sp.avg_refill_days::TEXT || ' days')::INTERVAL)))::INT AS days_overdue_for_refill,
        SUM(CASE WHEN EXTRACT(DAY FROM cr.ref_date - lp.last_purchase_date::TIMESTAMPTZ)
                      > sp.avg_refill_days * 1.5 THEN 1 ELSE 0 END) AS missed_refill_count
    FROM last_purchase_per_product lp
    JOIN vw_subscription_products sp ON lp.client_id = sp.client_id AND lp.product_id = sp.product_id
        AND sp.is_subscription_product = TRUE
    JOIN client_ref cr ON lp.client_id = cr.client_id
    GROUP BY lp.client_id, lp.customer_id, cr.ref_date
),
repeat_flag AS (
    SELECT oa.client_id, oa.customer_id,
        CASE WHEN oa.total_orders >= cr.min_repeat_orders THEN 1 ELSE 0 END AS is_repeat_customer
    FROM order_agg oa
    JOIN client_ref cr ON oa.client_id = cr.client_id
),
recent_gaps AS (
    SELECT client_id, customer_id,
        ROUND(AVG(gap_days)::NUMERIC, 1) AS recent_avg_gap_days
    FROM (
        SELECT g.client_id, g.customer_id, g.gap_days,
            ROW_NUMBER() OVER (PARTITION BY g.client_id, g.customer_id ORDER BY g.order_date DESC) AS rn,
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
spend_percentiles AS (
    SELECT oa.client_id, oa.customer_id, oa.total_spend_usd,
        PERCENT_RANK() OVER (PARTITION BY oa.client_id ORDER BY oa.total_spend_usd ASC) * 100 AS spend_pct_rank
    FROM order_agg oa
),
tier_assignment AS (
    SELECT sp.client_id, sp.customer_id,
        CASE WHEN cr.tier_method = 'quartile' THEN
            CASE WHEN sp.spend_pct_rank >= cr.high_value_percentile THEN 'Platinum'
                 WHEN sp.spend_pct_rank >= 50 THEN 'Gold'
                 WHEN sp.spend_pct_rank >= 25 THEN 'Silver'
                 ELSE 'Bronze' END
        ELSE
            CASE WHEN sp.total_spend_usd >= cr.custom_platinum_min THEN 'Platinum'
                 WHEN sp.total_spend_usd >= cr.custom_gold_min THEN 'Gold'
                 WHEN sp.total_spend_usd >= cr.custom_silver_min THEN 'Silver'
                 ELSE 'Bronze' END
        END AS customer_tier,
        CASE WHEN cr.tier_method = 'quartile' AND sp.spend_pct_rank >= cr.high_value_percentile THEN 1
             WHEN cr.tier_method != 'quartile' AND sp.total_spend_usd >= cr.custom_platinum_min THEN 1
             ELSE 0 END AS is_high_value
    FROM spend_percentiles sp
    JOIN client_ref cr ON sp.client_id = cr.client_id
)
SELECT
    c.client_id, c.customer_id,
    EXTRACT(DAY FROM cr.ref_date - c.account_created_date::TIMESTAMPTZ)::INT AS account_age_days,
    oa.first_order_date, oa.last_order_date, oa.days_since_last_order,
    oa.total_orders, oa.orders_last_30d, oa.orders_last_90d, oa.orders_last_180d,
    COALESCE(og.avg_days_between_orders, 0) AS avg_days_between_orders,
    COALESCE(og.median_days_between_orders, 0) AS median_days_between_orders,
    ROUND(ABS(COALESCE(og.avg_days_between_orders, 0) - COALESCE(og.median_days_between_orders, 0))::NUMERIC, 1) AS order_gap_mean_median_diff,
    COALESCE(rg.recent_avg_gap_days, 0) AS recent_avg_gap_days,
    oa.total_spend_usd, oa.avg_order_value_usd, oa.max_order_value_usd,
    oa.spend_last_30d_usd, oa.spend_last_90d_usd, oa.spend_last_180d_usd,
    oa.total_discount_usd,
    ROUND(oa.total_discount_usd * 100.0 / NULLIF(oa.total_spend_usd + oa.total_discount_usd, 0)::NUMERIC, 2) AS discount_rate_pct,
    oa.orders_with_discount,
    COALESCE(la.unique_products_purchased, 0) AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, 0) AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, 0) AS avg_items_per_order,
    COALESCE(la.return_rate_pct, 0) AS return_rate_pct,
    COALESCE(ra.total_reviews, 0) AS total_reviews,
    COALESCE(ra.avg_rating, 0) AS avg_rating,
    COALESCE(ra.pct_positive_reviews, 0) AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, 0) AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999) AS days_since_last_review,
    COALESCE(ta.total_tickets, 0) AS total_tickets,
    COALESCE(ta.open_tickets, 0) AS open_tickets,
    COALESCE(ta.critical_tickets, 0) AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, 0) AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, 0) AS pct_tickets_resolved,
    oa.total_spend_usd AS ltv_usd,
    rf.rfm_recency_score, rf.rfm_frequency_score, rf.rfm_monetary_score,
    (rf.rfm_recency_score + rf.rfm_frequency_score + rf.rfm_monetary_score) AS rfm_total_score,
    COALESCE(rpf.is_repeat_customer, 0) AS is_repeat_customer,
    COALESCE(ta2.customer_tier, 'Bronze') AS customer_tier,
    COALESCE(ta2.is_high_value, 0) AS is_high_value,
    COALESCE(sa.subscription_product_count, 0) AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, 0) AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0) AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, 0) AS missed_refill_count,
    CASE WHEN oa.days_since_last_order >= cr.churn_window_days THEN 1 ELSE 0 END AS churn_label,
    cr.ref_date AS computed_at
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
LEFT JOIN tier_assignment   ta2 ON c.client_id = ta2.client_id AND c.customer_id = ta2.customer_id
LEFT JOIN subscription_agg sa  ON c.client_id = sa.client_id  AND c.customer_id = sa.customer_id;

-- Indexes on materialized view
CREATE UNIQUE INDEX idx_mv_cf_pk      ON mv_customer_features (client_id, customer_id);
CREATE INDEX idx_mv_cf_churn          ON mv_customer_features (churn_label, rfm_total_score DESC);
CREATE INDEX idx_mv_cf_recency        ON mv_customer_features (days_since_last_order DESC);
CREATE INDEX idx_mv_cf_overdue        ON mv_customer_features (days_overdue_for_refill DESC);
CREATE INDEX idx_mv_cf_tier           ON mv_customer_features (customer_tier, is_high_value);


-- ── Step 7: Recreate Customer 360 + At-Risk views ───────────

CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT
    c.client_id, c.customer_id, c.customer_name, c.customer_email, c.customer_phone,
    c.account_created_date, c.registration_channel, c.state, c.city,
    c.preferred_device, c.email_opt_in, c.sms_opt_in,
    r.days_since_last_order, r.last_order_date, r.total_orders, r.orders_last_90d,
    r.avg_order_value_usd, r.total_spend_usd, r.ltv_usd,
    r.rfm_total_score, r.rfm_segment, r.customer_tier, r.return_rate_pct, r.account_age_days,
    cs.churn_probability, cs.risk_tier, cs.driver_1, cs.driver_2, cs.driver_3,
    cs.scored_at AS last_scored_at
FROM customers c
LEFT JOIN customer_rfm_features r ON c.client_id = r.client_id AND c.customer_id = r.customer_id
LEFT JOIN LATERAL (
    SELECT * FROM churn_scores s
    WHERE s.client_id = c.client_id AND s.customer_id = c.customer_id
    ORDER BY s.scored_at DESC LIMIT 1
) cs ON TRUE;

CREATE OR REPLACE VIEW vw_at_risk_customers AS
SELECT * FROM vw_customer_360
WHERE risk_tier IN ('HIGH', 'MEDIUM')
ORDER BY churn_probability DESC;


COMMIT;

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW mv_customer_features;
