-- ============================================================
-- MIGRATION: Drop FK constraints between catalog/reference tables
-- ============================================================
-- WHY: Upload order should NOT matter. Clients upload all their
--      Excel files at once, and the system should accept them
--      in any order. FK constraints between catalog tables
--      (categories → products → brands → vendors) force a
--      specific upload order, which is bad UX.
--
-- WHAT WE DROP: FKs between reference/catalog tables only
-- WHAT WE KEEP: FKs on transactional tables (orders → customers, etc.)
--
-- RUN WITH:
--   /Applications/Postgres.app/Contents/Versions/18/bin/psql -d walmart_crp -f db/migration_drop_catalog_fks.sql
-- ============================================================

-- sub_categories → categories
ALTER TABLE sub_categories         DROP CONSTRAINT IF EXISTS sub_categories_category_fk;

-- sub_sub_categories → sub_categories, categories
ALTER TABLE sub_sub_categories     DROP CONSTRAINT IF EXISTS sub_sub_categories_sub_category_fk;
ALTER TABLE sub_sub_categories     DROP CONSTRAINT IF EXISTS sub_sub_categories_category_fk;

-- brands → vendors
ALTER TABLE brands                 DROP CONSTRAINT IF EXISTS brands_vendor_fk;

-- products → categories, sub_categories, sub_sub_categories, brands
ALTER TABLE products               DROP CONSTRAINT IF EXISTS products_category_fk;
ALTER TABLE products               DROP CONSTRAINT IF EXISTS products_sub_category_fk;
ALTER TABLE products               DROP CONSTRAINT IF EXISTS products_sub_sub_category_fk;
ALTER TABLE products               DROP CONSTRAINT IF EXISTS products_brand_fk;

-- products ↔ product_prices (circular deferred FK)
ALTER TABLE products               DROP CONSTRAINT IF EXISTS fk_products_price;

-- product_prices → products
ALTER TABLE product_prices         DROP CONSTRAINT IF EXISTS product_prices_product_fk;

-- product_vendor_mapping → products, brands, vendors
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS pvm_product_fk;
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS pvm_brand_fk;
ALTER TABLE product_vendor_mapping DROP CONSTRAINT IF EXISTS pvm_vendor_fk;

-- line_items → products (this is a cross-reference table FK)
ALTER TABLE line_items             DROP CONSTRAINT IF EXISTS line_items_product_fk;

-- customer_reviews → products
ALTER TABLE customer_reviews       DROP CONSTRAINT IF EXISTS customer_reviews_product_fk;

-- ============================================================
-- KEPT (transactional integrity):
--   orders (client_id, customer_id) → customers
--   line_items (client_id, order_id) → orders
--   churn_scores (client_id, customer_id) → customers
--   customer_reviews (client_id, customer_id) → customers
--   support_tickets (client_id, customer_id) → customers
-- These ensure a customer exists before orders/reviews/tickets
-- are uploaded — which is the natural flow.
-- ============================================================

-- Verify: show remaining FKs
SELECT
    tc.table_name AS source_table,
    tc.constraint_name,
    ccu.table_name AS referenced_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.constraint_schema = 'public'
ORDER BY tc.table_name;
