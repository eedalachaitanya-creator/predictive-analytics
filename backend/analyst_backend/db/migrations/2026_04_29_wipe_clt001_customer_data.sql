-- ====================================================================
-- 2026_04_29_wipe_clt001_customer_data.sql
-- Clears all customer + transaction data for CLT-001 so the new
-- 700-customer dataset can be loaded onto a fresh slate.
--
-- WHAT'S DELETED (in FK-safe order, children first):
--   retention_interventions, churn_scores, customer_rfm_features,
--   customer_purchase_cycles, customer_reviews, support_tickets,
--   line_items, orders, outreach_messages, customer_price_context,
--   pipeline_outputs, customers
--
-- WHAT'S PRESERVED:
--   * client_config row for CLT-001 (the tenant definition itself)
--   * products, brands, categories, sub_categories, sub_sub_categories,
--     vendors, product_prices, product_vendor_mapping (the catalog —
--     same products will be referenced by the new orders)
--   * audit_log (compliance — never wipe historical audit trail)
--   * chat_messages, llm_cost_log (optional — keep agent history)
--
-- IF you want a FULL tenant wipe (also delete the catalog), uncomment
-- the "Scope B extension" block at the bottom.
--
-- After running this, refresh the MV (the script does it automatically)
-- and then upload the new dataset via the UI.
-- ====================================================================

BEGIN;

-- Show what we're about to delete (visible in psql output).
\echo '── Pre-delete row counts for CLT-001 ──'
SELECT 'customers'                AS table_name, COUNT(*) AS rows FROM customers                WHERE client_id = 'CLT-001'
UNION ALL SELECT 'orders',                       COUNT(*)        FROM orders                   WHERE client_id = 'CLT-001'
UNION ALL SELECT 'line_items',                   COUNT(*)        FROM line_items               WHERE client_id = 'CLT-001'
UNION ALL SELECT 'customer_reviews',             COUNT(*)        FROM customer_reviews         WHERE client_id = 'CLT-001'
UNION ALL SELECT 'support_tickets',              COUNT(*)        FROM support_tickets          WHERE client_id = 'CLT-001'
UNION ALL SELECT 'churn_scores',                 COUNT(*)        FROM churn_scores             WHERE client_id = 'CLT-001'
UNION ALL SELECT 'customer_rfm_features',        COUNT(*)        FROM customer_rfm_features    WHERE client_id = 'CLT-001'
UNION ALL SELECT 'customer_purchase_cycles',     COUNT(*)        FROM customer_purchase_cycles WHERE client_id = 'CLT-001'
UNION ALL SELECT 'retention_interventions',      COUNT(*)        FROM retention_interventions  WHERE client_id = 'CLT-001'
UNION ALL SELECT 'outreach_messages',            COUNT(*)        FROM outreach_messages        WHERE client_id = 'CLT-001'
UNION ALL SELECT 'customer_price_context',       COUNT(*)        FROM customer_price_context   WHERE client_id = 'CLT-001'
UNION ALL SELECT 'pipeline_outputs',             COUNT(*)        FROM pipeline_outputs         WHERE client_id = 'CLT-001'
ORDER BY table_name;


-- ── Children first, parents last (FK-safe order) ──────────────────────

-- 1. retention_interventions FK → churn_scores; delete this first.
DELETE FROM retention_interventions  WHERE client_id = 'CLT-001';

-- 2. ML output tables (no children).
DELETE FROM churn_scores             WHERE client_id = 'CLT-001';
DELETE FROM customer_rfm_features    WHERE client_id = 'CLT-001';
DELETE FROM customer_purchase_cycles WHERE client_id = 'CLT-001';

-- 3. Customer-attached data.
DELETE FROM customer_reviews         WHERE client_id = 'CLT-001';
DELETE FROM support_tickets          WHERE client_id = 'CLT-001';
DELETE FROM customer_price_context   WHERE client_id = 'CLT-001';
DELETE FROM outreach_messages        WHERE client_id = 'CLT-001';

-- 4. Order-side: line_items must go before orders (FK).
DELETE FROM line_items               WHERE client_id = 'CLT-001';
DELETE FROM orders                   WHERE client_id = 'CLT-001';

-- 5. Pipeline outputs (saved CSVs / reports from past runs — irrelevant now).
DELETE FROM pipeline_outputs         WHERE client_id = 'CLT-001';

-- 6. Customers last (parent of orders, reviews, tickets, etc.).
DELETE FROM customers                WHERE client_id = 'CLT-001';


-- ── Refresh the MV so dashboards reflect the empty state ──────────────
-- mv_customer_features is computed FROM customers, so a refresh now will
-- produce zero rows for CLT-001 (the rest of the tenants are unaffected
-- because the MV groups by client_id).
REFRESH MATERIALIZED VIEW mv_customer_features;


\echo ''
\echo '── Post-delete row counts for CLT-001 (should all be 0) ──'
SELECT 'customers'                AS table_name, COUNT(*) AS rows FROM customers                WHERE client_id = 'CLT-001'
UNION ALL SELECT 'orders',                       COUNT(*)        FROM orders                   WHERE client_id = 'CLT-001'
UNION ALL SELECT 'line_items',                   COUNT(*)        FROM line_items               WHERE client_id = 'CLT-001'
UNION ALL SELECT 'mv_customer_features',         COUNT(*)        FROM mv_customer_features     WHERE client_id = 'CLT-001'
ORDER BY table_name;


COMMIT;


-- ════════════════════════════════════════════════════════════════════
-- Scope B extension — uncomment if you ALSO want to wipe the catalog
-- (products, brands, categories, vendors, prices). DO NOT run inside
-- the BEGIN above; this is a separate transaction.
-- ════════════════════════════════════════════════════════════════════
--
-- BEGIN;
-- DELETE FROM product_vendor_mapping  WHERE client_id = 'CLT-001';
-- DELETE FROM product_prices          WHERE client_id = 'CLT-001';
-- DELETE FROM products                WHERE client_id = 'CLT-001';
-- DELETE FROM sub_sub_categories      WHERE client_id = 'CLT-001';
-- DELETE FROM sub_categories          WHERE client_id = 'CLT-001';
-- DELETE FROM categories              WHERE client_id = 'CLT-001';
-- DELETE FROM brands                  WHERE client_id = 'CLT-001';
-- DELETE FROM vendors                 WHERE client_id = 'CLT-001';
-- COMMIT;
