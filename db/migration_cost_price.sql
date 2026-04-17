-- Migration: Add cost_price column to product_prices
-- =====================================================
-- cost_price_usd = what the client pays their supplier for the product
-- unit_price_usd = what the client charges the customer
-- margin = unit_price_usd - cost_price_usd
--
-- This enables safe discount recommendations:
--   max_safe_discount = ((unit_price - cost_price) / unit_price) * 100
--
-- Run: psql -d walmart_crp -f db/migration_cost_price.sql

-- 1. Add the column
ALTER TABLE product_prices
    ADD COLUMN IF NOT EXISTS cost_price_usd NUMERIC(10,2);

-- 2. Generate realistic cost prices (40-70% of selling price)
--    Uses a seeded random so results are reproducible per product_id
--    Formula: cost = unit_price * (0.40 + (random_factor * 0.30))
--    This gives margins between 30% and 60% which is realistic for retail
UPDATE product_prices
SET cost_price_usd = ROUND(
    unit_price_usd * (0.40 + (('x' || substr(md5(price_id::text), 1, 8))::bit(32)::int::numeric / 2147483647.0 + 0.5) * 0.30),
    2
)
WHERE cost_price_usd IS NULL;

-- 3. Verify: show margin distribution
SELECT
    COUNT(*) AS total_products,
    ROUND(AVG(cost_price_usd), 2) AS avg_cost,
    ROUND(AVG(unit_price_usd), 2) AS avg_price,
    ROUND(AVG((unit_price_usd - cost_price_usd) / NULLIF(unit_price_usd, 0) * 100), 1) AS avg_margin_pct,
    ROUND(MIN((unit_price_usd - cost_price_usd) / NULLIF(unit_price_usd, 0) * 100), 1) AS min_margin_pct,
    ROUND(MAX((unit_price_usd - cost_price_usd) / NULLIF(unit_price_usd, 0) * 100), 1) AS max_margin_pct
FROM product_prices
WHERE cost_price_usd IS NOT NULL;
