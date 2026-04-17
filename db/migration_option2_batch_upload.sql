-- =====================================================================
-- migration_option2_batch_upload.sql
-- =====================================================================
-- Date:    2026-04-16
-- Purpose: Replace "insert immediately" upload flow with a staged
--          "upload all, then commit" flow.
--
-- What this migration does:
--   1. Creates `upload_batches` to track each client's pending uploads.
--   2. Creates 11 `staging_*` tables (mirrors of real tables, no PKs/FKs
--      except a synthetic staging_row_id). These act as a holding pen.
--   3. Restores the 15 catalog foreign keys on real tables as
--      DEFERRABLE INITIALLY DEFERRED so the commit transaction can
--      move rows in any order and validate all FKs at COMMIT.
--
-- Why DEFERRABLE: during the batch commit transaction we SET CONSTRAINTS
-- ALL DEFERRED, insert rows from staging into real tables in any order,
-- and let Postgres check referential integrity once at COMMIT. If any
-- FK is violated, the whole transaction rolls back — nothing partially
-- applied.
--
-- Safe to re-run: all DDL uses IF NOT EXISTS / IF EXISTS guards.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 0. Extension for UUID generation
-- ---------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";


-- ---------------------------------------------------------------------
-- 1. upload_batches — tracks one pending batch per client at a time
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS upload_batches (
    batch_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id      VARCHAR(20) NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at   TIMESTAMPTZ,
    discarded_at   TIMESTAMPTZ,
    error_message  TEXT,
    CONSTRAINT ck_batch_status
        CHECK (status IN ('pending', 'committed', 'discarded'))
);

CREATE INDEX IF NOT EXISTS ix_batch_client_status
    ON upload_batches (client_id, status);

-- Enforce at most ONE pending batch per client at any time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_pending_batch_per_client
    ON upload_batches (client_id)
    WHERE status = 'pending';


-- ---------------------------------------------------------------------
-- 2. Staging tables — one per uploadable master type
-- ---------------------------------------------------------------------
-- Each staging table:
--   - Copies all columns + NOT NULL + CHECK + DEFAULTS from the real table
--   - Does NOT copy PK, FK, or indexes (we want the staging area to
--     accept potentially-invalid rows so we can validate explicitly)
--   - Adds batch_id (which batch these rows belong to)
--   - Adds staging_row_id (synthetic surrogate so we can identify
--     individual rows in error messages like "row 42 of products failed")
-- ---------------------------------------------------------------------

-- ── Catalog tables ──
CREATE TABLE IF NOT EXISTS staging_categories (
    LIKE categories INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_categories_batch ON staging_categories (batch_id);

CREATE TABLE IF NOT EXISTS staging_sub_categories (
    LIKE sub_categories INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_sub_categories_batch ON staging_sub_categories (batch_id);

CREATE TABLE IF NOT EXISTS staging_sub_sub_categories (
    LIKE sub_sub_categories INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_sub_sub_categories_batch ON staging_sub_sub_categories (batch_id);

CREATE TABLE IF NOT EXISTS staging_vendors (
    LIKE vendors INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_vendors_batch ON staging_vendors (batch_id);

CREATE TABLE IF NOT EXISTS staging_brands (
    LIKE brands INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_brands_batch ON staging_brands (batch_id);

CREATE TABLE IF NOT EXISTS staging_products (
    LIKE products INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_products_batch ON staging_products (batch_id);

CREATE TABLE IF NOT EXISTS staging_product_prices (
    LIKE product_prices INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_product_prices_batch ON staging_product_prices (batch_id);

CREATE TABLE IF NOT EXISTS staging_product_vendor_mapping (
    LIKE product_vendor_mapping INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_pvm_batch ON staging_product_vendor_mapping (batch_id);

-- ── Transactional tables ──
CREATE TABLE IF NOT EXISTS staging_customers (
    LIKE customers INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_customers_batch ON staging_customers (batch_id);

CREATE TABLE IF NOT EXISTS staging_orders (
    LIKE orders INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_orders_batch ON staging_orders (batch_id);

CREATE TABLE IF NOT EXISTS staging_line_items (
    LIKE line_items INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_line_items_batch ON staging_line_items (batch_id);


-- ---------------------------------------------------------------------
-- 3. Restore 15 catalog FKs as DEFERRABLE INITIALLY DEFERRED
-- ---------------------------------------------------------------------
-- These were dropped in migration_drop_catalog_fks.sql (16:46 today).
-- We're putting them back, but deferred so they check only at COMMIT.
--
-- Note: we wrap each one in a DROP IF EXISTS + ADD CONSTRAINT so this
-- migration is safely re-runnable.
-- ---------------------------------------------------------------------

-- 3.1 Catalog chain
ALTER TABLE sub_categories
    DROP CONSTRAINT IF EXISTS sub_categories_category_fk,
    ADD CONSTRAINT sub_categories_category_fk
        FOREIGN KEY (client_id, category_id)
        REFERENCES categories (client_id, category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE sub_sub_categories
    DROP CONSTRAINT IF EXISTS sub_sub_categories_sub_category_fk,
    ADD CONSTRAINT sub_sub_categories_sub_category_fk
        FOREIGN KEY (client_id, sub_category_id)
        REFERENCES sub_categories (client_id, sub_category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE sub_sub_categories
    DROP CONSTRAINT IF EXISTS sub_sub_categories_category_fk,
    ADD CONSTRAINT sub_sub_categories_category_fk
        FOREIGN KEY (client_id, category_id)
        REFERENCES categories (client_id, category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE brands
    DROP CONSTRAINT IF EXISTS brands_vendor_fk,
    ADD CONSTRAINT brands_vendor_fk
        FOREIGN KEY (client_id, vendor_id)
        REFERENCES vendors (client_id, vendor_id)
        DEFERRABLE INITIALLY DEFERRED;

-- 3.2 Product hub
ALTER TABLE products
    DROP CONSTRAINT IF EXISTS products_category_fk,
    ADD CONSTRAINT products_category_fk
        FOREIGN KEY (client_id, category_id)
        REFERENCES categories (client_id, category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE products
    DROP CONSTRAINT IF EXISTS products_sub_category_fk,
    ADD CONSTRAINT products_sub_category_fk
        FOREIGN KEY (client_id, sub_category_id)
        REFERENCES sub_categories (client_id, sub_category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE products
    DROP CONSTRAINT IF EXISTS products_sub_sub_category_fk,
    ADD CONSTRAINT products_sub_sub_category_fk
        FOREIGN KEY (client_id, sub_sub_category_id)
        REFERENCES sub_sub_categories (client_id, sub_sub_category_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE products
    DROP CONSTRAINT IF EXISTS products_brand_fk,
    ADD CONSTRAINT products_brand_fk
        FOREIGN KEY (client_id, brand_id)
        REFERENCES brands (client_id, brand_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE products
    DROP CONSTRAINT IF EXISTS fk_products_price,
    ADD CONSTRAINT fk_products_price
        FOREIGN KEY (client_id, product_price_id)
        REFERENCES product_prices (client_id, price_id)
        DEFERRABLE INITIALLY DEFERRED;

-- 3.3 Product-dependent tables
ALTER TABLE product_prices
    DROP CONSTRAINT IF EXISTS product_prices_product_fk,
    ADD CONSTRAINT product_prices_product_fk
        FOREIGN KEY (client_id, product_id)
        REFERENCES products (client_id, product_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE product_vendor_mapping
    DROP CONSTRAINT IF EXISTS pvm_product_fk,
    ADD CONSTRAINT pvm_product_fk
        FOREIGN KEY (client_id, product_id)
        REFERENCES products (client_id, product_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE product_vendor_mapping
    DROP CONSTRAINT IF EXISTS pvm_brand_fk,
    ADD CONSTRAINT pvm_brand_fk
        FOREIGN KEY (client_id, brand_id)
        REFERENCES brands (client_id, brand_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE product_vendor_mapping
    DROP CONSTRAINT IF EXISTS pvm_vendor_fk,
    ADD CONSTRAINT pvm_vendor_fk
        FOREIGN KEY (client_id, vendor_id)
        REFERENCES vendors (client_id, vendor_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE line_items
    DROP CONSTRAINT IF EXISTS line_items_product_fk,
    ADD CONSTRAINT line_items_product_fk
        FOREIGN KEY (client_id, product_id)
        REFERENCES products (client_id, product_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE customer_reviews
    DROP CONSTRAINT IF EXISTS customer_reviews_product_fk,
    ADD CONSTRAINT customer_reviews_product_fk
        FOREIGN KEY (client_id, product_id)
        REFERENCES products (client_id, product_id)
        DEFERRABLE INITIALLY DEFERRED;


-- ---------------------------------------------------------------------
-- 4. Verification — fail loudly if something didn't land right
-- ---------------------------------------------------------------------
DO $$
DECLARE
    staging_count INT;
    fk_count      INT;
BEGIN
    SELECT COUNT(*) INTO staging_count
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE 'staging_%';

    IF staging_count < 11 THEN
        RAISE EXCEPTION 'Expected 11 staging tables, found %', staging_count;
    END IF;

    SELECT COUNT(*) INTO fk_count
    FROM information_schema.table_constraints
    WHERE constraint_type = 'FOREIGN KEY'
      AND table_schema = 'public'
      AND constraint_name IN (
          'sub_categories_category_fk',
          'sub_sub_categories_sub_category_fk',
          'sub_sub_categories_category_fk',
          'brands_vendor_fk',
          'products_category_fk',
          'products_sub_category_fk',
          'products_sub_sub_category_fk',
          'products_brand_fk',
          'fk_products_price',
          'product_prices_product_fk',
          'pvm_product_fk',
          'pvm_brand_fk',
          'pvm_vendor_fk',
          'line_items_product_fk',
          'customer_reviews_product_fk'
      );

    IF fk_count <> 15 THEN
        RAISE EXCEPTION 'Expected 15 catalog FKs, found %', fk_count;
    END IF;

    RAISE NOTICE 'OK: % staging tables, % catalog FKs restored', staging_count, fk_count;
END $$;

COMMIT;

-- =====================================================================
-- Post-migration sanity queries (run manually if you want to verify)
-- =====================================================================
--
-- 1. Count staging tables:
--    SELECT table_name FROM information_schema.tables
--    WHERE table_schema='public' AND table_name LIKE 'staging_%'
--    ORDER BY table_name;
--
-- 2. Check FKs are deferrable:
--    SELECT con.conname, con.condeferrable, con.condeferred
--    FROM pg_constraint con
--    JOIN pg_class rel ON rel.oid = con.conrelid
--    WHERE con.contype = 'f'
--      AND con.conname IN (
--          'sub_categories_category_fk', 'products_category_fk',
--          'line_items_product_fk', 'customer_reviews_product_fk'
--      );
--    -- Expected: condeferrable=t, condeferred=t for all four
-- =====================================================================
