-- =====================================================================
-- migration_option2_add_review_ticket_staging.sql
-- =====================================================================
-- Date:    2026-04-16
-- Purpose: Add staging tables for customer_reviews and support_tickets.
--
-- Context: The earlier migration_option2_batch_upload.sql created 11
-- staging tables for catalog + transactional data. But the existing
-- upload_router.py also supports uploading customer_reviews and
-- support_tickets (13 total master types). This migration fills the
-- gap so every uploadable master type has a staging table.
--
-- After this migration runs: 13 staging tables in total.
--
-- Safe to re-run: IF NOT EXISTS guards on everything.
-- =====================================================================

BEGIN;

-- staging_customer_reviews mirrors customer_reviews, without PK/FK/indexes
-- INCLUDING CONSTRAINTS carries over the CHECK (rating BETWEEN 1 AND 5)
CREATE TABLE IF NOT EXISTS staging_customer_reviews (
    LIKE customer_reviews INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_customer_reviews_batch
    ON staging_customer_reviews (batch_id);

CREATE TABLE IF NOT EXISTS staging_support_tickets (
    LIKE support_tickets INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    batch_id        UUID       NOT NULL,
    staging_row_id  BIGSERIAL  PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS ix_stg_support_tickets_batch
    ON staging_support_tickets (batch_id);


-- Verify we now have 13 staging tables total
DO $$
DECLARE
    staging_count INT;
BEGIN
    SELECT COUNT(*) INTO staging_count
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE 'staging_%';

    IF staging_count <> 13 THEN
        RAISE EXCEPTION 'Expected 13 staging tables, found %', staging_count;
    END IF;

    RAISE NOTICE 'OK: % staging tables total (customer_reviews and support_tickets added)', staging_count;
END $$;

COMMIT;
