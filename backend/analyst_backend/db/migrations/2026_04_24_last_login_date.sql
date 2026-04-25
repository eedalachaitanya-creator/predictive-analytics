-- ═══════════════════════════════════════════════════════════════════════════
-- 2026-04-24  Login-aware churn  —  Phase 1: STORAGE ONLY
-- ═══════════════════════════════════════════════════════════════════════════
-- Adds two columns:
--   1. customers.last_login_date         — DATE, captured at upload time
--   2. client_config.login_window_days   — INT, per-tenant threshold
--
-- This migration ONLY adds the columns and lets the upload pipeline begin
-- storing the new field. The materialized view `mv_customer_features` is
-- NOT recreated here — the model still uses the old single-condition churn
-- label until a separate Phase-2 migration drops/recreates the MV with the
-- two-condition rule.
--
-- Why split it: validating the data lands correctly in `customers` is much
-- safer than rebuilding the MV on top of unverified data. Run this, upload
-- the new customer master, run a `SELECT customer_id, last_login_date
-- FROM customers WHERE client_id = 'CLT-001' LIMIT 10;` to confirm the
-- column populated, THEN we ship Phase 2.
--
-- Run order: open pgAdmin → Query Tool → connect to walmart_crp database →
-- paste this whole file → execute. Both ALTERs are idempotent (`IF NOT
-- EXISTS`), so re-running is safe.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- 1. Customer master: track each customer's most recent login.
ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS last_login_date DATE;

COMMENT ON COLUMN customers.last_login_date IS
    'Most recent login (any session, ordered or not). Used by Phase 2 to '
    'compute days_since_last_login and apply the two-condition churn rule.';

-- 2. Client config: per-tenant threshold for the upcoming churn rule
--    (default 30 days — chosen because login is a much cheaper engagement
--    signal than ordering, so the inactivity window is shorter than the
--    90-day order window).
ALTER TABLE client_config
    ADD COLUMN IF NOT EXISTS login_window_days INT DEFAULT 30;

COMMENT ON COLUMN client_config.login_window_days IS
    'Days of login inactivity before a customer is eligible to be flagged '
    'churned. Combined with churn_window_days in mv_customer_features: '
    'a customer is churned only if BOTH thresholds are exceeded.';

COMMIT;

-- ── Verification queries (run after the migration) ──────────────────────────
-- 1. Both columns exist with correct types/defaults:
--      \d customers       -- look for last_login_date date
--      \d client_config   -- look for login_window_days int default 30
--
-- 2. After re-uploading customer master, last_login_date is populated:
--      SELECT customer_id, account_created_date, last_login_date
--      FROM customers
--      WHERE client_id = 'CLT-001'
--      ORDER BY last_login_date DESC NULLS LAST
--      LIMIT 10;
--
-- 3. Existing rows have NULL last_login_date until re-upload (expected):
--      SELECT COUNT(*) AS total,
--             COUNT(last_login_date) AS with_login_date,
--             COUNT(*) - COUNT(last_login_date) AS missing_login_date
--      FROM customers;
-- Should show last_login_date as a DATE column
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'customers' AND column_name = 'last_login_date';

-- Should show login_window_days INT default 30
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'client_config' AND column_name = 'login_window_days';

-- Existing customers should have NULL last_login_date (expected — they
-- predate the column)
SELECT COUNT(*) AS total,
       COUNT(last_login_date) AS populated,
       COUNT(*) - COUNT(last_login_date) AS missing
FROM customers;
