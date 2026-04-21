-- ============================================================
-- migration_remove_analyst_user.sql
-- Deletes the shared "Analyst User" (usr-003 / analyst@walmart.com).
--
-- WHY:
--   The seed originally inserted a single account with access to
--   BOTH CLT-001 and CLT-002. A shared cross-client account breaks
--   tenant isolation — each client should have its own scoped user.
--   The seed file (migration_users_table.sql) no longer creates this
--   row; this migration cleans it out of any live database where the
--   earlier seed was already run.
--
-- WHEN TO RUN:
--   Only once, on any live DB that still has the row.
--   Safe to re-run: idempotent (DELETE on a missing row is a no-op).
--
-- HOW TO RUN (pgAdmin 4):
--   1. Open pgAdmin 4.
--   2. Servers → your server → Databases → walmart_crp.
--   3. Right-click walmart_crp → Query Tool.
--   4. Paste this file's contents → Execute (F5).
--   5. Confirm the "Before" SELECT shows the row, the DELETE runs,
--      and the "After" SELECT returns 0 rows.
-- ============================================================

-- 1. Show the row we're about to delete (sanity check)
SELECT user_id, email, name, role, client_access
FROM users
WHERE user_id = 'usr-003'
   OR email   = 'analyst@walmart.com';

-- 2. Delete by both user_id AND email so we catch it either way
DELETE FROM users
WHERE user_id = 'usr-003'
   OR email   = 'analyst@walmart.com';

-- 3. Verify it's gone (expect 0 rows)
SELECT user_id, email, name, role, client_access
FROM users
WHERE user_id = 'usr-003'
   OR email   = 'analyst@walmart.com';

-- 4. Show remaining users so you can confirm the clean state
SELECT user_id, email, name, role, client_access
FROM users
ORDER BY user_id;
