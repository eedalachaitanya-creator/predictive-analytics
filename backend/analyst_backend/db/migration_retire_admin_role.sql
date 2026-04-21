-- ============================================================
-- migration_retire_admin_role.sql
-- One-time data migration. Retires the 'admin' user role by
-- converting every existing 'admin' row to 'client_user'
-- (their permission set was functionally identical).
--
-- Run this ONCE in pgAdmin on any database that was seeded
-- with the original migration_users_table.sql, which inserted
-- usr-003 (analyst@walmart.com) with role='admin'.
--
-- Safe to re-run: the UPDATE is idempotent (if no rows match,
-- nothing changes).
-- ============================================================
-- RUN THIS IN pgAdmin: right-click walmart_crp → Query Tool → paste → Execute (F5)
-- ============================================================

-- 1. Show what we're about to change (review before committing)
SELECT user_id, email, name, role
FROM users
WHERE role = 'admin'
ORDER BY user_id;

-- 2. Convert every 'admin' row to 'client_user'
UPDATE users
SET role = 'client_user'
WHERE role = 'admin';

-- 3. Verify: no admin rows should remain
SELECT user_id, email, name, role
FROM users
ORDER BY user_id;
