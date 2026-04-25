-- migration_retire_viewer_role.sql
--
-- Platform only recognises super_admin and client_user. Any existing rows
-- with role='viewer' (or the already-retired 'admin') are collapsed into
-- client_user. Safe to run multiple times.

UPDATE users
   SET role = 'client_user'
 WHERE role IN ('viewer', 'admin');

-- Optional: enforce the two-role invariant at the DB layer. Drop the old
-- constraint if present so this migration is idempotent on a DB that has
-- already been through it.
ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_role_chk;

ALTER TABLE users
    ADD CONSTRAINT users_role_chk
    CHECK (role IN ('super_admin', 'client_user'));
