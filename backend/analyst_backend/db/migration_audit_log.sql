-- ============================================================
-- migration_audit_log.sql
-- Creates the immutable audit_log table that records every
-- meaningful action taken inside the platform (login, pipeline
-- run, file upload, settings save, client create/delete, user
-- create/delete, user status toggle, etc.).
--
-- Design notes:
--   * user_id is nullable — we also record failed-login attempts
--     where no user session ever existed.
--   * user_email is stored directly (not a FK to users.email)
--     so deleting the user does NOT orphan the audit trail.
--   * client_id is nullable — SYSTEM events (global config
--     changes, super-admin logins) aren't scoped to a tenant.
--   * outcome enum: 'success' | 'warning' | 'failure'.
--     The /audit UI colours ✅ / ⚠️ / ❌ accordingly.
--   * The table is write-once: we never UPDATE or DELETE rows
--     from application code. A scheduled cron (not in this
--     migration) can purge rows older than 365 days.
-- ============================================================
-- RUN THIS IN pgAdmin: right-click walmart_crp → Query Tool → paste → Execute (F5)
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL     PRIMARY KEY,
    ts            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    user_id       VARCHAR(30),                       -- nullable: login-fail has no user
    user_email    VARCHAR(255),                      -- stored directly so user-delete doesn't orphan
    client_id     VARCHAR(30),                       -- nullable: SYSTEM events
    action_type   VARCHAR(60)   NOT NULL,            -- 'login', 'pipeline_run', 'file_upload', etc.
    details       TEXT,                              -- free-form human-readable summary
    ip_address    VARCHAR(45),                       -- IPv4 max 15, IPv6 max 45
    outcome       VARCHAR(20)   NOT NULL DEFAULT 'success'  -- 'success' | 'warning' | 'failure'
);

-- Indexes:
-- - ts DESC is the primary sort for the UI (most-recent first).
-- - client_id / user_id / action_type are the main filter columns.
-- - outcome is filtered in the UI and used for the "warnings" KPI.
CREATE INDEX IF NOT EXISTS idx_audit_ts          ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_client      ON audit_log(client_id);
CREATE INDEX IF NOT EXISTS idx_audit_user        ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_action      ON audit_log(action_type);
CREATE INDEX IF NOT EXISTS idx_audit_outcome     ON audit_log(outcome);

-- Sanity check: insert one SYSTEM row so the first page-load in the UI
-- isn't empty on a brand-new install. Idempotent via NOT EXISTS.
INSERT INTO audit_log (user_email, client_id, action_type, details, ip_address, outcome)
SELECT 'system', NULL, 'schema_migration', 'audit_log table created', '127.0.0.1', 'success'
WHERE NOT EXISTS (SELECT 1 FROM audit_log);

-- Done. Verify:
--   SELECT COUNT(*) FROM audit_log;       -- should be ≥ 1
--   \d audit_log                          -- shows columns + indexes
