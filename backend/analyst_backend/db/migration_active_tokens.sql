-- ============================================================
-- migration_active_tokens.sql
-- Stores auth tokens in PostgreSQL so they survive backend restarts
-- ============================================================
-- RUN THIS IN pgAdmin: right-click walmart_crp → Query Tool → paste → Execute (F5)
-- ============================================================

CREATE TABLE IF NOT EXISTS active_tokens (
    token       VARCHAR(64)  PRIMARY KEY,
    user_id     VARCHAR(30)  NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ  NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
);

CREATE INDEX IF NOT EXISTS idx_tokens_user ON active_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_tokens_expires ON active_tokens(expires_at);
