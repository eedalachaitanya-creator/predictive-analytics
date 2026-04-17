-- ============================================================
-- migration_users_table.sql
-- Creates a proper 'users' table in PostgreSQL
-- so user accounts are stored permanently (not in memory).
-- ============================================================
-- RUN THIS IN pgAdmin: right-click walmart_crp → Query Tool → paste → Execute (F5)
-- ============================================================

-- 1. Create the users table
CREATE TABLE IF NOT EXISTS users (
    user_id         VARCHAR(30)   PRIMARY KEY,
    email           VARCHAR(150)  NOT NULL UNIQUE,
    password_hash   VARCHAR(255)  NOT NULL,        -- plain text for now (use bcrypt in production!)
    name            VARCHAR(100)  NOT NULL,
    role            VARCHAR(20)   NOT NULL DEFAULT 'client_user',
                                                    -- 'super_admin', 'admin', 'client_user', 'viewer'
    client_access   TEXT[]        NOT NULL DEFAULT '{}',
                                                    -- e.g., {'CLT-001'} or {'CLT-001','CLT-002'} or {'*'}
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_login      TIMESTAMPTZ
);

-- 2. Insert the default users (same ones that were hardcoded)
INSERT INTO users (user_id, email, password_hash, name, role, client_access)
VALUES
    ('usr-001', 'admin@walmart.com',   'admin123',   'Admin User',   'super_admin', ARRAY['*']),
    ('usr-002', 'ops@walmart.com',     'ops123',     'Walmart Ops',  'client_user', ARRAY['CLT-001']),
    ('usr-003', 'analyst@walmart.com', 'analyst123', 'Analyst User', 'admin',       ARRAY['CLT-001', 'CLT-002'])
ON CONFLICT (user_id) DO NOTHING;

-- 3. Create an index on email for fast login lookups
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

-- 4. Verify
SELECT user_id, email, name, role, client_access, is_active, created_at
FROM users
ORDER BY user_id;
