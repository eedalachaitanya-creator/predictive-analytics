-- Per-tenant external-signal integration config (Jira today; Google/Zendesk later).
-- Each (client_id, provider) is one connection. The API token is stored ENCRYPTED
-- (Fernet ciphertext in api_token_enc) — NEVER plaintext. Decryption happens only
-- in-process, with the key from env INTEGRATION_ENC_KEY.
CREATE TABLE IF NOT EXISTS tenant_integrations (
    integration_id      SERIAL PRIMARY KEY,
    client_id           VARCHAR        NOT NULL,
    provider            VARCHAR        NOT NULL DEFAULT 'jira',
    base_url            VARCHAR,
    email               VARCHAR,
    api_token_enc       TEXT,                                  -- Fernet ciphertext
    project_key         VARCHAR,
    customer_strategy   VARCHAR        NOT NULL DEFAULT 'auto',        -- auto|field|label
    customer_field_name VARCHAR        NOT NULL DEFAULT 'Customer ID',
    enabled             BOOLEAN        NOT NULL DEFAULT FALSE,
    last_sync_at        TIMESTAMPTZ,
    last_sync_status    VARCHAR,                               -- ok | error
    last_sync_detail    VARCHAR,                               -- count or error text
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    UNIQUE (client_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_tenant_integrations_client
    ON tenant_integrations (client_id);
