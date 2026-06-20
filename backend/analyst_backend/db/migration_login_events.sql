-- Login event log — one row per login, the engagement analogue of `orders`.
-- Enables point-in-time login-recency features in the temporal churn model
-- (a login at time t is reconstructable as-of any cutoff T, unlike the single
-- mutable customers.last_login_date column). Append-only fact table.
CREATE TABLE IF NOT EXISTS login_events (
    client_id     TEXT        NOT NULL,
    login_id      TEXT        NOT NULL,
    customer_id   TEXT        NOT NULL,
    login_at      TIMESTAMPTZ NOT NULL,
    login_channel TEXT,
    CONSTRAINT pk_login_events PRIMARY KEY (client_id, login_id),
    CONSTRAINT fk_login_events_customer
        FOREIGN KEY (client_id, customer_id)
        REFERENCES customers (client_id, customer_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_login_events_client_cust_at
    ON login_events (client_id, customer_id, login_at);

-- Staging table — the upload pipeline writes parsed rows here (keyed by
-- batch_id) before committing into login_events. Mirrors the other
-- staging_<table>s: raw data cols + batch_id + a BIGSERIAL row id PK. No FKs
-- (the FK pre-flight validates customer_id separately).
CREATE TABLE IF NOT EXISTS staging_login_events (
    client_id      VARCHAR     NOT NULL,
    login_id       VARCHAR     NOT NULL,
    customer_id    VARCHAR     NOT NULL,
    login_at       TIMESTAMPTZ,
    login_channel  VARCHAR,
    batch_id       UUID        NOT NULL,
    staging_row_id BIGSERIAL   PRIMARY KEY
);
