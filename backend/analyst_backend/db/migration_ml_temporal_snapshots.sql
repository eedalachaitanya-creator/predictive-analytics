-- ─────────────────────────────────────────────────────────────────────────────
-- Migration: ml_temporal_snapshots — point-in-time (<=T) churn staging table
-- ─────────────────────────────────────────────────────────────────────────────
-- Part of the temporal churn redesign
-- (docs/superpowers/specs/2026-06-03-temporal-churn-redesign-design.md §10.1).
--
-- ADDITIVE & DECOUPLED:
--   * This table is SEPARATE from the live scoring table — the temporal builder
--     writes ONLY here, never to the live scores table or the live feature view.
--   * No foreign keys back into live tables, so it can be dropped/rebuilt freely
--     and never participates in live scoring, the dashboard, or the strategist.
--   * Tenant-scoped: every row carries client_id; a per-tenant rebuild touches
--     only that tenant's rows.
--   * It is NOT run by the live pipeline; apply it manually against a local DB:
--         psql "$DB_URL" -f db/migration_ml_temporal_snapshots.sql
--
-- One row per eligible (client_id, customer_id, cutoff_date): the forward churn
-- label plus the full <=T feature vector as a JSONB payload.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ml_temporal_snapshots (
    snapshot_id  BIGSERIAL PRIMARY KEY,
    client_id    TEXT        NOT NULL,
    customer_id  TEXT        NOT NULL,
    cutoff_date  DATE        NOT NULL,
    churned      SMALLINT    NOT NULL,
    features     JSONB       NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_ml_temporal_snapshot
        UNIQUE (client_id, customer_id, cutoff_date)
);

-- Read path: load a tenant's whole multi-cutoff dataset, ordered by cutoff.
CREATE INDEX IF NOT EXISTS idx_ml_temporal_snapshots_client_cutoff
    ON ml_temporal_snapshots (client_id, cutoff_date);
