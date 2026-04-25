-- migration_client_soft_delete.sql
--
-- Switches client deletion from hard-delete (cascade-wipes every tenant row)
-- to soft-delete: the client_config row stays, is_active flips to FALSE, and
-- the list endpoint filters inactive clients out of the admin UI.
--
-- Safe to run multiple times — IF NOT EXISTS guards on both column adds.

ALTER TABLE client_config
    ADD COLUMN IF NOT EXISTS is_active       BOOLEAN      NOT NULL DEFAULT TRUE;

ALTER TABLE client_config
    ADD COLUMN IF NOT EXISTS deactivated_at  TIMESTAMPTZ;

-- Fast filter for the "active clients only" list query.
CREATE INDEX IF NOT EXISTS idx_client_config_is_active
    ON client_config(is_active);
