-- Rename the support-ticket body column ticket_text -> description on both the
-- real and staging tables. Idempotent: only renames when the old column is still
-- present, so it is safe to re-run and a no-op once applied.
--
-- Apply order: after migration_external_signal_emotion.sql (which created
-- ticket_text). On a from-scratch build the column is created as ticket_text then
-- renamed here; on an existing DB it renames the populated column in place.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'support_tickets' AND column_name = 'ticket_text') THEN
    ALTER TABLE support_tickets RENAME COLUMN ticket_text TO description;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'staging_support_tickets' AND column_name = 'ticket_text') THEN
    ALTER TABLE staging_support_tickets RENAME COLUMN ticket_text TO description;
  END IF;
END $$;
