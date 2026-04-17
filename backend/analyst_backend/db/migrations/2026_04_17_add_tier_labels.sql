-- ─────────────────────────────────────────────────────────────────────────
-- Migration: add tier_label_* columns to client_config
-- Date:      2026-04-17
-- Reason:    The Settings page has four text inputs for tier display names
--            (Platinum/Gold/Silver/Bronze) but there was no column to persist
--            them. This adds the columns and seeds them with the default
--            emoji labels so existing rows are unchanged visually.
-- Run:       pgAdmin 4 → Query Tool against walmart_crp → paste + run
-- ─────────────────────────────────────────────────────────────────────────

BEGIN;

ALTER TABLE client_config
    ADD COLUMN IF NOT EXISTS tier_label_platinum VARCHAR(50) DEFAULT '💎 Platinum',
    ADD COLUMN IF NOT EXISTS tier_label_gold     VARCHAR(50) DEFAULT '🥇 Gold',
    ADD COLUMN IF NOT EXISTS tier_label_silver   VARCHAR(50) DEFAULT '🥈 Silver',
    ADD COLUMN IF NOT EXISTS tier_label_bronze   VARCHAR(50) DEFAULT '🥉 Bronze';

-- Backfill existing rows where NULL (the DEFAULT only applies to new rows)
UPDATE client_config
SET tier_label_platinum = COALESCE(tier_label_platinum, '💎 Platinum'),
    tier_label_gold     = COALESCE(tier_label_gold,     '🥇 Gold'),
    tier_label_silver   = COALESCE(tier_label_silver,   '🥈 Silver'),
    tier_label_bronze   = COALESCE(tier_label_bronze,   '🥉 Bronze');

COMMIT;

-- Verify:
-- SELECT client_id, tier_label_platinum, tier_label_gold,
--        tier_label_silver, tier_label_bronze
-- FROM client_config;
