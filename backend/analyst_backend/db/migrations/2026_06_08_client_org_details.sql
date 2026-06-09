-- 2026-06-08 · Client onboarding: organization details + admin phone
-- =============================================================================
-- The "Add New Client" flow now captures full organization details (address,
-- city, state/province, postal code, country, a company contact email + phone)
-- plus an administrator account (name, phone, login email, password) — mirroring
-- the English-Proficiency project's client onboarding form.
--
-- The Company Code field was removed from the form: client_id (auto-generated
-- CLT-###) is the sole identifier, and new clients get client_code = client_id.
--
-- All columns are NULLABLE so the 11 existing tenants and 12 existing users are
-- unaffected — required-ness is enforced at the API/form layer for NEW clients
-- only. Idempotent (IF NOT EXISTS) so it is safe to re-run and safe to replay on
-- the live DB later.

-- ── Organization details on the tenant row ───────────────────────────────────
ALTER TABLE client_config
  ADD COLUMN IF NOT EXISTS address        varchar(255),
  ADD COLUMN IF NOT EXISTS city           varchar(100),
  ADD COLUMN IF NOT EXISTS state_province varchar(100),
  ADD COLUMN IF NOT EXISTS postal_code    varchar(20),
  ADD COLUMN IF NOT EXISTS country         varchar(100),
  ADD COLUMN IF NOT EXISTS contact_email   varchar(150),  -- company-level contact (distinct from the admin login email)
  ADD COLUMN IF NOT EXISTS company_phone   varchar(40);

-- ── Administrator phone on the user row ──────────────────────────────────────
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS phone varchar(40);
