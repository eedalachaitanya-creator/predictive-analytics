-- Snapshot cadence (days between point-in-time training cutoffs) as its OWN
-- config column. Previously the temporal resolver hijacked login_window_days
-- as the cadence — overloading a knob the UI labelled "login inactivity".
-- Decoupling frees login_window_days to mean a real recent-login feature window.
ALTER TABLE client_config
    ADD COLUMN IF NOT EXISTS snapshot_cadence_days INTEGER NOT NULL DEFAULT 30;
