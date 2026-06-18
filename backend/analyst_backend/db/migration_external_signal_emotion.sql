-- Third-party app emotion signals: additive, idempotent.
ALTER TABLE support_tickets
  ADD COLUMN IF NOT EXISTS source            VARCHAR(30) NOT NULL DEFAULT 'internal',
  ADD COLUMN IF NOT EXISTS subject           VARCHAR(255),
  ADD COLUMN IF NOT EXISTS ticket_text       TEXT,
  ADD COLUMN IF NOT EXISTS emotion           VARCHAR(20),
  ADD COLUMN IF NOT EXISTS distress_score    NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS emotion_scored_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS emotion_model     VARCHAR(50);

ALTER TABLE customer_reviews
  ADD COLUMN IF NOT EXISTS source            VARCHAR(30) NOT NULL DEFAULT 'internal',
  ADD COLUMN IF NOT EXISTS emotion           VARCHAR(20),
  ADD COLUMN IF NOT EXISTS distress_score    NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS emotion_scored_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS emotion_model     VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_tickets_cust_opened
  ON support_tickets (client_id, customer_id, opened_date);
CREATE INDEX IF NOT EXISTS idx_reviews_cust_date
  ON customer_reviews (client_id, customer_id, review_date);

-- Widen staging tables so the commit SELECT...FROM staging can carry
-- ticket_text and source through to the real tables.  Idempotent (IF NOT EXISTS).
ALTER TABLE staging_support_tickets
  ADD COLUMN IF NOT EXISTS ticket_text TEXT,
  ADD COLUMN IF NOT EXISTS source      VARCHAR(30);
ALTER TABLE staging_customer_reviews
  ADD COLUMN IF NOT EXISTS source      VARCHAR(30);
