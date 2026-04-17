-- ============================================================
-- add_new_tables.sql
-- Adds customer_reviews and support_tickets tables to the
-- existing walmart_crp database (v6 upgrade).
--
-- Run this ONCE in pgAdmin4 Query Tool.
-- Safe to run on a live database — uses IF NOT EXISTS.
-- ============================================================

BEGIN;

-- ── 19. Customer Reviews ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_reviews (
    client_id    VARCHAR(20)   NOT NULL,
    review_id    VARCHAR(30)   NOT NULL,
    customer_id  VARCHAR(30)   NOT NULL,
    product_id   INT           REFERENCES products(product_id),
    order_id     VARCHAR(50),
    rating       SMALLINT      CHECK (rating BETWEEN 1 AND 5),
    review_text  TEXT,
    review_date  DATE,
    sentiment    VARCHAR(20),
    PRIMARY KEY (client_id, review_id),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_reviews_customer   ON customer_reviews(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_reviews_product    ON customer_reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating     ON customer_reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment  ON customer_reviews(sentiment);

-- ── 20. Support Tickets ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS support_tickets (
    client_id            VARCHAR(20)   NOT NULL,
    ticket_id            VARCHAR(30)   NOT NULL,
    customer_id          VARCHAR(30)   NOT NULL,
    ticket_type          VARCHAR(100),
    priority             VARCHAR(20),
    status               VARCHAR(30),
    channel              VARCHAR(50),
    opened_date          TIMESTAMPTZ,
    resolved_date        TIMESTAMPTZ,
    resolution_time_hrs  NUMERIC(8,2),
    PRIMARY KEY (client_id, ticket_id),
    FOREIGN KEY (client_id, customer_id) REFERENCES customers(client_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_tickets_customer  ON support_tickets(client_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status    ON support_tickets(status);
CREATE INDEX IF NOT EXISTS idx_tickets_priority  ON support_tickets(priority);
CREATE INDEX IF NOT EXISTS idx_tickets_type      ON support_tickets(ticket_type);

COMMIT;

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT table_name, 0 AS row_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('customer_reviews', 'support_tickets')
ORDER BY table_name;
