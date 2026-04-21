-- ============================================================
-- migration_churn_scores_unique.sql
--
-- Dedupe churn_scores + add a UNIQUE constraint on (client_id, customer_id).
--
-- WHY:
--   churn_scores was defined with only `score_id SERIAL PRIMARY KEY` plus an
--   INDEX on (client_id, customer_id). An index is NOT a constraint — the DB
--   was silently accepting multiple rows per customer. Combined with the
--   non-atomic DELETE+INSERT in ml/predict.py save_scores_to_db(), two
--   overlapping pipeline runs for the same client could both wipe and
--   re-insert, ending up with 2× (or more) rows per customer. The UI then
--   faithfully rendered each customer twice.
--
-- WHAT THIS DOES:
--   1. Shows the current duplicate situation (so you can see it shrink).
--   2. NULLs out retention_interventions.churn_score_id for any intervention
--      pointing to a row we're about to delete (the FK has no ON DELETE
--      action, so we must repoint or null out first).
--   3. Deletes the older row of every duplicate pair, keeping the newest
--      (highest score_id) per (client_id, customer_id).
--   4. Adds a UNIQUE constraint on (client_id, customer_id). Any future
--      double-INSERT will now fail loudly with 23505 instead of silently
--      doubling the table.
--
-- WHEN TO RUN:
--   Once, on the live DB. Idempotent — re-running after the constraint
--   exists is a no-op (the ALTER will error with "already exists", which
--   is fine; steps 1-3 are still safe to repeat).
--
-- HOW TO RUN (pgAdmin 4):
--   1. Open pgAdmin 4 → connect to walmart_crp.
--   2. Right-click walmart_crp → Query Tool.
--   3. Paste this file's contents → Execute (F5).
--   4. Watch the BEFORE and AFTER counts.
-- ============================================================

-- 1. BEFORE: how many duplicate (client_id, customer_id) pairs exist, and
--    what's the total row count?
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(*) - COUNT(DISTINCT (client_id, customer_id)) AS duplicate_rows,
    COUNT(DISTINCT (client_id, customer_id))            AS distinct_customers
FROM churn_scores;

-- 1b. Show a sample of duplicates (top 10 most-duplicated customers)
SELECT client_id, customer_id, COUNT(*) AS copies
FROM churn_scores
GROUP BY client_id, customer_id
HAVING COUNT(*) > 1
ORDER BY copies DESC, client_id, customer_id
LIMIT 10;

-- 2. Repoint retention_interventions off any older duplicate row.
--    We keep the NEWEST score_id per (client_id, customer_id); anything
--    pointing at an older duplicate becomes NULL (intervention history
--    is preserved, the dangling link is not).
UPDATE retention_interventions ri
SET    churn_score_id = NULL
WHERE  ri.churn_score_id IN (
    SELECT cs.score_id
    FROM   churn_scores cs
    WHERE  EXISTS (
        SELECT 1
        FROM   churn_scores newer
        WHERE  newer.client_id   = cs.client_id
          AND  newer.customer_id = cs.customer_id
          AND  newer.score_id    > cs.score_id
    )
);

-- 3. Delete every row that has a newer sibling for the same (client_id,
--    customer_id). This leaves exactly one row per customer — the newest.
DELETE FROM churn_scores cs
USING       churn_scores newer
WHERE       cs.client_id   = newer.client_id
  AND       cs.customer_id = newer.customer_id
  AND       cs.score_id    < newer.score_id;

-- 4. AFTER dedupe: should show duplicate_rows = 0
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(*) - COUNT(DISTINCT (client_id, customer_id)) AS duplicate_rows,
    COUNT(DISTINCT (client_id, customer_id))            AS distinct_customers
FROM churn_scores;

-- 5. Add the UNIQUE constraint. From now on the DB itself blocks
--    duplicates — belt-and-braces with the atomic DELETE+INSERT fix
--    applied in ml/predict.py.
ALTER TABLE churn_scores
    ADD CONSTRAINT churn_scores_client_customer_unique
    UNIQUE (client_id, customer_id);

-- 6. Confirm the constraint is in place
SELECT  conname, contype, pg_get_constraintdef(oid) AS definition
FROM    pg_constraint
WHERE   conrelid = 'churn_scores'::regclass
  AND   contype  = 'u';
