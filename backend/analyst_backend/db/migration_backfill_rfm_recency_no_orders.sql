-- migration_backfill_rfm_recency_no_orders.sql
-- =============================================================================
-- Backfill for the 9999-recency-sentinel QA bug (re-raised on CLT-017 "Reebok").
--
-- WHAT: The materialized view fills days_since_last_order with a 9999 sentinel
-- for customers who have never ordered (the churn-label pipeline relies on that
-- sentinel). ml/compute_rfm.py (commit 8e2b5f8) already nulls this in the
-- user-facing customer_rfm_features table for NEW computes — but rows computed
-- BEFORE that fix (e.g. CLT-017, computed 2026-06-08) still carry 9999 and the
-- super-admin "RFM Features" modal renders "9,999 days" for no-order customers.
--
-- WHY NULL: a customer with no orders has no "last order", so the recency is
-- undefined and must render as — / N/A (the modal already renders NULL that way,
-- same as last_order_date/last_order_status for these rows).
--
-- SAFETY: keyed on total_orders, NOT on the 9999 value — so any customer with
-- real orders (including one with a legitimately large recency) is never
-- touched. Mirrors ml/compute_rfm.py:_clear_recency_for_customers_without_orders.
-- ML scoring is unaffected: it reads days_since_last_order from the materialized
-- view (mv_customer_features), not from this table.
--
-- Idempotent: re-running is a no-op (the AND ... IS NOT NULL guard).
-- =============================================================================

UPDATE customer_rfm_features
   SET days_since_last_order = NULL
 WHERE (total_orders IS NULL OR total_orders = 0)
   AND days_since_last_order IS NOT NULL;
