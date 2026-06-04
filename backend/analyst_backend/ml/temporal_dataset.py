"""Point-in-time (<=T) churn feature builder + forward label.

Additive, standalone module for the temporal churn redesign
(see docs/superpowers/specs/2026-06-03-temporal-churn-redesign-design.md).

Given a tenant and a cutoff T, it emits one row per eligible (customer_id, T)
with every feature reconstructed strictly from data with a timestamp <= T, plus
a strictly forward-looking churn label. It writes to the tenant-scoped staging
table `ml_temporal_snapshots` and NEVER to `churn_scores` or the live MV.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Iterable, List, Mapping, Optional

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Canonical qualifying-order predicate (design §5, BLOCKER red-team H3)
# ──────────────────────────────────────────────────────────────────────────────
# ONE predicate, used VERBATIM on BOTH the <=T feature side and the forward-label
# side, so "a purchase" means exactly the same thing on each side of T. A
# cancelled or returned order is not revenue-qualifying and does not break churn.
QUALIFYING_STATUS_SQL = "order_status NOT IN ('Cancelled', 'Returned')"

# The Python mirror of the SQL predicate above (same set, kept in lock-step).
NON_QUALIFYING_STATUSES = frozenset({"Cancelled", "Returned"})


def is_qualifying_status(status: Optional[str]) -> bool:
    """Python mirror of QUALIFYING_STATUS_SQL — used by compute_forward_label."""
    return status not in NON_QUALIFYING_STATUSES


def compute_forward_label(
    orders: Iterable[Mapping],
    T: dt.date,
    label_window_days: int,
) -> int:
    """Forward-looking churn label (design §5).

        churned = 1  IFF the customer has NO qualifying order in the half-open
                     interval (T, T + label_window_days]
        churned = 0  otherwise

    The interval is half-open at T: an order placed exactly at T is a *feature*,
    never a forward-label event. An order exactly at the window end
    (T + label_window_days) IS inside the window. Cancelled/Returned orders in
    the window do not count as activity (canonical predicate).
    """
    window_end = T + dt.timedelta(days=label_window_days)
    for o in orders:
        od = o["order_date"]
        if isinstance(od, dt.datetime):
            od = od.date()
        if T < od <= window_end and is_qualifying_status(o.get("order_status")):
            return 0  # active — at least one revenue-qualifying forward order
    return 1  # churned — no qualifying order in the forward window


# ──────────────────────────────────────────────────────────────────────────────
# Cutoff generation + observability bound (design §4.1, §4.2)
# ──────────────────────────────────────────────────────────────────────────────

def generate_cutoffs(
    max_order_date: dt.date,
    label_window_days: int = 90,
    cadence_days: int = 30,
    observability_buffer_days: int = 0,
    earliest: Optional[dt.date] = None,
) -> List[dt.date]:
    """Sample cutoffs T backward from the tenant's observability bound.

    The forward label is only *defined* once the full label window has elapsed
    in observed data, so the latest valid cutoff is bounded by (design §4.2):

        T  <=  max_order_date − label_window_days − observability_buffer_days

    Cutoffs step backward by ``cadence_days`` (default 30) until they fall below
    ``earliest``. Cadence is deliberately decoupled from the label window so the
    recency-at-T feature cannot align to a single label bin (design §4.1, H7):
    ``cadence_days == label_window_days`` is rejected.

    Returns cutoffs in ascending date order.
    """
    if cadence_days <= 0:
        raise ValueError("cadence_days must be positive")
    if label_window_days <= 0:
        raise ValueError("label_window_days must be positive")
    if cadence_days == label_window_days:
        raise ValueError(
            "snapshot cadence must differ from the label window "
            f"(both {cadence_days}d) — see design §4.1 / red-team H7"
        )

    latest = max_order_date - dt.timedelta(
        days=label_window_days + observability_buffer_days
    )

    cutoffs: List[dt.date] = []
    t = latest
    while earliest is None or t >= earliest:
        cutoffs.append(t)
        t = t - dt.timedelta(days=cadence_days)
        if earliest is None and len(cutoffs) >= 10_000:  # safety stop
            break

    cutoffs.sort()
    return cutoffs


# ──────────────────────────────────────────────────────────────────────────────
# Point-in-time (<=T) snapshot builder (design §4.3, §4.4, §6) — the core
# ──────────────────────────────────────────────────────────────────────────────

# Columns that are identifiers / bookkeeping, never model features (design §6.5).
IDENTIFIER_COLS = [
    "client_id", "customer_id", "cutoff_date", "churned",
    "first_order_date", "last_order_date", "last_review_date", "computed_at",
]

# Excluded leak families (design §6.4) — asserted absent from every snapshot.
EXCLUDED_FEATURE_COLS = frozenset({
    "last_login_date", "days_since_last_login",
    "avg_refill_cycle_days", "subscription_product_count",
    "missed_refill_count", "days_overdue_for_refill",
    "churn_label", "churn_window_days", "login_window_days",
})


def _snapshot_sql() -> str:
    """Parameterized <=T snapshot SQL for one (client_id, T) cohort.

    Bound parameters: :client_id, :T, :min_tenure_days, :min_orders,
    :active_window_days, :label_window_days, :active_only (0/1).

    Every fact-table CTE carries the cutoff bound structurally (references :T)
    and the canonical qualifying-order predicate. RFM / percentile / tier are
    recomputed within THIS single cohort (never pooled across cutoffs). The
    forward label uses the SAME predicate on the (T, T+window] interval.
    """
    qual = QUALIFYING_STATUS_SQL  # canonical, used on BOTH sides of T
    return f"""
    WITH
    cfg AS (
        SELECT client_id, tier_method, high_value_percentile,
               custom_platinum_min, custom_gold_min, custom_silver_min,
               min_repeat_orders, recent_order_gap_window
        FROM client_config WHERE client_id = :client_id
    ),
    -- All <=T qualifying orders for this tenant (the spine for every order CTE).
    qorders AS MATERIALIZED (
        SELECT o.client_id, o.customer_id, o.order_id, o.order_date,
               o.order_value_usd, o.discount_usd
        FROM orders o
        WHERE o.client_id = :client_id
          AND o.order_date < (CAST(:T AS date) + 1)   -- include all of day T (day-granular, sargable)
          AND {qual}
    ),
    order_agg AS MATERIALIZED (
        SELECT q.client_id, q.customer_id,
            COUNT(*)                                   AS total_orders,
            MIN(q.order_date)                          AS first_order_date,
            MAX(q.order_date)                          AS last_order_date,
            (CAST(:T AS date) - MAX(q.order_date)::date)::INT
                                                       AS days_since_last_order,
            COALESCE(SUM(q.order_value_usd), 0)        AS total_spend_usd,
            ROUND(AVG(q.order_value_usd)::NUMERIC, 2)  AS avg_order_value_usd,
            MAX(q.order_value_usd)                     AS max_order_value_usd,
            COALESCE(SUM(q.discount_usd), 0)           AS total_discount_usd,
            SUM(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '30 days'
                     THEN q.order_value_usd ELSE 0 END) AS spend_last_30d_usd,
            SUM(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '90 days'
                     THEN q.order_value_usd ELSE 0 END) AS spend_last_90d_usd,
            SUM(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '180 days'
                     THEN q.order_value_usd ELSE 0 END) AS spend_last_180d_usd,
            COUNT(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '30 days'
                       THEN 1 END)                      AS orders_last_30d,
            COUNT(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '90 days'
                       THEN 1 END)                      AS orders_last_90d,
            COUNT(CASE WHEN q.order_date >= CAST(:T AS timestamptz) - INTERVAL '180 days'
                       THEN 1 END)                      AS orders_last_180d,
            COUNT(CASE WHEN q.discount_usd > 0 THEN 1 END) AS orders_with_discount
        FROM qorders q
        GROUP BY q.client_id, q.customer_id
    ),
    gaps_raw AS MATERIALIZED (
        SELECT q.client_id, q.customer_id, q.order_date,
            EXTRACT(DAY FROM (q.order_date - LAG(q.order_date) OVER (
                PARTITION BY q.client_id, q.customer_id ORDER BY q.order_date
            )))::NUMERIC AS gap_days
        FROM qorders q
    ),
    order_gaps AS (
        SELECT client_id, customer_id,
            ROUND(AVG(gap_days)::NUMERIC, 1) AS avg_days_between_orders,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap_days)::NUMERIC, 1)
                AS median_days_between_orders
        FROM gaps_raw WHERE gap_days IS NOT NULL
        GROUP BY client_id, customer_id
    ),
    recent_gaps AS (
        SELECT client_id, customer_id,
            ROUND(AVG(gap_days)::NUMERIC, 1) AS recent_avg_gap_days
        FROM (
            SELECT g.client_id, g.customer_id, g.gap_days,
                ROW_NUMBER() OVER (PARTITION BY g.client_id, g.customer_id
                                   ORDER BY g.order_date DESC) AS rn,
                cfg.recent_order_gap_window AS w
            FROM gaps_raw g CROSS JOIN cfg
            WHERE g.gap_days IS NOT NULL
        ) ranked
        WHERE rn <= w
        GROUP BY client_id, customer_id
    ),
    -- line_items joined to <=T qualifying orders (so item aggregates are <=T).
    line_agg AS (
        SELECT li.client_id, li.customer_id,
            COUNT(DISTINCT li.product_id)         AS unique_products_purchased,
            ROUND(AVG(li.quantity)::NUMERIC, 2)   AS avg_items_per_order,
            ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
                  / NULLIF(COUNT(*), 0), 1)       AS return_rate_pct
        FROM line_items li
        JOIN qorders q ON li.client_id = q.client_id AND li.order_id = q.order_id
        GROUP BY li.client_id, li.customer_id
    ),
    -- cat_agg: gets BOTH the <=T bound AND the order_status qualifying filter
    -- (via the qorders join) that the live MV lacks (design §3 — fix lives ONLY here).
    cat_agg AS (
        SELECT li.client_id, li.customer_id,
            COUNT(DISTINCT p.category_id) AS unique_categories_purchased
        FROM line_items li
        JOIN qorders q ON li.client_id = q.client_id AND li.order_id = q.order_id
        JOIN products p ON li.client_id = p.client_id AND li.product_id = p.product_id
        GROUP BY li.client_id, li.customer_id
    ),
    review_agg AS (
        SELECT r.client_id, r.customer_id,
            COUNT(*)                                   AS total_reviews,
            ROUND(AVG(r.rating)::NUMERIC, 2)           AS avg_rating,
            ROUND(COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) * 100.0
                  / NULLIF(COUNT(*), 0), 1)            AS pct_positive_reviews,
            ROUND(COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) * 100.0
                  / NULLIF(COUNT(*), 0), 1)            AS pct_negative_reviews,
            MAX(r.review_date)                         AS last_review_date,
            EXTRACT(DAY FROM (CAST(:T AS timestamptz) - MAX(r.review_date)::timestamptz))::INT
                                                       AS days_since_last_review
        FROM customer_reviews r
        WHERE r.client_id = :client_id
          AND r.review_date <= CAST(:T AS date)            -- day-granularity (design §5, L3)
        GROUP BY r.client_id, r.customer_id
    ),
    -- Tickets: opened_date <= T kept; resolution gated to resolved_date <= T.
    -- If a ticket is unresolved as of T, treat it as OPEN and recompute its
    -- open-duration-so-far = T − opened_date (design §6.4, H4/M11).
    ticket_agg AS (
        SELECT t.client_id, t.customer_id,
            COUNT(*)                                              AS total_tickets,
            COUNT(CASE WHEN t.opened_date >= CAST(:T AS timestamptz) - INTERVAL '30 days'
                       THEN 1 END)                                AS tickets_last_30d,
            COUNT(CASE WHEN t.opened_date >= CAST(:T AS timestamptz) - INTERVAL '90 days'
                       THEN 1 END)                                AS tickets_last_90d,
            COUNT(CASE WHEN LOWER(t.priority) IN ('critical', 'high')
                       THEN 1 END)                                AS high_priority_tickets,
            COUNT(CASE WHEN LOWER(t.priority) = 'critical' THEN 1 END)
                                                                  AS critical_tickets,
            -- open-as-of-T: not yet resolved, or resolved strictly after T.
            COUNT(CASE WHEN t.resolved_date IS NULL OR t.resolved_date > CAST(:T AS timestamptz)
                       THEN 1 END)                                AS open_tickets,
            COUNT(CASE WHEN t.resolved_date IS NOT NULL AND t.resolved_date <= CAST(:T AS timestamptz)
                       THEN 1 END)                                AS resolved_tickets,
            ROUND(AVG(
                CASE WHEN t.resolved_date IS NOT NULL AND t.resolved_date <= CAST(:T AS timestamptz)
                     THEN EXTRACT(EPOCH FROM (t.resolved_date - t.opened_date)) / 3600.0
                     ELSE EXTRACT(EPOCH FROM (CAST(:T AS timestamptz) - t.opened_date)) / 3600.0
                END
            )::NUMERIC, 1)                                        AS avg_resolution_time_hrs,
            ROUND(COUNT(CASE WHEN t.resolved_date IS NOT NULL AND t.resolved_date <= CAST(:T AS timestamptz)
                             THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1)
                                                                  AS pct_tickets_resolved
        FROM support_tickets t
        WHERE t.client_id = :client_id
          AND t.opened_date <= CAST(:T AS timestamptz)
        GROUP BY t.client_id, t.customer_id
    ),
    -- Customers with >=1 qualifying order in (T − active_window, T], set-based
    -- (NOT a per-row correlated EXISTS) so the active-at-T restriction (§4.4) is
    -- a cheap semi-join against the materialized qorders spine.
    active_at_T AS (
        SELECT DISTINCT q.client_id, q.customer_id
        FROM qorders q
        WHERE q.order_date >= (CAST(:T AS date) - :active_window_days + 1)  -- day-granular, sargable
          AND q.order_date <  (CAST(:T AS date) + 1)
    ),
    -- Eligibility cohort: existed at T, tenure & order-count thresholds, and
    -- (when active_only) at least one qualifying order in (T − active_window, T].
    eligible AS MATERIALIZED (
        SELECT oa.client_id, oa.customer_id,
            EXTRACT(DAY FROM (CAST(:T AS timestamptz) - c.account_created_date::timestamptz))::INT
                AS account_age_days
        FROM order_agg oa
        JOIN customers c ON oa.client_id = c.client_id AND oa.customer_id = c.customer_id
        LEFT JOIN active_at_T a
          ON oa.client_id = a.client_id AND oa.customer_id = a.customer_id
        WHERE c.account_created_date <= :T
          AND EXTRACT(DAY FROM (CAST(:T AS timestamptz) - c.account_created_date::timestamptz)) >= :min_tenure_days
          AND oa.total_orders >= :min_orders
          AND (:active_only = 0 OR a.customer_id IS NOT NULL)
    ),
    -- RFM / percentile / tier recomputed within THIS eligible cohort (design §6.3):
    -- every window function ranks each customer ONLY against the same-T cohort.
    rfm AS MATERIALIZED (
        SELECT oa.client_id, oa.customer_id,
            6 - NTILE(5) OVER (ORDER BY oa.days_since_last_order ASC) AS rfm_recency_score,
            NTILE(5) OVER (ORDER BY oa.total_orders ASC)             AS rfm_frequency_score,
            NTILE(5) OVER (ORDER BY oa.total_spend_usd ASC)          AS rfm_monetary_score,
            PERCENT_RANK() OVER (ORDER BY oa.total_spend_usd ASC) * 100 AS spend_pct_rank
        FROM order_agg oa
        JOIN eligible e ON oa.client_id = e.client_id AND oa.customer_id = e.customer_id
    ),
    tier AS (
        SELECT r.client_id, r.customer_id, r.spend_pct_rank,
            oa.total_spend_usd,
            CASE WHEN cfg.tier_method = 'quartile' THEN
                CASE WHEN r.spend_pct_rank >= 75 THEN 'Platinum'
                     WHEN r.spend_pct_rank >= 50 THEN 'Gold'
                     WHEN r.spend_pct_rank >= 25 THEN 'Silver'
                     ELSE 'Bronze' END
            ELSE
                CASE WHEN oa.total_spend_usd >= cfg.custom_platinum_min THEN 'Platinum'
                     WHEN oa.total_spend_usd >= cfg.custom_gold_min THEN 'Gold'
                     WHEN oa.total_spend_usd >= cfg.custom_silver_min THEN 'Silver'
                     ELSE 'Bronze' END
            END AS customer_tier,
            CASE WHEN cfg.tier_method = 'quartile' AND r.spend_pct_rank >= cfg.high_value_percentile THEN 1
                 WHEN cfg.tier_method != 'quartile' AND oa.total_spend_usd >= cfg.custom_platinum_min THEN 1
                 ELSE 0 END AS is_high_value
        FROM rfm r
        JOIN order_agg oa ON r.client_id = oa.client_id AND r.customer_id = oa.customer_id
        CROSS JOIN cfg
    ),
    -- Forward-window qualifying orders, set-based (NOT a per-row correlated
    -- subquery): same canonical predicate on the half-open (T, T+window].
    forward_active AS (
        SELECT DISTINCT o.client_id, o.customer_id
        FROM orders o
        WHERE o.client_id = :client_id
          AND o.order_date >= (CAST(:T AS date) + 1)                          -- strictly after day T (day-granular)
          AND o.order_date <  (CAST(:T AS date) + 1 + :label_window_days)     -- through day T+window inclusive
          AND {qual}
    ),
    label AS (
        SELECT e.client_id, e.customer_id,
            CASE WHEN fa.customer_id IS NULL THEN 1 ELSE 0 END AS churned
        FROM eligible e
        LEFT JOIN forward_active fa
          ON e.client_id = fa.client_id AND e.customer_id = fa.customer_id
    )
    SELECT
        e.client_id, e.customer_id, CAST(:T AS date) AS cutoff_date, l.churned,
        e.account_age_days,
        oa.first_order_date, oa.last_order_date, oa.days_since_last_order,
        oa.total_orders, oa.orders_last_30d, oa.orders_last_90d, oa.orders_last_180d,
        COALESCE(og.avg_days_between_orders, 0)    AS avg_days_between_orders,
        COALESCE(og.median_days_between_orders, 0) AS median_days_between_orders,
        ROUND(ABS(COALESCE(og.avg_days_between_orders, 0)
                  - COALESCE(og.median_days_between_orders, 0))::NUMERIC, 1)
            AS order_gap_mean_median_diff,
        COALESCE(rg.recent_avg_gap_days, 0)        AS recent_avg_gap_days,
        oa.total_spend_usd, oa.avg_order_value_usd, oa.max_order_value_usd,
        oa.spend_last_30d_usd, oa.spend_last_90d_usd, oa.spend_last_180d_usd,
        oa.total_discount_usd,
        ROUND(oa.total_discount_usd * 100.0
              / NULLIF(oa.total_spend_usd + oa.total_discount_usd, 0)::NUMERIC, 2)
            AS discount_rate_pct,
        oa.orders_with_discount,
        COALESCE(la.unique_products_purchased, 0)  AS unique_products_purchased,
        COALESCE(ca.unique_categories_purchased, 0) AS unique_categories_purchased,
        COALESCE(la.avg_items_per_order, 0)        AS avg_items_per_order,
        COALESCE(la.return_rate_pct, 0)            AS return_rate_pct,
        COALESCE(ra.total_reviews, 0)              AS total_reviews,
        COALESCE(ra.avg_rating, 0)                 AS avg_rating,
        COALESCE(ra.pct_positive_reviews, 0)       AS pct_positive_reviews,
        COALESCE(ra.pct_negative_reviews, 0)       AS pct_negative_reviews,
        ra.last_review_date,
        COALESCE(ra.days_since_last_review, 9999)  AS days_since_last_review,
        COALESCE(ta.total_tickets, 0)              AS total_tickets,
        COALESCE(ta.tickets_last_30d, 0)           AS tickets_last_30d,
        COALESCE(ta.tickets_last_90d, 0)           AS tickets_last_90d,
        COALESCE(ta.high_priority_tickets, 0)      AS high_priority_tickets,
        COALESCE(ta.critical_tickets, 0)           AS critical_tickets,
        COALESCE(ta.open_tickets, 0)               AS open_tickets,
        COALESCE(ta.resolved_tickets, 0)           AS resolved_tickets,
        COALESCE(ta.avg_resolution_time_hrs, 0)    AS avg_resolution_time_hrs,
        COALESCE(ta.pct_tickets_resolved, 0)       AS pct_tickets_resolved,
        oa.total_spend_usd                         AS ltv_usd,
        rf.rfm_recency_score, rf.rfm_frequency_score, rf.rfm_monetary_score,
        (rf.rfm_recency_score + rf.rfm_frequency_score + rf.rfm_monetary_score)
            AS rfm_total_score,
        rf.spend_pct_rank,
        CASE WHEN oa.total_orders >= cfg.min_repeat_orders THEN 1 ELSE 0 END
            AS is_repeat_customer,
        ti.customer_tier, ti.is_high_value
    FROM eligible e
    JOIN label     l   ON e.client_id = l.client_id  AND e.customer_id = l.customer_id
    JOIN order_agg oa  ON e.client_id = oa.client_id AND e.customer_id = oa.customer_id
    JOIN rfm       rf  ON e.client_id = rf.client_id AND e.customer_id = rf.customer_id
    JOIN tier      ti  ON e.client_id = ti.client_id AND e.customer_id = ti.customer_id
    CROSS JOIN cfg
    LEFT JOIN order_gaps  og ON e.client_id = og.client_id AND e.customer_id = og.customer_id
    LEFT JOIN recent_gaps rg ON e.client_id = rg.client_id AND e.customer_id = rg.customer_id
    LEFT JOIN line_agg    la ON e.client_id = la.client_id AND e.customer_id = la.customer_id
    LEFT JOIN cat_agg     ca ON e.client_id = ca.client_id AND e.customer_id = ca.customer_id
    LEFT JOIN review_agg  ra ON e.client_id = ra.client_id AND e.customer_id = ra.customer_id
    LEFT JOIN ticket_agg  ta ON e.client_id = ta.client_id AND e.customer_id = ta.customer_id
    """


def build_snapshot(
    engine_or_conn: Any,
    client_id: str,
    T: dt.date,
    *,
    label_window_days: int,
    min_tenure_days: int,
    min_orders: int,
    active_window_days: int,
    active_only: bool,
) -> pd.DataFrame:
    """Build the point-in-time (<=T) snapshot for one (client_id, T) cohort.

    ``engine_or_conn`` may be a SQLAlchemy Engine OR an already-open Connection
    (the latter lets tests inject a truncated `orders` temp view to prove the
    output is future-independent — design §6 / red-team M3). One row per eligible
    customer; every feature reconstructed strictly from data with a timestamp
    <= T; the forward label uses the canonical predicate on (T, T+window].
    """
    params = {
        "client_id": client_id,
        "T": T,
        "min_tenure_days": int(min_tenure_days),
        "min_orders": int(min_orders),
        "active_window_days": int(active_window_days),
        "label_window_days": int(label_window_days),
        "active_only": 1 if active_only else 0,
    }
    sql = text(_snapshot_sql())

    if hasattr(engine_or_conn, "begin") and not hasattr(engine_or_conn, "execute"):
        # An Engine: open our own connection.
        with engine_or_conn.connect() as cx:
            df = pd.read_sql(sql, cx, params=params)
    else:
        # An open Connection (or Engine that also exposes execute) — reuse it so
        # any session-local temp view the caller created is visible to this SQL.
        df = pd.read_sql(sql, engine_or_conn, params=params)

    # Normalize types: label int, cutoff_date a python date.
    if not df.empty:
        df["churned"] = df["churned"].astype(int)
        df["cutoff_date"] = pd.to_datetime(df["cutoff_date"]).dt.date

    # Hard guarantee: no excluded leak family leaked into the columns (design §6.4).
    leaked = EXCLUDED_FEATURE_COLS & set(df.columns)
    if leaked:
        raise AssertionError(f"excluded leak family present in snapshot: {sorted(leaked)}")

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Dataset assembly + staging writer + per-cutoff min-positives gate (design §4, §10)
# ──────────────────────────────────────────────────────────────────────────────

# Staging columns (match db/migration_ml_temporal_snapshots.sql). Everything NOT
# in this set is serialized into the JSONB `features` payload.
_STAGING_KEY_COLS = ("client_id", "customer_id", "cutoff_date", "churned")


def _json_default(value: Any) -> Any:
    """JSON-encode the non-native types a snapshot row can carry (dates, NaT)."""
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    raise TypeError(f"not JSON-serializable: {type(value).__name__}")


def _row_features(row: Mapping, feature_cols: List[str]) -> str:
    """Serialize one row's feature columns to a JSON string (NaN → null)."""
    payload = {}
    for col in feature_cols:
        v = row[col]
        if isinstance(v, float) and pd.isna(v):
            payload[col] = None
        elif v is pd.NaT or (not isinstance(v, (list, dict)) and pd.isna(v)):
            payload[col] = None
        else:
            payload[col] = v
    return json.dumps(payload, default=_json_default, sort_keys=True)


def _max_order_date(conn: Any, client_id: str) -> Optional[dt.date]:
    res = conn.execute(
        text("SELECT MAX(order_date)::date FROM orders WHERE client_id = :c"),
        {"c": client_id},
    ).scalar()
    return res


def _write_staging(conn: Any, df: pd.DataFrame) -> int:
    """Idempotent upsert of an assembled dataset into ml_temporal_snapshots.

    Keyed on the table's UNIQUE (client_id, customer_id, cutoff_date): re-running
    REPLACES the row's label + features in place rather than inserting a duplicate
    (design §10.1). Returns the number of rows written.
    """
    if df.empty:
        return 0
    feature_cols = [c for c in df.columns if c not in _STAGING_KEY_COLS]
    upsert = text(
        """
        INSERT INTO ml_temporal_snapshots
            (client_id, customer_id, cutoff_date, churned, features, computed_at)
        VALUES
            (:client_id, :customer_id, :cutoff_date, :churned,
             CAST(:features AS JSONB), now())
        ON CONFLICT (client_id, customer_id, cutoff_date)
        DO UPDATE SET churned     = EXCLUDED.churned,
                      features    = EXCLUDED.features,
                      computed_at = now()
        """
    )
    rows = [
        {
            "client_id": r["client_id"],
            "customer_id": r["customer_id"],
            "cutoff_date": r["cutoff_date"],
            "churned": int(r["churned"]),
            "features": _row_features(r, feature_cols),
        }
        for _, r in df.iterrows()
    ]
    conn.execute(upsert, rows)
    return len(rows)


def build_dataset(
    engine: Any,
    client_id: str,
    *,
    label_window_days: int = 90,
    cadence_days: int = 30,
    min_tenure_days: int = 90,
    min_orders: int = 2,
    active_window_days: int = 120,
    active_only: bool = True,
    min_positives_per_cutoff: int = 30,
    observability_buffer_days: int = 0,
    earliest: Optional[dt.date] = None,
    write: bool = True,
) -> pd.DataFrame:
    """Assemble the multi-cutoff temporal dataset for one tenant.

    Pipeline (design §4, §10):
      1. derive the tenant's max_order_date;
      2. generate cutoffs backward from the observability bound;
      3. build the <=T snapshot for each cutoff and concatenate;
      4. LOG per-cutoff (cutoff_date, n_rows, n_positives, base_rate) and DROP any
         cutoff with n_positives < ``min_positives_per_cutoff`` — logging exactly
         which cutoffs are dropped (NEVER a silent truncation);
      5. assert every emitted row honours the observability bound
         (cutoff + label_window <= max_order_date);
      6. when ``write`` is True, upsert the assembled frame into the tenant-scoped
         ``ml_temporal_snapshots`` staging table idempotently.

    Returns the assembled (kept-cutoffs-only) DataFrame.
    """
    own_conn = hasattr(engine, "connect")
    conn = engine.connect() if own_conn else engine
    try:
        max_order_date = _max_order_date(conn, client_id)
        if max_order_date is None:
            logger.warning("build_dataset: no orders for client_id=%s — empty dataset", client_id)
            return pd.DataFrame()

        cutoffs = generate_cutoffs(
            max_order_date=max_order_date,
            label_window_days=label_window_days,
            cadence_days=cadence_days,
            observability_buffer_days=observability_buffer_days,
            earliest=earliest,
        )
        logger.info(
            "build_dataset: client_id=%s max_order_date=%s cutoffs=%d (%s..%s)",
            client_id, max_order_date, len(cutoffs),
            cutoffs[0] if cutoffs else None, cutoffs[-1] if cutoffs else None,
        )

        bound = max_order_date - dt.timedelta(days=label_window_days + observability_buffer_days)
        kept: List[pd.DataFrame] = []
        for T in cutoffs:
            snap = build_snapshot(
                conn, client_id, T,
                label_window_days=label_window_days,
                min_tenure_days=min_tenure_days,
                min_orders=min_orders,
                active_window_days=active_window_days,
                active_only=active_only,
            )
            n_rows = len(snap)
            n_pos = int(snap["churned"].sum()) if n_rows else 0
            base_rate = (n_pos / n_rows) if n_rows else 0.0
            if n_pos < min_positives_per_cutoff:
                logger.info(
                    "build_dataset: DROP cutoff %s (n_rows=%d n_positives=%d "
                    "base_rate=%.4f < min_positives_per_cutoff=%d)",
                    T, n_rows, n_pos, base_rate, min_positives_per_cutoff,
                )
                continue
            logger.info(
                "build_dataset: KEEP cutoff %s (n_rows=%d n_positives=%d base_rate=%.4f)",
                T, n_rows, n_pos, base_rate,
            )
            kept.append(snap)

        if not kept:
            logger.info("build_dataset: client_id=%s — all cutoffs dropped, empty dataset", client_id)
            return pd.DataFrame()

        dataset = pd.concat(kept, ignore_index=True)

        # Observability bound holds for every emitted row (design §4.2).
        emitted = sorted(set(dataset["cutoff_date"]))
        for c in emitted:
            assert c <= bound, (
                f"cutoff {c} violates observability bound "
                f"(cutoff + {label_window_days}d must be <= {max_order_date})"
            )

        if write:
            # `pd.read_sql` above autobegins a read transaction on the connection,
            # so a plain `conn.begin()` would raise "already initialized". Use a
            # SAVEPOINT (begin_nested) — it nests cleanly inside the active
            # transaction whether we own the connection or a caller injected one,
            # and rolls back ONLY the write on error without disturbing the caller.
            tx = conn.begin_nested()
            try:
                n = _write_staging(conn, dataset)
                tx.commit()
                # Commit the outer transaction only when WE own the connection;
                # an injected connection's transaction is the caller's to manage.
                if own_conn and conn.in_transaction():
                    conn.commit()
                logger.info(
                    "build_dataset: wrote %d rows to ml_temporal_snapshots (client_id=%s, %d cutoffs)",
                    n, client_id, len(emitted),
                )
            except Exception:
                tx.rollback()
                raise

        return dataset
    finally:
        if own_conn:
            conn.close()
