"""
dashboard_router.py — GET /api/v1/dashboard
==============================================
Returns all data needed for the Dashboard page:
    - KPIs (total customers, orders, churn rate, etc.)
    - Customer segments (Champions, Hibernating, etc.)
    - Churn breakdown (Churned vs At-Risk vs Active)
    - Tier distribution (Platinum, Gold, Silver, Bronze)
    - Repeat vs One-Time buyers
    - Recent orders table

The Angular frontend calls this endpoint when the user opens the
Dashboard page. It sends clientId as a query parameter and expects
back a single JSON object with ALL the above data.

NOTE: The materialized view (schema_migration_v2) only stores
rfm_recency_score, rfm_frequency_score, rfm_monetary_score,
rfm_total_score, and churn_label. Higher-level fields like
rfm_segment, customer_tier, and is_repeat_customer are derived
here in SQL using CASE expressions.

2026-04-25 — Removed `is_high_value` references. The High Value KPI
tile and the "High Value" tab now key on `customer_tier = 'Platinum'`,
which is the source of truth for the value bucket since the redundant
`is_high_value` column was dropped from the MV.
"""

import math
from fastapi import APIRouter, Depends, Query
from app.auth_router import get_current_user
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["dashboard"], dependencies=[Depends(get_current_user)])  # audit-2026-04-29: router-level auth


@router.get("/dashboard")
def get_dashboard(
    clientId: str = Query(default="CLT-001"),
):
    # The orderPage / orderPageSize query params were removed 2026-04-29
    # along with the recentOrders payload section — see comment below.
    """
    All-in-one dashboard data for a specific client.

    This endpoint runs multiple queries and combines them into one
    response so the frontend only needs ONE API call to render the
    entire dashboard page.

    Since the materialized view may not contain pre-computed segment
    and tier columns, we derive them from RFM scores on the fly.
    """
    with engine.connect() as conn:

        # ═══════════════════════════════════════════════════════════════
        # 1. KPIs — Key Performance Indicators
        # ═══════════════════════════════════════════════════════════════
        # Read the live settings first so the counts below reflect the
        # latest values the user saved on the Settings page — not whatever
        # was current when the pipeline last refreshed the MV.
        #   churn_window_days       → drives the Lapsed Customer count.
        #   min_repeat_orders       → drives the Repeat Customer count.
        #   ref_date                → mirrors the MV's logic (fixed date if the
        #                             client is configured that way, else NOW()).
        # 2026-04-25: high_value_percentile dropped — High Value KPI now
        # keys on customer_tier='Platinum' (no per-client knob anymore).
        cfg_row = conn.execute(text("""
            SELECT
                COALESCE(churn_window_days, 90)                   AS churn_window_days,
                COALESCE(min_repeat_orders, 2)                    AS min_repeat_orders,
                CASE WHEN reference_date_mode = 'fixed'
                      AND reference_date IS NOT NULL
                     THEN reference_date::TIMESTAMPTZ
                     ELSE NOW()
                END                                               AS ref_date
            FROM client_config
            WHERE client_id = :cid
        """), {"cid": clientId}).fetchone()

        churn_window_days     = int(cfg_row[0]) if cfg_row else 90
        min_repeat_orders     = int(cfg_row[1]) if cfg_row else 2
        ref_date              = cfg_row[2]     if cfg_row else None

        # totalCustomers is the raw registered count — matches the Validation
        # page. The ML-derived counts below (repeat / high-value / scored)
        # come from mv_customer_features, which only contains customers with
        # at least one non-Cancelled order. Customers registered but never
        # ordered are surfaced as `unscoredCustomers`.
        total_customers = conn.execute(text(
            "SELECT COUNT(*) FROM customers WHERE client_id = :cid"
        ), {"cid": clientId}).scalar() or 0

        # Repeat threshold now reads from client_config (:min_repeat) — was
        # hardcoded to 2, which made the Settings page's value inert.
        # 2026-04-25: high_value count now keys on Platinum tier instead
        # of the dropped is_high_value column. Same rough magnitude (top
        # 25% of spenders) but pulled from a single source of truth.
        r = conn.execute(text("""
            SELECT
                COUNT(*)                                                  AS scored_customers,
                COUNT(*) FILTER (WHERE total_orders >= :min_repeat)       AS repeat_customers,
                COUNT(*) FILTER (WHERE customer_tier = 'Platinum')        AS high_value
            FROM mv_customer_features
            WHERE client_id = :cid
        """), {"cid": clientId, "min_repeat": min_repeat_orders})
        mv_row = r.fetchone()

        scored_customers = mv_row[0] or 0
        repeat_customers = mv_row[1] or 0
        high_value       = mv_row[2] or 0

        # Lapsed count: recomputed live from `orders` against the CURRENT
        # churn_window_days setting, instead of reading the MV's stale
        # churn_label (which was frozen when the pipeline last refreshed).
        # Matches the MV's logic: customer has ≥1 non-Cancelled order AND
        # their most recent one is older than churn_window_days from ref_date.
        #
        # We compute the cutoff in Python rather than inline-casting a bind
        # inside the SQL — SQLAlchemy's text() parser treats `::` as a
        # Postgres type-cast and refuses to substitute a bind immediately
        # before it (e.g. `:ref_date::TIMESTAMPTZ` silently ends up in the
        # rendered SQL unsubstituted, causing a 500). Binding a single
        # datetime sidesteps the parser collision entirely.
        from datetime import timedelta
        cutoff = ref_date - timedelta(days=churn_window_days) if ref_date else None
        if cutoff is not None:
            churned = conn.execute(text("""
                SELECT COUNT(*)
                FROM (
                    SELECT customer_id, MAX(order_date) AS last_order
                    FROM orders
                    WHERE client_id = :cid
                      AND order_status NOT IN ('Cancelled')
                    GROUP BY customer_id
                ) x
                WHERE x.last_order < :cutoff
            """), {"cid": clientId, "cutoff": cutoff}).scalar() or 0
        else:
            churned = 0

        churn_rate = round(churned * 100.0 / scored_customers, 1) if scored_customers else 0.0

        total_orders = conn.execute(text(
            "SELECT COUNT(*) FROM orders WHERE client_id = :cid"
        ), {"cid": clientId}).scalar() or 0

        kpis = {
            "totalCustomers":     total_customers,
            "scoredCustomers":    scored_customers,
            "unscoredCustomers":  max(total_customers - scored_customers, 0),
            "totalOrders":        total_orders,
            "repeatCustomers":    repeat_customers,
            "highValue":          high_value,
            "churned":            churned,
            "churnRate":          churn_rate,
            "churnWindowDays":     churn_window_days,
            # highValuePercentile removed 2026-04-25 — UI subtitle for the
            # High Value tile changed from "Top X% by spend" to a static
            # "Platinum tier" label since the percentile is no longer
            # client-configurable.
            "minRepeatOrders":    min_repeat_orders,
            "lastRunDate":        "",
        }

        # ═══════════════════════════════════════════════════════════════
        # 2. Customer Segments (derived from RFM scores)
        # ═══════════════════════════════════════════════════════════════
        # Map RFM score combinations → standard segment names.
        # Uses ALL THREE scores: R (Recency), F (Frequency), M (Monetary).
        # Must match the logic in ml/compute_rfm.py → _rfm_segment()
        seg_rows = conn.execute(text("""
            WITH seg AS (
                SELECT
                    CASE
                        WHEN rfm_recency_score >= 4 OR (rfm_recency_score >= 3 AND rfm_frequency_score >= 3)
                            THEN 'Good'
                        WHEN rfm_recency_score <= 1
                            THEN 'Churned'
                        ELSE 'At-Risk'
                    END AS segment
                FROM mv_customer_features
                WHERE client_id = :cid
            )
            SELECT
                segment AS label,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 1) AS pct
            FROM seg
            GROUP BY segment
            ORDER BY count DESC
        """), {"cid": clientId})

        segments = []
        for row in seg_rows:
            segments.append({
                "label": row[0],
                "count": row[1],
                "pct": float(row[2]) if row[2] else 0.0,
                "color": "",
            })

        # ═══════════════════════════════════════════════════════════════
        # 3. Churn Breakdown (Churned vs At-Risk vs Active)
        # ═══════════════════════════════════════════════════════════════
        # At-Risk = not yet churned but low RFM recency (score 1-2).
        churn_rows = conn.execute(text("""
            SELECT
                CASE
                    WHEN churn_label = 1 THEN 'Churned'
                    WHEN rfm_recency_score <= 2 AND churn_label = 0
                        THEN 'At-Risk'
                    ELSE 'Active'
                END AS label,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 1) AS pct
            FROM mv_customer_features
            WHERE client_id = :cid
            GROUP BY 1
            ORDER BY count DESC
        """), {"cid": clientId})

        churn_breakdown = []
        for row in churn_rows:
            churn_breakdown.append({
                "label": row[0],
                "count": row[1],
                "pct": float(row[2]) if row[2] else 0.0,
                "color": "",
            })

        # ═══════════════════════════════════════════════════════════════
        # 4. Tier Distribution (Platinum, Gold, Silver, Bronze)
        # ═══════════════════════════════════════════════════════════════
        # Read the tier that the ML pipeline already assigned per the
        # client's configured tier_method (quartile or custom thresholds).
        # Re-deriving from rfm_total_score here would ignore that setting.
        tier_rows = conn.execute(text("""
            SELECT
                customer_tier AS label,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 1) AS pct
            FROM mv_customer_features
            WHERE client_id = :cid AND customer_tier IS NOT NULL
            GROUP BY customer_tier
            ORDER BY count DESC
        """), {"cid": clientId})

        # Send bare canonical name ('Platinum', 'Gold', etc.) so the
        # frontend TierLabelService can apply the client's custom label.
        tiers = []
        for row in tier_rows:
            tiers.append({
                "label": row[0],
                "count": row[1],
                "pct": float(row[2]) if row[2] else 0.0,
                "color": "",
            })

        # ═══════════════════════════════════════════════════════════════
        # 5. Repeat vs One-Time Buyers
        # ═══════════════════════════════════════════════════════════════
        # Only customers with at least one order are "buyers" — unscored
        # customers (no orders at all) are neither repeat nor one-time.
        one_time = max(scored_customers - repeat_customers, 0)
        repeat_vs_one_time = {
            "repeat":  repeat_customers,
            "oneTime": one_time,
            # Denominator for the chart's percentages — must equal
            # repeat + oneTime so the bars sum to 100%. Unscored
            # customers are excluded because they aren't buyers.
            "total":   scored_customers,
        }

        # Section 6 (Recent Orders pagination) removed 2026-04-29:
        # the Dashboard UI never consumed the recentOrders / totalOrderPages
        # fields. Removing them eliminates a per-page DB roundtrip on every
        # dashboard load. The OrderRow type and the loadOrders/Service
        # method were removed in the same cleanup.

    return {
        "kpis": kpis,
        "segments": segments,
        "churnBreakdown": churn_breakdown,
        "tiers": tiers,
        "repeatVsOneTime": repeat_vs_one_time,
    }


@router.get("/dashboard/segment-customers")
def get_segment_customers(
    clientId: str = Query(default="CLT-001"),
    segment: str = Query(..., description="Segment name, e.g. 'Champions', 'Loyal Customers'"),
    page: int = Query(default=1, ge=1),
    # Default bumped from 10 → 100 per CTO direction. Max raised to 500
    # so the UI can request larger pages without a backend redeploy.
    pageSize: int = Query(default=100, ge=1, le=500),
):
    """
    Drill-down: returns customers belonging to a specific RFM segment.

    The segment CASE logic MUST match Section 2 above and ml/compute_rfm.py.
    The frontend calls this when the user clicks on a segment bar in the
    Retention / Segment Distribution chart.
    """
    offset = (page - 1) * pageSize

    # SQL segment CASE — identical to Section 2 above
    segment_case = """
        CASE
            WHEN rfm_recency_score >= 4 OR (rfm_recency_score >= 3 AND rfm_frequency_score >= 3)
                THEN 'Good'
            WHEN rfm_recency_score <= 1
                THEN 'Churned'
            ELSE 'At-Risk'
        END
    """

    with engine.connect() as conn:
        # Count total in this segment
        count_row = conn.execute(text(f"""
            SELECT COUNT(*) FROM (
                SELECT {segment_case} AS seg
                FROM mv_customer_features
                WHERE client_id = :cid
            ) t WHERE t.seg = :seg
        """), {"cid": clientId, "seg": segment}).fetchone()
        total = count_row[0] if count_row else 0

        # Fetch customer details for this segment
        rows = conn.execute(text(f"""
            SELECT
                mv.customer_id,
                c.customer_name,
                mv.customer_tier,
                mv.total_orders,
                mv.total_spend_usd,
                mv.avg_order_value_usd,
                mv.days_since_last_order,
                mv.rfm_recency_score,
                mv.rfm_frequency_score,
                mv.rfm_monetary_score,
                mv.rfm_total_score,
                mv.churn_label,
                cs.churn_probability,
                cs.risk_tier
            FROM mv_customer_features mv
            JOIN customers c
                ON mv.customer_id = c.customer_id AND mv.client_id = c.client_id
            LEFT JOIN churn_scores cs
                ON mv.customer_id = cs.customer_id AND mv.client_id = cs.client_id
            WHERE mv.client_id = :cid
              AND {segment_case} = :seg
            ORDER BY mv.total_spend_usd DESC
            LIMIT :limit OFFSET :offset
        """), {"cid": clientId, "seg": segment, "limit": pageSize, "offset": offset})

        customers = []
        for row in rows:
            customers.append({
                "customerId": row[0],
                "customerName": row[1] or "Unknown",
                "customerTier": row[2] or "N/A",
                "totalOrders": row[3] or 0,
                "totalSpend": round(float(row[4]), 2) if row[4] else 0.0,
                "avgOrderValue": round(float(row[5]), 2) if row[5] else 0.0,
                "daysSinceLastOrder": row[6] or 0,
                "rfmRecency": row[7] or 0,
                "rfmFrequency": row[8] or 0,
                "rfmMonetary": row[9] or 0,
                "rfmTotal": row[10] or 0,
                "churnLabel": "Churned" if row[11] == 1 else "Active",
                "churnProbability": round(float(row[12]), 4) if row[12] else None,
                "riskTier": row[13] or "N/A",
            })

    return {
        "segment": segment,
        "total": total,
        "pages": math.ceil(total / pageSize) if total > 0 else 0,
        "customers": customers,
    }




# /dashboard/orders endpoint removed 2026-04-29 — the Dashboard's
# Detail Data Tabs section was removed from the UI earlier in the
# same conversation. The endpoint served 9 tabs (Clean Orders,
# RFM, High Value, Repeat Analysis, Product Affinity, ML Features,
# Vendor Analysis, Audit Log, Quarantine) and was 475 lines. Only
# the disabled mock interceptor referenced the path. If any of
# those drilldowns ever return, restore from git history.
