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
from fastapi import APIRouter, Query
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


@router.get("/dashboard")
def get_dashboard(
    clientId: str = Query(default="CLT-001"),
    orderPage: int = Query(default=1, ge=1),
    # Default bumped from 10 → 100 per CTO direction: one page with a
    # vertical scroller is easier to scan than clicking through many
    # 10-row pages. The max was raised from 50 → 500 to match churn_router.
    orderPageSize: int = Query(default=100, ge=1, le=500),
):
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
                        WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 4 AND rfm_monetary_score >= 4
                            THEN 'Champions'
                        WHEN rfm_recency_score >= 3 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3
                            THEN 'Loyal Customers'
                        WHEN rfm_recency_score <= 2 AND rfm_frequency_score >= 4 AND rfm_monetary_score >= 4
                            THEN 'Can''t Lose Them'
                        WHEN rfm_recency_score <= 2 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3
                            THEN 'At Risk'
                        WHEN rfm_recency_score >= 4 AND rfm_frequency_score <= 2
                            THEN 'New Customers'
                        WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 2 AND rfm_monetary_score >= 2
                            THEN 'Potential Loyalists'
                        WHEN rfm_recency_score <= 2 AND rfm_frequency_score <= 2
                            THEN 'Hibernating'
                        ELSE 'Needs Attention'
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

        # Add emoji prefixes to match the frontend format
        tier_emojis = {
            "Platinum": "\U0001f48e",
            "Gold": "\U0001f947",
            "Silver": "\U0001f948",
            "Bronze": "\U0001f949",
        }

        tiers = []
        for row in tier_rows:
            tier_name = row[0]
            emoji = tier_emojis.get(tier_name, "")
            tiers.append({
                "label": f"{emoji} {tier_name}" if emoji else tier_name,
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

        # ═══════════════════════════════════════════════════════════════
        # 6. Recent Orders (paginated, with customer name joined)
        # ═══════════════════════════════════════════════════════════════
        order_offset = (orderPage - 1) * orderPageSize

        order_rows = conn.execute(text("""
            SELECT
                o.order_id,
                c.customer_name,
                o.order_date,
                o.order_item_count,
                o.order_value_usd,
                o.discount_usd,
                o.order_status,
                o.coupon_code,
                o.payment_method
            FROM orders o
            JOIN customers c
                ON o.customer_id = c.customer_id
                AND o.client_id = c.client_id
            WHERE o.client_id = :cid
            ORDER BY o.order_date DESC
            LIMIT :limit OFFSET :offset
        """), {"cid": clientId, "limit": orderPageSize, "offset": order_offset})

        recent_orders = []
        for row in order_rows:
            gross = float(row[4]) if row[4] else 0.0
            discount = float(row[5]) if row[5] else 0.0
            recent_orders.append({
                "orderId": row[0],
                "customer": row[1] or "Unknown",
                "date": str(row[2]) if row[2] else "",
                "items": row[3] or 0,
                "gross": gross,
                "discount": discount,
                "net": round(gross - discount, 2),
                "status": row[6] or "",
                "couponCode": row[7],
                "paymentMethod": row[8],
            })

        total_order_pages = math.ceil(total_orders / orderPageSize) if total_orders > 0 else 0

    return {
        "kpis": kpis,
        "segments": segments,
        "churnBreakdown": churn_breakdown,
        "tiers": tiers,
        "repeatVsOneTime": repeat_vs_one_time,
        "recentOrders": recent_orders,
        "totalOrderPages": total_order_pages,
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
            WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 4 AND rfm_monetary_score >= 4
                THEN 'Champions'
            WHEN rfm_recency_score >= 3 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3
                THEN 'Loyal Customers'
            WHEN rfm_recency_score <= 2 AND rfm_frequency_score >= 4 AND rfm_monetary_score >= 4
                THEN 'Can''t Lose Them'
            WHEN rfm_recency_score <= 2 AND rfm_frequency_score >= 3 AND rfm_monetary_score >= 3
                THEN 'At Risk'
            WHEN rfm_recency_score >= 4 AND rfm_frequency_score <= 2
                THEN 'New Customers'
            WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 2 AND rfm_monetary_score >= 2
                THEN 'Potential Loyalists'
            WHEN rfm_recency_score <= 2 AND rfm_frequency_score <= 2
                THEN 'Hibernating'
            ELSE 'Needs Attention'
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


@router.get("/dashboard/orders")
def get_dashboard_orders(
    clientId: str = Query(default="CLT-001"),
    page: int = Query(default=1, ge=1),
    tab: str = Query(default="Clean Orders"),
):
    """
    Paginated data for the dashboard detail tabs.

    The frontend sends a 'tab' parameter indicating which tab is active:
      - "Clean Orders"     → all orders, paginated
      - "RFM"              → customer RFM scores and segments
      - "High Value"       → high-value customers (rfm_total_score >= 12)
      - "Repeat Analysis"  → repeat customers (total_orders >= 2)
      - Others             → fallback to clean orders

    Each tab returns { orders: [...], total: N, pages: N } to match
    the frontend's expected response shape.
    """
    # page_size bumped from 10 → 100 per CTO direction: the detail tabs
    # (Clean Orders, RFM, High Value, etc.) on the Dashboard are now
    # served 100 rows at a time and scrolled vertically inside the
    # table wrapper, rather than paginated in 10-row chunks.
    page_size = 100
    offset = (page - 1) * page_size

    with engine.connect() as conn:

        # ── RFM Tab ──────────────────────────────────────────────────
        if tab == "RFM":
            # Count total customers
            r = conn.execute(text("""
                SELECT COUNT(*) FROM mv_customer_features
                WHERE client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    mv.customer_id,
                    c.customer_name,
                    mv.rfm_recency_score,
                    mv.rfm_frequency_score,
                    mv.rfm_monetary_score,
                    mv.rfm_total_score,
                    CASE
                        WHEN mv.rfm_recency_score >= 4 AND mv.rfm_frequency_score >= 4 THEN 'Champions'
                        WHEN mv.rfm_recency_score >= 3 AND mv.rfm_frequency_score >= 3 THEN 'Loyal'
                        WHEN mv.rfm_recency_score >= 4 AND mv.rfm_frequency_score <= 2 THEN 'New Customers'
                        WHEN mv.rfm_recency_score >= 3 AND mv.rfm_frequency_score BETWEEN 1 AND 3 THEN 'Potential Loyalists'
                        WHEN mv.rfm_recency_score BETWEEN 2 AND 3 AND mv.rfm_frequency_score BETWEEN 2 AND 3 THEN 'Need Attention'
                        WHEN mv.rfm_recency_score BETWEEN 2 AND 3 AND mv.rfm_frequency_score >= 4 THEN 'About to Sleep'
                        WHEN mv.rfm_recency_score <= 2 AND mv.rfm_frequency_score >= 3 THEN 'At Risk'
                        WHEN mv.rfm_recency_score <= 2 AND mv.rfm_frequency_score <= 2 THEN 'Hibernating'
                        ELSE 'Other'
                    END AS segment,
                    mv.customer_tier AS tier,
                    mv.total_orders,
                    mv.total_spend_usd,
                    mv.days_since_last_order,
                    mv.churn_label
                FROM mv_customer_features mv
                JOIN customers c ON mv.customer_id = c.customer_id AND mv.client_id = c.client_id
                WHERE mv.client_id = :cid
                ORDER BY mv.rfm_total_score DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": row[0],            # customer_id as row key
                    "customer": row[1] or "Unknown",
                    "date": f"R:{row[2]} F:{row[3]} M:{row[4]}",  # RFM scores
                    "items": int(row[5]) if row[5] else 0,       # total RFM score
                    "gross": float(row[9]) if row[9] else 0.0,   # total_spend
                    "discount": int(row[10]) if row[10] else 0,   # days_since_last_order
                    "net": int(row[8]) if row[8] else 0,          # total_orders
                    "status": row[6] or "",                       # segment
                    "couponCode": row[7],                          # tier
                    "paymentMethod": "Churned" if row[11] == 1 else "Active",
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── High Value Tab ───────────────────────────────────────────
        elif tab == "High Value":
            # 2026-04-25: High Value = customer_tier = 'Platinum' (top
            # spenders). Previously used the redundant `is_high_value`
            # flag; that column was dropped from the MV.
            r = conn.execute(text("""
                SELECT COUNT(*) FROM mv_customer_features
                WHERE client_id = :cid AND customer_tier = 'Platinum'
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    mv.customer_id,
                    c.customer_name,
                    mv.total_orders,
                    mv.total_spend_usd,
                    mv.avg_order_value_usd,
                    mv.days_since_last_order,
                    mv.rfm_total_score,
                    mv.customer_tier AS tier,
                    mv.total_reviews,
                    mv.avg_rating,
                    mv.total_tickets,
                    mv.churn_label
                FROM mv_customer_features mv
                JOIN customers c ON mv.customer_id = c.customer_id AND mv.client_id = c.client_id
                WHERE mv.client_id = :cid AND mv.customer_tier = 'Platinum'
                ORDER BY mv.total_spend_usd DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": row[0],
                    "customer": row[1] or "Unknown",
                    "date": f"{row[2]} orders",
                    "items": int(row[6]) if row[6] else 0,        # rfm_total_score
                    "gross": float(row[3]) if row[3] else 0.0,    # total_spend
                    "discount": float(row[4]) if row[4] else 0.0, # avg_order_value
                    "net": int(row[5]) if row[5] else 0,           # days_since_last
                    "status": row[7] or "",                        # tier
                    "couponCode": f"Reviews: {row[8]}, Avg: {row[9]}",
                    "paymentMethod": "Churned" if row[11] == 1 else "Active",
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Repeat Analysis Tab ──────────────────────────────────────
        elif tab == "Repeat Analysis":
            # Repeat customers = total_orders >= 2
            r = conn.execute(text("""
                SELECT COUNT(*) FROM mv_customer_features
                WHERE client_id = :cid AND total_orders >= 2
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    mv.customer_id,
                    c.customer_name,
                    mv.total_orders,
                    mv.total_spend_usd,
                    mv.avg_order_value_usd,
                    mv.avg_days_between_orders,
                    mv.days_since_last_order,
                    mv.orders_last_30d,
                    mv.orders_last_90d,
                    mv.return_rate_pct,
                    mv.unique_products_purchased,
                    mv.churn_label
                FROM mv_customer_features mv
                JOIN customers c ON mv.customer_id = c.customer_id AND mv.client_id = c.client_id
                WHERE mv.client_id = :cid AND mv.total_orders >= 2
                ORDER BY mv.total_orders DESC, mv.total_spend_usd DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": row[0],
                    "customer": row[1] or "Unknown",
                    "date": f"{row[2]} orders",                     # total_orders
                    "items": int(row[10]) if row[10] else 0,        # unique_products
                    "gross": float(row[3]) if row[3] else 0.0,     # total_spend
                    "discount": float(row[4]) if row[4] else 0.0,  # avg_order_value
                    "net": float(row[5]) if row[5] else 0.0,        # avg_days_between
                    "status": "Churned" if row[11] == 1 else "Active",
                    "couponCode": f"30d:{row[7]} 90d:{row[8]}",     # recent order counts
                    "paymentMethod": f"Return: {row[9]}%",          # return rate
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Product Affinity Tab ─────────────────────────────────────
        elif tab == "Product Affinity":
            # Top products by purchase count, with category and brand info
            r = conn.execute(text("""
                SELECT COUNT(DISTINCT li.product_id)
                FROM line_items li WHERE li.client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    p.product_id,
                    p.product_name,
                    cat.category_name,
                    b.brand_name,
                    COUNT(DISTINCT li.order_id) AS order_count,
                    COUNT(DISTINCT li.customer_id) AS customer_count,
                    SUM(li.quantity) AS total_qty_sold,
                    ROUND(SUM(li.final_line_total_usd)::NUMERIC, 2) AS total_revenue,
                    ROUND(AVG(li.unit_price_usd)::NUMERIC, 2) AS avg_price,
                    ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
                          / NULLIF(COUNT(*), 0), 1) AS return_rate
                FROM line_items li
                JOIN products p ON li.client_id = p.client_id AND li.product_id = p.product_id
                LEFT JOIN categories cat ON p.client_id = cat.client_id AND p.category_id = cat.category_id
                LEFT JOIN brands b ON p.client_id = b.client_id AND p.brand_id = b.brand_id
                WHERE li.client_id = :cid
                GROUP BY p.product_id, p.product_name, cat.category_name, b.brand_name
                ORDER BY order_count DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": f"P-{row[0]}",
                    "customer": row[1] or "Unknown",          # product_name
                    "date": row[2] or "N/A",                  # category
                    "items": int(row[6]) if row[6] else 0,    # total_qty_sold
                    "gross": float(row[7]) if row[7] else 0.0, # total_revenue
                    "discount": float(row[8]) if row[8] else 0.0, # avg_price
                    "net": int(row[4]) if row[4] else 0,      # order_count
                    "status": row[3] or "N/A",                # brand_name
                    "couponCode": f"{row[5]} customers",       # customer_count
                    "paymentMethod": f"Return: {row[9]}%",     # return_rate
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── ML Features Tab ──────────────────────────────────────────
        elif tab == "ML Features":
            # Raw ML feature columns from mv_customer_features
            r = conn.execute(text("""
                SELECT COUNT(*) FROM mv_customer_features
                WHERE client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    mv.customer_id,
                    c.customer_name,
                    mv.account_age_days,
                    mv.days_since_last_order,
                    mv.total_orders,
                    mv.avg_days_between_orders,
                    mv.total_spend_usd,
                    mv.avg_order_value_usd,
                    mv.discount_rate_pct,
                    mv.return_rate_pct,
                    mv.total_reviews,
                    mv.avg_rating,
                    mv.total_tickets,
                    mv.churn_label
                FROM mv_customer_features mv
                JOIN customers c ON mv.customer_id = c.customer_id AND mv.client_id = c.client_id
                WHERE mv.client_id = :cid
                ORDER BY mv.customer_id
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": row[0],
                    "customer": row[1] or "Unknown",
                    "date": f"Age: {row[2]}d",                   # account_age_days
                    "items": int(row[4]) if row[4] else 0,       # total_orders
                    "gross": float(row[6]) if row[6] else 0.0,   # total_spend
                    "discount": float(row[8]) if row[8] else 0.0, # discount_rate_pct
                    "net": float(row[9]) if row[9] else 0.0,      # return_rate_pct
                    "status": "Churned" if row[13] == 1 else "Active",
                    "couponCode": f"Reviews:{row[10]} Avg:{row[11]}",
                    "paymentMethod": f"Tickets:{row[12]}",
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Vendor Analysis Tab ──────────────────────────────────────
        elif tab == "Vendor Analysis":
            # Vendor performance: revenue, order count, products
            r = conn.execute(text("""
                SELECT COUNT(DISTINCT v.vendor_id)
                FROM vendors v
                JOIN product_vendor_mapping pvm ON v.client_id = pvm.client_id AND v.vendor_id = pvm.vendor_id
                JOIN line_items li ON pvm.client_id = li.client_id AND pvm.product_id = li.product_id
                WHERE v.client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    v.vendor_id,
                    v.vendor_name,
                    COUNT(DISTINCT pvm.product_id) AS product_count,
                    COUNT(DISTINCT li.order_id) AS order_count,
                    SUM(li.quantity) AS total_qty,
                    ROUND(SUM(li.final_line_total_usd)::NUMERIC, 2) AS total_revenue,
                    ROUND(AVG(li.unit_price_usd)::NUMERIC, 2) AS avg_price,
                    COUNT(DISTINCT li.customer_id) AS customer_reach,
                    ROUND(COUNT(CASE WHEN li.item_status = 'Returned' THEN 1 END) * 100.0
                          / NULLIF(COUNT(*), 0), 1) AS return_rate
                FROM vendors v
                JOIN product_vendor_mapping pvm ON v.client_id = pvm.client_id AND v.vendor_id = pvm.vendor_id
                JOIN products p ON pvm.client_id = p.client_id AND pvm.product_id = p.product_id
                JOIN line_items li ON p.client_id = li.client_id AND p.product_id = li.product_id
                WHERE v.client_id = :cid
                GROUP BY v.vendor_id, v.vendor_name
                ORDER BY total_revenue DESC NULLS LAST
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": f"V-{row[0]}",
                    "customer": row[1] or "Unknown",            # vendor_name
                    "date": f"{row[2]} products",               # product_count
                    "items": int(row[4]) if row[4] else 0,      # total_qty
                    "gross": float(row[5]) if row[5] else 0.0,  # total_revenue
                    "discount": float(row[6]) if row[6] else 0.0, # avg_price
                    "net": int(row[3]) if row[3] else 0,         # order_count
                    "status": f"{row[7]} customers",             # customer_reach
                    "couponCode": None,
                    "paymentMethod": f"Return: {row[8]}%",       # return_rate
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Audit Log Tab ────────────────────────────────────────────
        elif tab == "Audit Log":
            # Retention interventions as audit trail
            r = conn.execute(text("""
                SELECT COUNT(*) FROM retention_interventions
                WHERE client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    ri.intervention_id,
                    c.customer_name,
                    ri.created_at,
                    ri.risk_tier,
                    ri.offer_type,
                    ri.discount_pct,
                    ri.channel,
                    ri.churn_probability,
                    ri.guardrail_passed,
                    ri.escalated_to_human
                FROM retention_interventions ri
                JOIN customers c ON ri.customer_id = c.customer_id AND ri.client_id = c.client_id
                WHERE ri.client_id = :cid
                ORDER BY ri.created_at DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                orders.append({
                    "orderId": f"INT-{row[0]}",
                    "customer": row[1] or "Unknown",
                    "date": str(row[2]) if row[2] else "",
                    "items": 0,
                    "gross": float(row[7]) if row[7] else 0.0,   # churn_probability
                    "discount": float(row[5]) if row[5] else 0.0, # discount_pct
                    "net": 0,
                    "status": row[3] or "",                       # risk_tier
                    "couponCode": row[4],                          # offer_type
                    "paymentMethod": row[6] or "",                 # channel
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Quarantine Tab ───────────────────────────────────────────
        elif tab == "Quarantine":
            # Churn scores with high probability — flagged for review
            r = conn.execute(text("""
                SELECT COUNT(*) FROM churn_scores
                WHERE client_id = :cid
            """), {"cid": clientId})
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            rows = conn.execute(text("""
                SELECT
                    cs.score_id,
                    c.customer_name,
                    cs.scored_at,
                    cs.churn_probability,
                    cs.risk_tier,
                    cs.churn_label_simulated,
                    cs.driver_1,
                    cs.driver_2,
                    cs.driver_3,
                    cs.model_version
                FROM churn_scores cs
                JOIN customers c ON cs.customer_id = c.customer_id AND cs.client_id = c.client_id
                WHERE cs.client_id = :cid
                ORDER BY cs.churn_probability DESC
                LIMIT :limit OFFSET :offset
            """), {"cid": clientId, "limit": page_size, "offset": offset})

            orders = []
            for row in rows:
                prob = float(row[3]) if row[3] else 0.0
                orders.append({
                    "orderId": f"CS-{row[0]}",
                    "customer": row[1] or "Unknown",
                    "date": str(row[2]) if row[2] else "",
                    "items": 0,
                    "gross": prob,                                 # churn_probability
                    "discount": 0,
                    "net": 0,
                    "status": row[4] or "",                       # risk_tier
                    "couponCode": f"{row[6] or ''}, {row[7] or ''}",  # drivers
                    "paymentMethod": row[9] or "",                 # model_version
                })

            return {"orders": orders, "total": total, "pages": pages}

        # ── Clean Orders Tab (default) ───────────────────────────────
        else:
            # Build WHERE clause for clean orders
            where = "WHERE o.client_id = :cid"
            params: dict = {"cid": clientId, "limit": page_size, "offset": offset}

            # Count total
            r = conn.execute(
                text(f"SELECT COUNT(*) FROM orders o {where}"),
                params,
            )
            total = r.scalar() or 0
            pages = math.ceil(total / page_size) if total > 0 else 0

            # Get orders
            rows = conn.execute(text(f"""
                SELECT
                    o.order_id, c.customer_name, o.order_date,
                    o.order_item_count, o.order_value_usd, o.discount_usd,
                    o.order_status, o.coupon_code, o.payment_method
                FROM orders o
                JOIN customers c
                    ON o.customer_id = c.customer_id
                    AND o.client_id = c.client_id
                {where}
                ORDER BY o.order_date DESC
                LIMIT :limit OFFSET :offset
            """), params)

            orders = []
            for row in rows:
                gross = float(row[4]) if row[4] else 0.0
                discount = float(row[5]) if row[5] else 0.0
                orders.append({
                    "orderId": row[0],
                    "customer": row[1] or "Unknown",
                    "date": str(row[2]) if row[2] else "",
                    "items": row[3] or 0,
                    "gross": gross,
                    "discount": discount,
                    "net": round(gross - discount, 2),
                    "status": row[6] or "",
                    "couponCode": row[7],
                    "paymentMethod": row[8],
                })

            return {"orders": orders, "total": total, "pages": pages}
