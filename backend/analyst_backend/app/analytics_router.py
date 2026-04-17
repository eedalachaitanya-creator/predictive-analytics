"""
analytics_router.py — GET /api/v1/analytics
==============================================
Returns platform-wide KPIs, per-client metrics, and monthly trends.

The Angular frontend calls this endpoint when the user navigates to
the Analytics page. It expects:
    {
        platformKpis: { ... },
        clientMetrics: [ ... ],
        monthlyTrend: [ ... ]
    }

This endpoint aggregates data across ALL clients in the database,
giving a bird's-eye view of the entire platform.
"""

from fastapi import APIRouter
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["analytics"])


@router.get("/analytics")
def get_analytics():
    """
    Platform-wide analytics: KPIs, per-client breakdown, and monthly trends.

    This is a read-heavy endpoint that runs several aggregate queries
    across multiple tables. It's designed for an admin/analyst who wants
    to see the overall health of the platform.
    """
    with engine.connect() as conn:

        # ── 1. Platform KPIs (totals across ALL clients) ──
        # Count distinct clients that have customers
        r = conn.execute(text(
            "SELECT COUNT(DISTINCT client_id) FROM customers"
        ))
        total_clients = r.scalar() or 0

        # Total customers across all clients
        r = conn.execute(text("SELECT COUNT(*) FROM customers"))
        total_customers = r.scalar() or 0

        # Total orders across all clients
        r = conn.execute(text("SELECT COUNT(*) FROM orders"))
        total_orders = r.scalar() or 0

        # Average churn rate across all clients
        r = conn.execute(text("""
            SELECT ROUND(
                AVG(churn_label) * 100, 1
            ) FROM mv_customer_features
        """))
        avg_churn_rate = float(r.scalar() or 0)

        platform_kpis = {
            "activeClients": total_clients,
            "totalClients": total_clients,
            "totalCustomers": total_customers,
            "totalOrders": total_orders,
            "avgChurnRate": avg_churn_rate,
            "pipelineRunsLast30": 0,  # Would come from a pipeline_runs log table
        }

        # ── 2. Per-Client Metrics ──
        # For each client: customer count, order count, churn %, high-value count
        client_rows = conn.execute(text("""
            SELECT
                c.client_id,
                c.client_id AS client_name,
                COUNT(DISTINCT c.customer_id) AS customers,
                COALESCE(o.order_count, 0) AS orders,
                ROUND(AVG(mv.churn_label) * 100, 1) AS churn_pct,
                COUNT(DISTINCT CASE WHEN mv.rfm_total_score >= 12
                    THEN c.customer_id END) AS high_value
            FROM customers c
            LEFT JOIN mv_customer_features mv
                ON c.customer_id = mv.customer_id
                AND c.client_id = mv.client_id
            LEFT JOIN (
                SELECT client_id, COUNT(*) AS order_count
                FROM orders
                GROUP BY client_id
            ) o ON c.client_id = o.client_id
            GROUP BY c.client_id, o.order_count
            ORDER BY c.client_id
        """))

        client_metrics = []
        for row in client_rows:
            client_metrics.append({
                "clientId": row[0],
                "clientName": row[1],
                "customers": row[2] or 0,
                "orders": row[3] or 0,
                "churnPct": float(row[4]) if row[4] else 0.0,
                "highValue": row[5] or 0,
                "lastRun": "",
                "avgDuration": 0.0,
                "color": "",
            })

        # ── 3. Monthly Trend (orders per month) ──
        trend_rows = conn.execute(text("""
            SELECT
                TO_CHAR(order_date, 'Mon YYYY') AS month,
                client_id,
                COUNT(*) AS run_count
            FROM orders
            WHERE order_date IS NOT NULL
            GROUP BY TO_CHAR(order_date, 'Mon YYYY'),
                     DATE_TRUNC('month', order_date),
                     client_id
            ORDER BY DATE_TRUNC('month', MIN(order_date))
        """))

        # Group by month
        monthly_map = {}
        for row in trend_rows:
            month = row[0]
            client_id = row[1]
            count = row[2]

            if month not in monthly_map:
                monthly_map[month] = {"month": month, "runsByClient": {}, "totalRuns": 0, "avgDurationSeconds": 0.0}
            monthly_map[month]["runsByClient"][client_id] = count
            monthly_map[month]["totalRuns"] += count

        monthly_trend = list(monthly_map.values())

    return {
        "platformKpis": platform_kpis,
        "clientMetrics": client_metrics,
        "monthlyTrend": monthly_trend,
    }
