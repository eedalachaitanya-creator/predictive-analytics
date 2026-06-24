"""
analytics_router.py — GET /api/v1/analytics
==============================================
Returns platform-wide KPIs and per-client metrics.

The Angular frontend calls this endpoint when the user navigates to
the Admin Analytics page. It expects:
    {
        platformKpis:  { ... },
        clientMetrics: [ ... ]
    }

NOTE — Pipeline-run metrics (`pipelineRunsLast30`, `lastRun`,
`avgDuration`, `monthlyTrend`) were removed because there is no
`pipeline_runs` log table backing them. The previous implementation
either hardcoded them to zero/empty OR counted `orders` rows and
labelled them as "runs", which was misleading. When/if we add a real
pipeline-run tracking table, those metrics can come back as genuine
aggregates.
"""

from fastapi import APIRouter, Depends, HTTPException
from app.auth_router import get_current_user
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["analytics"], dependencies=[Depends(get_current_user)])  # audit-2026-04-29: router-level auth


@router.get("/analytics")
def get_analytics(user: dict = Depends(get_current_user)):
    """
    Platform-wide analytics: KPIs + per-client breakdown.

    Read-heavy endpoint that runs a handful of aggregates across
    `customers`, `orders`, and `mv_customer_features` to give admins a
    bird's-eye view of the platform.
    """
    # Cross-tenant data (every client's totals) — super_admin only.
    if user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Super admin access required")
    with engine.connect() as conn:

        # ── 1. Platform KPIs (totals across ALL clients) ──
        # Roster counts come from client_config so they MATCH the Clients page
        # (active vs total). The old code counted DISTINCT client_id in customers
        # = "clients that have data", which under-reported (Analytics showed 6
        # while Clients showed 13) and mislabelled freshly-onboarded clients.
        row = conn.execute(text(
            "SELECT COUNT(*) FILTER (WHERE is_active), COUNT(*) FROM client_config"
        )).fetchone()
        active_clients = int(row[0] or 0)
        total_clients = int(row[1] or 0)
        # Clients that actually have customer data (the comparison set below).
        clients_with_data = conn.execute(text(
            "SELECT COUNT(DISTINCT client_id) FROM customers"
        )).scalar() or 0

        # Total customers across all clients.
        r = conn.execute(text("SELECT COUNT(*) FROM customers"))
        total_customers = r.scalar() or 0

        # Total orders across all clients.
        r = conn.execute(text("SELECT COUNT(*) FROM orders"))
        total_orders = r.scalar() or 0

        # Average churn rate across all clients (from the materialized
        # feature view the pipeline refreshes after each run).
        r = conn.execute(text("""
            SELECT ROUND(
                AVG(churn_label) * 100, 1
            ) FROM mv_customer_features
        """))
        avg_churn_rate = float(r.scalar() or 0)

        platform_kpis = {
            "activeClients":   active_clients,
            "totalClients":    total_clients,
            "clientsWithData": int(clients_with_data),
            "totalCustomers":  total_customers,
            "totalOrders":     total_orders,
            "avgChurnRate":    avg_churn_rate,
        }

        # ── 2. Per-Client Metrics ──
        # For each client: customer count, order count, churn %, high-value count.
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
                "clientId":   row[0],
                "clientName": row[1],
                "customers":  row[2] or 0,
                "orders":     row[3] or 0,
                "churnPct":   float(row[4]) if row[4] else 0.0,
                "highValue":  row[5] or 0,
                "color":      "",
            })

    return {
        "platformKpis":  platform_kpis,
        "clientMetrics": client_metrics,
    }
