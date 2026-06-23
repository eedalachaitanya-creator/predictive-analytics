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

from fastapi import APIRouter, Depends, Query, HTTPException
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


# ──────────────────────────────────────────────────────────────────────────────
# Cross-client KPI drill-downs — the clickable tiles on the Admin Analytics page.
# Each tile opens a paginated modal listing the platform-wide records behind the
# number (mirrors /dashboard/kpi-drilldown, but spans ALL clients so every row
# carries its client_id). The `total` for the three COUNT cards is computed with
# the SAME query as the matching KPI in get_analytics() above, so the popup count
# can NEVER disagree with the card (pinned by test_analytics_kpi_drilldown.py).
# ──────────────────────────────────────────────────────────────────────────────

VALID_ANALYTICS_CARDS = {
    "active_clients", "total_customers", "total_orders", "avg_churn_rate",
}


@router.get("/analytics/kpi-drilldown")
def get_analytics_drilldown(
    card: str = Query(..., description="One of: active_clients, total_customers, "
                                       "total_orders, avg_churn_rate"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(get_current_user),
):
    """Paginated, cross-client drill-down behind an Admin Analytics summary card.

    Returns the generic ``{card, label, columns, rows, total, offset, limit}``
    table shape the existing data-viewer modal renders. Every row carries its
    ``client_id`` so an admin can see which tenant each record belongs to.
    """
    # Cross-tenant data — super_admin only (same gate as /analytics).
    if user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Super admin access required")
    if card not in VALID_ANALYTICS_CARDS:
        raise HTTPException(400, f"Unknown card '{card}'. Valid: {sorted(VALID_ANALYTICS_CARDS)}")

    p = {"limit": limit, "offset": offset}

    with engine.connect() as conn:
        if card == "active_clients":
            label = "Active Clients"
            columns = ["client_id", "client_name", "is_active", "customers", "orders"]
            # total mirrors platform_kpis.activeClients (COUNT FILTER WHERE is_active)
            total = conn.execute(text(
                "SELECT COUNT(*) FROM client_config WHERE is_active"
            )).scalar() or 0
            rows_sql = """
                SELECT cc.client_id, cc.client_name, cc.is_active,
                       COALESCE(cu.n, 0) AS customers,
                       COALESCE(o.n, 0)  AS orders
                FROM client_config cc
                LEFT JOIN (SELECT client_id, COUNT(*) n FROM customers GROUP BY client_id) cu
                       ON cu.client_id = cc.client_id
                LEFT JOIN (SELECT client_id, COUNT(*) n FROM orders GROUP BY client_id) o
                       ON o.client_id = cc.client_id
                WHERE cc.is_active
                ORDER BY cc.client_id
                LIMIT :limit OFFSET :offset
            """

        elif card == "total_customers":
            label = "Total Customers"
            columns = ["client_id", "customer_id", "customer_name", "customer_tier",
                       "total_orders", "total_spend_usd", "risk_tier"]
            total = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar() or 0
            rows_sql = """
                SELECT c.client_id, c.customer_id, c.customer_name, mv.customer_tier,
                       mv.total_orders, mv.total_spend_usd, cs.risk_tier
                FROM customers c
                LEFT JOIN mv_customer_features mv
                       ON mv.client_id = c.client_id AND mv.customer_id = c.customer_id
                LEFT JOIN churn_scores cs
                       ON cs.client_id = c.client_id AND cs.customer_id = c.customer_id
                ORDER BY c.client_id, mv.total_spend_usd DESC NULLS LAST, c.customer_id
                LIMIT :limit OFFSET :offset
            """

        elif card == "total_orders":
            label = "Total Orders"
            columns = ["client_id", "order_id", "customer_id", "order_date",
                       "order_value_usd", "order_status"]
            total = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0
            rows_sql = """
                SELECT client_id, order_id, customer_id, order_date,
                       order_value_usd, order_status
                FROM orders
                ORDER BY order_date DESC NULLS LAST, order_id DESC
                LIMIT :limit OFFSET :offset
            """

        else:  # avg_churn_rate — the per-client breakdown the platform average is built from
            label = "Avg Churn Rate by Client"
            columns = ["client_id", "client_name", "customers", "churned", "churn_pct"]
            # An average has no single "count" to match, so total = the number of
            # clients the average spans (one row per client in the feature view).
            total = conn.execute(text(
                "SELECT COUNT(DISTINCT client_id) FROM mv_customer_features"
            )).scalar() or 0
            rows_sql = """
                SELECT mv.client_id,
                       COALESCE(cc.client_name, mv.client_id) AS client_name,
                       COUNT(*) AS customers,
                       SUM(mv.churn_label) AS churned,
                       ROUND(AVG(mv.churn_label) * 100, 1) AS churn_pct
                FROM mv_customer_features mv
                LEFT JOIN client_config cc ON cc.client_id = mv.client_id
                GROUP BY mv.client_id, cc.client_name
                ORDER BY churn_pct DESC NULLS LAST, mv.client_id
                LIMIT :limit OFFSET :offset
            """

        rows = [dict(r._mapping) for r in conn.execute(text(rows_sql), p)]

    return {
        "card": card,
        "label": label,
        "columns": columns,
        "rows": rows,
        "total": total,
        "offset": offset,
        "limit": limit,
    }
