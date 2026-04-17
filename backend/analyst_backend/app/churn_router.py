"""
churn_router.py — Churn Scores API
====================================
GET /api/v1/churn-scores — Paginated churn scores with customer details,
                           sorted by churn probability (highest first)

This endpoint joins churn_scores with customers and mv_customer_features
to give a rich view: customer name, email, churn probability, risk tier,
top 3 drivers, RFM scores, total orders, and total spend.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Query
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token

router = APIRouter(prefix="/api/v1", tags=["churn"])
log = logging.getLogger("churn")


@router.get("/churn-scores")
def get_churn_scores(
    clientId: str = Query(...),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=25, ge=1, le=100),
    riskTier: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    authorization: Optional[str] = Header(default=None),
):
    """
    Get churn scores for a client, sorted by churn probability (descending).

    Features:
    - Paginated (default 25 per page)
    - Filter by risk tier (HIGH, MEDIUM, LOW)
    - Search by customer name or ID
    - Joined with customer details + RFM features
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Build WHERE clause
    where = "WHERE cs.client_id = :cid"
    params = {"cid": clientId}

    if riskTier:
        where += " AND cs.risk_tier = :tier"
        params["tier"] = riskTier.upper()

    if search:
        where += " AND (LOWER(c.customer_name) LIKE :search OR LOWER(cs.customer_id) LIKE :search)"
        params["search"] = f"%{search.lower()}%"

    try:
        with engine.connect() as conn:
            # Count total matching rows
            count_sql = text(f"""
                SELECT COUNT(*)
                FROM churn_scores cs
                JOIN customers c ON cs.client_id = c.client_id AND cs.customer_id = c.customer_id
                {where}
            """)
            total = conn.execute(count_sql, params).scalar() or 0
            total_pages = max(1, (total + pageSize - 1) // pageSize)

            # Fetch paginated results with customer details
            offset = (page - 1) * pageSize
            data_sql = text(f"""
                SELECT
                    cs.customer_id,
                    c.customer_name,
                    c.customer_email,
                    cs.churn_probability,
                    cs.risk_tier,
                    cs.driver_1,
                    cs.driver_2,
                    cs.driver_3,
                    cs.scored_at,
                    cs.model_version,
                    mv.total_orders,
                    mv.total_spend_usd,
                    mv.avg_order_value_usd,
                    mv.rfm_recency_score,
                    mv.rfm_frequency_score,
                    mv.rfm_monetary_score,
                    mv.rfm_total_score,
                    mv.customer_tier,
                    mv.avg_rating,
                    mv.total_tickets
                FROM churn_scores cs
                JOIN customers c ON cs.client_id = c.client_id AND cs.customer_id = c.customer_id
                LEFT JOIN mv_customer_features mv ON cs.client_id = mv.client_id AND cs.customer_id = mv.customer_id
                {where}
                ORDER BY cs.churn_probability DESC
                LIMIT :limit OFFSET :offset
            """)
            params["limit"] = pageSize
            params["offset"] = offset

            rows = conn.execute(data_sql, params).fetchall()

        # Build response
        scores = []
        for r in rows:
            scores.append({
                "customer_id": r[0],
                "customer_name": r[1],
                "customer_email": r[2],
                "churn_probability": float(r[3]) if r[3] is not None else 0,
                "risk_tier": r[4],
                "driver_1": r[5],
                "driver_2": r[6],
                "driver_3": r[7],
                "scored_at": r[8].isoformat() if r[8] else None,
                "model_version": r[9],
                "total_orders": r[10] or 0,
                "total_spend": float(r[11]) if r[11] else 0,
                "avg_order_value": float(r[12]) if r[12] else 0,
                "rfm_recency": r[13] or 0,
                "rfm_frequency": r[14] or 0,
                "rfm_monetary": r[15] or 0,
                "rfm_total": r[16] or 0,
                "tier": r[17] or "—",
                "avg_rating": float(r[18]) if r[18] else 0,
                "total_tickets": r[19] or 0,
            })

        # Summary stats (new connection since the previous one is closed)
        with engine.connect() as conn2:
            summary_sql = text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE cs.risk_tier = 'HIGH') AS high_risk,
                    COUNT(*) FILTER (WHERE cs.risk_tier = 'MEDIUM') AS medium_risk,
                    COUNT(*) FILTER (WHERE cs.risk_tier = 'LOW') AS low_risk,
                    ROUND(AVG(cs.churn_probability)::numeric, 3) AS avg_probability
                FROM churn_scores cs
                JOIN customers c ON cs.client_id = c.client_id AND cs.customer_id = c.customer_id
                WHERE cs.client_id = :cid
            """)
            summary_row = conn2.execute(summary_sql, {"cid": clientId}).fetchone()

        summary = {
            "total_scored": summary_row[0] if summary_row else 0,
            "high_risk": summary_row[1] if summary_row else 0,
            "medium_risk": summary_row[2] if summary_row else 0,
            "low_risk": summary_row[3] if summary_row else 0,
            "avg_probability": float(summary_row[4]) if summary_row and summary_row[4] else 0,
        }

        return {
            "scores": scores,
            "summary": summary,
            "page": page,
            "pageSize": pageSize,
            "totalRows": total,
            "totalPages": total_pages,
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error("Failed to fetch churn scores: %s", e)
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
