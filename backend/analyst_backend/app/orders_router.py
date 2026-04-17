"""
orders_router.py — GET /api/v1/orders
========================================
Returns paginated order list from the 'orders' table.

The Angular frontend calls this endpoint when the user navigates to
the Orders page. It sends clientId, page, and pageSize as query
parameters, and expects back:
    { data: Order[], total: number, pages: number }
"""

import math
from fastapi import APIRouter, Query
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["orders"])


@router.get("/orders")
def get_orders(
    clientId: str = Query(default="CLT-001"),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=50, ge=1, le=200),
):
    """
    Fetch paginated orders for a given client.

    Each order row includes:
    - Basic info: order_id, customer_id, order_date, status
    - Financial: order_value_usd, discount_usd, coupon_code
    - Meta: payment_method, order_item_count
    """
    offset = (page - 1) * pageSize

    with engine.connect() as conn:
        # Count total orders for this client
        count_result = conn.execute(
            text("SELECT COUNT(*) FROM orders WHERE client_id = :cid"),
            {"cid": clientId},
        )
        total = count_result.scalar() or 0

        # Fetch the page of orders (most recent first)
        rows = conn.execute(
            text("""
                SELECT client_id, order_id, customer_id, order_date,
                       order_status, order_value_usd, discount_usd,
                       coupon_code, payment_method, order_item_count
                FROM orders
                WHERE client_id = :cid
                ORDER BY order_date DESC
                LIMIT :limit OFFSET :offset
            """),
            {"cid": clientId, "limit": pageSize, "offset": offset},
        )

        data = []
        for row in rows:
            data.append({
                "client_id": row[0],
                "order_id": row[1],
                "customer_id": row[2],
                "order_date": str(row[3]) if row[3] else None,
                "order_status": row[4],
                "order_value_usd": float(row[5]) if row[5] else 0.0,
                "discount_usd": float(row[6]) if row[6] else 0.0,
                "coupon_code": row[7],
                "payment_method": row[8],
                "order_item_count": row[9] or 0,
            })

    pages = math.ceil(total / pageSize) if total > 0 else 0

    return {
        "data": data,
        "total": total,
        "pages": pages,
    }
