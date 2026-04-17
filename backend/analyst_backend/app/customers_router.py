"""
customers_router.py — GET /api/v1/customers
==============================================
Returns paginated customer list from the 'customers' table.

The Angular frontend calls this endpoint when the user navigates to
the Customers page. It sends clientId, page, and pageSize as query
parameters, and expects back a JSON object with:
    { data: Customer[], total: number, pages: number }
"""

import math
from fastapi import APIRouter, Query
from sqlalchemy import text

from app.database import engine

router = APIRouter(prefix="/api/v1", tags=["customers"])


@router.get("/customers")
def get_customers(
    clientId: str = Query(default="CLT-001"),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=50, ge=1, le=200),
):
    """
    Fetch paginated customers for a given client.

    How pagination works:
    - 'page' is which page of results to return (starting at 1)
    - 'pageSize' is how many customers per page
    - OFFSET skips rows: page 2 with pageSize 50 skips first 50 rows
    - LIMIT caps how many rows we return
    """
    offset = (page - 1) * pageSize

    with engine.connect() as conn:
        # First, count total customers for this client
        count_result = conn.execute(
            text("SELECT COUNT(*) FROM customers WHERE client_id = :cid"),
            {"cid": clientId},
        )
        total = count_result.scalar() or 0

        # Then fetch the page of customers
        rows = conn.execute(
            text("""
                SELECT client_id, customer_id, customer_email, customer_name,
                       customer_phone, account_created_date, registration_channel,
                       country_code, state, city, zip_code, shipping_address,
                       preferred_device, email_opt_in, sms_opt_in
                FROM customers
                WHERE client_id = :cid
                ORDER BY customer_id
                LIMIT :limit OFFSET :offset
            """),
            {"cid": clientId, "limit": pageSize, "offset": offset},
        )

        # Convert each row to a dictionary
        data = []
        for row in rows:
            data.append({
                "client_id": row[0],
                "customer_id": row[1],
                "customer_email": row[2],
                "customer_name": row[3],
                "customer_phone": row[4],
                "account_created_date": str(row[5]) if row[5] else None,
                "registration_channel": row[6],
                "country_code": row[7],
                "state": row[8],
                "city": row[9],
                "zip_code": row[10],
                "shipping_address": row[11],
                "preferred_device": row[12],
                "email_opt_in": row[13],
                "sms_opt_in": row[14],
            })

    # Calculate total number of pages
    pages = math.ceil(total / pageSize) if total > 0 else 0

    return {
        "data": data,
        "total": total,
        "pages": pages,
    }
