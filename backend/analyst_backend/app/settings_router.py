"""
settings_router.py — Client Settings / Configuration Endpoints
================================================================
GET  /api/v1/settings?clientId=CLT-001  — Get current settings for a client
PUT  /api/v1/settings?clientId=CLT-001  — Update settings for a client

These settings are stored in the client_config table and directly affect
how the ML pipeline processes data:
  - churn_window_days: defines what "churned" means (e.g., 90 days no order)
  - min_repeat_orders: minimum orders to count as repeat customer
  - high_value_percentile: percentile cutoff for high-value flag
  - tier thresholds: custom spend amounts for Platinum/Gold/Silver/Bronze
  - prediction_mode: churn | retention | segmentation | full
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token

router = APIRouter(prefix="/api/v1", tags=["settings"])
log = logging.getLogger("settings")


class SettingsUpdate(BaseModel):
    churn_window_days: Optional[int] = None
    min_repeat_orders: Optional[int] = None
    high_value_percentile: Optional[int] = None
    recent_order_gap_window: Optional[int] = None
    prediction_mode: Optional[str] = None
    tier_method: Optional[str] = None
    custom_platinum_min: Optional[float] = None
    custom_gold_min: Optional[float] = None
    custom_silver_min: Optional[float] = None
    custom_bronze_min: Optional[float] = None
    high_ltv_threshold: Optional[float] = None
    mid_ltv_threshold: Optional[float] = None
    max_discount_pct: Optional[float] = None


@router.get("/settings")
def get_settings(
    clientId: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    """Get current settings for a client from client_config table."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT client_id, client_name, client_code, currency, timezone,
                           churn_window_days, min_repeat_orders, high_value_percentile,
                           recent_order_gap_window, prediction_mode, tier_method,
                           custom_platinum_min, custom_gold_min, custom_silver_min, custom_bronze_min,
                           high_ltv_threshold, mid_ltv_threshold, max_discount_pct,
                           reference_date_mode, reference_date, fiscal_year_start
                    FROM client_config
                    WHERE client_id = :cid
                """),
                {"cid": clientId},
            ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"No config found for {clientId}")

        return {
            "client_id": row[0],
            "client_name": row[1],
            "client_code": row[2],
            "currency": row[3],
            "timezone": row[4],
            "churn_window_days": row[5],
            "min_repeat_orders": row[6],
            "high_value_percentile": row[7],
            "recent_order_gap_window": row[8],
            "prediction_mode": row[9],
            "tier_method": row[10],
            "custom_platinum_min": float(row[11]) if row[11] else 500.0,
            "custom_gold_min": float(row[12]) if row[12] else 250.0,
            "custom_silver_min": float(row[13]) if row[13] else 100.0,
            "custom_bronze_min": float(row[14]) if row[14] else 0.0,
            "high_ltv_threshold": float(row[15]) if row[15] else 1000.0,
            "mid_ltv_threshold": float(row[16]) if row[16] else 200.0,
            "max_discount_pct": float(row[17]) if row[17] else 30.0,
            "reference_date_mode": row[18],
            "reference_date": row[19].isoformat() if row[19] else None,
            "fiscal_year_start": row[20].isoformat() if row[20] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.put("/settings")
def update_settings(
    req: SettingsUpdate,
    clientId: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    """Update settings for a client. Only provided fields are changed."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    updates = []
    params = {"cid": clientId}

    field_map = {
        "churn_window_days": req.churn_window_days,
        "min_repeat_orders": req.min_repeat_orders,
        "high_value_percentile": req.high_value_percentile,
        "recent_order_gap_window": req.recent_order_gap_window,
        "prediction_mode": req.prediction_mode,
        "tier_method": req.tier_method,
        "custom_platinum_min": req.custom_platinum_min,
        "custom_gold_min": req.custom_gold_min,
        "custom_silver_min": req.custom_silver_min,
        "custom_bronze_min": req.custom_bronze_min,
        "high_ltv_threshold": req.high_ltv_threshold,
        "mid_ltv_threshold": req.mid_ltv_threshold,
        "max_discount_pct": req.max_discount_pct,
    }

    for col, val in field_map.items():
        if val is not None:
            updates.append(f"{col} = :{col}")
            params[col] = val

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        with engine.connect() as conn:
            conn.execute(
                text(f"UPDATE client_config SET {', '.join(updates)} WHERE client_id = :cid"),
                params,
            )
            conn.commit()

        log.info("Settings updated for %s: %s", clientId, list(field_map.keys()))
        return {"status": "saved", "client_id": clientId, "updated_fields": [k for k, v in field_map.items() if v is not None]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save settings: {e}")
