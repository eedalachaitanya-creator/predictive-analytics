"""
settings_router.py — Client Settings / Configuration Endpoints
================================================================
GET  /api/v1/settings?clientId=CLT-001  — Get current settings for a client
PUT  /api/v1/settings?clientId=CLT-001  — Update settings for a client

These settings are stored in the client_config table and directly affect
how the ML pipeline processes data:
  - churn_window_days: defines what "churned" means (e.g., 90 days no order)
  - login_window_days: second condition of the churn rule
  - min_repeat_orders: minimum orders to count as repeat customer
  - tier thresholds: custom spend amounts for Platinum/Gold/Silver/Bronze
  - prediction_mode: churn | retention | segmentation | full

2026-04-25 — Removed `high_value_percentile`. It drove the now-deleted
`is_high_value` column in mv_customer_features. customer_tier (Platinum
= top 25%) is the value bucket. Field is no longer accepted, returned,
or persisted.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token, get_current_user, require_client_access
from app.audit_logger import log_audit_event

router = APIRouter(prefix="/api/v1", tags=["settings"], dependencies=[Depends(get_current_user)])  # audit-2026-04-29: router-level auth
log = logging.getLogger("settings")


class SettingsUpdate(BaseModel):
    churn_window_days: Optional[int] = None
    # Login-aware churn rule (added 2026-04-24): customer is flagged churned
    # only if BOTH `days_since_last_order > churn_window_days` AND
    # `days_since_last_login > login_window_days`. Default is 30; adjustable
    # per tenant from the Settings page.
    login_window_days: Optional[int] = None
    min_repeat_orders: Optional[int] = None
    # high_value_percentile removed 2026-04-25 — see module docstring.
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
    # Display names for customer tiers — do not affect MV logic, only UI labels
    tier_label_platinum: Optional[str] = None
    tier_label_gold: Optional[str] = None
    tier_label_silver: Optional[str] = None
    tier_label_bronze: Optional[str] = None


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
    require_client_access(user, clientId)   # tenant authorization (prevent IDOR)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT client_id, client_name, client_code, currency, timezone,
                           churn_window_days, min_repeat_orders,
                           recent_order_gap_window, prediction_mode, tier_method,
                           custom_platinum_min, custom_gold_min, custom_silver_min, custom_bronze_min,
                           high_ltv_threshold, mid_ltv_threshold, max_discount_pct,
                           reference_date_mode, reference_date, fiscal_year_start,
                           tier_label_platinum, tier_label_gold,
                           tier_label_silver, tier_label_bronze,
                           login_window_days
                    FROM client_config
                    WHERE client_id = :cid
                """),
                {"cid": clientId},
            ).fetchone()

            # Whether the tenant has any uploaded login history — the UI uses
            # this to enable/disable the Recent Login Window setting (it only
            # affects the model once Login Events exist).
            has_login = bool(conn.execute(
                text("SELECT EXISTS(SELECT 1 FROM login_events WHERE client_id = :cid)"),
                {"cid": clientId},
            ).scalar())

        if not row:
            raise HTTPException(status_code=404, detail=f"No config found for {clientId}")

        # 2026-04-25: positional indexes shifted down by 1 starting at row[7]
        # because high_value_percentile was dropped from the SELECT list.
        return {
            "client_id": row[0],
            "client_name": row[1],
            "client_code": row[2],
            "currency": row[3],
            "timezone": row[4],
            "churn_window_days": row[5],
            "min_repeat_orders": row[6],
            "recent_order_gap_window": row[7],
            "prediction_mode": row[8],
            "tier_method": row[9],
            "custom_platinum_min": float(row[10]) if row[10] else 500.0,
            "custom_gold_min": float(row[11]) if row[11] else 250.0,
            "custom_silver_min": float(row[12]) if row[12] else 100.0,
            "custom_bronze_min": float(row[13]) if row[13] else 0.0,
            "high_ltv_threshold": float(row[14]) if row[14] else 1000.0,
            "mid_ltv_threshold": float(row[15]) if row[15] else 200.0,
            "max_discount_pct": float(row[16]) if row[16] else 30.0,
            "reference_date_mode": row[17],
            "reference_date": row[18].isoformat() if row[18] else None,
            "fiscal_year_start": row[19].isoformat() if row[19] else None,
            "tier_label_platinum": row[20] or '💎 Platinum',
            "tier_label_gold":     row[21] or '🥇 Gold',
            "tier_label_silver":   row[22] or '🥈 Silver',
            "tier_label_bronze":   row[23] or '🥉 Bronze',
            "login_window_days":   row[24] if row[24] is not None else 30,
            "has_login_data":      has_login,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.put("/settings")
def update_settings(
    req: SettingsUpdate,
    request: Request,
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
    require_client_access(user, clientId)   # tenant authorization (prevent cross-tenant write)

    updates = []
    params = {"cid": clientId}

    field_map = {
        "churn_window_days": req.churn_window_days,
        "login_window_days": req.login_window_days,
        "min_repeat_orders": req.min_repeat_orders,
        # high_value_percentile removed 2026-04-25 (column dropped from
        # client_config; see migration 2026_04_25_remove_is_high_value.sql).
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
        "tier_label_platinum": req.tier_label_platinum,
        "tier_label_gold":     req.tier_label_gold,
        "tier_label_silver":   req.tier_label_silver,
        "tier_label_bronze":   req.tier_label_bronze,
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

        # Audit the save. The "details" summarises which fields changed and
        # their new values so the audit reader can reconstruct the change.
        changed_pairs = [f"{k}→{v}" for k, v in field_map.items() if v is not None]
        log_audit_event(
            request,
            action_type="settings_saved",
            details=" · ".join(changed_pairs)[:1000],  # cap to keep audit rows lean
            client_id=clientId,
            user_id=user["id"],
            user_email=user["email"],
            outcome="success",
        )

        return {"status": "saved", "client_id": clientId, "updated_fields": [k for k, v in field_map.items() if v is not None]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save settings: {e}")
