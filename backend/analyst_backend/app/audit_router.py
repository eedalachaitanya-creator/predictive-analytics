"""
audit_router.py — super-admin-only endpoints that power the Audit Log UI.

Exposes:
  GET  /api/v1/audit/stats            — headline KPI numbers for the 3 top cards
  GET  /api/v1/audit/filter-options   — distinct client/user/action lists for the filter dropdowns
  GET  /api/v1/audit                  — paginated + filtered event list
  GET  /api/v1/audit/export           — same filters, returns CSV download

Auth model:
  All endpoints are super_admin only. The frontend is rendered inside the
  Admin Console sidebar group so non-super-admin users can't even see the
  tab — these checks are the backend belt to that front-end suspenders.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Header
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from app.auth_router import _find_user_by_token
from app.database import engine

router = APIRouter(prefix="/api/v1/audit", tags=["audit"])


# ─── Auth dependency (super_admin only) ────────────────────────────────
def _require_super_admin(authorization: Optional[str] = Header(default=None)):
    """Same dependency style used by other admin endpoints in this codebase."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(None, 1)[1].strip()
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Super admin access required")
    return user


# ─── Helpers ───────────────────────────────────────────────────────────
def _row_to_event(row) -> Dict[str, Any]:
    """Convert a SQLAlchemy RowMapping to a JSON-friendly dict for the UI."""
    ts = row["ts"]
    ts_str = ts.isoformat() if isinstance(ts, (datetime, date)) else str(ts)
    return {
        "id":          row["id"],
        "ts":          ts_str,
        "user_id":     row["user_id"],
        "user_email":  row["user_email"] or "system",
        "client_id":   row["client_id"] or "SYSTEM",
        "action_type": row["action_type"],
        "details":     row["details"] or "",
        "ip_address":  row["ip_address"] or "",
        "outcome":     row["outcome"] or "success",
    }


def _build_where_clause(
    start: Optional[str],
    end: Optional[str],
    client_id: Optional[str],
    user_email: Optional[str],
    action_type: Optional[str],
    outcome: Optional[str],
) -> tuple[str, Dict[str, Any]]:
    """Assemble the WHERE clause + parameter dict for list and export.

    All parameters use :bind placeholders (no string interpolation) so SQL
    injection isn't a concern even though this endpoint is super-admin only.
    """
    conds: List[str] = []
    params: Dict[str, Any] = {}

    if start:
        # Compare as DATE in the session timezone — same semantics as
        # /audit/stats' `ts::date = CURRENT_DATE`. Previously we appended
        # "+00:00" which forced UTC interpretation of the user's LOCAL date
        # bound. For users west of UTC that silently excluded today's rows
        # (whose UTC ts had already rolled over to "tomorrow") even though
        # /audit/stats counted them — making the KPI card and the events
        # table disagree. Using ts::date here keeps both endpoints in sync.
        conds.append("ts::date >= :start_d")
        params["start_d"] = start                # PG parses 'YYYY-MM-DD' as DATE
    if end:
        conds.append("ts::date <= :end_d")
        params["end_d"] = end
    if client_id and client_id != "ALL":
        # "SYSTEM" is stored as NULL in the table, so treat it specially.
        if client_id.upper() == "SYSTEM":
            conds.append("client_id IS NULL")
        else:
            conds.append("client_id = :cid")
            params["cid"] = client_id
    if user_email and user_email != "ALL":
        conds.append("user_email = :uemail")
        params["uemail"] = user_email
    if action_type and action_type != "ALL":
        conds.append("action_type = :atype")
        params["atype"] = action_type
    if outcome and outcome != "ALL":
        conds.append("outcome = :outcome")
        params["outcome"] = outcome

    where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""
    return where_sql, params


# ─── Endpoints ─────────────────────────────────────────────────────────
@router.get("/stats")
def audit_stats(user=Depends(_require_super_admin)):
    """Headline KPI cards at the top of the /audit page.

    Returns:
      events_today     — count of rows with ts::date = today (UTC)
      warnings         — count of rows with outcome IN ('warning','failure') today
      security_alerts  — warning/failure rows whose action_type is login or auth-related
    """
    with engine.connect() as conn:
        today_row = conn.execute(text(
            """
            SELECT
              COUNT(*) FILTER (WHERE ts::date = CURRENT_DATE) AS events_today,
              COUNT(*) FILTER (WHERE ts::date = CURRENT_DATE
                               AND outcome IN ('warning','failure')) AS warnings,
              COUNT(*) FILTER (WHERE ts::date = CURRENT_DATE
                               AND outcome IN ('warning','failure')
                               AND action_type IN ('login','logout','token_refresh')) AS security_alerts
            FROM audit_log
            """
        )).mappings().first()

    return {
        "events_today":    int(today_row["events_today"] or 0),
        "warnings":        int(today_row["warnings"] or 0),
        "security_alerts": int(today_row["security_alerts"] or 0),
    }


@router.get("/filter-options")
def audit_filter_options(user=Depends(_require_super_admin)):
    """Distinct values used to populate the filter dropdowns. Pulled from
    the actual audit_log so dropdowns never contain options with zero rows."""
    with engine.connect() as conn:
        # Clients: join audit_log → client_config so we can show the name too.
        clients = conn.execute(text(
            """
            SELECT DISTINCT a.client_id, c.client_name
              FROM audit_log a
              LEFT JOIN client_config c ON c.client_id = a.client_id
             WHERE a.client_id IS NOT NULL
             ORDER BY a.client_id
            """
        )).mappings().all()

        # Check whether there are any NULL-client rows — those are
        # "SYSTEM" events (platform-level actions not tied to a tenant).
        # We only expose the SYSTEM option when such rows actually exist,
        # so the dropdown never shows a choice that would return zero rows.
        has_system = conn.execute(text(
            "SELECT 1 FROM audit_log WHERE client_id IS NULL LIMIT 1"
        )).scalar() is not None

        users = conn.execute(text(
            """
            SELECT DISTINCT user_email
              FROM audit_log
             WHERE user_email IS NOT NULL
             ORDER BY user_email
            """
        )).scalars().all()

        action_types = conn.execute(text(
            """
            SELECT DISTINCT action_type FROM audit_log ORDER BY action_type
            """
        )).scalars().all()

    clients_list = [
        {"client_id": r["client_id"], "client_name": r["client_name"] or r["client_id"]}
        for r in clients
    ]
    # Append SYSTEM at the end so the tenant list stays alphabetically
    # sorted above it. The frontend sends this value as-is; the list
    # endpoint maps `client_id=SYSTEM` → `WHERE client_id IS NULL`.
    if has_system:
        clients_list.append({"client_id": "SYSTEM", "client_name": "System Events"})

    return {
        "clients": clients_list,
        "users": list(users),
        "action_types": list(action_types),
        "outcomes": ["success", "warning", "failure"],
    }


@router.get("")
def audit_list(
    user=Depends(_require_super_admin),
    start: Optional[str]       = Query(default=None, description="YYYY-MM-DD inclusive"),
    end:   Optional[str]       = Query(default=None, description="YYYY-MM-DD inclusive"),
    client_id:   Optional[str] = Query(default=None),
    user_email:  Optional[str] = Query(default=None),
    action_type: Optional[str] = Query(default=None),
    outcome:     Optional[str] = Query(default=None),
    limit:       int           = Query(default=100, ge=1, le=500),
    offset:      int           = Query(default=0,   ge=0),
):
    """Paginated, filtered list of audit events (most recent first)."""
    where_sql, params = _build_where_clause(start, end, client_id, user_email, action_type, outcome)

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM audit_log {where_sql}"),
            params,
        ).scalar_one()

        rows = conn.execute(
            text(
                f"""
                SELECT id, ts, user_id, user_email, client_id, action_type, details, ip_address, outcome
                  FROM audit_log
                 {where_sql}
                 ORDER BY ts DESC, id DESC
                 LIMIT :limit OFFSET :offset
                """
            ),
            {**params, "limit": limit, "offset": offset},
        ).mappings().all()

    return {
        "events": [_row_to_event(r) for r in rows],
        "total": int(total),
        "limit": limit,
        "offset": offset,
    }


@router.get("/export")
def audit_export(
    user=Depends(_require_super_admin),
    start: Optional[str]       = Query(default=None),
    end:   Optional[str]       = Query(default=None),
    client_id:   Optional[str] = Query(default=None),
    user_email:  Optional[str] = Query(default=None),
    action_type: Optional[str] = Query(default=None),
    outcome:     Optional[str] = Query(default=None),
):
    """Stream a CSV of every matching row. Uses the SAME WHERE clause as the
    list endpoint so what the super admin sees is what gets exported.

    No pagination — audit exports are typically fed into an SIEM or
    spreadsheet, and the 365-day retention keeps the row count manageable.
    """
    where_sql, params = _build_where_clause(start, end, client_id, user_email, action_type, outcome)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT id, ts, user_id, user_email, client_id, action_type, details, ip_address, outcome
                  FROM audit_log
                 {where_sql}
                 ORDER BY ts DESC, id DESC
                """
            ),
            params,
        ).mappings().all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "timestamp", "user_id", "user_email", "client_id",
                     "action_type", "details", "ip_address", "outcome"])
    for r in rows:
        writer.writerow([
            r["id"],
            r["ts"].isoformat() if isinstance(r["ts"], (datetime, date)) else r["ts"],
            r["user_id"] or "",
            r["user_email"] or "",
            r["client_id"] or "SYSTEM",
            r["action_type"],
            r["details"] or "",
            r["ip_address"] or "",
            r["outcome"],
        ])
    buf.seek(0)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="audit_log_{stamp}.csv"'},
    )
