"""
audit_logger.py — one-stop helper for writing rows to audit_log.

Every router that wants to record an event should call:

    from app.audit_logger import log_audit_event
    log_audit_event(
        request,
        action_type="pipeline_run",
        details="JOB-4821 started · Full Pipeline",
        client_id="CLT-001",
        user_id=current_user.user_id,
        user_email=current_user.email,
        outcome="success",   # or "warning" / "failure"
    )

Design choices:
  * The function opens its own `engine.begin()` transaction so a caller
    already inside another transaction doesn't need to thread the
    connection through. Audit writes are fire-and-forget — we do NOT
    want an audit failure to roll back the main business transaction.
  * We swallow exceptions and log them. Missing audit rows are bad,
    but crashing the API to report them is worse.
  * ip_address is pulled from the FastAPI Request object when
    available; callers can also pass it explicitly.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import Request
from sqlalchemy import text

from app.database import engine

log = logging.getLogger(__name__)


def _extract_ip(request: Optional[Request]) -> Optional[str]:
    """Best-effort client IP. Honours X-Forwarded-For when behind a proxy,
    falls back to the direct socket address. Returns None if neither is
    available (e.g., when called from a background task without a request)."""
    if request is None:
        return None
    xff = request.headers.get("x-forwarded-for") if request.headers else None
    if xff:
        # XFF is a comma-separated list; the first entry is the original client.
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return None


def log_audit_event(
    request: Optional[Request] = None,
    *,
    action_type: str,
    details: str = "",
    client_id: Optional[str] = None,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
    ip_address: Optional[str] = None,
    outcome: str = "success",
) -> None:
    """Insert a row into audit_log. Never raises — audit failures are logged
    but swallowed so they can't break the caller."""
    try:
        ip = ip_address or _extract_ip(request)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO audit_log
                        (user_id, user_email, client_id, action_type, details, ip_address, outcome)
                    VALUES
                        (:uid, :email, :cid, :action, :details, :ip, :outcome)
                    """
                ),
                {
                    "uid": user_id,
                    "email": user_email,
                    "cid": client_id,
                    "action": action_type,
                    "details": details or "",
                    "ip": ip,
                    "outcome": outcome,
                },
            )
    except Exception as exc:  # pragma: no cover — best-effort logger
        # Don't crash the API if the audit table is missing or the DB is
        # temporarily unavailable. We still want the primary action to go
        # through. An ops dashboard should watch this log line.
        log.warning("audit_log write failed (action=%s): %s", action_type, exc)
