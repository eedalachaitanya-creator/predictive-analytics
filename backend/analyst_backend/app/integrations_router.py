"""Per-tenant external-integration config endpoints (Jira today).

Security model:
  * The API token is WRITE-ONLY over HTTP — accepted on PUT, encrypted at rest
    (Fernet), and NEVER returned. GET reports ``token_set: bool`` only.
  * Every endpoint is scoped to the caller's OWN tenant via ``clientAccess`` — a
    client-user can't read or modify another tenant's integration; a super-admin
    (or a user with ``*`` access) may manage any.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from cryptography.fernet import InvalidToken

from app.auth_router import get_current_user, _find_user_by_token
from app.crypto import encrypt_secret, encryption_available, EncryptionUnavailable
from app.database import engine

log = logging.getLogger("integrations")

router = APIRouter(prefix="/api/v1/integrations", tags=["integrations"],
                   dependencies=[Depends(get_current_user)])

_STRATEGIES = {"auto", "field", "label"}


# ── auth helpers ─────────────────────────────────────────────────────────────
def _user_or_401(authorization: Optional[str]) -> dict:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    user = _find_user_by_token(authorization.replace("Bearer ", ""))
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


def _require_client_access(user: dict, client_id: str) -> None:
    """Tenant self-serve: a user may only manage clients they have access to."""
    access = user.get("clientAccess") or []
    if user.get("role") == "super_admin" or "*" in access or client_id in access:
        return
    raise HTTPException(status_code=403,
                        detail="You do not have access to this client's integrations")


# ── payload ──────────────────────────────────────────────────────────────────
class JiraConfigIn(BaseModel):
    base_url: Optional[str] = None
    email: Optional[str] = None
    api_token: Optional[str] = None          # write-only; omit/blank to keep existing
    project_key: Optional[str] = None
    customer_strategy: Optional[str] = None  # auto | field | label
    customer_field_name: Optional[str] = None
    enabled: Optional[bool] = None


def _status_row(conn, client_id: str) -> dict:
    """Public status — everything EXCEPT the token (only whether one is set)."""
    row = conn.execute(text(
        "SELECT base_url, email, api_token_enc, project_key, customer_strategy, "
        "customer_field_name, enabled, last_sync_at, last_sync_status, last_sync_detail "
        "FROM tenant_integrations WHERE client_id = :c AND provider = 'jira'"),
        {"c": client_id}).mappings().first()
    if not row:
        return {"configured": False, "enabled": False, "token_set": False}
    return {
        "configured": True,
        "enabled": row["enabled"],
        "base_url": row["base_url"],
        "email": row["email"],
        "project_key": row["project_key"],
        "customer_strategy": row["customer_strategy"],
        "customer_field_name": row["customer_field_name"],
        "token_set": bool(row["api_token_enc"]),         # NEVER the token itself
        "last_sync_at": row["last_sync_at"].isoformat() if row["last_sync_at"] else None,
        "last_sync_status": row["last_sync_status"],
        "last_sync_detail": row["last_sync_detail"],
    }


# ── endpoints ────────────────────────────────────────────────────────────────
@router.get("/jira")
def get_jira(clientId: str = Query(...),
             authorization: Optional[str] = Header(default=None)):
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    with engine.connect() as conn:
        return _status_row(conn, clientId)


@router.put("/jira")
def put_jira(cfg: JiraConfigIn, clientId: str = Query(...),
             authorization: Optional[str] = Header(default=None)):
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)

    if cfg.customer_strategy and cfg.customer_strategy not in _STRATEGIES:
        raise HTTPException(status_code=400,
                            detail=f"customer_strategy must be one of {sorted(_STRATEGIES)}")
    if cfg.base_url:
        # SSRF guard: https-only, host must resolve to a public address (no
        # loopback/private/link-local/metadata). Validated again at request time.
        from ml.connectors.jira import assert_public_https_url
        try:
            assert_public_https_url(cfg.base_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    if cfg.api_token and not encryption_available():
        raise HTTPException(status_code=503,
                            detail="Server cannot store secrets: INTEGRATION_ENC_KEY is not set — "
                                   "set it and restart the API.")

    # Only write columns that were supplied; the token column is touched ONLY when
    # a new token is provided (so other edits don't wipe a saved token).
    set_cols = {k: v for k, v in {
        "base_url": cfg.base_url, "email": cfg.email, "project_key": cfg.project_key,
        "customer_strategy": cfg.customer_strategy,
        "customer_field_name": cfg.customer_field_name, "enabled": cfg.enabled,
    }.items() if v is not None}
    if cfg.api_token:
        set_cols["api_token_enc"] = encrypt_secret(cfg.api_token)

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM tenant_integrations WHERE client_id=:c AND provider='jira'"),
            {"c": clientId}).first()
        if exists:
            if set_cols:
                assigns = ", ".join(f"{k} = :{k}" for k in set_cols)
                conn.execute(text(
                    f"UPDATE tenant_integrations SET {assigns}, updated_at=now() "
                    "WHERE client_id=:c AND provider='jira'"),
                    {**set_cols, "c": clientId})
        else:
            cols = ", ".join(["client_id", "provider"] + list(set_cols))
            vals = ", ".join([":c", "'jira'"] + [f":{k}" for k in set_cols])
            conn.execute(text(f"INSERT INTO tenant_integrations ({cols}) VALUES ({vals})"),
                         {**set_cols, "c": clientId})
        return _status_row(conn, clientId)


def _build_connector(client_id: str):
    """Build the tenant's connector, turning crypto/config failures into CLEAR
    HTTPExceptions (which keep CORS headers) instead of raw 500s that surface in
    the browser as an opaque '0 Unknown Error'."""
    from ml.connectors.jira import JiraConnector
    try:
        connector = JiraConnector.from_client(engine, client_id, require_enabled=False)
    except EncryptionUnavailable:
        raise HTTPException(status_code=503,
                            detail="Server is missing INTEGRATION_ENC_KEY — set it and restart the API "
                                   "before testing or syncing.")
    except InvalidToken:
        raise HTTPException(status_code=500,
                            detail="Stored token could not be decrypted — the encryption key may have changed.")
    except Exception as exc:  # noqa: BLE001
        log.warning("build connector failed for %s: %s", client_id, exc)
        raise HTTPException(status_code=500, detail="Could not build the Jira connector — see server logs.")
    if connector is None:
        raise HTTPException(status_code=400,
                            detail="No Jira credentials saved yet — save base URL, email and token first.")
    return connector


@router.post("/jira/test")
def test_jira(clientId: str = Query(...),
              authorization: Optional[str] = Header(default=None)):
    """Validate the SAVED credentials with a cheap Jira /myself call. Works even
    before the integration is enabled, so a tenant can verify before switching on."""
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    connector = _build_connector(clientId)
    try:
        return {"ok": True, "account": connector.verify()}
    except Exception as exc:  # noqa: BLE001 — friendly failure; never echo upstream body
        log.warning("Jira test failed for %s: %s", clientId, exc)
        return {"ok": False,
                "error": "Could not connect to Jira — check the base URL, email, and API token."}


@router.post("/jira/sync")
def sync_jira(clientId: str = Query(...),
              authorization: Optional[str] = Header(default=None)):
    """Manual 'Sync now' — pull this tenant's Jira tickets into support_tickets.
    Independent of the ``enabled`` flag (which governs pipeline auto-sync), so a
    tenant can pull on demand as soon as credentials are saved."""
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    from ml.connectors.ingest import run_ingest

    connector = _build_connector(clientId)
    try:
        totals = run_ingest(engine, clientId, connectors=[connector])
        status, detail = "ok", f"{totals.get('tickets', 0)} tickets"
    except Exception as exc:  # noqa: BLE001 — store a SAFE detail (type, not body)
        log.warning("Jira sync failed for %s: %s", clientId, exc)
        totals, status, detail = {"tickets": 0, "reviews": 0}, "error", type(exc).__name__

    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE tenant_integrations SET last_sync_at=now(), last_sync_status=:s, "
            "last_sync_detail=:d WHERE client_id=:c AND provider='jira'"),
            {"s": status, "d": detail, "c": clientId})

    if status == "error":
        raise HTTPException(status_code=502,
                            detail="Sync failed — check the integration settings or server logs.")
    return {"status": "ok", **totals}
