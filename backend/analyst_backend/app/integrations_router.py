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
from ml.connectors.registry import CONNECTOR_REGISTRY, PROVIDER_META

log = logging.getLogger("integrations")

router = APIRouter(prefix="/api/v1/integrations", tags=["integrations"],
                   dependencies=[Depends(get_current_user)])


def _require_provider(provider: str) -> None:
    """404 unless ``provider`` is a registered connector (no arbitrary dispatch)."""
    if provider not in CONNECTOR_REGISTRY:
        raise HTTPException(status_code=404,
                            detail=f"Unknown integration provider: {provider}")


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
class IntegrationConfigIn(BaseModel):
    base_url: Optional[str] = None
    email: Optional[str] = None
    api_token: Optional[str] = None          # write-only; omit/blank to keep existing
    project_key: Optional[str] = None
    customer_strategy: Optional[str] = None  # validated per-provider (PROVIDER_META)
    customer_field_name: Optional[str] = None
    enabled: Optional[bool] = None


def _status_row(conn, client_id: str, provider: str) -> dict:
    """Public status for one provider — everything EXCEPT the token (only whether
    one is set)."""
    row = conn.execute(text(
        "SELECT base_url, email, api_token_enc, project_key, customer_strategy, "
        "customer_field_name, enabled, last_sync_at, last_sync_status, last_sync_detail "
        "FROM tenant_integrations WHERE client_id = :c AND provider = :p"),
        {"c": client_id, "p": provider}).mappings().first()
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
@router.get("")
def list_integrations(clientId: str = Query(...),
                      authorization: Optional[str] = Header(default=None)):
    """Every provider's config status for this tenant, plus UI metadata — drives
    the Settings page (one card per provider)."""
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    out = {}
    with engine.connect() as conn:
        for provider, meta in PROVIDER_META.items():
            out[provider] = {**_status_row(conn, clientId, provider), "meta": meta}
    return out


@router.get("/{provider}")
def get_integration(provider: str, clientId: str = Query(...),
                    authorization: Optional[str] = Header(default=None)):
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    _require_provider(provider)
    with engine.connect() as conn:
        return _status_row(conn, clientId, provider)


@router.put("/{provider}")
def put_integration(provider: str, cfg: IntegrationConfigIn,
                    clientId: str = Query(...),
                    authorization: Optional[str] = Header(default=None)):
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    _require_provider(provider)

    valid_strategies = PROVIDER_META[provider]["strategies"]
    if cfg.customer_strategy and cfg.customer_strategy not in valid_strategies:
        raise HTTPException(status_code=400,
                            detail=f"customer_strategy for {provider} must be one of "
                                   f"{sorted(valid_strategies)}")
    # SSRF guard applies only to providers with a tenant-supplied base URL (Jira).
    if provider == "jira" and cfg.base_url:
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
        exists = conn.execute(text(
            "SELECT 1 FROM tenant_integrations WHERE client_id=:c AND provider=:p"),
            {"c": clientId, "p": provider}).first()
        if exists:
            if set_cols:
                assigns = ", ".join(f"{k} = :{k}" for k in set_cols)
                conn.execute(text(
                    f"UPDATE tenant_integrations SET {assigns}, updated_at=now() "
                    "WHERE client_id=:c AND provider=:p"),
                    {**set_cols, "c": clientId, "p": provider})
        else:
            cols = ", ".join(["client_id", "provider"] + list(set_cols))
            vals = ", ".join([":c", ":p"] + [f":{k}" for k in set_cols])
            conn.execute(text(f"INSERT INTO tenant_integrations ({cols}) VALUES ({vals})"),
                         {**set_cols, "c": clientId, "p": provider})
        return _status_row(conn, clientId, provider)


def _build_connector(provider: str, client_id: str):
    """Build the tenant's connector for ``provider``, turning crypto/config
    failures into CLEAR HTTPExceptions (which keep CORS headers) instead of raw
    500s that surface in the browser as an opaque '0 Unknown Error'."""
    cls = CONNECTOR_REGISTRY[provider]
    label = PROVIDER_META[provider]["label"]
    try:
        connector = cls.from_client(engine, client_id, require_enabled=False)
    except EncryptionUnavailable:
        raise HTTPException(status_code=503,
                            detail="Server is missing INTEGRATION_ENC_KEY — set it and restart the API "
                                   "before testing or syncing.")
    except InvalidToken:
        raise HTTPException(status_code=500,
                            detail="Stored token could not be decrypted — the encryption key may have changed.")
    except Exception as exc:  # noqa: BLE001
        log.warning("build connector failed for %s/%s: %s", client_id, provider, exc)
        raise HTTPException(status_code=500,
                            detail=f"Could not build the {label} connector — see server logs.")
    if connector is None:
        raise HTTPException(status_code=400,
                            detail=f"No {label} credentials saved yet — save the connection details first.")
    return connector


@router.post("/{provider}/test")
def test_integration(provider: str, clientId: str = Query(...),
                     authorization: Optional[str] = Header(default=None)):
    """Validate the SAVED credentials with a cheap call. Works even before the
    integration is enabled, so a tenant can verify before switching on."""
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    _require_provider(provider)
    label = PROVIDER_META[provider]["label"]
    connector = _build_connector(provider, clientId)
    try:
        return {"ok": True, "account": connector.verify()}
    except Exception as exc:  # noqa: BLE001 — friendly failure; never echo upstream body
        log.warning("%s test failed for %s: %s", provider, clientId, exc)
        return {"ok": False,
                "error": f"Could not connect to {label} — check the connection details."}


@router.post("/{provider}/sync")
def sync_integration(provider: str, clientId: str = Query(...),
                     authorization: Optional[str] = Header(default=None)):
    """Manual 'Sync now' — pull this tenant's records into support_tickets /
    customer_reviews. Independent of the ``enabled`` flag (which governs pipeline
    auto-sync), so a tenant can pull on demand as soon as credentials are saved."""
    user = _user_or_401(authorization)
    _require_client_access(user, clientId)
    _require_provider(provider)
    from ml.connectors.ingest import run_ingest

    connector = _build_connector(provider, clientId)
    try:
        totals = run_ingest(engine, clientId, connectors=[connector])
        status = "ok"
        detail = f"{totals.get('tickets', 0)} tickets, {totals.get('reviews', 0)} reviews"
    except Exception as exc:  # noqa: BLE001 — store a SAFE detail (type, not body)
        log.warning("%s sync failed for %s: %s", provider, clientId, exc)
        totals, status, detail = {"tickets": 0, "reviews": 0}, "error", type(exc).__name__

    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE tenant_integrations SET last_sync_at=now(), last_sync_status=:s, "
            "last_sync_detail=:d WHERE client_id=:c AND provider=:p"),
            {"s": status, "d": detail, "c": clientId, "p": provider})

    if status == "error":
        raise HTTPException(status_code=502,
                            detail="Sync failed — check the integration settings or server logs.")
    return {"status": "ok", **totals}
