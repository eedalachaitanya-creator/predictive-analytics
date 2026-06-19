"""
auth_router.py — POST /api/v1/auth/login, /logout, /me, /refresh
=================================================================
Auth system that reads users from the PostgreSQL 'users' table.

HOW IT WORKS:
- Login: checks email + password against the 'users' table in the database
- Returns a token (simple hash for now — use JWT in production)
- /me: validates the token and returns user info
- /logout: removes token from active sessions
- /refresh: swaps old token for new one

Accepts an optional 'loginRole' field in the login request identifying
which portal the user picked on the login page:
  - loginRole='super_admin' → user must have role super_admin
  - loginRole='client'      → user must have role client_user
Platform only recognises those two roles; older 'admin' / 'viewer' rows
are collapsed into client_user by db/migration_retire_admin_role.sql and
db/migration_retire_viewer_role.sql.
"""

import hashlib
import secrets
import uuid
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Depends, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.security import hash_password, verify_password, is_hashed, validate_password

router = APIRouter(prefix="/api/v1", tags=["auth"])
log = logging.getLogger("auth")


# ── Reusable dependency: get current user from token ────────────────────────
def get_current_user(authorization: Optional[str] = Header(default=None)) -> dict:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


def _ensure_tokens_table():
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS active_tokens (
                    token       VARCHAR(64)  PRIMARY KEY,
                    user_id     VARCHAR(30)  NOT NULL,
                    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    expires_at  TIMESTAMPTZ  NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
                )
            """))
            conn.commit()
    except Exception:
        pass

_ensure_tokens_table()


def _make_token(user_id: str) -> str:
    token = secrets.token_urlsafe(32)
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO active_tokens (token, user_id, expires_at)
                    VALUES (:token, :uid, NOW() + INTERVAL '24 hours')
                    ON CONFLICT (token) DO UPDATE SET user_id = :uid, expires_at = NOW() + INTERVAL '24 hours'
                """),
                {"token": token, "uid": user_id},
            )
            conn.execute(text("DELETE FROM active_tokens WHERE expires_at < NOW()"))
            conn.commit()
    except Exception as e:
        log.error("Could not store token: %s", e)
    return token


# In-memory token cache — avoids 2 DB queries on every request.
# Cache TTL is 60 seconds; after that the token is re-validated from DB.
# This keeps the connection pool free for actual business queries.
_token_cache: dict[str, tuple[dict, float]] = {}
_TOKEN_CACHE_TTL = 60  # seconds


def _find_user_by_token(token: str) -> Optional[dict]:
    import time
    # Fast path: return cached user if still fresh
    if token in _token_cache:
        user, cached_at = _token_cache[token]
        if time.time() - cached_at < _TOKEN_CACHE_TTL:
            return user
        else:
            del _token_cache[token]

    # Slow path: hit DB and cache the result
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT user_id FROM active_tokens WHERE token = :token AND expires_at > NOW()"),
                {"token": token},
            ).fetchone()
            if not row:
                return None
            user = _get_user_by_id(row[0])
            if user:
                _token_cache[token] = (user, time.time())
            return user
    except Exception as e:
        log.error("Error looking up token: %s", e)
        return None


def _revoke_token(token: str):
    # Remove from cache immediately so the token is invalid on next request
    _token_cache.pop(token, None)
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM active_tokens WHERE token = :token"), {"token": token})
            conn.commit()
    except Exception:
        pass


def _get_user_by_id(user_id: str) -> Optional[dict]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT user_id, email, password_hash, name, role, client_access, is_active FROM users WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "email": row[1],
                "password": row[2],
                "name": row[3],
                "role": row[4],
                "clientAccess": list(row[5]) if row[5] else [],
                "is_active": row[6],
            }
    except Exception as e:
        log.error("Error fetching user by ID: %s", e)
        return None


def _get_user_by_email(email: str) -> Optional[dict]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT user_id, email, password_hash, name, role, client_access, is_active FROM users WHERE LOWER(email) = LOWER(:email)"),
                {"email": email},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "email": row[1],
                "password": row[2],
                "name": row[3],
                "role": row[4],
                "clientAccess": list(row[5]) if row[5] else [],
                "is_active": row[6],
            }
    except Exception as e:
        log.error("Error fetching user by email: %s", e)
        return None


class LoginRequest(BaseModel):
    email: str
    password: str
    loginRole: Optional[str] = None


@router.post("/auth/login")
def login(req: LoginRequest, request: Request):
    from app.audit_logger import log_audit_event

    user = _get_user_by_email(req.email)

    if not user or not verify_password(req.password, user["password"]):
        log_audit_event(
            request,
            action_type="login",
            details=f"Failed login attempt for '{req.email}'",
            user_email=req.email,
            outcome="failure",
        )
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not is_hashed(user["password"]):
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE users SET password_hash = :pw WHERE user_id = :uid"),
                    {"pw": hash_password(req.password), "uid": user["id"]},
                )
        except Exception:
            pass

    if not user.get("is_active", True):
        log_audit_event(
            request,
            action_type="login",
            details="Login blocked — account deactivated",
            user_id=user["id"],
            user_email=user["email"],
            outcome="failure",
        )
        raise HTTPException(status_code=403, detail="Your account has been deactivated. Contact your administrator.")

    portal = req.loginRole
    if portal == "admin":
        portal = "super_admin"

    if portal == "super_admin":
        if user["role"] != "super_admin":
            log_audit_event(
                request,
                action_type="login",
                details="Wrong portal — used Super Admin tab but role is not super_admin",
                user_id=user["id"],
                user_email=user["email"],
                outcome="warning",
            )
            raise HTTPException(
                status_code=403,
                detail="This is the Super Admin login. Please use the Client tab to sign in.",
            )
    elif portal == "client":
        if user["role"] == "super_admin":
            log_audit_event(
                request,
                action_type="login",
                details="Wrong portal — super_admin used Client tab",
                user_id=user["id"],
                user_email=user["email"],
                outcome="warning",
            )
            raise HTTPException(
                status_code=403,
                detail="This is the Client login. Please use the Super Admin tab to sign in.",
            )

    if user["role"] != "super_admin" and "*" not in (user["clientAccess"] or []):
        access = user["clientAccess"] or []
        if not access:
            raise HTTPException(
                status_code=403,
                detail="Your account is not linked to any client. Contact your administrator.",
            )
        try:
            with engine.connect() as conn:
                placeholders = ", ".join([f":c{i}" for i in range(len(access))])
                params = {f"c{i}": cid for i, cid in enumerate(access)}
                active_count = conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM client_config "
                        f"WHERE is_active = TRUE AND client_id IN ({placeholders})"
                    ),
                    params,
                ).scalar() or 0
        except Exception:
            active_count = len(access)
        if active_count == 0:
            log_audit_event(
                request,
                action_type="login",
                details="Login blocked — all linked clients are deactivated",
                user_id=user["id"],
                user_email=user["email"],
                outcome="failure",
            )
            raise HTTPException(
                status_code=403,
                detail="Your client account has been deactivated. Contact your administrator.",
            )

    token = _make_token(user["id"])
    refresh_token = _make_token(user["id"])

    try:
        with engine.connect() as conn:
            conn.execute(
                text("UPDATE users SET last_login = NOW() WHERE user_id = :uid"),
                {"uid": user["id"]},
            )
            conn.commit()
    except Exception:
        pass

    audit_client = None
    if user["clientAccess"] and user["clientAccess"] != ["*"] and user["role"] != "super_admin":
        audit_client = user["clientAccess"][0]
    log_audit_event(
        request,
        action_type="login",
        details=f"Successful login via {req.loginRole or 'client'} portal",
        user_id=user["id"],
        user_email=user["email"],
        client_id=audit_client,
        outcome="success",
    )

    user_response = {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
        "clientAccess": user["clientAccess"],
        "token": token,
        "refreshToken": refresh_token,
    }

    return {
        "user": user_response,
        "token": token,
        "refreshToken": refresh_token,
    }


@router.get("/auth/me")
def get_me(authorization: Optional[str] = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="No authorization header")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
        "clientAccess": user["clientAccess"],
        "token": token,
    }


def _clients_to_clear(user: dict) -> list:
    """Concrete client_ids whose pending upload batch should be cleared when this
    user logs out. Super-admin ('*') is excluded — we never auto-discard every
    client's in-progress upload just because one admin signed out."""
    return [c for c in (user.get("clientAccess") or []) if c != "*"]


@router.post("/auth/logout")
def logout(request: Request, authorization: Optional[str] = Header(default=None)):
    from app.audit_logger import log_audit_event

    user_for_audit = None
    if authorization:
        token = authorization.replace("Bearer ", "")
        user_for_audit = _find_user_by_token(token)
        _revoke_token(token)

    # Clear any UNSAVED (pending) upload batch so it doesn't follow the user into
    # the next session (same or different browser). Scoped to the user's own
    # client(s); best-effort — a cleanup failure must never block sign-out.
    if user_for_audit:
        try:
            from app.upload_router import _discard_pending_batch_for_client
            for client_id in _clients_to_clear(user_for_audit):
                _discard_pending_batch_for_client(client_id)
        except Exception as e:
            log.warning("logout: pending-batch cleanup failed: %s", e)

    if user_for_audit:
        log_audit_event(
            request,
            action_type="logout",
            details="User signed out",
            user_id=user_for_audit["id"],
            user_email=user_for_audit["email"],
            outcome="success",
        )
    return {}


@router.post("/auth/refresh")
def refresh_token(body: dict):
    refresh = body.get("refreshToken", "")
    user = _find_user_by_token(refresh)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    _revoke_token(refresh)
    new_token = _make_token(user["id"])
    new_refresh = _make_token(user["id"])

    user_response = {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
        "clientAccess": user["clientAccess"],
        "token": new_token,
        "refreshToken": new_refresh,
    }

    return {
        "user": user_response,
        "token": new_token,
        "refreshToken": new_refresh,
    }


class ForgotPasswordRequest(BaseModel):
    email: str


def _generate_temp_password(length: int = 12) -> str:
    import secrets
    import string
    alphabet = "".join(
        c for c in (string.ascii_letters + string.digits) if c not in "0O1lI"
    )
    return "".join(secrets.choice(alphabet) for _ in range(length))


@router.post("/auth/forgot-password")
def forgot_password(req: ForgotPasswordRequest):
    email_trimmed = (req.email or "").strip()
    if not email_trimmed:
        raise HTTPException(status_code=400, detail="Email is required")

    user = _get_user_by_email(email_trimmed)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=f"No account found for {email_trimmed}",
        )

    if not user.get("is_active", True):
        raise HTTPException(
            status_code=403,
            detail="This account has been deactivated. Contact your administrator.",
        )

    new_password = _generate_temp_password()

    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET password_hash = :pw WHERE user_id = :uid"),
                {"pw": hash_password(new_password), "uid": user["id"]},
            )
            conn.execute(
                text("DELETE FROM active_tokens WHERE user_id = :uid"),
                {"uid": user["id"]},
            )
    except Exception as e:
        log.error("Failed to reset password for %s: %s", email_trimmed, e)
        raise HTTPException(
            status_code=500,
            detail="Could not reset password. Please try again.",
        )

    log.info("Password reset for user %s (%s)", user["id"], email_trimmed)

    # ── Send password reset email ─────────────────────────────────────
    try:
        from app.email_service import send_email
        send_email(
            to=user["email"],
            subject="Your Temporary Password — Loyaltix",
            html_body=f"""
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#ffffff;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
  <div style="background:#1a56db;padding:24px 32px">
    <div style="font-size:20px;font-weight:700;color:#ffffff">&#128202; Loyaltix</div>
    <div style="color:#c7d9ff;margin-top:4px;font-size:13px">Churn Prediction &amp; Retention Platform</div>
  </div>
  <div style="padding:32px">
    <h2 style="color:#1a1a1a;margin:0 0 16px;font-size:18px">Password Reset Request</h2>
    <p style="color:#444;line-height:1.6;margin:0 0 12px">Hi {user["name"]},</p>
    <p style="color:#444;line-height:1.6;margin:0 0 20px">We received a request to reset your password. Your temporary password is below:</p>
    <div style="background:#f0f4ff;border:1px solid #c7d9ff;border-radius:8px;padding:16px 24px;margin:0 0 24px;text-align:center">
      <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Temporary Password</div>
      <div style="font-size:24px;font-weight:bold;letter-spacing:3px;color:#1a56db">{new_password}</div>
    </div>
    <p style="color:#444;line-height:1.6;margin:0 0 12px">All existing sessions have been logged out. Please sign in with this temporary password and change it immediately.</p>
    <p style="color:#c0392b;font-size:13px;line-height:1.6;margin:0">&#9888;&#65039; If you did not request this password reset, please contact your administrator immediately.</p>
  </div>
  <div style="background:#f9f9f9;padding:16px 32px;border-top:1px solid #e0e0e0;text-align:center">
    <p style="margin:0;font-size:12px;color:#888">&copy; 2026 Loyaltix &middot; This is an automated message, please do not reply.</p>
  </div>
</div>
""",
        )
    except Exception as e:
        log.warning("Forgot-password email failed (non-fatal): %s", e)

    return {
        "email": user["email"],
        "temp_password": new_password,
        "message": (
            "Your temporary password is shown below. Copy it, sign in with "
            "it, and any open sessions have been logged out."
        ),
    }


# ── Change Password ───────────────────────────────────────────────────────────
class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

@router.post("/auth/change-password")
def change_password(
    req: ChangePasswordRequest,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if not verify_password(req.current_password, user["password"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    pw_error = validate_password(req.new_password)
    if pw_error:
        raise HTTPException(status_code=400, detail=pw_error)

    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET password_hash = :pw WHERE user_id = :uid"),
                {"pw": hash_password(req.new_password), "uid": user["id"]},
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not update password: {e}")

    # ── Send password-changed confirmation email ──────────────────────────
    try:
        from app.email_service import send_email
        from datetime import datetime, timezone
        changed_at = datetime.now(timezone.utc).strftime("%d %B %Y at %H:%M UTC")
        send_email(
            to=user["email"],
            subject="Your Password Has Been Changed — Loyaltix",
            html_body=f"""
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#ffffff;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
  <div style="background:#1a56db;padding:24px 32px">
    <div style="font-size:20px;font-weight:700;color:#ffffff">&#128202; Loyaltix</div>
    <div style="color:#c7d9ff;margin-top:4px;font-size:13px">Churn Prediction &amp; Retention Platform</div>
  </div>
  <div style="padding:32px">
    <h2 style="color:#1a1a1a;margin:0 0 16px;font-size:18px">Password Changed Successfully</h2>
    <p style="color:#444;line-height:1.6;margin:0 0 12px">Hi {user["name"]},</p>
    <p style="color:#444;line-height:1.6;margin:0 0 20px">Your account password has been changed successfully. You can now log in using your new password.</p>
    <div style="background:#f0fff4;border:1px solid #9ae6b4;border-radius:8px;padding:16px 24px;margin:0 0 24px">
      <p style="margin:0;font-size:14px;color:#276749">&#9989; Your password was updated on {changed_at}</p>
    </div>
    <p style="color:#c0392b;font-size:13px;line-height:1.6;margin:0">&#9888;&#65039; If you did not make this change, please contact your administrator immediately as your account may be compromised.</p>
  </div>
  <div style="background:#f9f9f9;padding:16px 32px;border-top:1px solid #e0e0e0;text-align:center">
    <p style="margin:0;font-size:12px;color:#888">&copy; 2026 Loyaltix &middot; This is an automated message, please do not reply.</p>
  </div>
</div>
""",
        )
    except Exception as e:
        log.warning("Password-changed confirmation email failed (non-fatal): %s", e)

    return {"message": "Password changed successfully"}