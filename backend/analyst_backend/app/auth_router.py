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
from app.security import hash_password, verify_password, is_hashed

router = APIRouter(prefix="/api/v1", tags=["auth"])
log = logging.getLogger("auth")


# ── Reusable dependency: get current user from token ────────────────────────
# Other routers import this to know WHO is making the request.
# Usage in another router:
#   from app.auth_router import get_current_user
#   @router.post("/something")
#   def do_something(user: dict = Depends(get_current_user)):
#       print(user["clientAccess"])  # → ["CLT-001"]

def get_current_user(authorization: Optional[str] = Header(default=None)) -> dict:
    """
    FastAPI dependency that extracts the current user from the Bearer token.
    If the token is missing or invalid, raises 401.
    Returns the full user dict (id, email, name, role, clientAccess).
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


def _ensure_tokens_table():
    """Create active_tokens table if it doesn't exist."""
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
        pass  # table may already exist

# Create table on module load
_ensure_tokens_table()


def _make_token(user_id: str) -> str:
    """Generate an opaque session token and store it in the database.

    The token is 256 bits straight from the OS CSPRNG via
    ``secrets.token_urlsafe(32)`` (URL-safe, 43 chars) — unguessable by
    construction. This replaces the prior ``sha256(user_id + uuid4 + timestamp)``
    which, while effectively random, derived its entropy indirectly. The token is
    an opaque server-side session key (NOT a JWT): it carries no user data and is
    validated / revoked via the ``active_tokens`` table. Old sha256 tokens already
    in active_tokens stay valid until they expire — the format change is forward-
    only and never invalidates a live session.
    """
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
            # Clean up expired tokens while we're here
            conn.execute(text("DELETE FROM active_tokens WHERE expires_at < NOW()"))
            conn.commit()
    except Exception as e:
        log.error("Could not store token: %s", e)

    return token


def _find_user_by_token(token: str) -> Optional[dict]:
    """Look up a user by their active token — reads from database."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT user_id FROM active_tokens WHERE token = :token AND expires_at > NOW()"),
                {"token": token},
            ).fetchone()
            if not row:
                return None
            return _get_user_by_id(row[0])
    except Exception as e:
        log.error("Error looking up token: %s", e)
        return None


def _revoke_token(token: str):
    """Remove a token from the database."""
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM active_tokens WHERE token = :token"), {"token": token})
            conn.commit()
    except Exception:
        pass


def _get_user_by_id(user_id: str) -> Optional[dict]:
    """Fetch a user from the database by user_id."""
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
    """Fetch a user from the database by email."""
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


# ── Request / Response models ────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str
    # Which portal the user picked: 'super_admin' or 'client'.
    loginRole: Optional[str] = None


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/auth/login")
def login(req: LoginRequest, request: Request):
    """
    Authenticate user with email and password.
    Reads from the 'users' table in PostgreSQL.

    If loginRole is provided:
    - 'super_admin' → user must have role super_admin
    - 'client'      → user must have role client_user
    This ensures super_admins use the Super Admin tab and clients use the
    Client tab.
    """
    from app.audit_logger import log_audit_event  # local import to avoid circular

    user = _get_user_by_email(req.email)

    if not user or not verify_password(req.password, user["password"]):
        # Audit failed-login: no user session yet, so user_id is null
        log_audit_event(
            request,
            action_type="login",
            details=f"Failed login attempt for '{req.email}'",
            user_email=req.email,
            outcome="failure",
        )
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Lazy password migration: this account authenticated, but if it still has a
    # legacy PLAINTEXT password, transparently upgrade it to a bcrypt hash now.
    # Best-effort — a migration write must never block a valid login.
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
        raise HTTPException(status_code=403, detail="Your account has been deactivated. Contact support.")

    # ── Role validation based on which tab they used ──────────────
    # Only two roles exist: super_admin and client_user. We accept the
    # legacy 'admin' wire value as an alias for 'super_admin' so old
    # frontends don't break mid-rollout.
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

    # ── Client users cannot log in once every client they have access to
    # ── has been soft-deleted (client_config.is_active = FALSE). Super
    # ── admins are exempt — they need to stay able to reactivate clients.
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
            # If the column is missing (migration not yet applied) fall open
            # rather than lock every user out. The client_router's module-load
            # _ensure_soft_delete_columns should have added it already.
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
                detail="Your client account has been deactivated. Contact support.",
            )

    # ── Generate tokens (stored in database, survive restarts) ──
    token = _make_token(user["id"])
    refresh_token = _make_token(user["id"])

    # ── Update last_login timestamp ───────────────────────────────
    try:
        with engine.connect() as conn:
            conn.execute(
                text("UPDATE users SET last_login = NOW() WHERE user_id = :uid"),
                {"uid": user["id"]},
            )
            conn.commit()
    except Exception:
        pass  # non-critical

    # Audit the successful login. Use the user's first client_access entry
    # as the scoping client_id (super_admins with "*" land under NULL/SYSTEM).
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
    """Validate token and return current user info."""
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


@router.post("/auth/logout")
def logout(request: Request, authorization: Optional[str] = Header(default=None)):
    """Remove token from active sessions (database)."""
    from app.audit_logger import log_audit_event  # local import to avoid circular

    user_for_audit = None
    if authorization:
        token = authorization.replace("Bearer ", "")
        # Snap the user BEFORE revoking, otherwise _find_user_by_token returns None.
        user_for_audit = _find_user_by_token(token)
        _revoke_token(token)

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
    """Swap refresh token for a new access token."""
    refresh = body.get("refreshToken", "")
    user = _find_user_by_token(refresh)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # Remove old refresh token
    _revoke_token(refresh)

    # Issue new tokens (stored in database automatically by _make_token)
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


# ═══════════════════════════════════════════════════════════════════════════
# Forgot password
# ---------------------------------------------------------------------------
# Resets the user's password to a freshly generated 12-character string and
# returns it to the caller so the UI can show it in a modal. No email is sent.
#
# Security notes:
#   - Passwords are stored as plaintext in `users.password_hash` today (see
#     migration_users_table.sql line 13). That's a project-wide convention,
#     not a forgot-password quirk. When the project switches to bcrypt, this
#     endpoint and /auth/login both need to move together.
#   - We deliberately return a 404 for unknown emails instead of a generic
#     success response. Email enumeration is acceptable for the current
#     stage of the product; UX wins over hiding which emails exist. Change
#     this to a generic message before going to production.
#   - All existing tokens for the user are revoked, forcing any open sessions
#     to re-log in with the new password.
# ═══════════════════════════════════════════════════════════════════════════

class ForgotPasswordRequest(BaseModel):
    email: str


def _generate_temp_password(length: int = 12) -> str:
    """Return a readable random password (no ambiguous chars like 0/O, 1/l/I)."""
    import secrets
    import string
    alphabet = "".join(
        c for c in (string.ascii_letters + string.digits) if c not in "0O1lI"
    )
    return "".join(secrets.choice(alphabet) for _ in range(length))


@router.post("/auth/forgot-password")
def forgot_password(req: ForgotPasswordRequest):
    """
    Generate a temporary password, save it, and return it in the response.

    Flow:
      1. Look up the user by email (case-insensitive).
      2. 404 if unknown, 403 if deactivated.
      3. Otherwise: generate new password, UPDATE users.password_hash,
         DELETE all active_tokens for this user, return the new password.
    """
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
            detail="This account has been deactivated. Contact support.",
        )

    new_password = _generate_temp_password()

    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE users SET password_hash = :pw WHERE user_id = :uid"),
                {"pw": hash_password(new_password), "uid": user["id"]},  # store hash; email the cleartext
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
            subject="Your Temporary Password — Predictive Analytics",
            html_body=f"""
            <h2>Password Reset</h2>
            <p>Hi {user["name"]},</p>
            <p>A temporary password has been generated for your account:</p>
            <p style="font-size:20px;font-weight:bold;letter-spacing:2px;
                      background:#f0f4ff;padding:12px 20px;border-radius:8px;
                      display:inline-block">{new_password}</p>
            <p>All existing sessions have been logged out. Sign in with this password and change it immediately.</p>
            <p>If you did not request this, contact IT Support immediately.</p>
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

    import re
    pw = req.new_password
    pw_ok = (
        len(pw) >= 8 and
        re.search(r'[A-Z]', pw) and
        re.search(r'[a-z]', pw) and
        re.search(r'\d', pw) and
        re.search(r'[^A-Za-z0-9]', pw)
    )
    if not pw_ok:
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters with uppercase, lowercase, number, and special character."
        )

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
        send_email(
            to=user["email"],
            subject="Your Password Has Been Changed — Predictive Analytics",
            html_body=f"""
            <h2>Password Changed Successfully</h2>
            <p>Hi {user["name"]},</p>
            <p>Your password was just changed successfully. You can now log in with your new password.</p>
            <p>If you did not make this change, please contact IT Support immediately.</p>
            """,
        )
    except Exception as e:
        log.warning("Password-changed confirmation email failed (non-fatal): %s", e)

    return {"message": "Password changed successfully"}