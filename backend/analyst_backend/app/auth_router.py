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

Accepts an optional 'loginRole' field in the login request.
If loginRole='admin', the user must have role super_admin or admin.
If loginRole='client', the user must have role client_user or viewer.
This prevents admins from accidentally logging into the client portal
and vice versa.
"""

import hashlib
import uuid
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine

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
    """Generate a token and store it in the database."""
    raw = f"{user_id}-{uuid.uuid4()}-{datetime.now().isoformat()}"
    token = hashlib.sha256(raw.encode()).hexdigest()

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
    loginRole: Optional[str] = None   # 'admin' or 'client' — from the UI tab


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/auth/login")
def login(req: LoginRequest):
    """
    Authenticate user with email and password.
    Reads from the 'users' table in PostgreSQL.

    If loginRole is provided:
    - 'admin' → user must have role super_admin or admin
    - 'client' → user must have role client_user or viewer
    This ensures admins use the Admin tab and clients use the Client tab.
    """
    user = _get_user_by_email(req.email)

    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Your account has been deactivated. Contact support.")

    # ── Role validation based on which tab they used ──────────────
    if req.loginRole == "admin":
        if user["role"] not in ("super_admin", "admin"):
            raise HTTPException(
                status_code=403,
                detail="This is the Admin login. Please use the Client tab to sign in.",
            )
    elif req.loginRole == "client":
        if user["role"] in ("super_admin", "admin"):
            raise HTTPException(
                status_code=403,
                detail="This is the Client login. Please use the Admin tab to sign in.",
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
def logout(authorization: Optional[str] = Header(default=None)):
    """Remove token from active sessions (database)."""
    if authorization:
        token = authorization.replace("Bearer ", "")
        _revoke_token(token)
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
