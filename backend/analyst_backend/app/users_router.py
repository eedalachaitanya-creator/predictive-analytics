"""
users_router.py — User Management Endpoints (Admin Only)
=========================================================
GET    /api/v1/users           — List all users
POST   /api/v1/users           — Create a new user
PUT    /api/v1/users/{user_id} — Update a user
DELETE /api/v1/users/{user_id} — Delete a user
"""

import uuid
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token
from app.audit_logger import log_audit_event

router = APIRouter(prefix="/api/v1", tags=["users"])
log = logging.getLogger("users")


# ── Request models ───────────────────────────────────────────────────────────

class CreateUserRequest(BaseModel):
    name: str
    email: str
    password: str
    role: str = "client_user"
    clientAccess: list[str] = []


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    clientAccess: Optional[list[str]] = None
    status: Optional[str] = None


# ── Auth helper ──────────────────────────────────────────────────────────────

def _require_admin(authorization: Optional[str]) -> dict:
    """Validate that the caller is a super_admin.

    The legacy 'admin' user role was retired (its permissions duplicated
    client_user). Only super_admin now has platform-level privileges like
    managing other users.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if user["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Super admin access required")
    return user


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/users")
def list_users(authorization: Optional[str] = Header(default=None)):
    """List all users (admin only)."""
    _require_admin(authorization)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT user_id, email, name, role, client_access, is_active,
                           created_at, last_login
                    FROM users
                    ORDER BY created_at
                """)
            ).fetchall()

        return [
            {
                "id": r[0],
                "email": r[1],
                "name": r[2],
                "role": r[3],
                "clientAccess": list(r[4]) if r[4] else [],
                "status": "active" if r[5] else "inactive",
                "createdAt": r[6].isoformat() if r[6] else None,
                "lastLogin": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.post("/users")
def create_user(
    req: CreateUserRequest,
    authorization: Optional[str] = Header(default=None),
):
    """Create a new user (admin only)."""
    _require_admin(authorization)

    if not req.name.strip() or not req.email.strip() or not req.password.strip():
        raise HTTPException(status_code=400, detail="Name, email, and password are required")

    new_id = f"usr-{uuid.uuid4().hex[:6]}"

    try:
        with engine.connect() as conn:
            # Check email uniqueness
            existing = conn.execute(
                text("SELECT user_id FROM users WHERE LOWER(email) = LOWER(:email)"),
                {"email": req.email},
            ).fetchone()
            if existing:
                raise HTTPException(status_code=409, detail="A user with this email already exists")

            conn.execute(
                text("""
                    INSERT INTO users (user_id, email, password_hash, name, role, client_access)
                    VALUES (:uid, :email, :password, :name, :role, :access)
                """),
                {
                    "uid": new_id,
                    "email": req.email,
                    "password": req.password,
                    "name": req.name,
                    "role": req.role,
                    "access": req.clientAccess,
                },
            )
            conn.commit()

        log.info("Created user: %s (%s) role=%s", req.name, req.email, req.role)

        return {
            "id": new_id,
            "email": req.email,
            "name": req.name,
            "role": req.role,
            "clientAccess": req.clientAccess,
            "status": "active",
            "createdAt": None,
            "lastLogin": None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not create user: {e}")


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    req: UpdateUserRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    """Update a user's details (admin only)."""
    caller = _require_admin(authorization)

    updates = []
    params = {"uid": user_id}

    if req.name is not None:
        updates.append("name = :name")
        params["name"] = req.name
    if req.email is not None:
        updates.append("email = :email")
        params["email"] = req.email
    if req.role is not None:
        updates.append("role = :role")
        params["role"] = req.role
    if req.clientAccess is not None:
        updates.append("client_access = :access")
        params["access"] = req.clientAccess
    if req.status is not None:
        updates.append("is_active = :active")
        params["active"] = req.status == "active"

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        with engine.connect() as conn:
            conn.execute(
                text(f"UPDATE users SET {', '.join(updates)} WHERE user_id = :uid"),
                params,
            )
            conn.commit()

            # Fetch updated user
            row = conn.execute(
                text("SELECT user_id, email, name, role, client_access, is_active, created_at, last_login FROM users WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        # Audit: describe which fields changed. Status toggles are the most
        # common audit-worthy event here, but we log any field change so the
        # audit trail stays complete when future UI exposes more fields.
        changed: list[str] = []
        if req.name is not None:       changed.append(f"name→{req.name}")
        if req.email is not None:      changed.append(f"email→{req.email}")
        if req.role is not None:       changed.append(f"role→{req.role}")
        if req.clientAccess is not None: changed.append(f"access→{','.join(req.clientAccess) or '∅'}")
        if req.status is not None:     changed.append(f"status→{req.status}")
        log_audit_event(
            request,
            action_type="user_updated",
            details=f"Updated {row[1]} ({user_id}) · " + " · ".join(changed),
            client_id=None,
            user_id=caller["id"],
            user_email=caller["email"],
            outcome="success",
        )

        return {
            "id": row[0],
            "email": row[1],
            "name": row[2],
            "role": row[3],
            "clientAccess": list(row[4]) if row[4] else [],
            "status": "active" if row[5] else "inactive",
            "createdAt": row[6].isoformat() if row[6] else None,
            "lastLogin": row[7].isoformat() if row[7] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not update user: {e}")


@router.delete("/users/{user_id}")
def delete_user(
    user_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    """Delete a user (admin only). Cannot delete yourself."""
    caller = _require_admin(authorization)

    if caller["id"] == user_id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")

    # Snapshot the target's email BEFORE the DELETE so the audit row still
    # carries a human-readable identity after the row is gone.
    try:
        with engine.connect() as conn:
            target_row = conn.execute(
                text("SELECT email, role FROM users WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
    except Exception:
        target_row = None
    target_email = target_row[0] if target_row else "unknown"
    target_role  = target_row[1] if target_row else "unknown"

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM users WHERE user_id = :uid"),
                {"uid": user_id},
            )
            conn.commit()

            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="User not found")

        log.info("Deleted user: %s", user_id)

        # Audit: record who deleted whom, plus the deleted user's role so the
        # audit reader can spot privileged-user deletions at a glance.
        log_audit_event(
            request,
            action_type="user_deleted",
            details=f"Deleted user {target_email} ({user_id}) · role {target_role}",
            client_id=None,
            user_id=caller["id"],
            user_email=caller["email"],
            outcome="success",
        )

        return {}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not delete user: {e}")
