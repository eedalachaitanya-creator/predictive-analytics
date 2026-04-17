"""
client_router.py — Client Registration & Management
=====================================================
POST   /api/v1/clients/register    — Register a new client (auto-generates client_id)
GET    /api/v1/clients              — List all registered clients
GET    /api/v1/clients/{client_id}  — Get a specific client's config

HOW AUTO client_id WORKS:
─────────────────────────
1. A new client (e.g., Costco) signs up on our platform
2. System looks at the LAST client_id in the database (e.g., CLT-002)
3. Auto-generates the NEXT one: CLT-003
4. Creates a client_config row with all default settings
5. Returns the new client_id — the client NEVER has to think about it

Now when this client uploads their data (products, orders, etc.),
the upload router auto-injects this client_id into every row.
No manual editing of Excel files needed!
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token

router = APIRouter(prefix="/api/v1", tags=["clients"])
log = logging.getLogger("clients")


# ── Request / Response models ────────────────────────────────────────────────

class ClientRegisterRequest(BaseModel):
    """
    What we need from a new client to register them.
    Only client_name and client_code are required — everything else has defaults.
    """
    client_name: str                          # e.g., "Costco Wholesale"
    client_code: str                          # e.g., "COSTCO" (short code)
    currency: str = "USD"
    timezone: str = "America/Chicago"
    churn_window_days: int = 90
    high_ltv_threshold: float = 1000.00
    mid_ltv_threshold: float = 200.00
    max_discount_pct: float = 30.00
    min_repeat_orders: int = 2
    prediction_mode: str = "churn"


class ClientResponse(BaseModel):
    client_id: str
    client_name: str
    client_code: str
    message: str


# ── Helper: generate next client_id ─────────────────────────────────────────

def _generate_next_client_id() -> str:
    """
    Look at the highest existing client_id in client_config and return the next one.

    Example:
        Database has: CLT-001, CLT-002
        Returns:      CLT-003

    HOW IT WORKS:
    - Query the database for the MAX client_id
    - Extract the number part (e.g., "002" from "CLT-002")
    - Add 1 and format with leading zeros
    - If no clients exist yet, start with CLT-001
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT client_id FROM client_config ORDER BY client_id DESC LIMIT 1")
            )
            row = result.fetchone()

            if row and row[0]:
                # Extract number from "CLT-002" → 2, then make "CLT-003"
                last_id = row[0]                      # e.g., "CLT-002"
                number_part = last_id.split("-")[-1]   # e.g., "002"
                next_number = int(number_part) + 1     # e.g., 3
                return f"CLT-{next_number:03d}"        # e.g., "CLT-003"
            else:
                return "CLT-001"
    except Exception as e:
        log.error("Could not generate client_id: %s", e)
        raise HTTPException(status_code=500, detail="Could not generate client ID")


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/clients/register")
def register_client(
    req: ClientRegisterRequest,
    authorization: Optional[str] = Header(default=None),
):
    """
    Register a new client on the platform.

    WHAT HAPPENS:
    1. Validates that the user is a super_admin or admin
    2. Checks if a client with same name/code already exists
    3. Auto-generates the next client_id (CLT-003, CLT-004, etc.)
    4. Creates a row in client_config with all their settings
    5. Returns the new client_id

    EXAMPLE:
        POST /api/v1/clients/register
        {
            "client_name": "Costco Wholesale",
            "client_code": "COSTCO"
        }
        → Returns: { "client_id": "CLT-003", "client_name": "Costco Wholesale", ... }
    """
    # ── Auth check: only admins can register new clients ──────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if user["role"] not in ("super_admin", "admin"):
        raise HTTPException(
            status_code=403,
            detail="Only admins can register new clients",
        )

    # ── Check if client already exists ────────────────────────────────
    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT client_id FROM client_config WHERE LOWER(client_name) = LOWER(:name)"),
                {"name": req.client_name},
            ).fetchone()

            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Client '{req.client_name}' already exists with ID: {existing[0]}",
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # ── Generate the next client_id ───────────────────────────────────
    new_client_id = _generate_next_client_id()

    # ── Insert into client_config ─────────────────────────────────────
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO client_config (
                        client_id, client_name, client_code,
                        currency, timezone, churn_window_days,
                        high_ltv_threshold, mid_ltv_threshold,
                        max_discount_pct, min_repeat_orders,
                        prediction_mode
                    ) VALUES (
                        :client_id, :client_name, :client_code,
                        :currency, :timezone, :churn_window_days,
                        :high_ltv_threshold, :mid_ltv_threshold,
                        :max_discount_pct, :min_repeat_orders,
                        :prediction_mode
                    )
                """),
                {
                    "client_id": new_client_id,
                    "client_name": req.client_name,
                    "client_code": req.client_code,
                    "currency": req.currency,
                    "timezone": req.timezone,
                    "churn_window_days": req.churn_window_days,
                    "high_ltv_threshold": req.high_ltv_threshold,
                    "mid_ltv_threshold": req.mid_ltv_threshold,
                    "max_discount_pct": req.max_discount_pct,
                    "min_repeat_orders": req.min_repeat_orders,
                    "prediction_mode": req.prediction_mode,
                },
            )
            conn.commit()

        log.info("New client registered: %s (%s) → %s", req.client_name, req.client_code, new_client_id)

        return {
            "client_id": new_client_id,
            "client_name": req.client_name,
            "client_code": req.client_code,
            "message": f"Client registered successfully! Your client ID is {new_client_id}",
        }

    except Exception as e:
        log.error("Failed to register client: %s", e)
        raise HTTPException(status_code=500, detail=f"Could not register client: {e}")


@router.get("/clients")
def list_clients(authorization: Optional[str] = Header(default=None)):
    """
    List all registered clients.
    Returns client_id, name, code, and created_at for each.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    try:
        with engine.connect() as conn:
            # If super_admin or admin with wildcard, show all clients
            if user["role"] == "super_admin" or "*" in user.get("clientAccess", []):
                rows = conn.execute(
                    text("SELECT client_id, client_name, client_code, created_at FROM client_config ORDER BY client_id")
                ).fetchall()
            else:
                # Regular users only see their allowed clients
                client_list = user.get("clientAccess", [])
                if not client_list:
                    return []
                placeholders = ", ".join([f":c{i}" for i in range(len(client_list))])
                params = {f"c{i}": cid for i, cid in enumerate(client_list)}
                rows = conn.execute(
                    text(f"SELECT client_id, client_name, client_code, created_at FROM client_config WHERE client_id IN ({placeholders}) ORDER BY client_id"),
                    params,
                ).fetchall()

        return [
            {
                "client_id": r[0],
                "client_name": r[1],
                "client_code": r[2],
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/clients/{client_id}")
def get_client(client_id: str, authorization: Optional[str] = Header(default=None)):
    """Get a specific client's configuration."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Check access
    if user["role"] != "super_admin" and "*" not in user.get("clientAccess", []):
        if client_id not in user.get("clientAccess", []):
            raise HTTPException(status_code=403, detail="You don't have access to this client")

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM client_config WHERE client_id = :cid"),
                {"cid": client_id},
            ).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"Client {client_id} not found")

            # Convert row to dict using column names
            columns = conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'client_config' ORDER BY ordinal_position")
            ).fetchall()
            col_names = [c[0] for c in columns]

            return {col_names[i]: (row[i].isoformat() if hasattr(row[i], 'isoformat') else row[i]) for i in range(len(col_names))}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# ── Self-Registration (PUBLIC — no auth needed) ─────────────────────────────

class SelfRegisterRequest(BaseModel):
    """
    What a new client fills in on the registration page.
    This creates BOTH a client_config entry AND a user account.
    """
    client_name: str       # "Costco Wholesale"
    client_code: str       # "COSTCO"
    contact_name: str      # "John Smith"
    contact_email: str     # "john@costco.com"
    password: str          # their chosen password


@router.post("/clients/self-register")
def self_register(req: SelfRegisterRequest):
    """
    PUBLIC endpoint — new clients register themselves.

    WHAT HAPPENS:
    1. Validates the company doesn't already exist
    2. Validates the email isn't already taken
    3. Auto-generates next client_id (CLT-003, CLT-004, etc.)
    4. Creates a client_config row in the database
    5. Creates a user account in memory (linked to the new client_id)
    6. Returns the new client_id so they know their ID

    The user can then log in immediately with their email + password.
    """
    # ── Validate inputs ───────────────────────────────────────────────
    if not req.client_name.strip():
        raise HTTPException(status_code=400, detail="Company name is required")
    if not req.client_code.strip():
        raise HTTPException(status_code=400, detail="Company code is required")
    if len(req.client_code) > 10:
        raise HTTPException(status_code=400, detail="Company code must be 10 characters or less")
    if not req.contact_email.strip() or "@" not in req.contact_email:
        raise HTTPException(status_code=400, detail="Valid email address is required")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    # ── Check if email is already taken (in database) ───────────────
    try:
        with engine.connect() as conn:
            existing_user = conn.execute(
                text("SELECT user_id FROM users WHERE LOWER(email) = LOWER(:email)"),
                {"email": req.contact_email},
            ).fetchone()
            if existing_user:
                raise HTTPException(status_code=409, detail="An account with this email already exists")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # ── Check if client already exists ────────────────────────────────
    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT client_id FROM client_config WHERE LOWER(client_name) = LOWER(:name)"),
                {"name": req.client_name},
            ).fetchone()

            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"A company named '{req.client_name}' is already registered",
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # ── Generate client_id ────────────────────────────────────────────
    new_client_id = _generate_next_client_id()

    # ── Create client_config in database ──────────────────────────────
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO client_config (
                        client_id, client_name, client_code
                    ) VALUES (
                        :client_id, :client_name, :client_code
                    )
                """),
                {
                    "client_id": new_client_id,
                    "client_name": req.client_name,
                    "client_code": req.client_code.upper(),
                },
            )
            conn.commit()
    except Exception as e:
        log.error("Failed to create client_config: %s", e)
        raise HTTPException(status_code=500, detail=f"Could not register company: {e}")

    # ── Create user account in database ─────────────────────────────
    new_user_id = f"usr-{uuid.uuid4().hex[:6]}"
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO users (user_id, email, password_hash, name, role, client_access)
                    VALUES (:user_id, :email, :password, :name, 'client_user', :client_access)
                """),
                {
                    "user_id": new_user_id,
                    "email": req.contact_email,
                    "password": req.password,
                    "name": req.contact_name,
                    "client_access": [new_client_id],
                },
            )
            conn.commit()
    except Exception as e:
        log.error("Failed to create user: %s", e)
        raise HTTPException(status_code=500, detail=f"Could not create user account: {e}")

    log.info(
        "Self-registration complete: %s (%s) → %s, user: %s (%s)",
        req.client_name, req.client_code, new_client_id,
        req.contact_name, req.contact_email,
    )

    return {
        "client_id": new_client_id,
        "client_name": req.client_name,
        "client_code": req.client_code.upper(),
        "user_email": req.contact_email,
        "message": f"Account created successfully! Your Client ID is {new_client_id}. You can now sign in.",
    }
