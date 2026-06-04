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
import re
from decimal import Decimal
from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import _find_user_by_token, get_current_user
from app.audit_logger import log_audit_event
from app.security import hash_password

router = APIRouter(prefix="/api/v1", tags=["clients"])  # audit-2026-04-29: router-level auth
log = logging.getLogger("clients")


# ── Schema safety net for soft-delete columns ───────────────────────────────
# Mirrors migration_client_soft_delete.sql. Runs once at import so dev DBs
# that haven't had the migration applied still get the columns before the
# delete/list endpoints try to use them.
def _ensure_soft_delete_columns() -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE client_config "
                "ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE"
            ))
            conn.execute(text(
                "ALTER TABLE client_config "
                "ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ"
            ))
    except Exception as exc:
        log.warning("Could not ensure client_config soft-delete columns: %s", exc)

_ensure_soft_delete_columns()


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

@router.post("/clients/register",dependencies=[Depends(get_current_user)])
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

    # 'admin' user role was retired — client registration now requires super_admin.
    if user["role"] != "super_admin":
        raise HTTPException(
            status_code=403,
            detail="Only super admins can register new clients",
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


@router.get("/clients",dependencies=[Depends(get_current_user)])
def list_clients(
    includeInactive: bool = Query(default=False),
    authorization: Optional[str] = Header(default=None),
):
    """
    List registered clients.

    By default returns only active (not soft-deleted) clients. Super admins
    can pass `?includeInactive=true` to also receive deactivated rows — used
    by the Admin Clients page to populate Total/Active/Inactive counters.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Only super_admin may see inactive rows — keep client_users on the
    # active-only path regardless of the flag they send.
    show_inactive = includeInactive and user["role"] == "super_admin"
    active_clause = "" if show_inactive else "WHERE is_active = TRUE"

    try:
        with engine.connect() as conn:
            if user["role"] == "super_admin" or "*" in user.get("clientAccess", []):
                rows = conn.execute(
                    text(
                        f"SELECT client_id, client_name, client_code, created_at, "
                        f"       is_active, deactivated_at "
                        f"FROM client_config "
                        f"{active_clause} "
                        f"ORDER BY client_id"
                    )
                ).fetchall()
            else:
                # Regular users always see active-only, restricted to their access list.
                client_list = user.get("clientAccess", [])
                if not client_list:
                    return []
                placeholders = ", ".join([f":c{i}" for i in range(len(client_list))])
                params = {f"c{i}": cid for i, cid in enumerate(client_list)}
                rows = conn.execute(
                    text(
                        f"SELECT client_id, client_name, client_code, created_at, "
                        f"       is_active, deactivated_at "
                        f"FROM client_config "
                        f"WHERE is_active = TRUE AND client_id IN ({placeholders}) "
                        f"ORDER BY client_id"
                    ),
                    params,
                ).fetchall()

        return [
            {
                "client_id":      r[0],
                "client_name":    r[1],
                "client_code":    r[2],
                "created_at":     r[3].isoformat() if r[3] else None,
                "is_active":      bool(r[4]),
                "deactivated_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/clients/{client_id}",dependencies=[Depends(get_current_user)])
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

def _send_welcome_email(
    to_email: str,
    contact_name: str,
    company_name: str,
    client_id: str,
    client_code: str,
    password: str = "",
) -> None:
    import os
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    smtp_from = os.getenv("SMTP_FROM", smtp_user) or "no-reply@predictive-analytics.io"

    if not smtp_user:
        log.warning("SMTP_USER not configured — skipping welcome email to %s", to_email)
        return

    subject = "Welcome to Predictive Analytics — Your Account is Ready"

    html_body = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Welcome to Predictive Analytics</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6fb;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6fb;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:12px;overflow:hidden;
                    box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <tr><td style="background:linear-gradient(90deg,#0071CE,#FFC220);height:4px;"></td></tr>

        <tr>
          <td style="padding:36px 40px 24px;border-bottom:1px solid #eef0f5;">
            <div style="font-size:22px;font-weight:700;color:#1a1a2e;">
              📊 <span style="color:#0071CE;">Predictive</span> Analytics
            </div>
            <div style="font-size:12px;color:#888;margin-top:4px;">
              Churn Prediction &amp; Retention Platform · v4.0
            </div>
          </td>
        </tr>

        <tr>
          <td style="padding:32px 40px 24px;">
            <p style="margin:0 0 12px;font-size:16px;font-weight:600;color:#1a1a2e;">
              Welcome, {contact_name}!
            </p>
            <p style="margin:0 0 20px;font-size:14px;color:#555;line-height:1.7;">
              Your account for <strong>{company_name}</strong> has been created successfully.
              You can now sign in and start using churn prediction and customer retention tools.
            </p>

            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:#f0f6ff;border:1px solid #c8dcf7;border-radius:8px;margin-bottom:24px;">
              <tr><td style="padding:20px 24px;">
                <p style="margin:0 0 4px;font-size:11px;font-weight:700;text-transform:uppercase;
                           letter-spacing:.07em;color:#0071CE;">Account Details</p>
                <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:12px;">
                  <tr>
                    <td style="font-size:13px;color:#666;padding:4px 0;width:140px;">Company Name</td>
                    <td style="font-size:13px;color:#1a1a2e;font-weight:600;padding:4px 0;">{company_name}</td>
                  </tr>
                  <tr>
                    <td style="font-size:13px;color:#666;padding:4px 0;">Company Code</td>
                    <td style="font-size:13px;color:#1a1a2e;font-weight:600;padding:4px 0;">{client_code}</td>
                  </tr>
                  <tr>
                    <td style="font-size:13px;color:#666;padding:4px 0;">Client ID</td>
                    <td style="font-size:15px;color:#0071CE;font-weight:700;padding:4px 0;
                               letter-spacing:1px;">{client_id}</td>
                  </tr>
                  <tr>
                    <td style="font-size:13px;color:#666;padding:4px 0;">Login Email</td>
                    <td style="font-size:13px;color:#1a1a2e;font-weight:600;padding:4px 0;">{to_email}</td>
                  </tr>
                  <tr>
                    <td style="font-size:13px;color:#666;padding:4px 0;">Temporary Password</td>
                    <td style="font-size:14px;color:#0071CE;font-weight:700;letter-spacing:1px;padding:4px 0;">{password if password else "As set during registration"}</td>
                  </tr>
                </table>
              </td></tr>
            </table>

            <table cellpadding="0" cellspacing="0" style="margin:0 auto 24px;">
              <tr>
                <td align="center" style="border-radius:8px;background:#0071CE;">
                  <a href="http://localhost:4200/login"
                     style="display:inline-block;padding:13px 32px;font-size:14px;font-weight:600;
                            color:#ffffff;text-decoration:none;border-radius:8px;">
                    Sign In to Your Account
                  </a>
                </td>
              </tr>
            </table>

            <p style="margin:0;font-size:13px;color:#555;line-height:1.7;">
              If you have any questions, please contact our support team.
              We're glad to have <strong>{company_name}</strong> on board.
            </p>
          </td>
        </tr>

        <tr>
          <td style="padding:20px 40px;border-top:1px solid #eef0f5;background:#fafbfd;">
            <p style="margin:0;font-size:11px;color:#aaa;line-height:1.6;text-align:center;">
              This email was sent because an account was created at Predictive Analytics.<br/>
              If you did not register, please contact support immediately.<br/>
              &copy; 2025 Predictive Analytics. All rights reserved.
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>
"""

    plain_body = (
        f"Welcome to Predictive Analytics, {contact_name}!\n\n"
        f"Your account for {company_name} has been created.\n\n"
        f"Account Details:\n"
        f"  Company Name : {company_name}\n"
        f"  Company Code : {client_code}\n"
        f"  Client ID    : {client_id}\n"
        f"  Login Email  : {to_email}\n"
        f"  Temp Password: {password}\n\n"
        f"Sign in at: http://localhost:4200/login\n\n"
        f"If you did not register, please contact support immediately.\n"
        f"© 2025 Predictive Analytics. All rights reserved."
    )

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = smtp_from
        msg["To"]      = to_email
        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.ehlo()
            if smtp_port != 465:
                server.starttls()
                server.ehlo()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_from, [to_email], msg.as_string())

        log.info("Welcome email sent to %s", to_email)

    except Exception as exc:
        log.warning("Could not send welcome email to %s: %s", to_email, exc)


@router.post("/clients/self-register", dependencies=[])
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
    import re
    email_re = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$')
    if not req.contact_email.strip() or not email_re.match(req.contact_email.strip()):
        raise HTTPException(status_code=400, detail="Valid email address is required")
    pw = req.password
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
            detail="Password must be at least 8 characters and include an uppercase letter, lowercase letter, number, and special character."
        )

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
                    "password": hash_password(req.password),
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
    # ── Send welcome email ────────────────────────────────────────────
    _send_welcome_email(
            to_email=req.contact_email,
            contact_name=req.contact_name,
            company_name=req.client_name,
            client_id=new_client_id,
            client_code=req.client_code.upper(),
            password=req.password, 
        )
    return {
        "client_id": new_client_id,
        "client_name": req.client_name,
        "client_code": req.client_code.upper(),
        "user_email": req.contact_email,
        "message": f"Account created successfully! Your Client ID is {new_client_id}. You can now sign in.",
    }


# ── Client Data Overview (for Client Management "View" action) ───────────────
#
# When a super admin clicks "View" on a client row, we show TWO tables:
#
#   A) Uploaded data — rows the client (or their data team) uploaded via the
#      Upload wizard. These are the client's raw facts: customers, orders,
#      line items, reviews, tickets.
#
#   B) Generated data — rows our ML pipeline produced from those uploads:
#      RFM features, purchase cycles, churn scores, retention interventions,
#      outreach messages.
#
# For each table we return:
#   - row_count           : how many rows exist for this client
#   - last_updated        : most recent timestamp (helps spot stale data)
#   - label               : human-readable name for the UI
#
# DESIGN CHOICE — counts + timestamps, not full dumps.
# Returning every row here would blow up the response size for large clients
# (Walmart has tens of thousands of customers). The UI already has dedicated
# pages for drilling into customers / orders / churn scores, so the overview
# just needs to show "what data exists and how fresh is it". A "View detail"
# button can deep-link to the right page for full rows.

# Catalog of tables to summarize. Each entry is:
#   (table_name, category, label, timestamp_column_or_None)
# timestamp_column is used to find the "most recent update" for that table;
# None means the table has no natural timestamp (we'll skip the freshness cell).
_DATA_OVERVIEW_TABLES = [
    # ── Uploaded by the client ─────────────────────────────────────────
    ("customers",          "uploaded",  "Customers",          "account_created_date"),
    ("orders",             "uploaded",  "Orders",             "order_date"),
    ("line_items",         "uploaded",  "Line Items",         None),
    ("customer_reviews",   "uploaded",  "Customer Reviews",   "review_date"),
    ("support_tickets",    "uploaded",  "Support Tickets",    "opened_date"),
    # ── Generated by the ML pipeline ───────────────────────────────────
    ("customer_rfm_features",     "generated", "RFM Features",            "computed_at"),
    ("customer_purchase_cycles",  "generated", "Purchase Cycles",         "computed_at"),
    ("churn_scores",              "generated", "Churn Scores",            "scored_at"),
    ("retention_interventions",   "generated", "Retention Interventions", "created_at"),
    ("outreach_messages",         "generated", "Outreach Messages",       "sent_at"),
]


@router.get("/clients/{client_id}/data-overview",dependencies=[Depends(get_current_user)])
def get_client_data_overview(
    client_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """
    Return row counts + freshness for every client-scoped table, split into
    "uploaded" vs "generated" buckets. Used by the Client Management "View"
    action on the UI.

    Response shape:
        {
          "client_id": "CLT-001",
          "client_name": "Walmart Inc.",
          "uploaded":  [ { table, label, row_count, last_updated }, ... ],
          "generated": [ { table, label, row_count, last_updated }, ... ],
          "totals": { "uploaded_rows": N, "generated_rows": M }
        }
    """
    # ── Auth ──────────────────────────────────────────────────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Only super admins or users with access to this client can view it.
    if user["role"] != "super_admin" and "*" not in user.get("clientAccess", []):
        if client_id not in user.get("clientAccess", []):
            raise HTTPException(status_code=403, detail="You don't have access to this client")

    # ── Confirm client exists + fetch display name ───────────────────
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT client_id, client_name FROM client_config WHERE client_id = :cid"),
                {"cid": client_id},
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
            client_name = row[1]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # ── Per-table count + last-updated ────────────────────────────────
    # We query each table independently and swallow per-table errors so a
    # single missing / renamed table doesn't take down the whole overview.
    uploaded: list[dict] = []
    generated: list[dict] = []
    uploaded_total = 0
    generated_total = 0

    with engine.connect() as conn:
        for table_name, category, label, ts_col in _DATA_OVERVIEW_TABLES:
            entry = {
                "table": table_name,
                "label": label,
                "row_count": 0,
                "last_updated": None,
            }
            try:
                count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE client_id = :cid"
                count = conn.execute(text(count_sql), {"cid": client_id}).scalar() or 0
                entry["row_count"] = int(count)

                if ts_col and count > 0:
                    ts_sql = f"SELECT MAX({ts_col}) FROM {table_name} WHERE client_id = :cid"
                    ts = conn.execute(text(ts_sql), {"cid": client_id}).scalar()
                    if ts is not None:
                        entry["last_updated"] = (
                            ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                        )
            except Exception as e:
                # Table missing or column renamed — log and keep going with zero.
                log.warning("data-overview: %s failed for %s: %s", table_name, client_id, e)

            if category == "uploaded":
                uploaded.append(entry)
                uploaded_total += entry["row_count"]
            else:
                generated.append(entry)
                generated_total += entry["row_count"]

    return {
        "client_id": client_id,
        "client_name": client_name,
        "uploaded": uploaded,
        "generated": generated,
        "totals": {
            "uploaded_rows": uploaded_total,
            "generated_rows": generated_total,
        },
    }


# ── Per-table row viewer ────────────────────────────────────────────────────
#
# When the super admin clicks "View" on a specific table row in the data
# overview (e.g., "View" next to Customers), we hit this endpoint. It returns
# a page of actual rows from that table, scoped to the client_id.
#
# SECURITY — whitelisted tables only.
# We can't drop {table} straight into SQL: that's a textbook injection vector.
# Instead we look it up in _TABLE_VIEW_META below. If the table isn't in the
# whitelist, we return 400. This also means any attempt to query, e.g.,
# client_config or users via this endpoint is refused.
#
# ORDER — newest first where a timestamp exists.
# Each entry declares an order_by column; we sort DESC so the latest rows
# show up on page 1. For tables without a natural timestamp (line_items),
# we fall back to the primary-key column.
_TABLE_VIEW_META: dict[str, dict[str, str]] = {
    # Uploaded
    "customers":                {"order_by": "account_created_date", "direction": "DESC"},
    "orders":                   {"order_by": "order_date",            "direction": "DESC"},
    "line_items":               {"order_by": "line_item_id",          "direction": "ASC"},
    "customer_reviews":         {"order_by": "review_date",           "direction": "DESC"},
    "support_tickets":          {"order_by": "opened_date",           "direction": "DESC"},
    # Generated
    "customer_rfm_features":    {"order_by": "computed_at",           "direction": "DESC"},
    "customer_purchase_cycles": {"order_by": "computed_at",           "direction": "DESC"},
    "churn_scores":             {"order_by": "scored_at",             "direction": "DESC"},
    "retention_interventions":  {"order_by": "created_at",            "direction": "DESC"},
    "outreach_messages":        {"order_by": "sent_at",               "direction": "DESC"},
}


def _jsonify_cell(v: Any) -> Any:
    """
    Make a single DB value safe to JSON-encode.

    - datetime / date → ISO string (FastAPI handles these, but being explicit
      avoids surprises on older pydantic versions)
    - Decimal         → float (so the UI gets a real number, not a string)
    - everything else → passed through unchanged
    """
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        # float is lossy for very large decimals, but the values here are
        # prices / probabilities / counts — float is fine for display.
        return float(v)
    return v


@router.get("/clients/{client_id}/data/{table}")
def get_client_table_rows(
    client_id: str,
    table: str,
    # Default bumped from 50 → 100 per CTO direction: the data-viewer
    # modal on the Clients page now shows 100 rows per page with
    # vertical scroll instead of paginating in 50-row chunks.
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    authorization: Optional[str] = Header(default=None),
):
    """
    Return a page of rows from `table` for this client.

    Response shape:
        {
          "table": "customers",
          "client_id": "CLT-001",
          "columns": ["client_id", "customer_id", "customer_email", ...],
          "rows": [ {col: val, ...}, ... ],
          "total":  <int>,
          "limit":  <int>,
          "offset": <int>
        }
    """
    # ── Auth ──────────────────────────────────────────────────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if user["role"] != "super_admin" and "*" not in user.get("clientAccess", []):
        if client_id not in user.get("clientAccess", []):
            raise HTTPException(status_code=403, detail="You don't have access to this client")

    # ── Whitelist check — NEVER trust `table` from URL without this ───
    meta = _TABLE_VIEW_META.get(table)
    if not meta:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table}' is not viewable through this endpoint",
        )

    order_by = meta["order_by"]
    direction = meta["direction"]

    try:
        with engine.connect() as conn:
            # Total count (for the pagination footer)
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid"),
                {"cid": client_id},
            ).scalar() or 0

            # Actual page of rows. order_by/direction are from the whitelist,
            # not user input, so safe to f-string.
            result = conn.execute(
                text(
                    f"SELECT * FROM {table} "
                    f"WHERE client_id = :cid "
                    f"ORDER BY {order_by} {direction} NULLS LAST "
                    f"LIMIT :limit OFFSET :offset"
                ),
                {"cid": client_id, "limit": limit, "offset": offset},
            )
            columns = list(result.keys())
            raw_rows = result.fetchall()
            rows = [
                {col: _jsonify_cell(r[i]) for i, col in enumerate(columns)}
                for r in raw_rows
            ]

        return {
            "table":     table,
            "client_id": client_id,
            "columns":   columns,
            "rows":      rows,
            "total":     int(total),
            "limit":     limit,
            "offset":    offset,
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error("get_client_table_rows failed: table=%s client=%s err=%s", table, client_id, e)
        raise HTTPException(status_code=500, detail=f"Could not load rows from {table}: {e}")


# ── Admin: create a new client + first user (single call) ───────────────────
#
# /clients/register (super-admin only) creates a client_config row and nothing
# else — the client still needs a separate user row to actually log in.
# /clients/self-register (public) does both but has no auth.
#
# This endpoint is the admin-console equivalent of self-register: it requires
# super_admin auth AND creates a client_config + first user in one transaction.
# The super admin on the Clients page uses this when onboarding a new tenant
# without making them go through the public registration page.

class AdminCreateClientRequest(BaseModel):
    client_name: str
    client_code: str
    contact_name: str
    contact_email: str
    password: str


@router.post("/clients/admin-create",dependencies=[Depends(get_current_user)])
def admin_create_client(
    req: AdminCreateClientRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    """
    Super-admin-only: create a new client + their first user account in one go.
    Response matches /clients/self-register so the frontend can reuse the
    same confirmation UI.
    """
    # ── Auth: super_admin only ────────────────────────────────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can create clients")

    # ── Validate inputs ───────────────────────────────────────────────
    # Same rules as self-register — keeps both entry points consistent so
    # admin-created and self-registered clients look identical in the DB.
    if not req.client_name.strip():
        raise HTTPException(status_code=400, detail="Company name is required")
    if not req.client_code.strip():
        raise HTTPException(status_code=400, detail="Company code is required")
    if len(req.client_code) > 10:
        raise HTTPException(status_code=400, detail="Company code must be 10 characters or less")
    import re
    email_re = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$')
    if not req.contact_email.strip() or not email_re.match(req.contact_email.strip()):
        raise HTTPException(status_code=400, detail="Valid email address is required")
    pw = req.password
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
            detail="Password must be at least 8 characters and include an uppercase letter, lowercase letter, number, and special character."
        )

    # ── Check for duplicates BEFORE creating anything ─────────────────
    try:
        with engine.connect() as conn:
            dup_email = conn.execute(
                text("SELECT user_id FROM users WHERE LOWER(email) = LOWER(:email)"),
                {"email": req.contact_email},
            ).fetchone()
            if dup_email:
                raise HTTPException(status_code=409, detail="An account with this email already exists")

            dup_client = conn.execute(
                text("SELECT client_id FROM client_config WHERE LOWER(client_name) = LOWER(:name)"),
                {"name": req.client_name},
            ).fetchone()
            if dup_client:
                raise HTTPException(
                    status_code=409,
                    detail=f"A company named '{req.client_name}' is already registered (ID: {dup_client[0]})",
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # ── Create both rows in a single transaction ──────────────────────
    # engine.begin() wraps everything in a transaction — if the user INSERT
    # fails after the client_config INSERT, Postgres rolls back both so we
    # don't leave an orphan client row behind.
    new_client_id = _generate_next_client_id()
    new_user_id = f"usr-{uuid.uuid4().hex[:6]}"
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO client_config (client_id, client_name, client_code)
                    VALUES (:cid, :name, :code)
                """),
                {
                    "cid": new_client_id,
                    "name": req.client_name,
                    "code": req.client_code.upper(),
                },
            )
            conn.execute(
                text("""
                    INSERT INTO users (user_id, email, password_hash, name, role, client_access)
                    VALUES (:uid, :email, :pw, :name, 'client_user', :access)
                """),
                {
                    "uid":    new_user_id,
                    "email":  req.contact_email,
                    "pw":     hash_password(req.password),
                    "name":   req.contact_name,
                    "access": [new_client_id],
                },
            )
    except Exception as e:
        log.error("admin_create_client failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Could not create client: {e}")

    log.info(
        "Admin-created client: %s (%s) → %s, user: %s (%s)",
        req.client_name, req.client_code, new_client_id,
        req.contact_name, req.contact_email,
    )

    # Audit: record tenant creation — who the super admin is, the generated
    # client_id, the human-readable name, and the first user's email. This
    # is one of the few events we deliberately attribute to the CREATED
    # client (not the admin's own client), so client_id in the audit row
    # reflects the new tenant.
    log_audit_event(
        request,
        action_type="client_created",
        details=(
            f"Created client {new_client_id} · {req.client_name} "
            f"(code {req.client_code.upper()}) · initial user {req.contact_email}"
        ),
        client_id=new_client_id,
        user_id=caller["id"],
        user_email=caller["email"],
        outcome="success",
    )

    _send_welcome_email(
        to_email=req.contact_email,
        contact_name=req.contact_name,
        company_name=req.client_name,
        client_id=new_client_id,
        client_code=req.client_code.upper(),
        password=req.password,
    )

    return {
        "client_id":   new_client_id,
        "client_name": req.client_name,
        "client_code": req.client_code.upper(),
        "user_email":  req.contact_email,
        "message":     f"Client {new_client_id} created successfully with initial user {req.contact_email}.",
    }


# ── Admin: soft-delete (deactivate) a client ─────────────────────────────────
#
# Hard deletion previously cascaded through every client_id-scoped table,
# leaving no record the client ever existed. We now flip is_active=FALSE and
# stamp deactivated_at instead — all tenant data stays in the DB for audit
# and potential reactivation, but the client disappears from the admin list.


@router.delete("/clients/{client_id}",dependencies=[Depends(get_current_user)])
def delete_client(
    client_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    """
    Super-admin-only: soft-delete a client. The client_config row is flagged
    is_active=FALSE and hidden from the admin UI, but no tenant data is wiped.
    A follow-up reactivate endpoint can restore the client without data loss.
    """
    # ── Auth: super_admin only ────────────────────────────────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can delete clients")

    # ── Confirm client exists and is currently active ─────────────────
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT is_active FROM client_config WHERE client_id = :cid"),
            {"cid": client_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
        if row[0] is False:
            # Idempotent — already deactivated.
            return {
                "client_id": client_id,
                "deleted": {},
                "message": f"Client {client_id} was already deactivated.",
            }

    # ── Flip the flag (and cascade to single-client users) ────────────
    # Two UPDATEs in ONE transaction so the client + its users go inactive
    # together. Without the user cascade, the Users page would still show
    # the user as "active" while the auth gate blocks their login —
    # confusing for super admins reviewing the roster. Multi-client users
    # (super_admin role, or anyone with client_access of length > 1) are
    # NOT cascaded; they have other tenants and stay active.
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE client_config "
                    "SET is_active = FALSE, deactivated_at = NOW() "
                    "WHERE client_id = :cid"
                ),
                {"cid": client_id},
            )
            cascade_result = conn.execute(
                text(
                    "UPDATE users "
                    "SET is_active = FALSE "
                    "WHERE :cid = ANY(client_access) "
                    "  AND COALESCE(array_length(client_access, 1), 0) = 1 "
                    "  AND role <> 'super_admin' "
                    "  AND is_active = TRUE"
                ),
                {"cid": client_id},
            )
            cascaded_user_count = cascade_result.rowcount or 0
    except Exception as e:
        log.error("delete_client failed for %s: %s", client_id, e)
        raise HTTPException(status_code=500, detail=f"Could not deactivate client: {e}")

    log.info(
        "Deactivated client %s (soft-delete; %d associated user(s) also deactivated).",
        client_id, cascaded_user_count,
    )

    log_audit_event(
        request,
        action_type="client_deactivated",
        details=(
            f"Soft-deleted client {client_id} (is_active=FALSE; tenant data retained). "
            f"Cascaded user deactivation to {cascaded_user_count} single-client user(s)."
        ),
        client_id=client_id,
        user_id=caller["id"],
        user_email=caller["email"],
        outcome="success",
    )

    return {
        "client_id": client_id,
        "deleted": {},
        "users_deactivated": cascaded_user_count,
        "message": f"Client {client_id} has been deactivated.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Reactivate (soft-undelete) a client
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/clients/{client_id}/reactivate",dependencies=[Depends(get_current_user)])
def reactivate_client(
    client_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    """
    Super-admin-only: reactivate a previously soft-deleted client.
    Sets is_active=TRUE and clears deactivated_at. All tenant data
    (customers, orders, line_items, reviews, tickets, churn_scores,
    mv_customer_features, etc.) was preserved during deactivation
    because no FK cascades exist on client_config — the data is still
    queryable by client_id and the client simply re-appears in the
    admin list, dropdowns, and dashboards.

    Idempotent: reactivating an already-active client returns success.
    """
    # ── Auth: super_admin only ────────────────────────────────────────
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can reactivate clients")

    # ── Confirm client exists ─────────────────────────────────────────
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT is_active FROM client_config WHERE client_id = :cid"),
            {"cid": client_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
        if row[0] is True:
            # Idempotent — already active.
            return {
                "client_id": client_id,
                "message": f"Client {client_id} was already active.",
            }

    # ── Flip the flag back (and cascade to single-client users) ──────
    # Mirror of the cascade in delete_client: when the tenant comes back
    # online, its single-client users come back online too. This is the
    # path that brings users back into the Users page roster as "active"
    # after a client is reactivated. Multi-client users were never
    # touched by the delete cascade, so they don't need re-flipping.
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE client_config "
                    "SET is_active = TRUE, deactivated_at = NULL "
                    "WHERE client_id = :cid"
                ),
                {"cid": client_id},
            )
            cascade_result = conn.execute(
                text(
                    "UPDATE users "
                    "SET is_active = TRUE "
                    "WHERE :cid = ANY(client_access) "
                    "  AND COALESCE(array_length(client_access, 1), 0) = 1 "
                    "  AND role <> 'super_admin' "
                    "  AND is_active = FALSE"
                ),
                {"cid": client_id},
            )
            cascaded_user_count = cascade_result.rowcount or 0
    except Exception as e:
        log.error("reactivate_client failed for %s: %s", client_id, e)
        raise HTTPException(status_code=500, detail=f"Could not reactivate client: {e}")

    log.info(
        "Reactivated client %s (is_active=TRUE; %d associated user(s) also reactivated).",
        client_id, cascaded_user_count,
    )

    log_audit_event(
        request,
        action_type="client_reactivated",
        details=(
            f"Reactivated client {client_id} (is_active=TRUE; tenant data was "
            f"never wiped during deactivation, so all customers/orders/scores "
            f"are immediately available for queries and model training). "
            f"Cascaded user reactivation to {cascaded_user_count} single-client user(s)."
        ),
        client_id=client_id,
        user_id=caller["id"],
        user_email=caller["email"],
        outcome="success",
    )

    return {
        "client_id": client_id,
        "users_reactivated": cascaded_user_count,
        "message": f"Client {client_id} has been reactivated.",
    }
