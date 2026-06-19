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
from app.security import hash_password, validate_password
from app.email_service import LOGO_SRC

router = APIRouter(prefix="/api/v1", tags=["clients"])
log = logging.getLogger("clients")


# ── Schema safety net for soft-delete columns ───────────────────────────────
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


# ── Schema safety net for client onboarding columns ──────────────────────────
def _ensure_client_org_columns() -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE client_config
                  ADD COLUMN IF NOT EXISTS address        varchar(255),
                  ADD COLUMN IF NOT EXISTS city           varchar(100),
                  ADD COLUMN IF NOT EXISTS state_province varchar(100),
                  ADD COLUMN IF NOT EXISTS postal_code    varchar(20),
                  ADD COLUMN IF NOT EXISTS country        varchar(100),
                  ADD COLUMN IF NOT EXISTS contact_email  varchar(150),
                  ADD COLUMN IF NOT EXISTS company_phone  varchar(40)
            """))
            conn.execute(text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone varchar(40)"
            ))
    except Exception as exc:
        log.warning("Could not ensure client onboarding columns: %s", exc)

_ensure_client_org_columns()


# ── Shared validation patterns ───────────────────────────────────────────────
EMAIL_RE = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$')
PHONE_RE = re.compile(r'^\d{10,12}$')


# ── Request / Response models ────────────────────────────────────────────────

class ClientRegisterRequest(BaseModel):
    client_name: str
    client_code: str
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
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT client_id FROM client_config ORDER BY client_id DESC LIMIT 1")
            )
            row = result.fetchone()

            if row and row[0]:
                last_id = row[0]
                number_part = last_id.split("-")[-1]
                next_number = int(number_part) + 1
                return f"CLT-{next_number:03d}"
            else:
                return "CLT-001"
    except Exception as e:
        log.error("Could not generate client_id: %s", e)
        raise HTTPException(status_code=500, detail="Could not generate client ID")


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/clients/register", dependencies=[Depends(get_current_user)])
def register_client(
    req: ClientRegisterRequest,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if user["role"] != "super_admin":
        raise HTTPException(
            status_code=403,
            detail="Only super admins can register new clients",
        )

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

    new_client_id = _generate_next_client_id()

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


_CLIENT_COLS = (
    "client_id, client_name, client_code, created_at, is_active, deactivated_at, "
    "address, city, state_province, postal_code, country, contact_email, company_phone"
)


def _client_row_to_dict(r) -> dict:
    return {
        "client_id":      r[0],
        "client_name":    r[1],
        "client_code":    r[2],
        "created_at":     r[3].isoformat() if r[3] else None,
        "is_active":      bool(r[4]),
        "deactivated_at": r[5].isoformat() if r[5] else None,
        "address":        r[6],
        "city":           r[7],
        "state_province": r[8],
        "postal_code":    r[9],
        "country":        r[10],
        "contact_email":  r[11],
        "company_phone":  r[12],
    }


@router.get("/clients", dependencies=[Depends(get_current_user)])
def list_clients(
    includeInactive: bool = Query(default=False),
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    show_inactive = includeInactive and user["role"] == "super_admin"
    active_clause = "" if show_inactive else "WHERE is_active = TRUE"

    try:
        with engine.connect() as conn:
            if user["role"] == "super_admin" or "*" in user.get("clientAccess", []):
                rows = conn.execute(
                    text(
                        f"SELECT {_CLIENT_COLS} "
                        f"FROM client_config "
                        f"{active_clause} "
                        f"ORDER BY client_id"
                    )
                ).fetchall()
            else:
                client_list = user.get("clientAccess", [])
                if not client_list:
                    return []
                placeholders = ", ".join([f":c{i}" for i in range(len(client_list))])
                params = {f"c{i}": cid for i, cid in enumerate(client_list)}
                rows = conn.execute(
                    text(
                        f"SELECT {_CLIENT_COLS} "
                        f"FROM client_config "
                        f"WHERE is_active = TRUE AND client_id IN ({placeholders}) "
                        f"ORDER BY client_id"
                    ),
                    params,
                ).fetchall()

        return [_client_row_to_dict(r) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/clients/{client_id}", dependencies=[Depends(get_current_user)])
def get_client(client_id: str, authorization: Optional[str] = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")

    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

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
    client_name: str
    client_code: str
    contact_name: str
    contact_email: str
    password: str


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
    smtp_from = os.getenv("SMTP_FROM", smtp_user) or "no-reply@loyaltix.io"

    app_url = os.getenv("APP_URL", "https://loyaltix.io").rstrip("/")

    if not smtp_user:
        log.warning("SMTP_USER not configured — skipping welcome email to %s", to_email)
        return

    subject = "Welcome to Loyaltix — Your Account is Ready"

    # Logo is a base64 data URI — renders in Gmail, Outlook, Apple Mail,
    # and Yopmail without any CID attachment or external URL.
    html_body = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Welcome to Loyaltix</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6fb;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6fb;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:12px;overflow:hidden;
                    box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <tr><td style="background:linear-gradient(90deg,#ef5f24,#f8991e,#219bcb);height:4px;"></td></tr>

        <tr>
          <td style="padding:28px 40px 20px;border-bottom:1px solid #eef0f5;text-align:center;">
            <img src="http://10.0.0.14/Crp_QA/media/Loyaltix_logo-5HUR76FK.svg" alt="Loyaltix" height="50" width="150"
                 style="display:block;margin:0 auto;height:auto;border:0;max-width:100%;" />
            <div style="font-size:12px;color:#888;margin-top:6px;">
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
                  <a href="{app_url}/login"
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
              This email was sent because an account was created at Loyaltix.<br/>
              If you did not register, please contact your administrator immediately.<br/>
              &copy; 2026 Loyaltix. All rights reserved.
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
        f"Welcome to Loyaltix, {contact_name}!\n\n"
        f"Your account for {company_name} has been created.\n\n"
        f"Account Details:\n"
        f"  Company Name : {company_name}\n"
        f"  Company Code : {client_code}\n"
        f"  Client ID    : {client_id}\n"
        f"  Login Email  : {to_email}\n"
        f"  Temp Password: {password}\n\n"
        f"Sign in at: {app_url}/login\n\n"
        f"If you did not register, please contact your administrator immediately.\n"
        f"© 2026 Loyaltix. All rights reserved."
    )

    try:
        # Plain "alternative" wrapper — no multipart/related needed because
        # the logo is now a data URI inside the HTML, not a CID attachment.
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
    pw_error = validate_password(req.password)
    if pw_error:
        raise HTTPException(status_code=400, detail=pw_error)

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

    new_client_id = _generate_next_client_id()

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


# ── Client Data Overview ─────────────────────────────────────────────────────

_DATA_OVERVIEW_TABLES = [
    ("customers",          "uploaded",  "Customers",          "account_created_date"),
    ("orders",             "uploaded",  "Orders",             "order_date"),
    ("line_items",         "uploaded",  "Line Items",         None),
    ("customer_reviews",   "uploaded",  "Customer Reviews",   "review_date"),
    ("support_tickets",    "uploaded",  "Support Tickets",    "opened_date"),
    ("customer_rfm_features",     "generated", "RFM Features",            "computed_at"),
    ("customer_purchase_cycles",  "generated", "Purchase Cycles",         "computed_at"),
    ("churn_scores",              "generated", "Churn Scores",            "scored_at"),
    ("retention_interventions",   "generated", "Retention Interventions", "created_at"),
    ("outreach_messages",         "generated", "Outreach Messages",       "sent_at"),
]

_UPLOAD_MASTER_TYPE = {
    "customers":        "customer",
    "orders":           "order",
    "line_items":       "line_items",
    "customer_reviews": "customer_reviews",
    "support_tickets":  "support_tickets",
}


@router.get("/clients/{client_id}/data-overview", dependencies=[Depends(get_current_user)])
def get_client_data_overview(
    client_id: str,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if user["role"] != "super_admin" and "*" not in user.get("clientAccess", []):
        if client_id not in user.get("clientAccess", []):
            raise HTTPException(status_code=403, detail="You don't have access to this client")

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
                "uploaded_at": None,
            }
            try:
                count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE client_id = :cid"
                count = conn.execute(text(count_sql), {"cid": client_id}).scalar() or 0
                entry["row_count"] = int(count)

                ts = None
                if count > 0:
                    if ts_col:
                        ts = conn.execute(
                            text(f"SELECT MAX({ts_col}) FROM {table_name} WHERE client_id = :cid"),
                            {"cid": client_id},
                        ).scalar()
                    elif table_name == "line_items":
                        ts = conn.execute(
                            text(
                                "SELECT MAX(o.order_date) FROM line_items li "
                                "JOIN orders o ON o.client_id = li.client_id "
                                "AND o.order_id = li.order_id WHERE li.client_id = :cid"
                            ),
                            {"cid": client_id},
                        ).scalar()
                if ts is not None:
                    entry["last_updated"] = (
                        ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                    )

                master_type = _UPLOAD_MASTER_TYPE.get(table_name)
                if master_type:
                    up = conn.execute(
                        text(
                            "SELECT MAX(ts) FROM audit_log WHERE client_id = :cid "
                            "AND action_type = 'file_upload' "
                            "AND split_part(details, ' · ', 1) = :mt"
                        ),
                        {"cid": client_id, "mt": master_type},
                    ).scalar()
                    if up is not None:
                        entry["uploaded_at"] = (
                            up.isoformat() if hasattr(up, "isoformat") else str(up)
                        )
            except Exception as e:
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

_TABLE_VIEW_META: dict[str, dict[str, str]] = {
    "customers":                {"order_by": "account_created_date", "direction": "DESC"},
    "orders":                   {"order_by": "order_date",            "direction": "DESC"},
    "line_items":               {"order_by": "line_item_id",          "direction": "ASC"},
    "customer_reviews":         {"order_by": "review_date",           "direction": "DESC"},
    "support_tickets":          {"order_by": "opened_date",           "direction": "DESC"},
    "customer_rfm_features":    {"order_by": "computed_at",           "direction": "DESC"},
    "customer_purchase_cycles": {"order_by": "computed_at",           "direction": "DESC"},
    "churn_scores":             {"order_by": "scored_at",             "direction": "DESC"},
    "retention_interventions":  {"order_by": "created_at",            "direction": "DESC"},
    "outreach_messages":        {"order_by": "sent_at",               "direction": "DESC"},
}


def _jsonify_cell(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


@router.get("/clients/{client_id}/data/{table}")
def get_client_table_rows(
    client_id: str,
    table: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    user = _find_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if user["role"] != "super_admin" and "*" not in user.get("clientAccess", []):
        if client_id not in user.get("clientAccess", []):
            raise HTTPException(status_code=403, detail="You don't have access to this client")

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
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid"),
                {"cid": client_id},
            ).scalar() or 0

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

class AdminCreateClientRequest(BaseModel):
    organization_name: str
    address: str
    city: str
    state_province: str
    postal_code: str
    country: str
    company_contact_email: str
    company_phone: str
    admin_name: str
    admin_phone: str
    admin_email: str
    password: str


def validate_admin_create_payload(req: "AdminCreateClientRequest") -> list[str]:
    errors: list[str] = []

    def _require(value: str, label: str) -> None:
        if not (value or "").strip():
            errors.append(f"{label} is required.")

    _require(req.organization_name, "Organization name")
    _require(req.address, "Address")
    _require(req.city, "City")
    _require(req.state_province, "State / Province")
    _require(req.postal_code, "Zip / Postal code")
    _require(req.country, "Country")
    _require(req.admin_name, "Admin name")

    def _max_len(value: str, limit: int, label: str) -> None:
        if len((value or "").strip()) > limit:
            errors.append(f"{label} must be {limit} characters or fewer.")

    _max_len(req.organization_name,     100, "Organization name")
    _max_len(req.address,               255, "Address")
    _max_len(req.city,                  100, "City")
    _max_len(req.state_province,        100, "State / Province")
    _max_len(req.postal_code,            20, "Zip / Postal code")
    _max_len(req.country,               100, "Country")
    _max_len(req.company_contact_email, 150, "Company contact email")
    _max_len(req.admin_name,            100, "Admin name")
    _max_len(req.admin_email,           150, "Admin login email")

    cce = (req.company_contact_email or "").strip()
    if not cce or not EMAIL_RE.match(cce):
        errors.append("A valid company contact email is required.")
    ae = (req.admin_email or "").strip()
    if not ae or not EMAIL_RE.match(ae):
        errors.append("A valid admin login email is required.")

    cp = (req.company_phone or "").strip()
    if not cp:
        errors.append("Company phone is required.")
    elif not PHONE_RE.match(cp):
        errors.append("Company phone must be 10–12 digits.")
    ap = (req.admin_phone or "").strip()
    if not ap:
        errors.append("Admin phone is required.")
    elif not PHONE_RE.match(ap):
        errors.append("Admin phone must be 10–12 digits.")

    pw_error = validate_password(req.password)
    if pw_error:
        errors.append(pw_error)
    return errors


def _resolve_client_code(client_id: str) -> str:
    return client_id


@router.post("/clients/admin-create", dependencies=[Depends(get_current_user)])
def admin_create_client(
    req: AdminCreateClientRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can create clients")

    errors = validate_admin_create_payload(req)
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))

    org_name    = req.organization_name.strip()
    admin_email = req.admin_email.strip()

    try:
        with engine.connect() as conn:
            dup_email = conn.execute(
                text("SELECT user_id FROM users WHERE LOWER(email) = LOWER(:email)"),
                {"email": admin_email},
            ).fetchone()
            if dup_email:
                raise HTTPException(status_code=409, detail="An account with this email already exists")

            dup_client = conn.execute(
                text("SELECT client_id FROM client_config WHERE LOWER(client_name) = LOWER(:name)"),
                {"name": org_name},
            ).fetchone()
            if dup_client:
                raise HTTPException(
                    status_code=409,
                    detail=f"A company named '{org_name}' is already registered (ID: {dup_client[0]})",
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    new_client_id   = _generate_next_client_id()
    new_client_code = _resolve_client_code(new_client_id)
    new_user_id     = f"usr-{uuid.uuid4().hex[:6]}"

    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO client_config
                        (client_id, client_name, client_code,
                         address, city, state_province, postal_code, country,
                         contact_email, company_phone)
                    VALUES
                        (:cid, :name, :code,
                         :address, :city, :state, :postal, :country,
                         :contact_email, :company_phone)
                """),
                {
                    "cid":           new_client_id,
                    "name":          org_name,
                    "code":          new_client_code,
                    "address":       req.address.strip(),
                    "city":          req.city.strip(),
                    "state":         req.state_province.strip(),
                    "postal":        req.postal_code.strip(),
                    "country":       req.country.strip(),
                    "contact_email": req.company_contact_email.strip(),
                    "company_phone": req.company_phone.strip(),
                },
            )
            conn.execute(
                text("""
                    INSERT INTO users (user_id, email, password_hash, name, role, client_access, phone)
                    VALUES (:uid, :email, :pw, :name, 'client_user', :access, :phone)
                """),
                {
                    "uid":    new_user_id,
                    "email":  admin_email,
                    "pw":     hash_password(req.password),
                    "name":   req.admin_name.strip(),
                    "access": [new_client_id],
                    "phone":  req.admin_phone.strip(),
                },
            )
    except Exception as e:
        log.error("admin_create_client failed: %s", e)
        raise HTTPException(
            status_code=500,
            detail="Could not create the client due to a server error. Please review the form and try again.",
        )

    log.info(
        "Admin-created client: %s → %s, admin: %s (%s)",
        org_name, new_client_id, req.admin_name.strip(), admin_email,
    )

    log_audit_event(
        request,
        action_type="client_created",
        details=f"Created client {new_client_id} · {org_name} · admin {admin_email}",
        client_id=new_client_id,
        user_id=caller["id"],
        user_email=caller["email"],
        outcome="success",
    )

    _send_welcome_email(
        to_email=admin_email,
        contact_name=req.admin_name.strip(),
        company_name=org_name,
        client_id=new_client_id,
        client_code=new_client_code,
        password=req.password,
    )

    return {
        "client_id":   new_client_id,
        "client_name": org_name,
        "client_code": new_client_code,
        "user_email":  admin_email,
        "message":     f"Client {new_client_id} created successfully with administrator {admin_email}.",
    }


# ── Admin: soft-delete (deactivate) a client ─────────────────────────────────

@router.delete("/clients/{client_id}", dependencies=[Depends(get_current_user)])
def delete_client(
    client_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can delete clients")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT is_active FROM client_config WHERE client_id = :cid"),
            {"cid": client_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
        if row[0] is False:
            return {
                "client_id": client_id,
                "deleted": {},
                "message": f"Client {client_id} was already deactivated.",
            }

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


# ── Reactivate (soft-undelete) a client ──────────────────────────────────────

@router.post("/clients/{client_id}/reactivate", dependencies=[Depends(get_current_user)])
def reactivate_client(
    client_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    token = authorization.replace("Bearer ", "")
    caller = _find_user_by_token(token)
    if not caller:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if caller["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Only super admins can reactivate clients")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT is_active FROM client_config WHERE client_id = :cid"),
            {"cid": client_id},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
        if row[0] is True:
            return {
                "client_id": client_id,
                "message": f"Client {client_id} was already active.",
            }

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