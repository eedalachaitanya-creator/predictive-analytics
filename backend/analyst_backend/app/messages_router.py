"""
messages_router.py — Outreach Messages & Template API
=======================================================
Endpoints:
  GET  /messages/templates           — Load templates (DB or defaults)
  POST /messages/templates           — Save / update templates
  POST /messages/generate-outreach   — Generate personalized outreach emails
  GET  /messages/outreach-history    — Past outreach messages sent

How it works (for CTO review):
  1. Templates live in value_propositions table (16 rows = 4 tiers x 4 risk levels).
     Extended fields (subject, body, active) are stored in a companion
     message_templates table that we create if missing.
  2. Generate-outreach reads churn_scores + mv_customer_features, matches
     each customer to the best template by tier + risk, fills placeholders
     ({customer_name}, {days_since_order}, etc.), and returns the drafts.
  3. Optionally writes drafts to outreach_messages table for audit trail.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db

log = logging.getLogger("crp_api.messages")
router = APIRouter(prefix="/api/v1", tags=["messages"])


# ═══════════════════════════════════════════════════════════════════════════
# Pydantic models
# ═══════════════════════════════════════════════════════════════════════════

class TemplateItem(BaseModel):
    id: str
    tier_name: str
    risk_level: str
    discount_pct: float = 0
    channel: str = "email"
    action_type: str = ""
    message_template: str = ""
    priority: int = 5
    subject: str = ""
    body: str = ""
    active: bool = True
    updatedAt: str = ""


class SaveTemplatesRequest(BaseModel):
    clientId: str
    templates: list[TemplateItem]


class GenerateOutreachRequest(BaseModel):
    clientId: str
    riskFilter: Optional[str] = None      # HIGH, MEDIUM, LOW, or None for all
    tierFilter: Optional[str] = None      # Platinum, Gold, Silver, Bronze
    customerIds: Optional[list[str]] = None  # specific customers, overrides filters
    saveToDb: bool = True                 # persist generated messages


class OutreachDraft(BaseModel):
    customerId: str
    customerName: str
    customerEmail: str
    tier: str
    riskTier: str
    churnProbability: Optional[float] = None
    daysSinceOrder: int = 0
    totalSpend: float = 0
    subject: str
    body: str
    discountPct: float = 0
    channel: str = "email"


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

# Default 16 templates — matches the frontend DEFAULT_TEMPLATES exactly
DEFAULT_TEMPLATES = [
    {"id":"tpl-001","tier_name":"platinum","risk_level":"at_risk","discount_pct":15,"channel":"email_sms","action_type":"Personal Outreach","message_template":"Hi {name}, we miss you! Here's 15% off your favourite category.","priority":1,"subject":"We miss you, {customer_name} - here's 15% off","body":"Hi {customer_name}, it's been {days_since_order} days since your last order. As a valued Platinum member, enjoy 15% off your next purchase. Use code PLAT15. Your favourite: {top_product}. Offer expires in 7 days.","active":True,"updatedAt":""},
    {"id":"tpl-002","tier_name":"platinum","risk_level":"returning","discount_pct":10,"channel":"email","action_type":"Loyalty Reward","message_template":"Great to see you back, {name}! 10% off - no minimum spend.","priority":2,"subject":"Welcome back, {customer_name}! Your 10% loyalty reward is waiting","body":"Great to see you back, {customer_name}! As a Platinum member returning to us, we're rewarding you with 10% off - no minimum spend. Code: BACK10.","active":True,"updatedAt":""},
    {"id":"tpl-003","tier_name":"platinum","risk_level":"reactivated","discount_pct":12,"channel":"email_push","action_type":"Reactivation","message_template":"Welcome back to Platinum, {name}! 12% off your reactivation order.","priority":3,"subject":"{customer_name}, you're back - celebrating with 12% off","body":"Hi {customer_name}, welcome back to Platinum! We've missed you. Enjoy 12% off your reactivation order. Code: REACT12.","active":True,"updatedAt":""},
    {"id":"tpl-004","tier_name":"platinum","risk_level":"new","discount_pct":5,"channel":"email","action_type":"Welcome","message_template":"Congratulations {name} on reaching Platinum status! 5% off to start.","priority":4,"subject":"Welcome to Platinum, {customer_name} - a 5% head start","body":"Congratulations {customer_name} on reaching Platinum status! Start your journey with 5% off. Code: NEW5.","active":True,"updatedAt":""},
    {"id":"tpl-005","tier_name":"gold","risk_level":"at_risk","discount_pct":10,"channel":"email_sms","action_type":"Personal Outreach","message_template":"Hi {name}, don't let your Gold status slip - 10% off.","priority":1,"subject":"{customer_name}, don't let your Gold status slip - 10% off","body":"Hi {customer_name}, we noticed it's been {days_since_order} days. Here's 10% off. Code: GOLD10.","active":True,"updatedAt":""},
    {"id":"tpl-006","tier_name":"gold","risk_level":"returning","discount_pct":8,"channel":"email","action_type":"Loyalty Reward","message_template":"You're back, {name}! Gold members deserve a treat - 8% off.","priority":2,"subject":"You're back, {customer_name}! Enjoy 8% off","body":"Welcome back, {customer_name}! Gold members deserve a treat. Here's 8% off. Code: GOLDBACK8.","active":True,"updatedAt":""},
    {"id":"tpl-007","tier_name":"gold","risk_level":"reactivated","discount_pct":10,"channel":"email_push","action_type":"Reactivation","message_template":"Reactivated and ready - 10% off your Gold comeback.","priority":3,"subject":"Reactivated and ready - 10% off your Gold comeback","body":"Hi {customer_name}, great to see you again! Enjoy 10% off. Code: GREACT10.","active":True,"updatedAt":""},
    {"id":"tpl-008","tier_name":"gold","risk_level":"new","discount_pct":5,"channel":"email","action_type":"Welcome","message_template":"Welcome to Gold, {name} - 5% off to celebrate.","priority":4,"subject":"Welcome to Gold, {customer_name} - 5% off to celebrate","body":"Hi {customer_name}, you've earned Gold status! 5% off your next order. Code: GNEW5.","active":True,"updatedAt":""},
    {"id":"tpl-009","tier_name":"silver","risk_level":"at_risk","discount_pct":8,"channel":"push_sms","action_type":"Personal Outreach","message_template":"Hi {name}, we miss you! Come back with 8% off.","priority":1,"subject":"Come back, {customer_name} - 8% off just for you","body":"Hi {customer_name}, we miss you! {days_since_order} days since your last order. Come back with 8% off. Code: SIL8.","active":True,"updatedAt":""},
    {"id":"tpl-010","tier_name":"silver","risk_level":"returning","discount_pct":5,"channel":"email","action_type":"Loyalty Reward","message_template":"Good to see you again, {name} - 5% off.","priority":2,"subject":"Good to see you again, {customer_name} - 5% off","body":"Welcome back, {customer_name}! Enjoy 5% off as a returning Silver member. Code: SILBACK5.","active":True,"updatedAt":""},
    {"id":"tpl-011","tier_name":"silver","risk_level":"reactivated","discount_pct":7,"channel":"email","action_type":"Reactivation","message_template":"You're reactivated, {name} - 7% off.","priority":3,"subject":"{customer_name}, you're reactivated - 7% off","body":"Hi {customer_name}, glad you're back! 7% off your reactivation order. Code: SREACT7.","active":True,"updatedAt":""},
    {"id":"tpl-012","tier_name":"silver","risk_level":"new","discount_pct":3,"channel":"email","action_type":"Welcome","message_template":"Welcome to Silver, {name} - 3% off your first order.","priority":4,"subject":"Welcome to Silver, {customer_name} - 3% off","body":"Congratulations {customer_name}! You've reached Silver. 3% off your next order. Code: SNEW3.","active":True,"updatedAt":""},
    {"id":"tpl-013","tier_name":"bronze","risk_level":"at_risk","discount_pct":5,"channel":"push_sms","action_type":"Personal Outreach","message_template":"Hi {name}, it's been {days_since_order} days. Come back with 5% off.","priority":1,"subject":"We haven't seen you in a while - 5% off to come back","body":"Hi {customer_name}, it's been {days_since_order} days. Come back with 5% off. Code: BRZ5.","active":True,"updatedAt":""},
    {"id":"tpl-014","tier_name":"bronze","risk_level":"returning","discount_pct":3,"channel":"push","action_type":"Loyalty Reward","message_template":"Welcome back, {name}! 3% off your next order.","priority":2,"subject":"Welcome back, {customer_name}! 3% off","body":"Good to see you, {customer_name}! 3% off your next order. Code: BRZBACK3.","active":True,"updatedAt":""},
    {"id":"tpl-015","tier_name":"bronze","risk_level":"reactivated","discount_pct":5,"channel":"email","action_type":"Reactivation","message_template":"Welcome back! 5% off your reactivation order.","priority":3,"subject":"You're back - 5% off your reactivation order","body":"Hi {customer_name}, welcome back! 5% off your reactivation order. Code: BREACT5.","active":True,"updatedAt":""},
    {"id":"tpl-016","tier_name":"bronze","risk_level":"new","discount_pct":0,"channel":"email","action_type":"Welcome","message_template":"Welcome, {name}! Explore our full catalogue.","priority":4,"subject":"Welcome, {customer_name}! Explore our catalogue","body":"Hi {customer_name}, welcome aboard! Browse our full range at walmart.com.","active":True,"updatedAt":""},
]

# Map churn risk_tier (HIGH/MEDIUM/LOW) to template risk_level
RISK_TO_TEMPLATE_RISK = {
    "HIGH":   "at_risk",
    "MEDIUM": "returning",
    "LOW":    "new",
    None:     "new",
}


def _ensure_message_templates_table(db: Session):
    """
    Create the message_templates table if it doesn't exist.
    This stores the extended fields (subject, body, active) that aren't
    in the original value_propositions seed table.
    """
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS message_templates (
            id          VARCHAR(20) NOT NULL,
            client_id   VARCHAR(20) NOT NULL,
            tier_name   VARCHAR(20) NOT NULL,
            risk_level  VARCHAR(20) NOT NULL,
            discount_pct NUMERIC(5,2) DEFAULT 0,
            channel     VARCHAR(50) DEFAULT 'email',
            action_type VARCHAR(100) DEFAULT '',
            message_template TEXT DEFAULT '',
            priority    INT DEFAULT 5,
            subject     TEXT DEFAULT '',
            body        TEXT DEFAULT '',
            active      BOOLEAN DEFAULT TRUE,
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (client_id, id)
        )
    """))
    db.commit()


def _fill_placeholders(template_body: str, customer: dict) -> str:
    """
    Replace {placeholders} in the template with actual customer data.
    This is the heart of personalization — each placeholder maps to a
    real data point from the customer's profile or churn scores.
    """
    replacements = {
        "{customer_name}":       customer.get("customer_name") or "Valued Customer",
        "{name}":                customer.get("customer_name") or "Valued Customer",
        "{tier}":                customer.get("customer_tier") or "",
        "{discount_pct}":        str(int(customer.get("discount_pct") or 0)),
        "{last_order_date}":     str(customer.get("last_order_date") or ""),
        "{days_since_order}":    str(int(customer.get("days_since_last_order") or 0)),
        "{top_product}":         customer.get("top_product") or "your favourites",
        "{recommended_product}": customer.get("recommended_product") or "our top picks",
        "{support_email}":       customer.get("support_email") or "support@store.com",
    }
    result = template_body
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# 1. GET /messages/templates  — Load templates
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/messages/templates")
def get_templates(clientId: str = Query(...), db: Session = Depends(get_db)):
    """
    Returns the 16 message templates for a given client.
    Tries the message_templates table first; falls back to defaults if empty.
    """
    _ensure_message_templates_table(db)

    rows = db.execute(text("""
        SELECT id, tier_name, risk_level, discount_pct, channel, action_type,
               message_template, priority, subject, body, active, updated_at
        FROM message_templates
        WHERE client_id = :cid
        ORDER BY priority
    """), {"cid": clientId}).fetchall()

    if rows:
        templates = []
        for r in rows:
            templates.append({
                "id":               r.id,
                "tier_name":        r.tier_name,
                "risk_level":       r.risk_level,
                "discount_pct":     float(r.discount_pct or 0),
                "channel":          r.channel or "email",
                "action_type":      r.action_type or "",
                "message_template": r.message_template or "",
                "priority":         r.priority or 5,
                "subject":          r.subject or "",
                "body":             r.body or "",
                "active":           r.active if r.active is not None else True,
                "updatedAt":        str(r.updated_at or ""),
            })
        log.info("Loaded %d templates from DB for %s", len(templates), clientId)
        return templates

    # No saved templates — return defaults
    log.info("No saved templates for %s, returning defaults", clientId)
    return DEFAULT_TEMPLATES


# ═══════════════════════════════════════════════════════════════════════════
# 2. POST /messages/templates  — Save templates
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/messages/templates")
def save_templates(req: SaveTemplatesRequest, db: Session = Depends(get_db)):
    """
    Upserts all 16 templates for a client.
    The frontend sends the full array every time the user clicks "Save All".
    We delete old rows and insert fresh — simple and safe for 16 rows.
    """
    _ensure_message_templates_table(db)

    # Delete existing templates for this client
    db.execute(text("DELETE FROM message_templates WHERE client_id = :cid"),
               {"cid": req.clientId})

    # Insert all templates
    for t in req.templates:
        db.execute(text("""
            INSERT INTO message_templates
                (id, client_id, tier_name, risk_level, discount_pct, channel,
                 action_type, message_template, priority, subject, body, active, updated_at)
            VALUES
                (:id, :cid, :tier, :risk, :disc, :ch,
                 :action, :msg, :pri, :sub, :body, :active, NOW())
        """), {
            "id":     t.id,
            "cid":    req.clientId,
            "tier":   t.tier_name,
            "risk":   t.risk_level,
            "disc":   t.discount_pct,
            "ch":     t.channel,
            "action": t.action_type,
            "msg":    t.message_template,
            "pri":    t.priority,
            "sub":    t.subject,
            "body":   t.body,
            "active": t.active,
        })

    db.commit()
    log.info("Saved %d templates for client %s", len(req.templates), req.clientId)

    # Return the saved templates back
    return [t.model_dump() for t in req.templates]


# ═══════════════════════════════════════════════════════════════════════════
# 3. POST /messages/generate-outreach  — Generate personalized emails
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/messages/generate-outreach")
def generate_outreach(req: GenerateOutreachRequest, db: Session = Depends(get_db)):
    """
    The main outreach generator.

    Flow:
      1. Query churn_scores + mv_customer_features to get customer profiles
      2. Apply filters (risk tier, customer tier, or specific IDs)
      3. For each customer, find the matching template (by tier + risk)
      4. Fill placeholders with real customer data
      5. Optionally save to outreach_messages table
      6. Return the generated drafts

    This is called:
      - Manually from the UI "Generate Outreach" button
      - Automatically after pipeline runs (stage 9)
    """
    client_id = req.clientId

    # ── Step 1: Build the customer query with filters ──
    where_clauses = ["cs.client_id = :cid"]
    params: dict = {"cid": client_id}

    if req.customerIds:
        # Specific customers requested
        placeholders = ", ".join([f":cust_{i}" for i in range(len(req.customerIds))])
        where_clauses.append(f"cs.customer_id IN ({placeholders})")
        for i, cid in enumerate(req.customerIds):
            params[f"cust_{i}"] = cid
    else:
        # Apply risk/tier filters
        if req.riskFilter:
            where_clauses.append("cs.risk_tier = :risk")
            params["risk"] = req.riskFilter
        if req.tierFilter:
            where_clauses.append("mv.customer_tier = :tier")
            params["tier"] = req.tierFilter

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            cs.customer_id,
            cs.churn_probability,
            cs.risk_tier,
            c.customer_name,
            c.customer_email,
            COALESCE(mv.customer_tier, 'Bronze')  AS customer_tier,
            COALESCE(mv.days_since_last_order, 0)  AS days_since_last_order,
            COALESCE(mv.total_spend_usd, 0)        AS total_spend,
            COALESCE(mv.total_orders, 0)            AS total_orders,
            mv.last_order_date
        FROM churn_scores cs
        JOIN customers c
            ON cs.customer_id = c.customer_id AND c.client_id = :cid
        LEFT JOIN mv_customer_features mv
            ON cs.customer_id = mv.customer_id AND mv.client_id = :cid
        WHERE cs.scored_at = (
            SELECT MAX(scored_at) FROM churn_scores WHERE client_id = :cid
        )
        AND {where_sql}
        ORDER BY cs.churn_probability DESC
        LIMIT 500
    """

    rows = db.execute(text(sql), params).fetchall()

    if not rows:
        return {
            "success": True,
            "message": "No customers match the criteria",
            "drafts": [],
            "total": 0
        }

    log.info("Found %d customers for outreach (client=%s)", len(rows), client_id)

    # ── Step 2: Load templates (saved or defaults) ──
    templates = get_templates(clientId=client_id, db=db)
    # Build lookup: (tier_name, risk_level) -> template
    tpl_lookup: dict = {}
    for t in templates:
        key = (t["tier_name"], t["risk_level"])
        tpl_lookup[key] = t

    # ── Step 3: Find top product per customer (for {top_product} placeholder) ──
    top_products = {}
    try:
        tp_rows = db.execute(text("""
            SELECT li.customer_id, p.product_name,
                   SUM(li.quantity) AS qty
            FROM line_items li
            JOIN products p ON li.client_id = p.client_id AND li.product_id = p.product_id
            WHERE li.client_id = :cid
            GROUP BY li.customer_id, p.product_name
            ORDER BY li.customer_id, qty DESC
        """), {"cid": client_id}).fetchall()
        seen = set()
        for r in tp_rows:
            if r.customer_id not in seen:
                top_products[r.customer_id] = r.product_name
                seen.add(r.customer_id)
    except Exception as e:
        log.warning("Could not fetch top products: %s", e)

    # ── Step 3b: Compute max safe discount per customer ──
    #    max_safe_discount = avg margin % across their purchased products
    #    This prevents offering discounts that push the price below cost.
    #    Example: product costs $7, sells for $10 → margin = 30%
    #             → max safe discount = 30% (any more and client loses money)
    customer_max_discount: dict = {}
    try:
        margin_rows = db.execute(text("""
            SELECT li.customer_id,
                   ROUND(AVG(
                       CASE WHEN pp.unit_price_usd > 0 AND pp.cost_price_usd IS NOT NULL
                            THEN ((pp.unit_price_usd - pp.cost_price_usd) / pp.unit_price_usd) * 100
                            ELSE NULL
                       END
                   ), 1) AS avg_margin_pct
            FROM line_items li
            JOIN products p ON li.client_id = p.client_id AND li.product_id = p.product_id
            JOIN product_prices pp ON p.client_id = pp.client_id AND p.product_price_id = pp.price_id
            WHERE li.client_id = :cid
              AND pp.cost_price_usd IS NOT NULL
            GROUP BY li.customer_id
        """), {"cid": client_id}).fetchall()
        for r in margin_rows:
            if r.avg_margin_pct is not None:
                customer_max_discount[r.customer_id] = float(r.avg_margin_pct)
        log.info("Loaded margin data for %d customers (avg margin: %.1f%%)",
                 len(customer_max_discount),
                 sum(customer_max_discount.values()) / max(len(customer_max_discount), 1))
    except Exception as e:
        log.warning("Could not compute margins (cost_price_usd may not exist yet): %s", e)

    # ── Step 4: Generate drafts ──
    drafts = []
    outreach_records = []
    margin_capped_count = 0

    for row in rows:
        # Map DB risk_tier to template risk_level
        tier_lower = (row.customer_tier or "Bronze").lower()
        risk_key = RISK_TO_TEMPLATE_RISK.get(row.risk_tier, "new")

        # Find the matching template
        tpl = tpl_lookup.get((tier_lower, risk_key))
        if not tpl:
            # Fallback: try the generic "new" template for this tier
            tpl = tpl_lookup.get((tier_lower, "new"))
        if not tpl:
            # Ultimate fallback: use the first default template
            tpl = DEFAULT_TEMPLATES[0]

        # Skip inactive templates
        if not tpl.get("active", True):
            continue

        # ── Margin safety check ──
        # Cap the template discount so the client never sells below cost.
        # We keep a 2% buffer so the client still makes *some* profit.
        template_discount = float(tpl["discount_pct"])
        max_margin = customer_max_discount.get(row.customer_id)
        safe_discount = template_discount

        if max_margin is not None and max_margin > 0:
            # Leave 2% margin buffer so client still profits
            max_safe = max(max_margin - 2.0, 0)
            if template_discount > max_safe:
                safe_discount = round(max_safe, 1)
                margin_capped_count += 1

        # Build customer data dict for placeholder filling
        customer_data = {
            "customer_name":         row.customer_name or "Valued Customer",
            "customer_tier":         row.customer_tier or "Bronze",
            "days_since_last_order": row.days_since_last_order or 0,
            "last_order_date":       str(row.last_order_date or ""),
            "discount_pct":          safe_discount,
            "top_product":           top_products.get(row.customer_id, "your favourites"),
            "recommended_product":   "our top picks",
            "support_email":         "support@store.com",
        }

        filled_subject = _fill_placeholders(tpl["subject"], customer_data)
        filled_body    = _fill_placeholders(tpl["body"], customer_data)

        draft = OutreachDraft(
            customerId=row.customer_id,
            customerName=row.customer_name or "",
            customerEmail=row.customer_email or "",
            tier=row.customer_tier or "Bronze",
            riskTier=row.risk_tier or "N/A",
            churnProbability=float(row.churn_probability) if row.churn_probability else None,
            daysSinceOrder=int(row.days_since_last_order or 0),
            totalSpend=float(row.total_spend or 0),
            subject=filled_subject,
            body=filled_body,
            discountPct=safe_discount,
            channel=tpl["channel"],
        )
        drafts.append(draft)

        # Prepare DB record for outreach_messages table
        if req.saveToDb:
            outreach_records.append({
                "cid":      client_id,
                "custid":   row.customer_id,
                "mtype":    "churn_outreach",
                "trigger":  f"Risk: {row.risk_tier}, Churn: {float(row.churn_probability or 0):.2f}",
                "msg":      filled_body,
                "channel":  tpl["channel"],
                "days":     int(row.days_since_last_order or 0),
                "disc":     safe_discount,
            })

    if margin_capped_count > 0:
        log.info("Margin safety: capped discount for %d/%d customers to protect client profit",
                 margin_capped_count, len(drafts))

    # ── Step 5: Save to outreach_messages if requested ──
    if req.saveToDb and outreach_records:
        try:
            for rec in outreach_records:
                db.execute(text("""
                    INSERT INTO outreach_messages
                        (client_id, customer_id, message_type, trigger_reason,
                         message_text, channel, days_overdue, discount_offered, sent_at)
                    VALUES
                        (:cid, :custid, :mtype, :trigger,
                         :msg, :channel, :days, :disc, NOW())
                """), rec)
            db.commit()
            log.info("Saved %d outreach records to DB", len(outreach_records))
        except Exception as e:
            log.warning("Failed to save outreach to DB: %s", e)
            db.rollback()

    log.info("Generated %d outreach drafts", len(drafts))
    margin_msg = f" ({margin_capped_count} capped by margin)" if margin_capped_count > 0 else ""
    return {
        "success": True,
        "message": f"Generated {len(drafts)} outreach emails{margin_msg}",
        "drafts": [d.model_dump() for d in drafts],
        "total": len(drafts),
        "marginCapped": margin_capped_count,
    }


# ═══════════════════════════════════════════════════════════════════════════
# 4. GET /messages/outreach-history  — Past outreach messages
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/messages/outreach-history")
def get_outreach_history(
    clientId: str = Query(...),
    page: int = Query(1, ge=1),
    pageSize: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Returns previously generated outreach messages for audit / review.
    """
    offset = (page - 1) * pageSize

    # Total count
    total_row = db.execute(text("""
        SELECT COUNT(*) AS cnt FROM outreach_messages WHERE client_id = :cid
    """), {"cid": clientId}).fetchone()
    total = total_row.cnt if total_row else 0

    rows = db.execute(text("""
        SELECT message_id, customer_id, message_type, trigger_reason,
               message_text, channel, days_overdue, discount_offered,
               sent_at, responded, outcome
        FROM outreach_messages
        WHERE client_id = :cid
        ORDER BY sent_at DESC
        LIMIT :lim OFFSET :off
    """), {"cid": clientId, "lim": pageSize, "off": offset}).fetchall()

    messages = []
    for r in rows:
        messages.append({
            "messageId":      r.message_id,
            "customerId":     r.customer_id,
            "messageType":    r.message_type,
            "triggerReason":  r.trigger_reason,
            "messageText":    r.message_text,
            "channel":        r.channel,
            "daysOverdue":    r.days_overdue,
            "discountOffered": float(r.discount_offered or 0),
            "sentAt":         str(r.sent_at or ""),
            "responded":      r.responded or False,
            "outcome":        r.outcome,
        })

    return {
        "messages": messages,
        "total": total,
        "page": page,
        "pages": max(1, -(-total // pageSize)),  # ceil division
    }
