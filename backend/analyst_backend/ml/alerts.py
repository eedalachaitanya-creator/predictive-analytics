"""
alerts.py — Subscription Refill Alerts + Outreach Generation
==============================================================
Detects customers who are overdue for subscription product refills,
combines churn risk signals, and generates personalized outreach
emails + structured JSON alerts for the Strategist Agent.

Pipeline:
    1. Query customer_purchase_cycles for overdue refills
    2. Enrich with churn scores and customer profiles
    3. Generate personalized email drafts using LLM (Groq)
    4. Output JSON alerts for Strategist Agent
    5. Log outreach to outreach_messages table

Usage:
    python -m ml.alerts                          # run full pipeline
    python -m ml.alerts --threshold 0            # all overdue (default)
    python -m ml.alerts --threshold 30           # only 30+ days overdue
    python -m ml.alerts --no-email               # JSON alerts only, skip emails
    python -m ml.alerts --output json            # output format
    python -m ml.alerts --dry-run                # preview without saving to DB

Requirements:
    pip install langchain-groq sqlalchemy pandas python-dotenv
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("analyst_agent.alerts")

# ── Paths ──
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
ALERTS_DIR = OUTPUT_DIR / "alerts"
ALERTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Database ──
DB_URL = os.getenv("DATABASE_URL", os.getenv("DB_URL", ""))

# ── LLM ──
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════════════
# 1. DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _get_engine():
    """Create SQLAlchemy engine."""
    if not DB_URL:
        raise ValueError("DATABASE_URL not set in .env file.")
    return create_engine(DB_URL, pool_pre_ping=True)


def _run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute SQL and return DataFrame."""
    engine = _get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    engine.dispose()
    return df


def _execute_write(sql: str, params: dict = None):
    """Execute a write operation (INSERT/UPDATE)."""
    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(text(sql), params or {})
        conn.commit()
    engine.dispose()


# ═══════════════════════════════════════════════════════════════════════════
# 2. DETECT OVERDUE REFILLS
# ═══════════════════════════════════════════════════════════════════════════

def detect_overdue_refills(threshold_days: int = 0) -> pd.DataFrame:
    """
    Find customers with overdue subscription refills.
    Queries mv_customer_features which computes subscription metrics
    from order history + subscription product detection.

    Args:
        threshold_days: Minimum days overdue to include (default: 0 = all overdue)

    Returns:
        DataFrame with overdue customers and their subscription/profile details
    """
    log.info("Detecting overdue refills (threshold: %d+ days)...", threshold_days)

    sql = """
        SELECT
            mv.client_id,
            mv.customer_id,
            mv.subscription_product_count,
            mv.avg_refill_cycle_days,
            mv.days_overdue_for_refill AS days_overdue,
            mv.missed_refill_count,
            mv.last_order_date AS last_purchase_date,
            mv.customer_tier,
            mv.total_spend_usd,
            mv.total_orders,
            mv.avg_order_value_usd,
            mv.avg_rating,
            mv.open_tickets,
            mv.rfm_total_score,
            mv.days_since_last_order,
            mv.is_high_value,
            mv.return_rate_pct,
            mv.total_reviews,
            mv.total_tickets
        FROM mv_customer_features mv
        WHERE mv.subscription_product_count > 0
          AND mv.days_overdue_for_refill >= :threshold
        ORDER BY mv.days_overdue_for_refill DESC
    """
    df = _run_query(sql, {"threshold": threshold_days})
    log.info("Found %d overdue refill records.", len(df))
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 3. ENRICH WITH CHURN SCORES
# ═══════════════════════════════════════════════════════════════════════════

RISK_THRESHOLDS = {"high": 0.65, "medium": 0.35}


def enrich_with_churn_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add churn probability and risk level to overdue refill records.
    Tries database first, falls back to CSV scores.
    """
    if df.empty:
        return df

    log.info("Enriching with churn scores...")

    # Try database
    try:
        customer_ids = df["customer_id"].unique().tolist()
        placeholders = ", ".join([f":id_{i}" for i in range(len(customer_ids))])
        params = {f"id_{i}": cid for i, cid in enumerate(customer_ids)}

        sql = f"""
            SELECT customer_id, churn_probability, risk_tier AS risk_level
            FROM churn_scores
            WHERE scored_at = (SELECT MAX(scored_at) FROM churn_scores)
              AND customer_id IN ({placeholders})
        """
        scores_df = _run_query(sql, params)

        if not scores_df.empty:
            df = df.merge(scores_df, on="customer_id", how="left")
            log.info("Enriched from database (%d scores matched).", len(scores_df))
            return df
    except Exception as e:
        log.warning("DB churn scores unavailable: %s. Trying CSV...", e)

    # Fallback to CSV
    csv_path = OUTPUT_DIR / "churn_scores.csv"
    if csv_path.exists():
        scores_df = pd.read_csv(csv_path)[["customer_id", "churn_probability", "risk_level"]]
        df = df.merge(scores_df, on="customer_id", how="left")
        log.info("Enriched from CSV (%d scores matched).", len(scores_df))
    else:
        log.warning("No churn scores found. Proceeding without churn data.")
        df["churn_probability"] = None
        df["risk_level"] = None

    return df


# ═══════════════════════════════════════════════════════════════════════════
# 4. DETERMINE URGENCY AND DISCOUNT
# ═══════════════════════════════════════════════════════════════════════════

def assign_urgency_and_discount(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign urgency level and recommended discount based on
    days overdue, churn risk, and customer tier.
    """
    if df.empty:
        return df

    log.info("Assigning urgency levels and discounts...")

    def _urgency(row):
        days = row.get("days_overdue", 0) or 0
        risk = str(row.get("risk_level", "")).upper()
        if days >= 30 or risk == "HIGH":
            return "CRITICAL"
        elif days >= 14 or risk == "MEDIUM":
            return "HIGH"
        elif days >= 7:
            return "MEDIUM"
        return "LOW"

    def _discount(row):
        urgency = row.get("urgency", "LOW")
        tier = str(row.get("customer_tier", "")).lower()

        base = {"CRITICAL": 20, "HIGH": 15, "MEDIUM": 10, "LOW": 5}
        discount = base.get(urgency, 5)

        # Boost for high-value tiers
        if tier in ("platinum", "gold"):
            discount += 5

        return min(discount, 30)  # cap at 30%

    df["urgency"] = df.apply(_urgency, axis=1)
    df["discount_offered"] = df.apply(_discount, axis=1)

    log.info("Urgency distribution: %s", df["urgency"].value_counts().to_dict())
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5. GENERATE OUTREACH EMAILS (LLM)
# ═══════════════════════════════════════════════════════════════════════════

def generate_outreach_emails(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate personalized retention emails using Groq LLM.
    Falls back to template-based emails if LLM is unavailable.
    """
    if df.empty:
        return df

    log.info("Generating outreach emails for %d customers...", len(df))

    # Try LLM-powered generation
    if GROQ_API_KEY:
        try:
            return _generate_with_llm(df)
        except Exception as e:
            log.warning("LLM generation failed: %s. Using templates.", e)

    # Fallback to templates
    return _generate_with_templates(df)


def _generate_with_llm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate emails using Groq LLM — BATCHED approach.
    Instead of 1 API call per customer, we send ALL customers in ONE prompt
    and parse the batch response. This cuts Stage 9 from ~200s to ~10s.
    """
    from langchain_groq import ChatGroq
    from langchain_core.messages import HumanMessage, SystemMessage

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.7,
        groq_api_key=GROQ_API_KEY,
        max_tokens=4096,
    )

    # Attach LangFuse callback for cost tracking if available
    try:
        from app.langfuse_tracker import get_langfuse_handler
        handler = get_langfuse_handler(
            trace_name="outreach_email_generation",
            metadata={"component": "alerts", "model": "llama-3.3-70b-versatile"},
        )
        if handler:
            llm = llm.with_config({"callbacks": [handler]})
    except ImportError:
        pass  # LangFuse not available, continue without tracking

    # ── Build ONE batched prompt with all customers ──
    system_msg = SystemMessage(content="""You are a customer retention specialist for a retail platform.
You will receive a batch of customers. For EACH customer, write a short, warm, personalized email (3-4 sentences) to encourage them to reorder.
Include the discount offered. Be friendly but professional. Do not use placeholder brackets.

IMPORTANT FORMAT: Return each email separated by the exact delimiter "---EMAIL_SEP---" on its own line.
Return ONLY the email body texts separated by the delimiter, nothing else. No numbering, no headers.""")

    # Build batch prompt
    customer_blocks = []
    for _, row in df.iterrows():
        customer_id = row.get("customer_id", "Customer")
        sub_count = row.get("subscription_product_count", 1)
        days = row.get("days_overdue", 0)
        tier = row.get("customer_tier", "Valued")
        discount = row.get("discount_offered", 10)
        urgency = row.get("urgency", "MEDIUM")
        spend = row.get("total_spend_usd", 0)

        customer_blocks.append(
            f"Customer: {customer_id} ({tier} tier, ${spend:,.0f} total spend) | "
            f"Products: {sub_count} | Overdue: {days}d | Urgency: {urgency} | Discount: {discount}%"
        )

    batch_prompt = f"Generate {len(customer_blocks)} emails, one for each customer below:\n\n"
    batch_prompt += "\n".join(f"{i+1}. {block}" for i, block in enumerate(customer_blocks))

    # ── Single LLM call for entire batch ──
    try:
        log.info("Sending batch of %d customers to LLM (single call)...", len(df))
        response = llm.invoke([system_msg, HumanMessage(content=batch_prompt)])
        raw = response.content.strip()

        # Parse batch response
        email_bodies = [e.strip() for e in raw.split("---EMAIL_SEP---") if e.strip()]

        # If parsing didn't produce enough emails, fall back to templates for missing ones
        if len(email_bodies) < len(df):
            log.warning("LLM returned %d emails for %d customers, filling rest with templates",
                        len(email_bodies), len(df))
            while len(email_bodies) < len(df):
                idx = len(email_bodies)
                email_bodies.append(_template_email(df.iloc[idx]))

        # Trim if LLM returned too many
        email_bodies = email_bodies[:len(df)]

        log.info("Parsed %d emails from single LLM batch call.", len(email_bodies))

    except Exception as e:
        log.warning("Batch LLM generation failed: %s. Using templates for all.", e)
        email_bodies = [_template_email(row) for _, row in df.iterrows()]

    # ── Generate subject lines (no LLM needed) ──
    subjects = []
    for _, row in df.iterrows():
        discount = row.get("discount_offered", 10)
        urgency = row.get("urgency", "MEDIUM")
        if urgency == "CRITICAL":
            subjects.append(f"We miss you! {discount}% off your subscription refill")
        elif urgency == "HIGH":
            subjects.append(f"Time to restock? {discount}% off your essentials")
        else:
            subjects.append(f"Your subscription refill is ready - {discount}% off!")

    df["email_subject"] = subjects
    df["email_body"] = email_bodies
    log.info("Generated %d LLM-powered emails.", len(email_bodies))
    return df


def _generate_with_templates(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback: generate emails from templates."""
    emails = []
    subjects = []

    for _, row in df.iterrows():
        emails.append(_template_email(row))
        discount = row.get("discount_offered", 10)
        subjects.append(f"Your subscription refill is ready - {discount}% off!")

    df["email_subject"] = subjects
    df["email_body"] = emails
    log.info("Generated %d template emails.", len(emails))
    return df


def _template_email(row) -> str:
    """Single template-based email."""
    days = row.get("days_overdue", 0)
    discount = row.get("discount_offered", 10)
    tier = row.get("customer_tier", "Valued")
    sub_count = row.get("subscription_product_count", 1)

    return (
        f"Hi there,\n\n"
        f"We noticed it's been {days} days since your last subscription refill. "
        f"You have {sub_count} subscription product(s) that may need restocking. "
        f"As a {tier} member, we'd love to help you stay stocked up.\n\n"
        f"Use your exclusive {discount}% discount to reorder today. "
        f"Simply visit your account to place your order with the discount applied automatically.\n\n"
        f"Thank you for being a valued customer!\n"
        f"- The Walmart CRP Team"
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. JSON ALERTS FOR STRATEGIST AGENT
# ═══════════════════════════════════════════════════════════════════════════

def generate_json_alerts(df: pd.DataFrame) -> dict:
    """
    Create structured JSON alert payload for the Strategist Agent.
    Contains all data needed for automated decision-making.
    """
    if df.empty:
        return {"generated_at": datetime.now().isoformat(), "total_alerts": 0, "alerts": []}

    alerts = []
    for _, row in df.iterrows():
        alert = {
            "customer_id": row.get("customer_id"),
            "client_id": row.get("client_id"),
            "subscription_product_count": int(row.get("subscription_product_count", 0) or 0),
            "alert_type": "subscription_refill",
            "urgency": row.get("urgency"),
            "days_overdue": int(row.get("days_overdue", 0) or 0),
            "missed_refill_count": int(row.get("missed_refill_count", 0) or 0),
            "avg_refill_cycle_days": float(row.get("avg_refill_cycle_days", 0) or 0),
            "last_purchase_date": str(row.get("last_purchase_date", "")),
            "customer_profile": {
                "tier": row.get("customer_tier"),
                "total_spend_usd": float(row.get("total_spend_usd", 0) or 0),
                "total_orders": int(row.get("total_orders", 0) or 0),
                "avg_order_value_usd": float(row.get("avg_order_value_usd", 0) or 0),
                "avg_rating": float(row.get("avg_rating", 0) or 0),
                "open_tickets": int(row.get("open_tickets", 0) or 0),
                "rfm_total_score": int(row.get("rfm_total_score", 0) or 0),
                "days_since_last_order": int(row.get("days_since_last_order", 0) or 0),
            },
            "churn_signal": {
                "churn_probability": float(row["churn_probability"]) if pd.notna(row.get("churn_probability")) else None,
                "risk_level": row.get("risk_level"),
            },
            "recommended_action": {
                "discount_offered": float(row.get("discount_offered", 0)),
                "channel": "email",
                "email_subject": row.get("email_subject", ""),
                "email_body": row.get("email_body", ""),
            },
        }
        alerts.append(alert)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "total_alerts": len(alerts),
        "summary": {
            "critical": len(df[df["urgency"] == "CRITICAL"]),
            "high": len(df[df["urgency"] == "HIGH"]),
            "medium": len(df[df["urgency"] == "MEDIUM"]),
            "low": len(df[df["urgency"] == "LOW"]),
            "avg_days_overdue": round(df["days_overdue"].mean(), 1),
            "total_customers_affected": df["customer_id"].nunique(),
        },
        "alerts": alerts,
    }

    return payload


# ═══════════════════════════════════════════════════════════════════════════
# 7. SAVE OUTPUTS
# ═══════════════════════════════════════════════════════════════════════════

def save_json_alerts(payload: dict) -> Path:
    """Save JSON alerts to file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = ALERTS_DIR / f"refill_alerts_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved JSON alerts → %s (%d alerts)", out_path, payload["total_alerts"])
    return out_path


def save_email_drafts(df: pd.DataFrame) -> Path:
    """Save email drafts to CSV for review."""
    if df.empty:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = ALERTS_DIR / f"email_drafts_{timestamp}.csv"

    cols = ["customer_id", "subscription_product_count", "days_overdue", "urgency",
            "customer_tier", "churn_probability", "risk_level",
            "discount_offered", "email_subject", "email_body"]
    available = [c for c in cols if c in df.columns]

    df[available].to_csv(out_path, index=False)
    log.info("Saved email drafts → %s (%d emails)", out_path, len(df))
    return out_path


def log_outreach_to_db(df: pd.DataFrame, dry_run: bool = False):
    """Log outreach messages to the outreach_messages table — BULK version."""
    if df.empty or dry_run:
        if dry_run:
            log.info("DRY RUN: Skipping database logging.")
        return

    log.info("Logging %d outreach messages to database (bulk)...", len(df))

    sql = text("""
        INSERT INTO outreach_messages (
            client_id, customer_id, message_type,
            trigger_reason, message_text, channel, days_overdue,
            discount_offered, sent_at
        ) VALUES (
            :client_id, :customer_id, 'refill_alert',
            :trigger_reason, :message_text, 'email', :days_overdue,
            :discount_offered, NOW()
        )
    """)

    # Build all params at once, then execute in ONE connection
    records = []
    for _, row in df.iterrows():
        records.append({
            "client_id": row.get("client_id"),
            "customer_id": row.get("customer_id"),
            "trigger_reason": f"Subscription refill overdue by {row.get('days_overdue', 0)} days (urgency: {row.get('urgency', 'N/A')})",
            "message_text": row.get("email_body", ""),
            "days_overdue": int(row.get("days_overdue", 0) or 0),
            "discount_offered": float(row.get("discount_offered", 0)),
        })

    engine = _get_engine()
    try:
        with engine.begin() as conn:
            for rec in records:
                conn.execute(sql, rec)
        log.info("Outreach messages logged to database.")
    except Exception as e:
        log.warning("Failed to log outreach messages: %s", e)
    finally:
        engine.dispose()


# ═══════════════════════════════════════════════════════════════════════════
# 8. PRINT SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

def print_summary(payload: dict):
    """Print a human-readable summary."""
    summary = payload.get("summary", {})
    total = payload.get("total_alerts", 0)

    print("\n" + "=" * 65)
    print("  SUBSCRIPTION REFILL ALERTS — SUMMARY")
    print("=" * 65)
    print(f"  Generated: {payload.get('generated_at', 'N/A')}")
    print(f"  Total Alerts: {total}")
    print(f"  Unique Customers: {summary.get('total_customers_affected', 0)}")
    print(f"  Avg Days Overdue: {summary.get('avg_days_overdue', 0)}")
    print()
    print("  Urgency Breakdown:")
    print(f"    CRITICAL: {summary.get('critical', 0)}")
    print(f"    HIGH:     {summary.get('high', 0)}")
    print(f"    MEDIUM:   {summary.get('medium', 0)}")
    print(f"    LOW:      {summary.get('low', 0)}")
    print("=" * 65)

    # Show top 5 critical alerts
    alerts = payload.get("alerts", [])
    critical = [a for a in alerts if a.get("urgency") == "CRITICAL"]
    if critical:
        print("\n  TOP CRITICAL ALERTS:")
        print("-" * 65)
        for a in critical[:5]:
            churn = a.get("churn_signal", {}).get("churn_probability")
            churn_str = f"{churn:.2f}" if churn else "N/A"
            tier = a.get("customer_profile", {}).get("tier", "N/A")
            print(
                f"  {a['customer_id']} | {tier:<10s} | "
                f"Overdue: {a['days_overdue']}d | Churn: {churn_str} | "
                f"Discount: {a['recommended_action']['discount_offered']}%"
            )
        print()


# ═══════════════════════════════════════════════════════════════════════════
# 9. MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def run_alerts_pipeline(
    threshold_days: int = 0,
    generate_emails: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Run the full subscription refill alerts pipeline.

    Args:
        threshold_days: Minimum days overdue to trigger alert
        generate_emails: Whether to generate email drafts using LLM
        dry_run: If True, don't write to database

    Returns:
        JSON alerts payload
    """
    log.info("=" * 65)
    log.info("  SUBSCRIPTION REFILL ALERTS PIPELINE")
    log.info("=" * 65)

    # Step 1: Detect overdue refills
    df = detect_overdue_refills(threshold_days)

    if df.empty:
        log.info("No overdue refills found. Pipeline complete.")
        return {"generated_at": datetime.now().isoformat(), "total_alerts": 0, "alerts": []}

    # Step 2: Enrich with churn scores
    df = enrich_with_churn_scores(df)

    # Step 3: Assign urgency and discount
    df = assign_urgency_and_discount(df)

    # Step 4: Generate outreach emails (optional)
    if generate_emails:
        df = generate_outreach_emails(df)

    # Step 5: Build JSON alerts
    payload = generate_json_alerts(df)

    # Step 6: Save outputs
    json_path = save_json_alerts(payload)

    if generate_emails:
        csv_path = save_email_drafts(df)

    # Step 7: Log to database
    log_outreach_to_db(df, dry_run=dry_run)

    # Step 8: Print summary
    print_summary(payload)

    log.info("Pipeline complete.")
    return payload


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Subscription Refill Alerts + Outreach Generation"
    )
    parser.add_argument(
        "--threshold", type=int, default=0,
        help="Minimum days overdue to trigger alert (default: 0)"
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Skip email generation (JSON alerts only)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without saving to database"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_alerts_pipeline(
        threshold_days=args.threshold,
        generate_emails=not args.no_email,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
