"""
strategist/services/email_service.py
=====================================
Sends retention offer emails via Office 365 SMTP.
Credentials are read from .env via app.config.settings — never hardcoded.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _build_subject(offer_type: str, discount_pct: any, tier: str, first_name: str = "") -> str:
    pct = float(discount_pct or 0)
    greeting = first_name if first_name else f"valued {tier} member"
    if offer_type == "re_engagement":
        return f"We miss you, {greeting} — exclusive picks waiting for you"
    if pct > 0:
        return f"Your exclusive {int(pct)}% offer, {greeting}"
    return f"A personal message for you, {greeting}"


def _build_html(offer_message: str, customer_id: str) -> str:
    paragraphs = "".join(
        f"<p>{p.strip()}</p>"
        for p in offer_message.split(".")
        if p.strip()
    )
    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;padding:20px;">
      <div style="background:#f8f9fa;padding:20px;border-radius:8px;">
        {paragraphs}
        <hr style="margin:20px 0;border:none;border-top:1px solid #dee2e6;">
        <p style="color:#6c757d;font-size:12px;">
          This is a personalised offer for you.
          To unsubscribe, reply with UNSUBSCRIBE.
        </p>
      </div>
    </body></html>
    """


async def send_retention_emails(
    interventions: list,
    customer_emails: dict[str, str],
    customer_names: dict[str, str] | None = None,
) -> dict:
    """
    Send retention offer emails for all email-channel interventions.
    Reads SMTP credentials from .env via settings — never from code.
    """
    from app.config import settings

    if not settings.smtp_enabled:
        logger.info("email_service: SMTP_ENABLED=false — skipping.")
        return {"sent": 0, "skipped": len(interventions), "failed": 0}

    if not settings.smtp_user or not settings.smtp_password:
        logger.warning("email_service: SMTP credentials not set in .env — skipping.")
        return {"sent": 0, "skipped": len(interventions), "failed": 0}

    email_interventions = [i for i in interventions if i.channel == "email"]
    if not email_interventions:
        return {"sent": 0, "skipped": 0, "failed": 0}

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        _send_batch_sync,
        email_interventions,
        customer_emails,
        settings,
        customer_names or {},
    )


def _send_batch_sync(interventions, customer_emails, settings, customer_names=None) -> dict:
    """Synchronous SMTP batch — runs in thread pool so async loop is not blocked."""
    sent = skipped = failed = 0

    try:
        server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(settings.smtp_user, settings.smtp_password)
    except Exception as exc:
        logger.error("email_service: SMTP connection failed: %s", exc)
        return {"sent": 0, "skipped": 0, "failed": len(interventions)}

    for intervention in interventions:
        to_email = customer_emails.get(intervention.customer_id)
        if not to_email:
            logger.debug("email_service: no email for %s — skipping", intervention.customer_id)
            skipped += 1
            continue

        try:
            msg = MIMEMultipart("alternative")
            msg["From"]    = settings.smtp_from or settings.smtp_user
            msg["To"]      = to_email
            customer_name = (customer_names or {}).get(intervention.customer_id, "")
            first_name = customer_name.split()[0] if customer_name else ""
            msg["Subject"] = _build_subject(
                intervention.offer_type,
                float(intervention.discount_pct or 0),
                intervention.risk_tier,
                first_name,
            )
            msg.attach(MIMEText(intervention.offer_message, "plain"))
            msg.attach(MIMEText(
                _build_html(intervention.offer_message, intervention.customer_id),
                "html"
            ))
            server.sendmail(settings.smtp_user, to_email, msg.as_string())
            sent += 1
            logger.info(
                "email_service: sent to %s (%s)",
                intervention.customer_id, to_email
            )
        except Exception as exc:
            logger.error(
                "email_service: failed for %s: %s",
                intervention.customer_id, exc
            )
            failed += 1

    try:
        server.quit()
    except Exception:
        pass

    logger.info(
        "email_service: batch complete — sent=%d skipped=%d failed=%d",
        sent, skipped, failed
    )
    return {"sent": sent, "skipped": skipped, "failed": failed}