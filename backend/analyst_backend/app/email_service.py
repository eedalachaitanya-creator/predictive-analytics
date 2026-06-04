"""
email_service.py — Simple SMTP email sender
Configure via .env:
  SMTP_HOST=smtp.office365.com
  SMTP_PORT=587
  SMTP_USER=noreply@stixis.com
  SMTP_PASSWORD=Stixis@123
  SMTP_FROM=Predictive Analytics <noreply@stixis.com>
"""
import smtplib
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger("email_service")

SMTP_HOST     = os.getenv("SMTP_HOST", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM    = os.getenv("SMTP_FROM", SMTP_USER)


def send_email(to: str, subject: str, html_body: str) -> bool:
    """
    Send an HTML email. Returns True on success, False on failure.
    Fails silently (logs error) so a missing SMTP config never
    breaks registration or forgot-password.
    """
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASSWORD:
        log.warning("Email not sent — SMTP_HOST/SMTP_USER/SMTP_PASSWORD not configured.")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = to
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, to, msg.as_string())

        log.info("Email sent to %s: %s", to, subject)
        return True
    except Exception as e:
        log.error("Failed to send email to %s: %s", to, e)
        return False