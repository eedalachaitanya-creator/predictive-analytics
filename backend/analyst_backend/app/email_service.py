"""
email_service.py — Simple SMTP email sender + shared branding asset.

Configure via .env:
  SMTP_HOST=smtp.office365.com
  SMTP_PORT=587
  SMTP_USER=noreply@stixis.com
  SMTP_PASSWORD=Stixis@123
  SMTP_FROM=Predictive Analytics <noreply@stixis.com>

BRANDING — single source of truth
----------------------------------
The Loyaltix logo is embedded directly INSIDE each email as a base64
data URI (data:image/png;base64,...) in the <img src="..."> attribute.

WHY WE SWITCHED FROM CID TO BASE64 DATA URI:
  - CID inline attachments work in Gmail and Outlook but are BLOCKED by
    Yopmail and many other webmail clients / spam filters, which strip
    multipart/related attachments and leave a broken-image icon.
  - A base64 data URI is part of the HTML itself — no separate MIME part,
    no external request, nothing for a mail client to strip. It renders
    correctly in Gmail, Outlook, Apple Mail, and Yopmail.
  - Trade-off: the email is slightly larger (logo PNG ~20 KB → ~27 KB
    base64), but well within any SMTP size limit.

HOW IT WORKS:
  - At module load, LOGO_B64 reads the PNG from disk once and encodes it.
  - Every template uses: <img src="{{ LOGO_SRC }}" ...>
    where LOGO_SRC = "data:image/png;base64,<encoded bytes>"
  - If the file is missing, LOGO_SRC falls back to "" (no broken icon,
    just a missing image — the alt text is always present as fallback).
"""
import base64
import smtplib
import logging
import os
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger("email_service")

SMTP_HOST     = os.getenv("SMTP_HOST", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM    = os.getenv("SMTP_FROM", SMTP_USER)

# ── Branding (single source of truth) ───────────────────────────────────────
# Logo is read once at startup and cached as a base64 data URI string.
# Path: backend/analyst_backend/static/loyaltix-logo.png
LOGO_PATH = Path(__file__).resolve().parent.parent / "static" / "loyaltix-logo.png"

def _build_logo_src() -> str:
    """
    Read the logo PNG from disk and return a base64 data URI string.
    This is called ONCE at module import and the result is cached in LOGO_SRC.

    Returns "" if the file is missing — callers should always set a meaningful
    alt="" on the <img> tag so the email still makes sense without the image.
    """
    try:
        data = LOGO_PATH.read_bytes()
        b64  = base64.b64encode(data).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception as e:
        log.warning("Could not load logo from %s: %s — emails will have no logo image.", LOGO_PATH, e)
        return ""

# Cached at import time — zero I/O per email sent.
LOGO_SRC = _build_logo_src()
print("LOGO_PATH =", LOGO_PATH)
print("FILE_EXISTS =", LOGO_PATH.exists())
print("LOGO_SRC_LENGTH =", len(LOGO_SRC))

def send_email(to: str, subject: str, html_body: str) -> bool:
    """
    Send an HTML email with the Loyaltix logo embedded as a base64 data URI.

    Templates reference the logo via LOGO_SRC (imported from this module):
        from app.email_service import LOGO_SRC
        html = f'<img src="{LOGO_SRC}" alt="Loyaltix" width="200" ...>'

    Returns True on success, False on failure. Fails silently (logs error)
    so a missing SMTP config never breaks registration or forgot-password.
    """
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASSWORD:
        log.warning("Email not sent — SMTP_HOST/SMTP_USER/SMTP_PASSWORD not configured.")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = to

        # The logo is already baked into html_body as a data URI — no
        # multipart/related wrapper or CID attachment needed any more.
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