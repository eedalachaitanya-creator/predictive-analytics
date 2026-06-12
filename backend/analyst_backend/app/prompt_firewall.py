"""Prompt-injection firewall shim for the Analyst chat endpoint.

Wraps the standalone `ai_firewall` package (a separate, pip-installable Stixis
library). Defensive on purpose: if the package isn't installed, this degrades to a
no-op so the API never breaks — the firewall simply does nothing until present.

Currently MONITOR mode: suspicious prompts are LOGGED (with client_id) but never
blocked, so we can tune thresholds on real traffic before enforcing. To start
rejecting high-severity input, change `_MODE` to "enforce" below.
"""

import logging

log = logging.getLogger("crp_api.firewall")

# ENFORCE: high/critical-severity prompt injections are blocked (HTTP 400) before
# reaching the LLM, and every verdict is persisted to audit_log. Set back to
# "monitor" to detect+log+persist without blocking.
_MODE = "enforce"

try:
    from ai_firewall import Firewall, Policy

    _POLICY = Policy(mode=_MODE)
    _FW = Firewall(_POLICY, logger=log)
    _ENABLED = True
except Exception as exc:  # package missing / import error — fail open (no-op)
    _FW = None
    _POLICY = None
    _ENABLED = False
    log.warning("ai_firewall unavailable (%s); prompt firewall disabled (no-op).", exc)


def guard_question(
    question: str,
    client_id: str | None = None,
    *,
    user: dict | None = None,
    request=None,
) -> str:
    """Screen a user chat question for prompt injection.

    Returns the question unchanged (monitor mode never alters the prompt). On a
    suspicious prompt it (1) logs a warning and (2) PERSISTS the verdict to
    audit_log — so the team can tune thresholds on real traffic (false-positive
    rate per severity/category) and keep an audit trail of what was flagged,
    instead of relying on ephemeral stderr. In enforce mode a high/critical-severity
    prompt raises HTTPException(400). Never breaks the request on its own errors —
    security tooling must not take the chat endpoint down.
    """
    if not _ENABLED or not isinstance(question, str):
        return question

    try:
        verdict = _FW.detector.detect(question[: _POLICY.max_input_length])
    except Exception as exc:
        log.error("prompt_firewall detection failed (%s); passing input through.", exc)
        return question

    if verdict.is_suspicious:
        blocked = _MODE == "enforce" and verdict.severity in ("high", "critical")
        log.warning(
            "prompt_firewall %s: suspicious input client=%s severity=%s risk=%s categories=%s",
            _MODE.upper(), client_id, verdict.severity, verdict.risk_score,
            ",".join(verdict.matched_categories),
        )
        _persist_verdict(verdict, client_id, user, request, blocked)
        if blocked:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=400,
                detail="Your message was blocked by the security filter.",
            )

    return question


def _persist_verdict(verdict, client_id, user, request, blocked) -> None:
    """Write the firewall verdict to audit_log so it survives restarts and is
    queryable (severity/category counts for threshold tuning + an audit trail).
    Stores METADATA ONLY (severity/risk/categories), never the prompt text — so we
    don't persist user input / PII in the audit table. Best-effort: log_audit_event
    never raises, and we guard the surrounding code so auditing can't break chat.
    """
    try:
        from app.audit_logger import log_audit_event
        details = (
            f"{_MODE} · severity={verdict.severity} risk={verdict.risk_score} "
            f"categories={','.join(verdict.matched_categories)}"
        )
        log_audit_event(
            request,
            action_type="prompt_firewall_block" if blocked else "prompt_firewall_flag",
            details=details,
            client_id=client_id,
            user_id=(user or {}).get("id"),
            user_email=(user or {}).get("email"),
            outcome="blocked" if blocked else "flagged",
        )
    except Exception as exc:  # auditing must never break the chat request
        log.error("prompt_firewall audit write failed (%s).", exc)
