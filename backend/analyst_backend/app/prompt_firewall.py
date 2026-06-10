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

# Flip to "enforce" once monitor-mode logs confirm the false-positive rate is low.
_MODE = "monitor"

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


def guard_question(question: str, client_id: str | None = None) -> str:
    """Screen a user chat question for prompt injection.

    Returns the question unchanged (monitor mode never alters the prompt). Logs a
    warning, scoped to client_id, when the input is suspicious. In enforce mode a
    high/critical-severity prompt raises HTTPException(400). Never breaks the request
    on its own errors — security tooling must not take the chat endpoint down.
    """
    if not _ENABLED or not isinstance(question, str):
        return question

    try:
        verdict = _FW.detector.detect(question[: _POLICY.max_input_length])
    except Exception as exc:
        log.error("prompt_firewall detection failed (%s); passing input through.", exc)
        return question

    if verdict.is_suspicious:
        log.warning(
            "prompt_firewall %s: suspicious input client=%s severity=%s risk=%s categories=%s",
            _MODE.upper(), client_id, verdict.severity, verdict.risk_score,
            ",".join(verdict.matched_categories),
        )
        if _MODE == "enforce" and verdict.severity in ("high", "critical"):
            from fastapi import HTTPException
            raise HTTPException(
                status_code=400,
                detail="Your message was blocked by the security filter.",
            )

    return question
