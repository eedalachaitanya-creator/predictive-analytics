"""Resolve per-tenant temporal windows from client_config.

label_window_days  <- churn_window_days  (the tenant's "no purchase in N days" rule)
cadence_days       <- login_window_days  (sampling cadence; login can't be a label)

NEVER raises: a missing row, bad value, or read error yields safe defaults so the
temporal stage can always proceed (and fall back later if the data is insufficient).
"""
from __future__ import annotations

import logging
from typing import Optional, Tuple

from sqlalchemy import text

logger = logging.getLogger("ml.temporal_windows")

DEFAULT_LABEL_WINDOW_DAYS = 90
DEFAULT_CADENCE_DAYS = 30


def _coerce_positive_int(v) -> Optional[int]:
    try:
        iv = int(v)
    except (TypeError, ValueError):
        return None
    return iv if iv > 0 else None


def ensure_distinct_cadence(label: int, cadence: int) -> int:
    """Guarantee cadence != label_window — generate_cutoffs() rejects equality
    (the H7 anti-leakage guard). Nudges by one day on collision. Single source of
    truth so every assembly point (resolver AND half-explicit wiring) agrees.
    A single ±1 step can never re-collide (label±1 != label in integers)."""
    if cadence == label:
        return label - 1 if label > 1 else label + 1
    return cadence


def _resolve(cw, lw, default_label: int = DEFAULT_LABEL_WINDOW_DAYS,
             default_cadence: int = DEFAULT_CADENCE_DAYS) -> Tuple[int, int]:
    """Pure policy: raw (churn_window, login_window) -> (label_window, cadence)."""
    label_opt = _coerce_positive_int(cw)
    cadence_opt = _coerce_positive_int(lw)
    label = label_opt if label_opt is not None else default_label
    cadence = cadence_opt if cadence_opt is not None else default_cadence
    return label, ensure_distinct_cadence(label, cadence)


def resolve_windows(engine, client_id: str, *,
                    default_label: int = DEFAULT_LABEL_WINDOW_DAYS,
                    default_cadence: int = DEFAULT_CADENCE_DAYS) -> Tuple[int, int]:
    """Read churn_window_days / login_window_days for `client_id` and resolve."""
    cw = lw = None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT churn_window_days, login_window_days "
                     "FROM client_config WHERE client_id = :c"),
                {"c": client_id},
            ).first()
        if row is not None:
            cw, lw = row[0], row[1]
    except Exception as exc:  # noqa: BLE001 — never let a config read break the stage
        logger.warning("resolve_windows[%s]: config read failed (%s); using defaults",
                       client_id, exc)
    label, cadence = _resolve(cw, lw, default_label, default_cadence)
    logger.info("resolve_windows[%s]: label_window=%dd cadence=%dd "
                "(churn_window=%s login_window=%s)", client_id, label, cadence, cw, lw)
    return label, cadence
