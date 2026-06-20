"""Temporal churn pipeline orchestrator (build → train → score) with a
bulletproof fallback — the single entry point the live pipeline calls.

DESIGN (pipeline integration):
  * The live pipeline first runs its existing train→evaluate→score stages, which
    populate ``churn_scores`` with the LEGACY model's predictions (the baseline).
  * THEN this orchestrator runs as one additional stage and, on success,
    OVERWRITES ``churn_scores`` with the temporal (forward-90-day) predictions.
  * If the tenant lacks enough history (too few cutoffs / churners) or the
    leakage gate hard-fails, ``run_or_fallback`` returns ``("fallback", reason)``
    WITHOUT raising and WITHOUT touching ``churn_scores`` — the baseline rows
    stand, so the dashboard simply shows the legacy model for that tenant.

This makes the temporal stage incapable of leaving the system worse than the
proven baseline: it can only replace good scores with better ones, or no-op.
"""
from __future__ import annotations

import logging
from typing import Optional, Tuple

from sqlalchemy import create_engine

# Imported as module-level names so tests can monkeypatch the seams.
from ml.temporal_dataset import build_dataset, ensure_snapshots_table
from ml.train_temporal import run as train_temporal_run
from ml.score_temporal import score as score_temporal, export_scores_to_disk
from ml.leakage_gate import LeakageGateError
from ml.temporal_windows import resolve_windows, resolve_login_window, ensure_distinct_cadence

logger = logging.getLogger("ml.temporal_pipeline")

# Exceptions that mean "this tenant can't support a temporal model right now"
# (insufficient cutoffs/positives, degenerate split, a banned leaky feature).
# All are caught and converted to a graceful fallback, never propagated.
_FALLBACK_ERRORS = (LeakageGateError, ValueError)


def run_or_fallback(
    client_id: str,
    db_url: str,
    *,
    write: bool = True,
    label_window_days: Optional[int] = None,   # None -> resolve from client_config
    cadence_days: Optional[int] = None,         # None -> resolve from client_config
    min_positives_per_cutoff: int = 30,
    max_cutoffs: int = 15,
    test_frac: float = 0.20,
) -> Tuple[str, str]:
    """Run the temporal path for one tenant, falling back gracefully on any
    insufficiency. Returns ``(mode, reason)`` where ``mode`` is ``"temporal"``
    (churn_scores overwritten with temporal predictions) or ``"fallback"``
    (left untouched). NEVER raises.
    """
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        # Per-tenant windows: label_window <- churn_window_days, cadence <-
        # snapshot_cadence_days (from client_config). Explicit args override.
        if label_window_days is None or cadence_days is None:
            r_label, r_cadence = resolve_windows(engine, client_id)
            if label_window_days is None:
                label_window_days = r_label
            if cadence_days is None:
                cadence_days = r_cadence
        # Guard the assembled pair (covers the half-explicit case where an
        # explicit label collides with a resolved cadence): generate_cutoffs
        # rejects cadence == label_window.
        cadence_days = ensure_distinct_cadence(label_window_days, cadence_days)
        # Recent-login feature window (separate from cadence) — login_window_days.
        login_window_days = resolve_login_window(engine, client_id)

        ensure_snapshots_table(engine)

        dataset = build_dataset(
            engine, client_id,
            label_window_days=label_window_days,
            cadence_days=cadence_days,
            min_positives_per_cutoff=min_positives_per_cutoff,
            max_cutoffs=max_cutoffs,
            login_window_days=login_window_days,
            write=True,
        )
        if dataset is None or dataset.empty:
            reason = "no temporal snapshots (insufficient order history for any cutoff)"
            logger.info("temporal_pipeline[%s]: fallback — %s", client_id, reason)
            return "fallback", reason

        result = train_temporal_run(
            client_id=client_id, db_url=db_url,
            label_window_days=label_window_days, test_frac=test_frac,
        )

        if write:
            score_temporal(
                engine, client_id,
                db_url=db_url, bundle_path=result["bundle_path"], write=True,
            )
            # The legacy Stage-7 wrote churn_scores.{csv,json} on disk, but we
            # just overwrote the churn_scores TABLE with temporal predictions.
            # Stage-12 copies the DISK files into pipeline_outputs (what the
            # Downloads page serves), so re-derive them from the now-temporal
            # table to keep downloads in lock-step with the dashboard. Best-effort:
            # a disk-export failure must never downgrade a successful temporal run
            # to a fallback (the DB scores are already temporal).
            try:
                export_scores_to_disk(engine, client_id)
            except Exception as exc:  # noqa: BLE001 — export is non-critical
                logger.warning("temporal_pipeline[%s]: churn_scores disk export "
                               "failed (Downloads may show legacy scores): %s",
                               client_id, exc)

        pr = (result.get("metrics") or {}).get("pr_auc")
        pr_str = f" pr_auc={pr:.4f}" if isinstance(pr, (int, float)) else ""
        reason = f"winner={result.get('winner')}{pr_str}"
        logger.info("temporal_pipeline[%s]: temporal — %s", client_id, reason)
        return "temporal", reason

    except _FALLBACK_ERRORS as exc:
        reason = f"{type(exc).__name__}: {exc}"
        logger.warning("temporal_pipeline[%s]: fallback — %s", client_id, reason)
        return "fallback", reason
    except Exception as exc:  # noqa: BLE001 — never let this stage fail the pipeline
        reason = f"unexpected {type(exc).__name__}: {exc}"
        logger.exception("temporal_pipeline[%s]: fallback (unexpected) — %s",
                         client_id, reason)
        return "fallback", reason
    finally:
        engine.dispose()


# ──────────────────────────────────────────────────────────────────────────────
# CLI — the live pipeline shells `python -m ml.temporal_pipeline`. ALWAYS exits 0
# (both temporal-success and graceful-fallback) so the pipeline stage can never
# be marked failed; the chosen mode is printed as `MODE=temporal|fallback`.
# ──────────────────────────────────────────────────────────────────────────────

def _parse_args(argv=None):
    import argparse

    p = argparse.ArgumentParser(
        description="Temporal churn pipeline (build→train→score) with fallback.")
    p.add_argument("--client-id", required=True)
    p.add_argument("--db-url", default=None,
                   help="Postgres URL (falls back to DB_URL / DATABASE_URL env).")
    p.add_argument("--no-write", dest="write", action="store_false",
                   help="Compute but do not overwrite churn_scores.")
    p.add_argument("--label-window-days", type=int, default=None,
                   help="Forward label window. Default: tenant churn_window_days.")
    p.add_argument("--cadence-days", type=int, default=None,
                   help="Snapshot cadence. Default: tenant login_window_days.")
    p.add_argument("--min-positives-per-cutoff", type=int, default=30)
    p.add_argument("--max-cutoffs", type=int, default=15,
                   help="Cap training to the most recent N cutoffs (run cost).")
    p.add_argument("--test-frac", type=float, default=0.20)
    p.set_defaults(write=True)
    return p.parse_args(argv)


def main(argv=None) -> int:
    import os

    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    db_url = args.db_url or os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("MODE=fallback reason=no-db-url")
        return 0  # still exit 0: a config gap must not fail the pipeline stage

    mode, reason = run_or_fallback(
        args.client_id, db_url, write=args.write,
        label_window_days=args.label_window_days, cadence_days=args.cadence_days,
        min_positives_per_cutoff=args.min_positives_per_cutoff,
        max_cutoffs=args.max_cutoffs,
        test_frac=args.test_frac,
    )
    print(f"MODE={mode} reason={reason}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
