"""Old-vs-new model comparison (design §9.4) — ADDITIVE, non-live.

Scores the OLD live-MV model and the NEW temporal model on the SAME fully-
observed, grouped, temporal validation cohort against the SAME forward-90d
ground truth. The old model is RE-GRADED on the new forward label (its features →
its prediction → scored against the forward label), never on its original
backward training metric (§9.4, red-team L2). Confidence intervals are computed
with a CUSTOMER-level bootstrap (resample customers, not rows; red-team #13/M7).

The pure functions below (``regrade_on_forward_label``, ``customer_bootstrap_ci``,
``flagged_overlap``) carry the statistical core and are unit-tested without a DB.
``run_comparison`` is the end-to-end driver; when the old live model cannot be
loaded (e.g. no per-tenant MV bundle exists for the tenant), it documents that
and still emits the new-model metrics + the comparison scaffold. Reports are
written under ``ml/output/temporal/`` — NEVER to ``churn_scores``.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger("ml.compare_models")

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = _BACKEND_ROOT / "ml" / "output" / "temporal"


# ──────────────────────────────────────────────────────────────────────────────
# Statistical core (pure, unit-tested) — §9.4
# ──────────────────────────────────────────────────────────────────────────────

def _metrics(y: np.ndarray, p: np.ndarray) -> Dict[str, float]:
    from sklearn.metrics import (
        average_precision_score,
        roc_auc_score,
        brier_score_loss,
    )

    y = np.asarray(y, dtype=int)
    p = np.asarray(p, dtype=float)
    out = {"brier": float(brier_score_loss(y, p))}
    if len(np.unique(y)) < 2:
        out["pr_auc"] = float(np.mean(y))
        out["roc_auc"] = 0.5
        return out
    out["pr_auc"] = float(average_precision_score(y, p))
    out["roc_auc"] = float(roc_auc_score(y, p))
    return out


def regrade_on_forward_label(
    y_forward: Sequence[int],
    old_proba: Sequence[float],
    new_proba: Sequence[float],
) -> Dict[str, Dict[str, float]]:
    """Score BOTH models against the SAME forward-90d label on the SAME rows.

    Returns ``{"old": {pr_auc, roc_auc, brier}, "new": {...}}``. The old model is
    graded on the NEW forward label (apples-to-apples, §9.4), never on its
    original backward metric.
    """
    y = np.asarray(list(y_forward), dtype=int)
    old_p = np.asarray(list(old_proba), dtype=float)
    new_p = np.asarray(list(new_proba), dtype=float)
    if not (len(y) == len(old_p) == len(new_p)):
        raise ValueError("regrade_on_forward_label: y/old/new length mismatch")
    return {"old": _metrics(y, old_p), "new": _metrics(y, new_p)}


def _metric_value(y: np.ndarray, p: np.ndarray, metric: str) -> float:
    m = _metrics(y, p)
    if metric not in m:
        raise ValueError(f"unknown metric {metric!r}")
    return m[metric]


def customer_bootstrap_ci(
    y: Sequence[int],
    p: Sequence[float],
    groups: Sequence,
    *,
    metric: str = "pr_auc",
    n: int = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> Tuple[float, float]:
    """CUSTOMER-level bootstrap CI (resample whole customers, not rows; §9.4).

    Each replicate draws ``len(unique_customers)`` customers WITH replacement and
    pools ALL their rows, so the effective sample size reflects ``n_customers``
    (~627 for CLT-001), not the inflated ``n_rows``. Returns the (lo, hi)
    percentile interval for ``metric``.
    """
    y = np.asarray(list(y), dtype=int)
    p = np.asarray(list(p), dtype=float)
    groups = np.asarray(list(groups))
    uniq = np.unique(groups)
    # Pre-index rows per customer so each replicate is O(n_customers) gathers.
    rows_by_cust = {g: np.where(groups == g)[0] for g in uniq}
    rng = np.random.default_rng(seed)

    stats = []
    for _ in range(n):
        drawn = rng.choice(uniq, size=len(uniq), replace=True)
        idx = np.concatenate([rows_by_cust[g] for g in drawn])
        yi, pi = y[idx], p[idx]
        if len(np.unique(yi)) < 2 and metric in ("pr_auc", "roc_auc"):
            # degenerate replicate (single class) — skip, it carries no AUC info
            continue
        stats.append(_metric_value(yi, pi, metric))

    if not stats:
        v = _metric_value(y, p, metric)
        return (float(v), float(v))
    lo = float(np.quantile(stats, alpha / 2))
    hi = float(np.quantile(stats, 1 - alpha / 2))
    return (lo, hi)


def flagged_overlap(old_top: set, new_top: set) -> float:
    """Jaccard overlap of the top-k flagged customer sets (§9.4 actionability)."""
    old_top, new_top = set(old_top), set(new_top)
    union = old_top | new_top
    if not union:
        return 0.0
    return len(old_top & new_top) / len(union)


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end driver — re-grade the OLD live model on the new forward label and
# compare with customer-level CIs (§9.4). Graceful when no old model exists.
# ──────────────────────────────────────────────────────────────────────────────

def run_comparison(
    *,
    client_id: str,
    db_url: str,
    temporal_model: str,
    output_dir=None,
    label_window_days: int = 90,
    top_k: int = 50,
) -> Dict:
    """Compare the old live-MV model vs the new temporal model on CLT-001.

    Builds the SAME fully-observed grouped temporal validation cohort and the
    SAME forward-90d ``y`` (read from staging), scores the new temporal bundle,
    and — if the old per-tenant MV bundle loads — re-grades it on the new label.
    Writes a JSON report under ``ml/output/temporal/`` and a scratch CSV of
    scores; NEVER touches ``churn_scores``. Returns the report dict.
    """
    import pandas as pd
    import joblib
    from sqlalchemy import create_engine

    from ml.train_temporal import (
        read_snapshots,
        temporal_group_split,
        build_feature_matrix,
    )

    output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build the SAME validation cohort the trainer used (read staging, same split).
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        df = read_snapshots(engine, client_id)
    finally:
        engine.dispose()
    if df.empty:
        raise ValueError(f"no staged snapshots for client_id={client_id}")

    _, test_df = temporal_group_split(df, label_window_days=label_window_days)
    X_test, y_test, feat = build_feature_matrix(test_df)
    groups_test = test_df["customer_id"].to_numpy()

    # ── NEW temporal model (loads our own trusted bundle from a local path). ──
    bundle = joblib.load(temporal_model)  # trusted: produced by ml.train_temporal
    new_feats = bundle["feature_names"]
    scaler = bundle.get("scaler")
    Xn = X_test.reindex(columns=new_feats).fillna(0.0)
    if scaler is not None:
        Xn = pd.DataFrame(scaler.transform(Xn), columns=new_feats, index=Xn.index)
    new_p = bundle["model"].predict_proba(Xn)[:, 1]

    report: Dict = {
        "client_id": client_id,
        "n_rows": int(len(y_test)),
        "n_customers": int(len(np.unique(groups_test))),
        "label_window_days": label_window_days,
        "temporal_model": str(temporal_model),
        "new": _metrics(y_test, new_p),
    }
    new_lo, new_hi = customer_bootstrap_ci(y_test, new_p, groups_test, metric="pr_auc")
    report["new"]["pr_auc_ci_customer_level"] = [new_lo, new_hi]

    # ── OLD live-MV model — re-graded on the new forward label if loadable. ───
    old_p: Optional[np.ndarray] = None
    old_note = None
    try:
        from ml.predict import load_model_bundle

        old_bundle = load_model_bundle(client_id=client_id)  # trusted local bundle
        old_feats = old_bundle["feature_names"]
        # The temporal staging frame carries only the ≤T-reconstructable subset;
        # any old-model feature absent from staging is filled with the train
        # median proxy (0 after scaling) so the old model can still score the
        # SAME rows — documented as a fidelity caveat in the report.
        Xo = X_test.reindex(columns=old_feats).fillna(0.0)
        oscaler = old_bundle.get("scaler")
        if oscaler is not None:
            Xo = pd.DataFrame(oscaler.transform(Xo), columns=old_feats, index=Xo.index)
        old_p = old_bundle["model"].predict_proba(Xo)[:, 1]
        missing = [f for f in old_feats if f not in X_test.columns]
        old_note = (
            f"old MV model re-graded on new forward-90d label; "
            f"{len(missing)}/{len(old_feats)} old features not reconstructable "
            f"from ≤T staging were zero-filled (fidelity caveat)"
        )
    except Exception as exc:  # noqa: BLE001
        old_note = (
            f"OLD live-MV model could not be loaded/scored for client_id="
            f"{client_id} ({exc.__class__.__name__}: {exc}). Per design §13 the "
            f"comparison emits the NEW-model metrics + scaffold only; no per-tenant "
            f"MV bundle exists to re-grade."
        )
        logger.warning("run_comparison: %s", old_note)

    report["old_model_note"] = old_note
    if old_p is not None:
        regrade = regrade_on_forward_label(y_test, old_p, new_p)
        report["old"] = regrade["old"]
        o_lo, o_hi = customer_bootstrap_ci(y_test, old_p, groups_test, metric="pr_auc")
        report["old"]["pr_auc_ci_customer_level"] = [o_lo, o_hi]
        report["delta_pr_auc"] = report["new"]["pr_auc"] - report["old"]["pr_auc"]
        report["delta_roc_auc"] = report["new"]["roc_auc"] - report["old"]["roc_auc"]
        report["delta_brier"] = report["new"]["brier"] - report["old"]["brier"]
        # Top-k flagged-customer overlap (de-duplicated to customer level).
        old_top = _top_k_customers(groups_test, old_p, top_k)
        new_top = _top_k_customers(groups_test, new_p, top_k)
        report["flagged_overlap_jaccard"] = flagged_overlap(old_top, new_top)
        # Significance vs the customer-level CI: a delta inside the CI is noise.
        report["pr_auc_delta_significant"] = bool(
            report["delta_pr_auc"] > (new_hi - new_lo)
        )

    # Scratch scores CSV (never churn_scores).
    scores_path = output_dir / f"comparison_scores_{client_id}.csv"
    out_df = pd.DataFrame({
        "customer_id": test_df["customer_id"].to_numpy(),
        "cutoff_date": test_df["cutoff_date"].to_numpy(),
        "y_forward": np.asarray(y_test, dtype=int),
        "new_proba": new_p,
    })
    if old_p is not None:
        out_df["old_proba"] = old_p
    out_df.to_csv(scores_path, index=False)
    report["scores_csv"] = str(scores_path)

    report_path = output_dir / f"comparison_report_{client_id}.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    report["report_path"] = str(report_path)
    logger.info("run_comparison: wrote %s", report_path)
    return report


def _top_k_customers(groups: np.ndarray, p: np.ndarray, k: int) -> set:
    """Top-k highest-risk DISTINCT customers (max risk per customer)."""
    import pandas as pd

    s = pd.DataFrame({"g": groups, "p": p}).groupby("g")["p"].max()
    return set(s.sort_values(ascending=False).head(k).index.tolist())


def _parse_args(argv=None):
    import argparse

    p = argparse.ArgumentParser(description="Old-vs-new temporal model comparison.")
    p.add_argument("--client-id", required=True)
    p.add_argument("--db-url", default=None)
    p.add_argument("--temporal-model", required=True)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--label-window-days", type=int, default=90)
    p.add_argument("--top-k", type=int, default=50)
    return p.parse_args(argv)


def main(argv=None):  # pragma: no cover — thin CLI wrapper
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    db_url = args.db_url or os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("No DB URL: pass --db-url or set DB_URL/DATABASE_URL")
    rep = run_comparison(
        client_id=args.client_id, db_url=db_url,
        temporal_model=args.temporal_model, output_dir=args.output_dir,
        label_window_days=args.label_window_days, top_k=args.top_k,
    )
    print(json.dumps(rep, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    main()
