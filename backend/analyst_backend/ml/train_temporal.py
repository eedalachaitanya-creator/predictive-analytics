"""Temporal churn trainer (design §7, §8, §9) — ADDITIVE, non-live.

Reads the assembled point-in-time dataset from the ``ml_temporal_snapshots``
staging table (populated by ``ml.temporal_dataset.build_dataset``), applies the
purged + embargoed time-then-group split (§7), grouped CV + grouped/recency-
anchored calibration (§8), PR-AUC winner selection (§8.5), and the leakage gate
(§9). Persists a NON-DISCOVERABLE bundle under ``ml/models/temporal/`` with the
``temporal_`` prefix so ``predict.py:discover_best_model`` cannot auto-promote it.

The estimator builders, scaler, and bundle writer are IMPORTED from
``ml.train_model`` (never copied, never edited). No boundary-row exclusion and no
recency-column amputation: recency-at-T is a first-class feature here because the
label is forward-looking.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Building blocks reused from the live trainer BY IMPORT (design §8.1, §8.7).
# These names are re-exported so the temporal path provably shares the exact
# same objects (verified by tests_temporal/test_trainer_building_blocks.py).
from ml.train_model import (  # noqa: F401  (re-exported on purpose)
    build_xgboost,
    build_random_forest,
    build_model,
    scale_features,
    save_model,
    generate_training_report,
)

logger = logging.getLogger("ml.train_temporal")

# Identifier / bookkeeping columns — never model features (design §6.5). Kept
# through the split for grouping/time-ordering, then dropped from X.
NON_FEATURE_COLS = (
    "client_id", "customer_id", "cutoff_date", "churned",
    "first_order_date", "last_order_date", "last_review_date", "computed_at",
    "snapshot_id",
)


# ──────────────────────────────────────────────────────────────────────────────
# Staging-table reader — train_temporal READS the assembled dataset, never
# rebuilds it (PERFORMANCE constraint). Features live in the JSONB `features`
# column written by ml.temporal_dataset.build_dataset / _write_staging.
# ──────────────────────────────────────────────────────────────────────────────

def read_snapshots(engine, client_id: str) -> pd.DataFrame:
    """Read ``ml_temporal_snapshots`` for one tenant and explode JSONB features.

    Returns a frame with the key columns (client_id, customer_id, cutoff_date,
    churned) plus one column per feature recovered from the JSONB payload — the
    same wide shape ``build_snapshot`` produces, but sourced from staging so the
    trainer does NOT re-run the (slow) point-in-time SQL.
    """
    from sqlalchemy import text

    sql = text(
        """
        SELECT client_id, customer_id, cutoff_date, churned, features
        FROM ml_temporal_snapshots
        WHERE client_id = :client_id
        ORDER BY cutoff_date, customer_id
        """
    )
    with engine.connect() as cx:
        raw = pd.read_sql(sql, cx, params={"client_id": client_id})

    if raw.empty:
        return raw.drop(columns=["features"], errors="ignore")

    # Explode the JSONB payload (already a dict via psycopg, or a JSON string).
    import json

    def _as_dict(v):
        if isinstance(v, dict):
            return v
        if v is None:
            return {}
        return json.loads(v)

    feats = pd.json_normalize(raw["features"].map(_as_dict))
    feats.index = raw.index
    out = pd.concat([raw.drop(columns=["features"]), feats], axis=1)
    out["churned"] = out["churned"].astype(int)
    out["cutoff_date"] = pd.to_datetime(out["cutoff_date"]).dt.date
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Feature matrix — drop identifiers, KEEP recency (design §6.5, §8)
# ──────────────────────────────────────────────────────────────────────────────

def build_feature_matrix(df: pd.DataFrame):
    """Split an assembled snapshot frame into (X, y, feature_names).

    Drops the identifier/bookkeeping columns (§6.5) and the label; retains every
    remaining numeric feature — recency-at-T included. No gray-zone row drop, no
    recency-column amputation. Returns ``(X, y, feature_names)``.
    """
    y = df["churned"].astype(int).reset_index(drop=True)
    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLS]
    # Keep only numeric features (date/string columns are never model inputs).
    X = df[feature_cols].copy()
    numeric = X.select_dtypes(include=[np.number]).columns.tolist()
    X = X[numeric].reset_index(drop=True)
    return X, y, numeric


def compute_scale_pos_weight(y_train) -> float:
    """scale_pos_weight = n_neg / n_pos, computed on the TRAIN split ONLY (§8.2)."""
    y_train = np.asarray(list(y_train), dtype=int)
    n_pos = int((y_train == 1).sum())
    n_neg = int((y_train == 0).sum())
    if n_pos == 0:
        raise ValueError("compute_scale_pos_weight: no positive examples in train")
    return n_neg / n_pos


# ──────────────────────────────────────────────────────────────────────────────
# Step 8 — purged + embargoed time-then-group split (design §7)
# ──────────────────────────────────────────────────────────────────────────────

def temporal_group_split(
    df: pd.DataFrame,
    *,
    label_window_days: int,
    test_frac: float = 0.20,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Time-then-group split with a label-window embargo and group purge (§7).

    Given snapshot rows with ``customer_id``, ``cutoff_date`` (and ``churned``):

      1. choose a boundary cutoff ``T*`` so the latest ~``test_frac`` of the
         distinct cutoffs fall in the test PERIOD, and an embargo boundary
         ``T_emb = T* - label_window_days``;
      2. assign CUSTOMERS (not cutoffs) to exactly one side so the split is
         group-disjoint by construction: a deterministic ~``test_frac`` sample of
         the customers present in the test period (cutoffs ``>= T*``) becomes the
         TEST group; every other customer is a TRAIN customer;
      3. ``test``  = the test customers' rows at cutoffs ``>= T*``;
      4. ``train`` = the train customers' rows at cutoffs ``<= T_emb`` — the
         embargo guarantees no training row's forward window ``(T, T+window]``
         overlaps any test cutoff's feature window.

    Assigning whole customers (rather than purging every customer that recurs
    across the boundary) is what keeps TRAIN non-empty under the absorbing-state,
    strictly-nested cohorts of CLT-001, where every early customer is still active
    at every later cutoff — a blanket purge would empty train. It still satisfies
    all three §7 invariants below. This REPLACES the live trainer's random
    stratified ``train_test_split`` for the temporal path; it does not touch
    train_model.py.
    """
    if df.empty:
        raise ValueError("temporal_group_split: empty dataset")
    if not (0.0 < test_frac < 1.0):
        raise ValueError(f"test_frac must be in (0,1), got {test_frac}")

    cutoffs = sorted(pd.to_datetime(df["cutoff_date"]).dt.date.unique())
    if len(cutoffs) < 2:
        raise ValueError(
            f"temporal_group_split needs >=2 distinct cutoffs, got {len(cutoffs)}"
        )

    # T* = the boundary putting the latest ~test_frac of distinct cutoffs in the
    # test period, walked later if needed so an embargoed-train cutoff survives.
    n_test = max(1, int(round(len(cutoffs) * test_frac)))
    star_idx = len(cutoffs) - n_test
    cutoff_dates = pd.to_datetime(df["cutoff_date"]).dt.date
    embargo = dt.timedelta(days=label_window_days)
    while star_idx < len(cutoffs):
        T_star = cutoffs[star_idx]
        if (cutoff_dates <= (T_star - embargo)).any():
            break
        star_idx += 1
    else:  # pragma: no cover — only if no embargoed train cutoff exists at all
        raise ValueError(
            "temporal_group_split: no train cutoff survives the label-window "
            f"embargo (label_window_days={label_window_days}); widen the date range"
        )

    T_star = cutoffs[star_idx]
    T_emb = T_star - embargo

    test_period = df[cutoff_dates >= T_star]
    embargo_period = df[cutoff_dates <= T_emb]

    # ── §7 step 2 — assign whole customers to exactly one side (group-disjoint).
    test_period_customers = sorted(set(test_period["customer_id"]))
    # Deterministic ~test_frac sample of test-period customers → TEST group.
    rng = np.random.default_rng(0)
    k_test = max(1, int(round(len(test_period_customers) * test_frac)))
    k_test = min(k_test, max(1, len(test_period_customers) - 1))  # leave >=1 for train
    test_customers = set(
        rng.choice(np.array(test_period_customers, dtype=object),
                   size=k_test, replace=False).tolist()
    )

    test = test_period[test_period["customer_id"].isin(test_customers)].copy()
    # TRAIN = every customer NOT in the test group, embargo-clear rows only.
    train = embargo_period[~embargo_period["customer_id"].isin(test_customers)].copy()

    # §7 post-split invariants — fail the run if any is violated.
    if set(train["customer_id"]) & set(test["customer_id"]):
        raise ValueError("§7 violated: a customer_id appears in BOTH train and test")
    if train.empty or test.empty:
        raise ValueError(
            f"§7 split produced an empty side (train={len(train)}, test={len(test)}); "
            "check cutoff coverage vs label_window_days/test_frac"
        )
    tr_max = pd.to_datetime(train["cutoff_date"]).dt.date.max()
    te_min = pd.to_datetime(test["cutoff_date"]).dt.date.min()
    if not (tr_max + embargo <= te_min):
        raise ValueError(
            f"§7 embargo violated: max(train.cutoff)+{label_window_days}d="
            f"{tr_max + embargo} must be <= min(test.cutoff)={te_min}"
        )

    logger.info(
        "temporal_group_split: T*=%s | train rows=%d (<= %s) | test rows=%d (>= %s) | "
        "train customers=%d test customers=%d (purged disjoint)",
        T_star, len(train), tr_max, len(test), te_min,
        train["customer_id"].nunique(), test["customer_id"].nunique(),
    )
    return train, test


# ──────────────────────────────────────────────────────────────────────────────
# Step 11 — grouped CV, grouped calibration, metrics, PR-AUC winner (design §8)
# ──────────────────────────────────────────────────────────────────────────────

def grouped_cv_iterator(X, y, groups, *, n_splits: int = 5):
    """Yield (train_idx, val_idx) from StratifiedGroupKFold on ``groups``.

    Replaces the live trainer's non-grouped ``StratifiedKFold(shuffle=True)`` for
    the temporal path: no customer's near-duplicate rows span train/val within a
    fold (§8.4). ``n_splits`` is clamped to the smaller of the requested value,
    the number of distinct groups, and the minority-class count so calibration on
    small temporal folds never raises.
    """
    from sklearn.model_selection import StratifiedGroupKFold

    y = np.asarray(list(y), dtype=int)
    groups = np.asarray(list(groups))
    n_groups = len(np.unique(groups))
    minority = int(min((y == 0).sum(), (y == 1).sum()))
    safe = max(2, min(n_splits, n_groups, max(minority, 2)))
    sgkf = StratifiedGroupKFold(n_splits=safe, shuffle=True, random_state=0)
    return sgkf.split(X, y, groups=groups)


def fit_calibrated(base_estimator, X, y, groups, *, n_splits: int = 5):
    """Fit ``CalibratedClassifierCV(method='isotonic')`` with a GROUPED CV iterator.

    The wrapper's default internal ``cv=5`` StratifiedKFold is neither grouped nor
    temporal and would calibrate on folds where a customer's near-duplicate rows
    sit on both sides (§8.3). Passing a materialized list of grouped splits forces
    group-disjoint calibration folds.
    """
    from sklearn.calibration import CalibratedClassifierCV

    y = np.asarray(list(y), dtype=int)
    splits = list(grouped_cv_iterator(X, y, groups, n_splits=n_splits))
    calibrated = CalibratedClassifierCV(base_estimator, method="isotonic", cv=splits)
    calibrated.fit(X, y)
    return calibrated


def compute_fold_metrics(y_true, y_proba) -> Dict[str, float]:
    """PR-AUC (primary), ROC-AUC (secondary), Brier (calibration) for one fold."""
    from sklearn.metrics import (
        average_precision_score,
        roc_auc_score,
        brier_score_loss,
    )

    y_true = np.asarray(list(y_true), dtype=int)
    y_proba = np.asarray(list(y_proba), dtype=float)
    out: Dict[str, float] = {"brier": float(brier_score_loss(y_true, y_proba))}
    if len(np.unique(y_true)) < 2:
        # ROC/PR-AUC undefined on a single-class fold (design §8.5 rationale);
        # report the base rate for PR-AUC and 0.5 for ROC-AUC, flagged via NaN-free
        # defaults so winner selection still works.
        out["pr_auc"] = float(np.mean(y_true))
        out["roc_auc"] = 0.5
        return out
    out["pr_auc"] = float(average_precision_score(y_true, y_proba))
    out["roc_auc"] = float(roc_auc_score(y_true, y_proba))
    return out


def precision_at_k(y_true, y_proba, k: int) -> float:
    """Precision among the top-``k`` highest-risk rows (retention-team capacity)."""
    y_true = np.asarray(list(y_true), dtype=int)
    y_proba = np.asarray(list(y_proba), dtype=float)
    k = int(min(max(k, 1), len(y_true)))
    top = np.argsort(-y_proba)[:k]
    return float(y_true[top].mean()) if k else 0.0


def select_winner_by_pr_auc(candidates: Dict[str, Dict[str, float]]) -> str:
    """Pick the model with the highest PR-AUC (§8.5); break ties on lower Brier.

    ``candidates``: ``{model_name: {"pr_auc":..., "roc_auc":..., "brier":...}}``.
    """
    if not candidates:
        raise ValueError("select_winner_by_pr_auc: no candidates")

    def _key(item):
        name, m = item
        # max PR-AUC, then min Brier, then name for determinism.
        return (m.get("pr_auc", 0.0), -m.get("brier", 1.0), name)

    return max(candidates.items(), key=_key)[0]


# ──────────────────────────────────────────────────────────────────────────────
# Step 12 — end-to-end orchestrator: staging → split → gate → calibrate →
# PR-AUC winner → NON-DISCOVERABLE artifact (design §9.3, §10.1, red-team #14)
# ──────────────────────────────────────────────────────────────────────────────

# Default non-discoverable artifact roots (subdir + temporal_ prefix). The live
# discover_best_model glob `churn_model_*_<id>.joblib` in ml/models/ is NOT
# recursive, so a file under ml/models/temporal/ can never be auto-promoted.
from pathlib import Path  # noqa: E402  (kept local to the orchestrator section)

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MODELS_DIR = _BACKEND_ROOT / "ml" / "models" / "temporal"
DEFAULT_OUTPUT_DIR = _BACKEND_ROOT / "ml" / "output" / "temporal"

# Forward-90d label definition — recorded in metadata so a downstream
# check_feature_freeze_drift keys off the new label, not the old MV churn_label.
LABEL_DEFINITION = "forward_no_qualifying_order_in_(T, T+label_window_days]"

_MODEL_TYPES = {"xgboost", "random_forest"}


def _imbalanced_estimator(model_type: str, y_train):
    """Build a base estimator with class-imbalance handling computed on the TRAIN
    split only (§8.2): scale_pos_weight for XGB, class_weight='balanced' for RF."""
    if model_type == "xgboost":
        return build_xgboost(class_weight_ratio=compute_scale_pos_weight(y_train))
    if model_type == "random_forest":
        return build_random_forest(class_weight="balanced")
    raise ValueError(f"unknown model_type {model_type!r}")


def run(
    *,
    client_id: str,
    db_url: str,
    model_type: str = "all",
    models_dir=None,
    output_dir=None,
    label_window_days: int = 90,
    test_frac: float = 0.20,
    min_positives_per_fold: int = 30,
    active_only: bool = True,
    cv_splits: int = 5,
    retention_capacity_k: int = 50,
    whitelist=(),
) -> Dict:
    """Train the temporal model from the staging table and persist the winner.

    Pipeline (design §7–§9): READ ``ml_temporal_snapshots`` (never rebuild) →
    purged/embargoed split (§7) → per-fold §4.5 min-positives guard → fit scaler
    on train only (§8.6) → run the leakage gate on the grouped TRAIN split (abort
    on ``LeakageGateError``; §9.3) → grouped calibration (§8.3) → PR-AUC winner
    (§8.5) → ``save_model`` to a NON-DISCOVERABLE path. Returns a result dict with
    ``bundle_path`` and the persisted metrics/gate.
    """
    import json

    from sqlalchemy import create_engine

    from ml.leakage_gate import run_leakage_gate

    models_dir = Path(models_dir) if models_dir else DEFAULT_MODELS_DIR
    output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
    models_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        df = read_snapshots(engine, client_id)
    finally:
        engine.dispose()
    if df.empty:
        raise ValueError(
            f"no staged snapshots for client_id={client_id} — run "
            f"`python -m ml.temporal_dataset --client-id {client_id} --write` first"
        )

    # §7 purged + embargoed split.
    train_df, test_df = temporal_group_split(
        df, label_window_days=label_window_days, test_frac=test_frac
    )

    # §4.5 per-fold min-positives guard on the TRAINING fold (fail loudly).
    train_pos = int(train_df["churned"].sum())
    if train_pos < min_positives_per_fold:
        raise ValueError(
            f"§4.5 training fold has {train_pos} positives < "
            f"min_positives_per_fold={min_positives_per_fold}; refusing to train a "
            "degenerate model"
        )

    X_train, y_train, feat_train = build_feature_matrix(train_df)
    X_test, y_test, feat_test = build_feature_matrix(test_df)
    # Align test columns to the train feature set (same order).
    X_test = X_test.reindex(columns=feat_train).fillna(0.0)
    groups_train = train_df["customer_id"].to_numpy()

    # Median imputation + scaler fit on TRAIN ONLY (§8.6); applied to test.
    train_medians = X_train.median(numeric_only=True)
    X_train = X_train.fillna(train_medians).fillna(0.0)
    X_test = X_test.fillna(train_medians).fillna(0.0)
    X_train_s, X_test_s, scaler = scale_features(X_train, X_test, method="standard")

    # §9 leakage gate on the grouped TRAIN split (abort on hard fail BEFORE any
    # model is persisted). Uses the UNSCALED train frame for interpretable
    # univariate/stump statistics; the excluded-family + stump checks are
    # scale-invariant either way.
    gate = run_leakage_gate(
        X_train, y_train, groups_train,
        X_test=X_test, y_test=y_test, whitelist=whitelist,
    )  # raises LeakageGateError on a hard fail → no bundle written

    model_types = sorted(_MODEL_TYPES) if model_type == "all" else [model_type]
    candidates: Dict[str, Dict] = {}
    fitted: Dict[str, object] = {}
    for mt in model_types:
        base = _imbalanced_estimator(mt, y_train)
        calibrated = fit_calibrated(base, X_train_s, y_train, groups_train,
                                    n_splits=cv_splits)
        p_test = calibrated.predict_proba(X_test_s)[:, 1]
        metrics = compute_fold_metrics(y_test, p_test)
        metrics["precision_at_k"] = precision_at_k(y_test, p_test, retention_capacity_k)
        candidates[mt] = metrics
        fitted[mt] = calibrated
        logger.info(
            "train_temporal: %s | pr_auc=%.4f roc_auc=%.4f brier=%.4f p@%d=%.4f",
            mt, metrics["pr_auc"], metrics["roc_auc"], metrics["brier"],
            retention_capacity_k, metrics["precision_at_k"],
        )

    winner = select_winner_by_pr_auc(candidates)
    win_metrics = candidates[winner]
    win_model = fitted[winner]

    metadata = {
        "model_type": winner,
        "client_id": client_id,
        "trained_at": dt.datetime.now().isoformat(),
        "temporal": True,
        "label_definition": LABEL_DEFINITION,
        "label_window_days": label_window_days,
        "active_only": active_only,
        "n_train_rows": int(len(X_train)),
        "n_test_rows": int(len(X_test)),
        "n_train_customers": int(train_df["customer_id"].nunique()),
        "n_test_customers": int(test_df["customer_id"].nunique()),
        "train_positives": train_pos,
        "metrics": {
            "pr_auc": win_metrics["pr_auc"],
            "roc_auc": win_metrics["roc_auc"],
            "brier": win_metrics["brier"],
            "precision_at_k": win_metrics["precision_at_k"],
            # AUC-ROC under the live metadata key so any tool reading it still
            # works; PR-AUC is the headline/selection metric.
            "auc_roc": win_metrics["roc_auc"],
            "all_candidates": candidates,
        },
        "leakage_gate": gate,
        "scaler_means": {c: float(v) for c, v in zip(feat_train, scaler.mean_)}
        if scaler is not None else {},
        "train_medians": {c: float(v) for c, v in train_medians.items()},
        # Forward-90d label freeze so check_feature_freeze_drift does not raise.
        "feature_freeze": {
            "label_window_days": label_window_days,
            "label_definition": LABEL_DEFINITION,
        },
    }

    bundle_name = f"churn_model_temporal_{winner}_{client_id}.joblib"
    bundle_path = models_dir / bundle_name
    save_model(win_model, scaler, feat_train, metadata, bundle_path)

    # Reports under the non-discoverable output dir.
    report_path = output_dir / f"temporal_training_report_{client_id}.json"
    report_path.write_text(json.dumps({
        "client_id": client_id,
        "winner": winner,
        "candidates": candidates,
        "leakage_gate_passed": gate["passed"],
        "n_features": len(feat_train),
        "feature_names": feat_train,
    }, indent=2, default=str))

    logger.info(
        "train_temporal: WINNER=%s pr_auc=%.4f → %s (gate passed=%s)",
        winner, win_metrics["pr_auc"], bundle_path, gate["passed"],
    )
    return {
        "bundle_path": str(bundle_path),
        "report_path": str(report_path),
        "winner": winner,
        "metrics": win_metrics,
        "candidates": candidates,
        "leakage_gate": gate,
    }


def _parse_args(argv=None):
    import argparse

    p = argparse.ArgumentParser(
        description="Temporal churn trainer (reads ml_temporal_snapshots; "
                    "additive; non-discoverable artifacts).")
    p.add_argument("--client-id", required=True)
    p.add_argument("--db-url", default=None,
                   help="Postgres URL (falls back to DB_URL / DATABASE_URL env).")
    p.add_argument("--model-type", choices=["xgboost", "random_forest", "all"],
                   default="all")
    p.add_argument("--models-dir", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--label-window-days", type=int, default=90)
    p.add_argument("--test-frac", type=float, default=0.20)
    p.add_argument("--min-positives-per-fold", type=int, default=30)
    p.add_argument("--cv-splits", type=int, default=5)
    p.add_argument("--retention-capacity-k", type=int, default=50)
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--active-only", dest="active_only", action="store_true")
    grp.add_argument("--no-active-only", dest="active_only", action="store_false")
    p.set_defaults(active_only=True)
    return p.parse_args(argv)


def main(argv=None):  # pragma: no cover — thin CLI wrapper around run()
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    db_url = args.db_url or os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("No DB URL: pass --db-url or set DB_URL/DATABASE_URL")
    res = run(
        client_id=args.client_id, db_url=db_url, model_type=args.model_type,
        models_dir=args.models_dir, output_dir=args.output_dir,
        label_window_days=args.label_window_days, test_frac=args.test_frac,
        min_positives_per_fold=args.min_positives_per_fold,
        active_only=args.active_only, cv_splits=args.cv_splits,
        retention_capacity_k=args.retention_capacity_k,
    )
    print(f"WINNER={res['winner']} bundle={res['bundle_path']}")


if __name__ == "__main__":  # pragma: no cover
    main()
