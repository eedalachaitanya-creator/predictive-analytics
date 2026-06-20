"""
predict.py — Analyst Agent | Churn Prediction & Scoring + FastAPI Endpoint
===========================================================================
Scores customers using a trained churn model. Supports two modes:

    1. CLI MODE — Score all customers from DB or CSV, save results
    2. API MODE — FastAPI server with /predict, /predict/batch, /health endpoints

ARCHITECTURE:
    Section 0 — Configuration (paths, constants, risk thresholds)
    Section 1 — Model Loading (auto-discover or explicit path)
    Section 2 — Data Loading (from DB or CSV)
    Section 3 — Feature Preparation (align features, encode tier, scale)
    Section 4 — Scoring (predict probabilities, assign risk levels)
    Section 5 — Output (CSV, database, risk report)
    Section 6 — FastAPI Endpoints (/predict, /predict/batch, /health)
    Section 7 — CLI Pipeline (end-to-end batch scoring)
    Section 8 — Main (CLI or API mode)

Every step is its own function — add new risk tiers, output formats,
or API endpoints without changing the overall structure.

Usage (CLI):
    # Score all customers from database:
    python -m analyst_agent.ml.predict --mode cli --source db

    # Score from CSV with specific model:
    python -m analyst_agent.ml.predict --mode cli --source csv \
        --model-path ml/models/churn_model_random_forest.joblib

Usage (API):
    # Start FastAPI server:
    python -m analyst_agent.ml.predict --mode api --port 8000

    # Then call endpoints:
    # GET  /health                → Server status + model info
    # POST /predict               → Single customer prediction
    # POST /predict/batch         → Batch prediction (list of customers)
    # GET  /scores                → All scored customers (after CLI run)

Requirements:
    pip install fastapi uvicorn pandas numpy joblib psycopg2-binary sqlalchemy python-dotenv
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, List, Any, Optional

import numpy as np
import pandas as pd
import joblib
from dotenv import load_dotenv


def _force_utf8_stdout() -> None:
    """Reconfigure stdout to UTF-8 so the █ risk-bar chars print on a Windows
    console. Called ONLY when this module runs as a script — never at import,
    which would clobber the caller's (and pytest's) stdout."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # py3.7+
    except Exception:  # noqa: BLE001 — best-effort; redirected/odd stdout is fine
        pass

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("predict")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 0: CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
MODEL_DIR = BASE_DIR / "models"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

TARGET_COL = 'churn_label'

# Columns that are NOT features (must match train_model.py)
NON_FEATURE_COLS = [
    'client_id', 'customer_id', 'first_order_date', 'last_order_date',
    'last_review_date', 'computed_at',
]

# Ordinal encoding (must match train_model.py)
TIER_ORDER = {'Bronze': 1, 'Silver': 2, 'Gold': 3, 'Platinum': 4}

# Risk level thresholds on churn_probability — DEFAULTS only.
# Audit issue #7: per-client thresholds are read from `client_config`
# at scoring time when columns `risk_high_threshold` /
# `risk_medium_threshold` exist. The defaults here apply when those
# columns are absent (older schemas) or NULL for a given tenant.
RISK_THRESHOLDS = {
    'high': 0.65,     # >= 0.65 → HIGH risk
    'medium': 0.35,   # >= 0.35 → MEDIUM risk
                       # < 0.35  → LOW risk
}


def load_risk_thresholds(
    db_url: Optional[str],
    client_id: Optional[str],
) -> Dict[str, float]:
    """
    Resolve per-client risk thresholds, falling back to RISK_THRESHOLDS.

    Audit issue #7: the previous design hard-coded 0.65 / 0.35 globally,
    so all tenants got the same cutoffs regardless of their churn
    distribution. This helper reads `risk_high_threshold` and
    `risk_medium_threshold` from the client_config row when they exist
    and are non-null.

    Graceful degradation: if the columns don't exist (older schema),
    the SELECT raises and we fall back to defaults. Same when
    db_url/client_id is missing or the row isn't found. The fallback
    means this fix is safe to deploy without a schema migration; the
    migration just makes the per-client knobs functional.
    """
    if not db_url or not client_id:
        return dict(RISK_THRESHOLDS)
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT risk_high_threshold, risk_medium_threshold
                    FROM client_config
                    WHERE client_id = :cid
                """), {"cid": client_id}).fetchone()
        finally:
            engine.dispose()
    except Exception:
        # Most likely cause: columns don't exist yet. Silently fall
        # through to defaults — the schema migration introducing those
        # columns will activate the per-client behaviour automatically.
        return dict(RISK_THRESHOLDS)

    thresholds = dict(RISK_THRESHOLDS)
    if row is not None:
        if row[0] is not None:
            thresholds['high'] = float(row[0])
        if row[1] is not None:
            thresholds['medium'] = float(row[1])
    if thresholds != RISK_THRESHOLDS:
        log.info(
            "  Per-client risk thresholds for %s: high=%.2f medium=%.2f",
            client_id, thresholds['high'], thresholds['medium'],
        )
    return thresholds

# Business tier adjustments — applied AFTER model scoring to prioritize
# high-value customers. Losing a Platinum customer costs far more than
# losing a Bronze customer, so we shift their churn probability toward
# HIGH risk without destroying calibration.
#
# WHY LOG-ODDS SHIFT INSTEAD OF MULTIPLICATION:
#   The previous implementation multiplied p × weight then clipped to
#   [0, 1]. That saturated at the top: any Platinum customer with raw
#   p ≥ 0.80 became 0.80 × 1.25 = 1.00 → clipped to exactly 1.0. Dozens
#   of customers ended up with identical 100.0% scores even though their
#   true probabilities differed (visible in the Churn Scores page where
#   8+ rows all showed 100.0% on a single screen).
#
#   A log-odds shift is monotonic and non-saturating:
#     logit(p)  = ln(p / (1 - p))    # map (0, 1) → (-∞, +∞)
#     logit(p) + shift               # add bias in log-odds space
#     p_adj    = 1 / (1 + exp(-x))   # map back to (0, 1)
#
#   Ordering is preserved and probabilities stay strictly inside (0, 1) —
#   even a raw 0.99 Platinum ends up at ~0.993, not saturated to 1.000.
#
# BEHAVIOR AT TYPICAL RAW PROBABILITIES (reference table):
#   Raw 0.50 → Platinum +0.4 → 0.599 | Gold +0.2 → 0.550 | Bronze -0.2 → 0.450
#   Raw 0.80 → Platinum +0.4 → 0.856 | Gold +0.2 → 0.830 | Bronze -0.2 → 0.766
#   Raw 0.95 → Platinum +0.4 → 0.966 | Gold +0.2 → 0.958 | Bronze -0.2 → 0.939
TIER_LOGIT_SHIFTS = {
    'Platinum':  0.4,   # aggressive boost (most revenue at stake)
    'Gold':      0.2,   # moderate boost
    'Silver':    0.0,   # no change (baseline)
    'Bronze':   -0.2,   # slight reduction (lower business impact)
}
TIER_LOGIT_SHIFT_DEFAULT = 0.0  # Used when tier is missing or unknown

# Default model preference order (best AUC-ROC first). LogisticRegression
# was removed from the supported model set — see ml/train_model.py — so we
# only fall back between the two tree ensembles here.
MODEL_PREFERENCE = ['random_forest', 'xgboost']

# API default port
DEFAULT_API_PORT = 8000


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: MODEL LOADING
# ═══════════════════════════════════════════════════════════════════════════

def discover_best_model(client_id: Optional[str] = None) -> Path:
    """
    Auto-discover the best available model in MODEL_DIR.

    Per-tenant aware (2026-04-27): when ``client_id`` is provided, only
    per-tenant model files matching ``churn_model_*_<client_id>.joblib``
    are considered, and the AUC winner among those is returned. When
    ``client_id`` is None, the legacy single-global behaviour is used:
    files matching ``churn_model_<type>.joblib`` (no client suffix) are
    considered first, falling back to ANY churn_model_*.joblib if no
    legacy global model exists.

    Selection order WITH client_id:
      1. Highest-AUC ``churn_model_*_<client_id>.joblib`` file
         (reads metadata.metrics.auc_roc from each candidate's joblib)
      2. Raises FileNotFoundError if no per-tenant model exists for
         this client_id (caller should retrain).

    Selection order WITHOUT client_id (legacy/global):
      1. evaluation_summary.json — written by ml/evaluate_model.py
      2. MODEL_PREFERENCE list — hardcoded fallback
      3. Most recently modified .joblib — last-resort fallback

    Returns:
        Path to the selected model file.
    """
    log.info("Auto-discovering best model in %s...", MODEL_DIR)

    # ── Per-tenant path ─────────────────────────────────────────────────────
    if client_id:
        candidates = list(MODEL_DIR.glob(f"churn_model_*_{client_id}.joblib"))
        if not candidates:
            raise FileNotFoundError(
                f"No per-tenant model found for client_id={client_id}. "
                f"Looked for: churn_model_*_{client_id}.joblib in {MODEL_DIR}. "
                f"Did training fail for this client?"
            )

        # Pick the highest-AUC candidate by reading each joblib's metadata.
        best_path = None
        best_auc = -1.0
        for p in candidates:
            try:
                pkg = joblib.load(p)
                auc = pkg.get("metadata", {}).get("metrics", {}).get("auc_roc", 0.0)
                if auc > best_auc:
                    best_auc = auc
                    best_path = p
            except Exception as e:
                log.warning("  Could not read AUC from %s: %s", p.name, e)

        if best_path is None:
            # Fall back to most-recently-modified per-tenant file
            best_path = max(candidates, key=lambda p: p.stat().st_mtime)

        log.info(
            "  Per-tenant winner for %s: %s (AUC-ROC = %.4f)",
            client_id, best_path.name, best_auc if best_auc >= 0 else 0.0,
        )
        return best_path

    # ── Legacy / global path ────────────────────────────────────────────────
    model_files = list(MODEL_DIR.glob("churn_model_*.joblib"))
    if not model_files:
        raise FileNotFoundError(f"No model files found in {MODEL_DIR}")

    # Filter to LEGACY (non-per-tenant) names only — i.e., files of the form
    # `churn_model_<type>.joblib` where <type> is the algorithm name, not a
    # client_id. We detect this by counting underscores: legacy filenames
    # have exactly 2 underscores (churn_model_TYPE.joblib), per-tenant
    # filenames have 3 (churn_model_TYPE_CLT-XXX.joblib).
    legacy_files = [p for p in model_files if p.stem.count("_") == 2]
    if legacy_files:
        # 1) Prefer the AUC-winner recorded by evaluate_model.py.
        summary_path = OUTPUT_DIR / "evaluation_summary.json"
        if summary_path.exists():
            try:
                with summary_path.open("r") as fh:
                    summary = json.load(fh)
                best_name = summary.get("best_model")
                if best_name:
                    expected = MODEL_DIR / f"churn_model_{best_name}.joblib"
                    if expected.exists():
                        log.info(
                            "  Using AUC-winning model from evaluation_summary.json: %s",
                            expected.name,
                        )
                        return expected
                    log.warning(
                        "  evaluation_summary.json names '%s' but %s is missing — falling back.",
                        best_name, expected.name,
                    )
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("  Could not read evaluation_summary.json (%s) — falling back.", exc)

        # 2) Preference order (only reached if the summary is absent or stale)
        for preferred in MODEL_PREFERENCE:
            for f in legacy_files:
                if preferred in f.name:
                    log.info("  Found preferred model: %s", f.name)
                    return f

        # 3) Last resort: most recently modified legacy .joblib
        best = max(legacy_files, key=lambda p: p.stat().st_mtime)
        log.info("  Using most recent legacy model: %s", best.name)
        return best

    # No legacy global model exists — fall back to ANY available model.
    # This typically means someone has trained per-tenant models but is now
    # running predict.py without --client-id. Pick the highest-AUC across
    # all per-tenant files, but warn loudly that this is probably wrong.
    log.warning(
        "  No legacy global model found; only per-tenant models exist. "
        "Picking the highest-AUC across all tenants — but you should "
        "probably pass --client-id <CLT-XXX> to score one tenant at a time."
    )
    best_path = None
    best_auc = -1.0
    for p in model_files:
        try:
            pkg = joblib.load(p)
            auc = pkg.get("metadata", {}).get("metrics", {}).get("auc_roc", 0.0)
            if auc > best_auc:
                best_auc = auc
                best_path = p
        except Exception:
            continue
    if best_path is None:
        best_path = max(model_files, key=lambda p: p.stat().st_mtime)
    log.info("  Falling back to: %s", best_path.name)
    return best_path


def load_model_bundle(
    model_path: Optional[str] = None,
    client_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load a trained model bundle (.joblib) containing model, scaler,
    feature names, and training metadata.

    Args:
        model_path: Explicit path to .joblib file. If provided, used directly.
        client_id: When set AND model_path is None, picks the per-tenant
            AUC-winning model for this client. When both are None, falls
            back to legacy global discovery.

    Returns:
        Dict with keys: 'model', 'scaler', 'feature_names', 'metadata'
    """
    if model_path is None:
        path = discover_best_model(client_id=client_id)
    else:
        path = Path(model_path)
        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")

    log.info("Loading model from %s...", path.name)
    bundle = joblib.load(path)

    model = bundle['model']
    scaler = bundle.get('scaler')
    feature_names = bundle['feature_names']
    metadata = bundle.get('metadata', {})

    log.info("  Model type:  %s", metadata.get('model_type', type(model).__name__))
    log.info("  Features:    %d", len(feature_names))
    log.info("  AUC-ROC:     %s", metadata.get('metrics', {}).get('auc_roc', 'N/A'))
    log.info("  Trained on:  %s", metadata.get('training_date', 'unknown'))

    return {
        'model': model,
        'scaler': scaler,
        'feature_names': feature_names,
        'metadata': metadata,
        'model_path': str(path),
    }


# Fields whose drift between training and scoring time changes the
# meaning of either the LABEL or the feature WINDOWS. If any of these
# differ from what the model was trained against, scoring with that
# model is unsound — the customer's features are computed under one
# definition while the decision boundary the model learned is for
# another. Treated as a hard error unless --ignore-drift is set.
LABEL_DEFINING_FREEZE_FIELDS = {
    'churn_window_days',
    'login_window_days',
    'reference_date_mode',
    'reference_date',
}

# Non-label fields are softer: a change shifts feature distributions but
# doesn't move the label boundary. We log a WARNING and continue.
NON_LABEL_FREEZE_FIELDS = {
    'min_repeat_orders',
    'recent_order_gap_window',
    'tier_method',
    'custom_platinum_min',
    'custom_gold_min',
    'custom_silver_min',
    'custom_bronze_min',
}


def check_feature_freeze_drift(
    metadata: Dict[str, Any],
    db_url: Optional[str],
    client_id: Optional[str],
    ignore_drift: bool = False,
) -> Dict[str, Any]:
    """
    Compare the feature_freeze snapshot saved at training time against
    the live client_config row. Audit issue #3.

    Behavior:
        - For LABEL_DEFINING_FREEZE_FIELDS that differ → raise unless
          ignore_drift is True (in which case we log an ERROR and continue).
        - For NON_LABEL_FREEZE_FIELDS that differ → log a WARNING.
        - Cross-tenant or older-metadata models (no feature_freeze block)
          → skip silently; nothing to compare against.

    Returns:
        Dict describing the comparison so callers / tests can inspect it.
    """
    freeze = metadata.get('feature_freeze') or {}
    if not freeze or freeze.get('mode') != 'per_tenant':
        # No snapshot to compare against — older models, cross-tenant
        # training, or a snapshot that errored at training time.
        log.info("Drift check skipped: no per-tenant feature_freeze in metadata")
        return {'checked': False, 'reason': 'no_per_tenant_freeze'}

    if not client_id or not db_url:
        log.info("Drift check skipped: client_id or db_url missing")
        return {'checked': False, 'reason': 'missing_client_or_db'}

    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT
                        churn_window_days,
                        login_window_days,
                        min_repeat_orders,
                        recent_order_gap_window,
                        tier_method,
                        custom_platinum_min,
                        custom_gold_min,
                        custom_silver_min,
                        custom_bronze_min,
                        reference_date_mode,
                        reference_date
                    FROM client_config
                    WHERE client_id = :cid
                """), {"cid": client_id}).fetchone()
        finally:
            engine.dispose()
    except Exception as e:
        log.warning(
            "Drift check could not query client_config (%s) — proceeding "
            "without comparison.", e,
        )
        return {'checked': False, 'reason': 'query_failed', 'error': str(e)}

    if row is None:
        log.warning("Drift check: no client_config row for %s", client_id)
        return {'checked': False, 'reason': 'no_config_row'}

    live = {
        'churn_window_days':       int(row[0])   if row[0]  is not None else None,
        'login_window_days':       int(row[1])   if row[1]  is not None else None,
        'min_repeat_orders':       int(row[2])   if row[2]  is not None else None,
        'recent_order_gap_window': int(row[3])   if row[3]  is not None else None,
        'tier_method':             row[4],
        'custom_platinum_min':     float(row[5]) if row[5]  is not None else None,
        'custom_gold_min':         float(row[6]) if row[6]  is not None else None,
        'custom_silver_min':       float(row[7]) if row[7]  is not None else None,
        'custom_bronze_min':       float(row[8]) if row[8]  is not None else None,
        'reference_date_mode':     row[9],
        'reference_date':          str(row[10]) if row[10] is not None else None,
    }

    diffs_label: Dict[str, Tuple[Any, Any]] = {}
    diffs_other: Dict[str, Tuple[Any, Any]] = {}
    for field, live_value in live.items():
        train_value = freeze.get(field)
        if train_value != live_value:
            slot = diffs_label if field in LABEL_DEFINING_FREEZE_FIELDS else diffs_other
            slot[field] = (train_value, live_value)

    if not diffs_label and not diffs_other:
        log.info("Drift check: client_config matches training-time snapshot ✓")
        return {'checked': True, 'drifted': False, 'live': live, 'training': dict(freeze)}

    if diffs_other:
        log.warning(
            "Drift check: %d non-label setting(s) changed since training:",
            len(diffs_other),
        )
        for field, (was, now) in diffs_other.items():
            log.warning("    %s: %r → %r", field, was, now)

    if diffs_label:
        msg_lines = [
            f"  {f}: trained_with={was!r}, live={now!r}"
            for f, (was, now) in diffs_label.items()
        ]
        message = (
            f"Label-defining settings have drifted since training "
            f"({len(diffs_label)} field(s)):\n" + "\n".join(msg_lines) +
            "\n\nThe model was trained against a different label/window "
            "definition than the live MV uses. Retrain before scoring, "
            "or pass --ignore-drift to override (predictions will be "
            "biased)."
        )
        if ignore_drift:
            log.error("DRIFT IGNORED VIA --ignore-drift:\n%s", message)
        else:
            raise RuntimeError(message)

    return {
        'checked': True,
        'drifted': True,
        'label_drift': diffs_label,
        'other_drift': diffs_other,
        'live': live,
        'training': dict(freeze),
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_customers_from_db(
    db_url: str,
    client_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load customer features from mv_customer_features.

    Args:
        db_url: PostgreSQL connection string
        client_id: If provided, only load this client's customers

    Returns:
        DataFrame with all columns from the materialized view
    """
    log.info("Loading customers from database...")
    from sqlalchemy import create_engine, text

    # Audit issue #8: dispose the engine even if pd.read_sql raises so
    # the connection pool isn't leaked.
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  Connected successfully")

        if client_id:
            log.info("  Filtering by client_id=%s", client_id)
            df = pd.read_sql(
                text("SELECT * FROM mv_customer_features WHERE client_id = :cid"),
                engine, params={"cid": client_id},
            )
        else:
            df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)
    finally:
        engine.dispose()
    log.info("  Loaded %d customers x %d columns", df.shape[0], df.shape[1])
    return df


def load_customers_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Load customer features from CSV (feature_matrix.csv or customer_features.csv).

    Returns:
        DataFrame with all columns from CSV
    """
    log.info("Loading customers from CSV: %s", csv_path)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    log.info("  Loaded %d customers x %d columns", df.shape[0], df.shape[1])
    return df


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: FEATURE PREPARATION
# ═══════════════════════════════════════════════════════════════════════════

def prepare_features_for_scoring(
    df: pd.DataFrame,
    feature_names: List[str],
    imputation_values: Optional[Dict[str, float]] = None,
    allow_missing_features: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare customer data for model scoring.

    Steps:
        1. Extract customer identifiers (client_id, customer_id)
        2. Encode customer_tier → ordinal (matching train_model.py)
        3. Align columns to the exact feature list the model expects
        4. Fill missing features with 0  (or RAISE if allow_missing_features=False)
        5. Apply training-time imputation values for known semantic-null columns
        6. Safety-net fillna(0) for any remaining NaN

    Args:
        df: Raw customer DataFrame
        feature_names: The exact list of features the model was trained on
        imputation_values: dict of column → fill-value, sourced from
            metadata['imputation_values'] saved at training time. Applied
            BEFORE the safety-net fillna so semantic-null columns
            (median_days_between_orders, etc.) get the same value here as
            they did during training. Audit issue #1.
        allow_missing_features: when False (default), raises if any model
            feature is absent from the scoring DataFrame. When True, the
            old behaviour of silently filling with 0 is preserved (debug
            only). Audit issue #2.

    Returns:
        (X_score, customer_info) — features for model + identifiers for output
    """
    log.info("Preparing features for scoring...")
    df_work = df.copy()

    # Extract identifiers
    id_cols = ['client_id', 'customer_id']
    available_ids = [c for c in id_cols if c in df_work.columns]
    customer_info = df_work[available_ids].copy() if available_ids else pd.DataFrame()

    # Encode customer_tier (raw string → ordinal) if present
    if 'customer_tier' in df_work.columns:
        df_work['customer_tier_encoded'] = (
            df_work['customer_tier'].map(TIER_ORDER).fillna(1).astype(int)
        )
        log.info("  Encoded customer_tier → customer_tier_encoded (1-4)")

    # Align to model features
    missing = [f for f in feature_names if f not in df_work.columns]

    if missing:
        # Audit issue #2: silently filling missing features with 0 produced
        # nonsense scores when the MV dropped a column the model expected.
        # Fail loudly by default; --allow-missing-features keeps the old
        # behaviour for one-off debugging.
        if not allow_missing_features:
            raise RuntimeError(
                f"Model expects {len(feature_names)} features but the input "
                f"DataFrame is missing {len(missing)}: {missing}. The MV may "
                "have drifted from the training schema. Re-run the pipeline "
                "to refresh mv_customer_features, retrain the model, or pass "
                "--allow-missing-features to fall back to zero-fill (NOT "
                "recommended for production scoring)."
            )
        log.warning(
            "  --allow-missing-features set: filling %d missing model "
            "feature(s) with 0 — predictions will be unreliable: %s",
            len(missing), missing,
        )
        for feat in missing:
            df_work[feat] = 0

    X_score = df_work[feature_names].copy()

    # ── Apply training-time imputation (audit issue #1) ─────────────────────
    # Older models without imputation_values in metadata get an empty dict
    # here, so the safety-net fillna(0) below behaves exactly as it did
    # before this fix. New models capture median_days_between_orders /
    # order_gap_mean_median_diff at training time so scoring uses the
    # same value, eliminating train/predict skew on semantic-null columns.
    if imputation_values:
        applied = 0
        for col, value in imputation_values.items():
            if col in X_score.columns and X_score[col].isna().any():
                n = int(X_score[col].isna().sum())
                X_score[col] = X_score[col].fillna(value)
                applied += n
                log.info(
                    "  Imputed %d NULL(s) in '%s' with training-time value %.4f",
                    n, col, float(value),
                )
        if applied == 0 and imputation_values:
            log.info(
                "  Training metadata carried %d imputation value(s) but no "
                "matching NULLs at scoring time — nothing to apply",
                len(imputation_values),
            )

    # Safety-net: any remaining NaN columns get zero-filled. This handles
    # columns that legitimately mean zero when null (e.g., counts), and
    # preserves backward compatibility with older models that don't have
    # imputation_values in their metadata.
    per_col_nan = X_score.isna().sum()
    nan_cols = per_col_nan[per_col_nan > 0]
    if not nan_cols.empty:
        log.warning(
            "  Filling NaN with 0 across %d column(s) (total %d cells). "
            "If a column is semantic-null (\"unknown\" rather than \"zero\"), "
            "add it to the explicit-imputation block in train_model.py.",
            len(nan_cols), int(nan_cols.sum()),
        )
        for col, count in nan_cols.items():
            log.warning("    %-40s  %d NaN", col, int(count))
        X_score = X_score.fillna(0)

    log.info("  Scoring matrix: %d customers x %d features", X_score.shape[0], X_score.shape[1])
    return X_score, customer_info


def scale_for_scoring(
    X_score: pd.DataFrame,
    scaler: Any
) -> pd.DataFrame:
    """
    Apply the trained scaler to scoring features.

    Args:
        X_score: Feature matrix
        scaler: Fitted scaler from training (or None)

    Returns:
        Scaled feature matrix
    """
    if scaler is None:
        return X_score

    log.info("  Applying scaler...")
    X_scaled = pd.DataFrame(
        scaler.transform(X_score),
        columns=X_score.columns,
        index=X_score.index
    )
    return X_scaled


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: SCORING
# ═══════════════════════════════════════════════════════════════════════════

def predict_churn_probability(
    model: Any,
    X_score: pd.DataFrame
) -> np.ndarray:
    """
    Generate churn probability for each customer.

    Args:
        model: Trained classifier with predict_proba
        X_score: Scaled feature matrix

    Returns:
        Array of probabilities (0 = definitely active, 1 = definitely churned)
    """
    log.info("Generating churn predictions...")

    if hasattr(model, 'predict_proba'):
        probabilities = model.predict_proba(X_score)[:, 1]
    else:
        probabilities = model.predict(X_score).astype(float)

    log.info("  Predictions: %d customers", len(probabilities))
    log.info("  Probability range: [%.4f, %.4f]", probabilities.min(), probabilities.max())
    log.info("  Mean probability: %.4f", probabilities.mean())
    return probabilities


def assign_risk_levels(
    probabilities: np.ndarray,
    thresholds: Optional[Dict[str, float]] = None
) -> np.ndarray:
    """
    Map churn probabilities to risk categories (HIGH / MEDIUM / LOW).

    Args:
        probabilities: Array of churn probabilities
        thresholds: Custom thresholds dict with 'high' and 'medium' keys

    Returns:
        Array of risk level strings
    """
    if thresholds is None:
        thresholds = RISK_THRESHOLDS

    log.info("Assigning risk levels (high>=%.2f, medium>=%.2f)...",
             thresholds['high'], thresholds['medium'])

    risk_levels = np.where(
        probabilities >= thresholds['high'], 'HIGH',
        np.where(probabilities >= thresholds['medium'], 'MEDIUM', 'LOW')
    )

    unique, counts = np.unique(risk_levels, return_counts=True)
    for level, count in zip(unique, counts):
        pct = 100 * count / len(risk_levels)
        log.info("  %s: %d (%.1f%%)", level, count, pct)

    return risk_levels


def apply_tier_weighting(
    probabilities: np.ndarray,
    original_df: pd.DataFrame,
) -> np.ndarray:
    """
    Apply a business-tier log-odds shift to raw churn probabilities.

    WHY: The ML model treats all customers equally — a Platinum customer
    with 50% churn probability and a Bronze customer with 50% are scored
    the same. But from a BUSINESS perspective, losing a Platinum customer
    is far more costly. This function nudges high-tier probabilities up
    (and low-tier down) so the top of the High Risk list is biased toward
    customers whose loss costs more.

    HOW: Instead of multiplying probability by a weight (which saturated
    at the 1.0 ceiling and collapsed many Platinum/Gold customers to
    identical 100% scores), we shift in log-odds space:

        logit(p)  = ln(p / (1 - p))    # probability → real line
        logit_adj = logit(p) + shift   # add tier bias
        p_adj     = sigmoid(logit_adj) # real line → probability

    Log-odds addition is monotonic and non-saturating — ordering is
    preserved and probabilities stay strictly inside (0, 1). The shift
    values are defined in TIER_LOGIT_SHIFTS (Section 0).

    Args:
        probabilities: Raw model-output churn probabilities (0 to 1)
        original_df: Full customer DataFrame (must have 'customer_tier' column)

    Returns:
        Adjusted probabilities (same length, in the open interval (0, 1))
    """
    if 'customer_tier' not in original_df.columns:
        # Audit issue #11: bumped from WARNING → ERROR. Skipping tier
        # weighting silently gives every customer the same global risk
        # threshold, which collapses the Platinum-prioritization the
        # business depends on. Including the batch size makes the
        # impact obvious in production logs.
        log.error(
            "No customer_tier column found in input (batch size: %d) — "
            "tier-weighting skipped for ALL customers. Platinum/Gold "
            "boost and Bronze damping will not apply. Add customer_tier "
            "to your data source (CSV header or MV column).",
            len(original_df),
        )
        return probabilities

    log.info("Applying business-tier log-odds shift...")

    # Map each customer's tier to its log-odds shift
    tier_series = original_df['customer_tier'].fillna('Unknown')
    shifts = (
        tier_series.map(TIER_LOGIT_SHIFTS)
                   .fillna(TIER_LOGIT_SHIFT_DEFAULT)
                   .values.astype(float)
    )

    # Guard against exactly 0.0 or 1.0 (logit is ±∞ there). Clamp a
    # hair inside the open interval so the transform stays finite.
    eps = 1e-6
    p_safe = np.clip(probabilities.astype(float), eps, 1.0 - eps)

    # Forward: probability → log-odds, add shift, back to probability.
    logits = np.log(p_safe / (1.0 - p_safe))
    logits_adj = logits + shifts
    adjusted = 1.0 / (1.0 + np.exp(-logits_adj))

    # Log the impact per tier for observability
    changed_count = int((shifts != 0.0).sum())
    if changed_count > 0:
        log.info(
            "  Tier adjustment affected %d / %d customers:",
            changed_count, len(probabilities),
        )
        for tier in ['Platinum', 'Gold', 'Silver', 'Bronze']:
            mask = (tier_series == tier).values
            if mask.any():
                tier_count = int(mask.sum())
                avg_before = float(probabilities[mask].mean())
                avg_after = float(adjusted[mask].mean())
                log.info(
                    "    %s (%d): avg probability %.4f → %.4f (logit shift %+.2f)",
                    tier, tier_count, avg_before, avg_after,
                    TIER_LOGIT_SHIFTS.get(tier, 0.0),
                )

    return adjusted


def _unwrap_calibrated(model: Any) -> Any:
    """
    Return the underlying base estimator when `model` is a
    CalibratedClassifierCV wrapper, otherwise return the model unchanged.

    Why: after audit fix #7 (2026-04-24) models are wrapped with
    CalibratedClassifierCV(method='isotonic', cv=5) at train time so
    predict_proba returns well-calibrated probabilities. The wrapper
    does NOT expose `feature_importances_` or satisfy
    `isinstance(m, xgb.XGBClassifier)`, which breaks SHAP driver
    computation below. This helper hands back the first internal base
    classifier so SHAP can read tree importances like it always has.

    All 5 internal base classifiers (one per calibration fold) share the
    same hyperparameters and have near-identical feature importance
    rankings; picking [0] is a faithful representative.
    """
    try:
        from sklearn.calibration import CalibratedClassifierCV
        if isinstance(model, CalibratedClassifierCV):
            # Each entry is a _CalibratedClassifier with `.estimator`
            # (sklearn ≥ 1.1). Older sklearns used `.base_estimator`.
            inner = model.calibrated_classifiers_[0]
            return getattr(inner, 'estimator', None) or getattr(inner, 'base_estimator', model)
    except Exception:
        # Any failure (wrong sklearn version, corrupted pickle, etc.)
        # degrades gracefully — fall through to the original model and
        # let the caller's own fallback logic take over.
        pass
    return model


def compute_churn_drivers(
    model: Any,
    X_score: pd.DataFrame,
    top_n: int = 3,
    probabilities: Optional[np.ndarray] = None,
    low_risk_cutoff: float = 0.0,
) -> pd.DataFrame:
    """
    Identify the top N churn drivers per customer using SHAP values.

    SHAP = signed, per-customer contribution of each feature to the
    predicted log-odds of churn. This replaces the previous logic
    (|scaled_value| * global_importance), which had two bugs:

      1. Used np.abs on feature values — so a PROTECTIVE signal (e.g.,
         high rfm_frequency_score meaning the customer orders a lot)
         got counted as a churn driver just because it had a large
         absolute value. SHAP is signed, so protective features have
         NEGATIVE SHAP and are filtered out here.

      2. Weighted every customer by the same global feature importance,
         which meant high-importance features dominated the top-3 for
         almost every customer. SHAP is computed per-row, so each
         customer gets their own personalized contributions.

    We keep only features with POSITIVE SHAP (features that actually
    pushed THIS customer toward churn) and rank them descending.

    For XGBoost models, TreeSHAP is built-in (no extra library needed).
    For other tree / linear models we fall back to the `shap` package
    if it's installed.

    Args:
        model: Trained classifier (XGBoost or RandomForest)
        X_score: Scaled feature matrix (customers × features)
        top_n: Number of top drivers to extract (default: 3)
        probabilities: Optional per-customer churn probabilities. When
            provided alongside ``low_risk_cutoff > 0``, customers whose
            probability is below the cutoff get None drivers regardless
            of which SHAP path is taken — the heuristic fallback would
            otherwise emit "drivers" even for low-risk rows. Audit
            issue #9.
        low_risk_cutoff: Probability threshold below which drivers are
            suppressed. Set to MEDIUM threshold by callers so the UI
            doesn't show driver attribution for LOW-risk customers.

    Returns:
        DataFrame with columns: driver_1, driver_2, ..., driver_N.
        Entries are None when no feature pushed the customer toward
        churn or the customer is below the low-risk cutoff.
    """
    log.info("Computing per-customer churn drivers (SHAP)...")

    feature_names = list(X_score.columns)
    n_customers = len(X_score)

    # ── 1. Compute signed SHAP contributions (positive → pushes toward churn) ──
    # Unwrap CalibratedClassifierCV (added by audit fix #7) to get the base
    # tree model. SHAP needs raw tree-importances or an XGBClassifier
    # instance, neither of which the calibration wrapper exposes.
    shap_model = _unwrap_calibrated(model)

    shap_values = None
    try:
        # Prefer XGBoost's built-in TreeSHAP — no extra dependency required.
        try:
            import xgboost as xgb
        except ImportError:
            xgb = None

        if xgb is not None and isinstance(shap_model, xgb.XGBClassifier):
            booster = shap_model.get_booster()
            dmatrix = xgb.DMatrix(X_score.values, feature_names=feature_names)
            # pred_contribs shape: (n_rows, n_features + 1); last column is bias
            contribs = booster.predict(dmatrix, pred_contribs=True)
            shap_values = contribs[:, :-1]
        else:
            # Fallback for RandomForest (or any other tree/linear model a
            # future developer plugs in): use the `shap` package.
            import shap as _shap
            if hasattr(shap_model, 'feature_importances_'):
                explainer = _shap.TreeExplainer(shap_model)
            elif hasattr(shap_model, 'coef_'):
                explainer = _shap.LinearExplainer(shap_model, X_score)
            else:
                raise RuntimeError(
                    "Unsupported model type for SHAP — needs tree_importances_ or coef_"
                )
            sv = explainer.shap_values(X_score)
            # Normalise to a 2-D (n_samples, n_features) matrix of SHAP
            # contributions for the POSITIVE (churn) class.
            #
            # SHAP's return shape for binary tree classifiers has changed
            # across versions (2026-04-25 incident):
            #   * shap < 0.42:  list -> [neg_class_2d, pos_class_2d]
            #     handled by `sv[1]`.
            #   * shap >= 0.42: numpy ndarray of shape
            #     (n_samples, n_features, n_classes) for sklearn binary
            #     classifiers like RandomForestClassifier. Indexing
            #     `sv[row_idx]` later returns a 2-D (n_features,
            #     n_classes) slice instead of a 1-D row, which then breaks
            #     argsort + scalar comparisons downstream with
            #     "The truth value of an array with more than one element
            #     is ambiguous." We must slice on axis=-1 to keep the
            #     positive-class column only.
            #   * Some explainers / single-output models return a plain
            #     2-D (n_samples, n_features) array directly — pass-through.
            if isinstance(sv, list):
                shap_values = sv[1]            # legacy: pick positive class
            elif isinstance(sv, np.ndarray) and sv.ndim == 3:
                shap_values = sv[:, :, 1]      # new SHAP: positive-class slice
            else:
                shap_values = sv               # already 2-D
    except Exception as e:
        # Last-resort fallback — SHAP unavailable or failed.
        #
        # Previously this returned a DataFrame of all-None, so the DB wrote
        # NULL drivers and the UI showed "—" for every row. That happened
        # in production when the AUC winner was Random Forest and the shap
        # package wasn't installed.
        #
        # Instead of giving up, synthesize an approximate per-customer
        # contribution using the model's feature_importances_ / coef_ and
        # each customer's deviation from the population median:
        #
        #   contribution[row, feat] = weight[feat] * sign(x_row - median) * |x_row - median|
        #
        # This is NOT true SHAP — it doesn't account for interactions — but
        # it's:
        #   * signed (so protective features with below-median values don't
        #     masquerade as drivers)
        #   * per-customer (drivers vary row by row, not collapsed to the
        #     same global top-3 for everyone)
        #   * non-zero for high-risk customers (who deviate most from median)
        #
        # It's a safety net so drivers are never blank. Install `shap` for
        # the real thing.
        log.warning(
            "  SHAP computation failed (%s) — falling back to "
            "importance × signed-deviation heuristic. Install the 'shap' "
            "package for accurate per-customer drivers.", e
        )
        try:
            X_arr = X_score.values.astype(float)
            # Use the unwrapped base model here for the same reason as
            # above — CalibratedClassifierCV has no feature_importances_.
            if hasattr(shap_model, 'feature_importances_'):
                weights = np.asarray(shap_model.feature_importances_, dtype=float)
            elif hasattr(shap_model, 'coef_'):
                # Defensive branch — kept in case a future linear model is
                # added back. Binary linear coef_ is (1, n_features); squeeze.
                weights = np.asarray(shap_model.coef_, dtype=float).reshape(-1)
            else:
                raise RuntimeError("model has neither feature_importances_ nor coef_")

            median = np.median(X_arr, axis=0)
            deviation = X_arr - median                 # signed, per-row
            # Sign of deviation × magnitude × global weight gives a crude
            # per-customer push toward churn (positive = toward churn).
            shap_values = deviation * weights[np.newaxis, :]
        except Exception as fallback_err:
            log.warning(
                "  Heuristic fallback also failed (%s) — writing empty drivers",
                fallback_err,
            )
            return pd.DataFrame({
                f'driver_{i+1}': [None] * n_customers for i in range(top_n)
            })

    def _clean_feature_name(name: str) -> str:
        """Convert snake_case feature name to readable label."""
        return name.replace('_', ' ').replace('usd', 'USD').replace('pct', '%').title()

    # ── 2. For each customer, keep only features that pushed TOWARD churn ──
    driver_cols = {f'driver_{i+1}': [] for i in range(top_n)}

    # Audit issue #9: suppress driver attribution for LOW-risk
    # customers. Without this, the heuristic fallback emits a top-N
    # list for every row, including customers with probability ≈ 0.05,
    # and the UI shows "this 5%-churn customer's #1 driver is X".
    suppress_low_risk = (
        probabilities is not None and low_risk_cutoff > 0.0
    )

    for row_idx in range(n_customers):
        if suppress_low_risk and probabilities[row_idx] < low_risk_cutoff:
            for rank in range(top_n):
                driver_cols[f'driver_{rank + 1}'].append(None)
            continue

        row_shap = shap_values[row_idx]

        # Zero out protective features (negative SHAP) so they are never
        # ranked. This fixes the "high RFM frequency shown as a driver" bug.
        churn_push = np.where(row_shap > 0, row_shap, 0.0)

        top_indices = np.argsort(churn_push)[::-1][:top_n]

        for rank, feat_idx in enumerate(top_indices):
            col_key = f'driver_{rank + 1}'
            if churn_push[feat_idx] <= 0:
                # No more features actually push toward churn for this customer
                driver_cols[col_key].append(None)
            else:
                driver_cols[col_key].append(_clean_feature_name(feature_names[feat_idx]))

    drivers_df = pd.DataFrame(driver_cols)
    log.info("  Extracted top %d drivers (SHAP-based) for %d customers",
             top_n, n_customers)

    # Log driver-1 distribution — should vary across customers now,
    # not collapse to the same 1-2 global features.
    if len(drivers_df) > 0:
        non_null = drivers_df['driver_1'].dropna()
        if len(non_null) > 0:
            top_driver_counts = non_null.value_counts().head(5)
            log.info("  Most common #1 driver:")
            for name, count in top_driver_counts.items():
                log.info("    %s — %d customers (%.0f%%)",
                         name, count, 100 * count / n_customers)
        n_no_driver = int(drivers_df['driver_1'].isna().sum())
        if n_no_driver > 0:
            log.info("  %d customers have NO positive-SHAP churn drivers "
                     "(low-risk profiles)", n_no_driver)

    return drivers_df


def build_score_table(
    customer_info: pd.DataFrame,
    probabilities: np.ndarray,
    risk_levels: np.ndarray,
    original_df: pd.DataFrame,
    drivers_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Combine predictions with customer identifiers and context columns.

    Output columns:
        client_id, customer_id, churn_probability, risk_level,
        customer_tier, total_spend_usd, total_orders, avg_rating,
        driver_1, driver_2, driver_3

    Returns:
        DataFrame sorted by churn_probability descending (highest risk first)
    """
    log.info("Building score table...")

    score_df = customer_info.copy()
    # Audit issue #10: keep full precision in the score table. The
    # display layer (CSV / report / UI) is free to round when
    # rendering, but persisting a 4-dp value loses information for
    # downstream calibration / AUC-recompute / archival use.
    score_df['churn_probability'] = probabilities.astype(float)
    score_df['risk_level'] = risk_levels

    # Add context columns for the strategist agent.
    # 2026-04-25: dropped 'is_high_value' — it was redundant with
    # customer_tier (Platinum is already the value-bucket signal). The
    # MV no longer carries the column.
    context_cols = [
        'customer_tier', 'total_spend_usd', 'total_orders',
        'avg_order_value_usd', 'avg_rating', 'days_since_last_order',
        'rfm_total_score',
    ]
    for col in context_cols:
        if col in original_df.columns:
            score_df[col] = original_df[col].values

    # Add churn drivers
    if drivers_df is not None and len(drivers_df) == len(score_df):
        for col in drivers_df.columns:
            score_df[col] = drivers_df[col].values
        log.info("  Added %d driver columns", len(drivers_df.columns))

    # Add churn_label if available (for validation)
    if TARGET_COL in original_df.columns:
        score_df['actual_churn_label'] = original_df[TARGET_COL].values

    score_df = score_df.sort_values('churn_probability', ascending=False).reset_index(drop=True)
    log.info("  Score table: %d rows x %d columns", score_df.shape[0], score_df.shape[1])
    return score_df


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: OUTPUT
# ═══════════════════════════════════════════════════════════════════════════

def save_scores_csv(score_df: pd.DataFrame, output_path: Optional[Path] = None) -> Path:
    """Save scoring results to CSV."""
    if output_path is None:
        output_path = OUTPUT_DIR / "churn_scores.csv"

    score_df.to_csv(output_path, index=False)
    log.info("Saved scores → %s (%d rows)", output_path, len(score_df))
    return output_path


def save_scores_json(score_df: pd.DataFrame, output_path: Optional[Path] = None) -> Path:
    """
    Save scoring results to JSON file.

    Output structure:
        {
            "generated_at": "...",
            "total_customers": N,
            "scores": [ { customer fields + churn_probability + risk_level }, ... ]
        }
    """
    if output_path is None:
        output_path = OUTPUT_DIR / "churn_scores.json"

    payload = {
        'generated_at': datetime.now().isoformat(),
        'total_customers': len(score_df),
        'scores': score_df.to_dict(orient='records'),
    }

    with open(output_path, 'w') as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("Saved JSON scores → %s (%d rows)", output_path, len(score_df))
    return output_path


def save_scores_to_db(
    score_df: pd.DataFrame,
    db_url: str,
    model_version: str = 'unknown',
) -> int:
    """
    Save scoring results to PostgreSQL churn_scores table.

    The teammate's schema has churn_scores with columns:
        score_id (serial PK), client_id, customer_id, scored_at,
        churn_probability, risk_tier, churn_label_simulated,
        model_version, batch_run_id

    `model_version` is now derived from the training metadata (audit
    issue #4) instead of being hardcoded — every row is tagged with
    the model_type / training_date / AUC of the model that produced
    it, so a downstream auditor can reconstruct which trained model
    each prediction came from.

    We map our DataFrame columns to match this schema, then clear
    only the client(s) in this batch (NOT TRUNCATE) to preserve both
    the FK from retention_interventions AND other clients' scores.
    """
    log.info("Saving scores to database...")
    from sqlalchemy import create_engine, text, inspect
    from datetime import datetime

    # Audit issue #8: wrap the engine lifecycle in try/finally so the
    # connection pool is released even when something below raises.
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        inspector = inspect(engine)

        if 'churn_scores' not in inspector.get_table_names():
            # Audit issue #5: previously we silently created a malformed
            # `churn_scores` table with whatever columns score_df happened
            # to have, breaking the FK from retention_interventions and
            # the UNIQUE (client_id, customer_id) constraint that the rest
            # of the application relies on. Fail loudly instead so the
            # operator runs the missing migration.
            raise RuntimeError(
                "churn_scores table is missing. The application schema "
                "(and the FK from retention_interventions) require it. "
                "Run db/schema_postgresql.sql plus "
                "migration_churn_scores_unique.sql before scoring."
            )

        # Table exists with teammate's schema — clear only this run's
        # clients, then insert matching rows.
        #
        # WHY per-client DELETE instead of TRUNCATE:
        #   The schema is multi-tenant. TRUNCATE wipes every client's
        #   scores, so scoring CLT-001 alone would wipe CLT-002's
        #   predictions. We restrict the delete to clients present in
        #   the current batch.
        #
        # WHY one transaction (engine.begin) around DELETE + INSERT:
        #   Previously the DELETE committed on one connection and the
        #   INSERT ran on a second connection. Two overlapping pipeline
        #   runs for the same client could both DELETE (each seeing the
        #   other's already-deleted table), then both INSERT — producing
        #   2× rows per customer. Inside a single engine.begin() the
        #   DELETE holds row locks until the INSERT is committed, so a
        #   concurrent run blocks until this one finishes, then
        #   overwrites cleanly. Belt-and-braces: the churn_scores
        #   (client_id, customer_id) UNIQUE constraint
        #   (migration_churn_scores_unique.sql) makes a duplicate INSERT
        #   fail loudly rather than silently duplicating.
        clients_in_batch = score_df['client_id'].dropna().unique().tolist()
        log.info("  Clearing existing churn_scores for %d client(s): %s",
                 len(clients_in_batch), clients_in_batch)

        # Map our columns to the teammate's schema BEFORE opening the
        # transaction so any conversion error aborts without holding DB
        # locks.
        db_df = pd.DataFrame()
        db_df['client_id'] = score_df['client_id']
        db_df['customer_id'] = score_df['customer_id']
        db_df['scored_at'] = datetime.now()
        db_df['churn_probability'] = score_df['churn_probability']
        db_df['risk_tier'] = score_df['risk_level'].map({
            'HIGH': 'HIGH', 'MEDIUM': 'MEDIUM', 'LOW': 'LOW'
        }).fillna('LOW')
        # Audit issue #6: align churn_label_simulated with the risk_tier
        # boundary instead of using a separate 0.5 cutoff. Previously a
        # customer at probability 0.55 was risk_tier='MEDIUM' but
        # churn_label_simulated=True — joining the two columns produced
        # garbage. Now the simulated label is True iff the customer is
        # in the HIGH risk tier (probability >= RISK_THRESHOLDS['high']).
        db_df['churn_label_simulated'] = (score_df['risk_level'] == 'HIGH')
        # Top 3 features contributing to this customer's score
        db_df['driver_1'] = score_df.get('driver_1')
        db_df['driver_2'] = score_df.get('driver_2')
        db_df['driver_3'] = score_df.get('driver_3')
        # Audit issue #4: derive model_version from training metadata so
        # every row is traceable back to the trained model that produced
        # it. Format: <model_type>_<yyyy-mm-dd>_auc<auc>. Caller passes
        # 'unknown' if the model lacks metadata (older bundles).
        db_df['model_version'] = model_version
        db_df['batch_run_id'] = (
            f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Defensive: even if score_df somehow carries duplicates for a
        # (client_id, customer_id) pair, keep only the first. Without
        # this the INSERT would itself violate the UNIQUE constraint.
        before_dedupe = len(db_df)
        db_df = db_df.drop_duplicates(
            subset=['client_id', 'customer_id'], keep='first'
        )
        if len(db_df) != before_dedupe:
            log.warning(
                "  Dropped %d duplicate (client_id, customer_id) rows "
                "from score_df before insert",
                before_dedupe - len(db_df),
            )

        # engine.begin() = single transactional scope. DELETE + INSERT
        # commit together at the end of the with-block; if anything
        # raises, the whole thing rolls back and the table is untouched.
        with engine.begin() as conn:
            if clients_in_batch:
                # First null out any retention_interventions FK pointing
                # at the rows we're about to delete — the FK has no
                # ON DELETE action so PG would otherwise raise 23503.
                conn.execute(
                    text(
                        "UPDATE retention_interventions "
                        "SET churn_score_id = NULL "
                        "WHERE churn_score_id IN ("
                        "  SELECT score_id FROM churn_scores "
                        "  WHERE client_id = ANY(:cids)"
                        ")"
                    ),
                    {"cids": clients_in_batch},
                )
                conn.execute(
                    text(
                        "DELETE FROM churn_scores "
                        "WHERE client_id = ANY(:cids)"
                    ),
                    {"cids": clients_in_batch},
                )
            else:
                log.warning(
                    "  No client_id values in score_df — skipping delete"
                )

            # Pass the live connection into pandas so the INSERT joins
            # the same transaction instead of grabbing a fresh pooled
            # connection.
            db_df.to_sql(
                'churn_scores', conn, if_exists='append', index=False
            )

        log.info("  Inserted %d rows into churn_scores", len(score_df))
        return len(score_df)
    finally:
        engine.dispose()


def generate_risk_report(score_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute risk summary statistics from scored data.

    Returns:
        Dict with risk distribution, probability stats, top at-risk customers
    """
    log.info("Generating risk summary...")

    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_customers': len(score_df),
        'risk_distribution': {},
        'probability_stats': {
            'mean': float(score_df['churn_probability'].mean()),
            'median': float(score_df['churn_probability'].median()),
            'std': float(score_df['churn_probability'].std()),
            'min': float(score_df['churn_probability'].min()),
            'max': float(score_df['churn_probability'].max()),
        },
        'top_at_risk': [],
    }

    # Risk distribution
    for level in ['HIGH', 'MEDIUM', 'LOW']:
        count = int((score_df['risk_level'] == level).sum())
        pct = round(100 * count / len(score_df), 1)
        summary['risk_distribution'][level] = {'count': count, 'pct': pct}

    # Top 10 at-risk customers
    top_cols = ['client_id', 'customer_id', 'churn_probability', 'risk_level']
    available_top = [c for c in top_cols if c in score_df.columns]
    summary['top_at_risk'] = (
        score_df.head(10)[available_top].to_dict('records')
    )

    return summary


def save_risk_report(summary: Dict[str, Any], output_path: Optional[Path] = None) -> Path:
    """Save risk report as formatted text file."""
    if output_path is None:
        output_path = OUTPUT_DIR / "risk_summary.txt"

    lines = []
    lines.append("=" * 70)
    lines.append("  CHURN RISK REPORT")
    lines.append("=" * 70)
    lines.append(f"  Generated:        {summary['timestamp']}")
    lines.append(f"  Total customers:  {summary['total_customers']}")
    lines.append("")

    lines.append("-" * 70)
    lines.append("  RISK DISTRIBUTION")
    lines.append("-" * 70)
    for level in ['HIGH', 'MEDIUM', 'LOW']:
        info = summary['risk_distribution'].get(level, {'count': 0, 'pct': 0})
        lines.append(f"  {level:8s}  {info['count']:5d}  ({info['pct']:.1f}%)")
    lines.append("")

    lines.append("-" * 70)
    lines.append("  PROBABILITY STATISTICS")
    lines.append("-" * 70)
    stats = summary['probability_stats']
    lines.append(f"  Mean:    {stats['mean']:.4f}")
    lines.append(f"  Median:  {stats['median']:.4f}")
    lines.append(f"  Std:     {stats['std']:.4f}")
    lines.append(f"  Min:     {stats['min']:.4f}")
    lines.append(f"  Max:     {stats['max']:.4f}")
    lines.append("")

    lines.append("-" * 70)
    lines.append("  TOP 10 AT-RISK CUSTOMERS")
    lines.append("-" * 70)
    for i, cust in enumerate(summary['top_at_risk'], 1):
        cid = cust.get('customer_id', 'N/A')
        prob = cust.get('churn_probability', 0)
        risk = cust.get('risk_level', 'N/A')
        lines.append(f"  {i:2d}. Customer {str(cid):>8s}  |  Prob: {prob:.4f}  |  Risk: {risk}")
    lines.append("")
    lines.append("=" * 70)

    with open(output_path, 'w') as f:
        f.write("\n".join(lines))

    log.info("Saved risk report → %s", output_path)
    return output_path


def print_risk_report(summary: Dict[str, Any]) -> None:
    """Print risk report to console."""
    print()
    print("=" * 70)
    print("  CHURN RISK REPORT")
    print("=" * 70)
    print(f"  Total customers: {summary['total_customers']}")
    print()

    print("  Risk Distribution:")
    for level in ['HIGH', 'MEDIUM', 'LOW']:
        info = summary['risk_distribution'].get(level, {'count': 0, 'pct': 0})
        bar = "█" * int(info['pct'] / 2)
        print(f"    {level:8s}  {info['count']:4d}  ({info['pct']:5.1f}%)  {bar}".encode('utf-8', errors='replace').decode('utf-8'))
    print()

    stats = summary['probability_stats']
    print(f"  Avg churn probability: {stats['mean']:.4f}")
    print()

    print("  Top 10 At-Risk Customers:")
    for i, cust in enumerate(summary['top_at_risk'], 1):
        cid = cust.get('customer_id', 'N/A')
        prob = cust.get('churn_probability', 0)
        risk = cust.get('risk_level', 'N/A')
        print(f"    {i:2d}. Customer {str(cid):>8s}  |  Prob: {prob:.4f}  |  {risk}")
    print("=" * 70)
    print()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6: FASTAPI ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

def create_api_app(model_bundle: Dict[str, Any]) -> Any:
    """
    Create a FastAPI application with prediction endpoints.

    The model is loaded once at startup and reused for all requests.

    Endpoints:
        GET  /health          → Model info and server status
        POST /predict          → Single customer prediction
        POST /predict/batch    → Batch prediction
        GET  /scores           → Read saved churn_scores.csv

    Args:
        model_bundle: Dict from load_model_bundle()

    Returns:
        FastAPI app instance
    """
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.middleware.cors import CORSMiddleware
        from pydantic import BaseModel
    except ImportError:
        log.error("FastAPI not installed. Run: pip install fastapi uvicorn")
        raise

    app = FastAPI(
        title="Analyst Agent — Churn Prediction API",
        description="Predict customer churn probability and risk level",
        version="1.0.0",
    )

    # CORS for frontend access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store model in app state
    app.state.model = model_bundle['model']
    app.state.scaler = model_bundle['scaler']
    app.state.feature_names = model_bundle['feature_names']
    app.state.metadata = model_bundle['metadata']

    # ── Request/Response schemas ──────────────────────────────────────

    class CustomerFeatures(BaseModel):
        """Input: customer features as key-value pairs."""
        features: Dict[str, float]
        customer_id: Optional[str] = None

    class BatchRequest(BaseModel):
        """Input: list of customers for batch prediction."""
        customers: List[CustomerFeatures]

    class PredictionResponse(BaseModel):
        """Output: prediction for a single customer."""
        customer_id: Optional[str]
        churn_probability: float
        risk_level: str
        model_type: str

    class BatchResponse(BaseModel):
        """Output: batch prediction results."""
        predictions: List[PredictionResponse]
        total: int

    class HealthResponse(BaseModel):
        """Output: server health and model info."""
        status: str
        model_type: str
        n_features: int
        auc_roc: Optional[float]
        trained_on: Optional[str]

    # ── Endpoints ─────────────────────────────────────────────────────

    @app.get("/health", response_model=HealthResponse)
    def health_check():
        """Server status and loaded model info."""
        metrics = app.state.metadata.get('metrics', {})
        return HealthResponse(
            status="healthy",
            model_type=app.state.metadata.get('model_type', 'unknown'),
            n_features=len(app.state.feature_names),
            auc_roc=metrics.get('auc_roc'),
            trained_on=app.state.metadata.get('training_date'),
        )

    @app.post("/predict", response_model=PredictionResponse)
    def predict_single(customer: CustomerFeatures):
        """
        Predict churn for a single customer.

        Send customer features as a JSON dict. Missing features are filled with 0.
        Example:
            {
                "customer_id": "CUST-001",
                "features": {
                    "median_days_between_orders": 30.5,
                    "orders_with_discount": 12,
                    "total_discount_usd": 150.0,
                    "avg_order_value_usd": 65.0,
                    ...
                }
            }
        """
        try:
            # Build single-row DataFrame aligned to model features
            row = {}
            for feat in app.state.feature_names:
                row[feat] = customer.features.get(feat, 0.0)

            X = pd.DataFrame([row])

            # Scale
            if app.state.scaler is not None:
                X = pd.DataFrame(
                    app.state.scaler.transform(X),
                    columns=X.columns
                )

            # Predict
            prob = float(app.state.model.predict_proba(X)[0, 1])
            risk = (
                'HIGH' if prob >= RISK_THRESHOLDS['high']
                else 'MEDIUM' if prob >= RISK_THRESHOLDS['medium']
                else 'LOW'
            )

            return PredictionResponse(
                customer_id=customer.customer_id,
                churn_probability=round(prob, 4),
                risk_level=risk,
                model_type=app.state.metadata.get('model_type', 'unknown'),
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/predict/batch", response_model=BatchResponse)
    def predict_batch(request: BatchRequest):
        """
        Predict churn for multiple customers at once.

        More efficient than calling /predict in a loop — processes all
        customers in a single model pass.
        """
        try:
            rows = []
            ids = []
            for cust in request.customers:
                row = {}
                for feat in app.state.feature_names:
                    row[feat] = cust.features.get(feat, 0.0)
                rows.append(row)
                ids.append(cust.customer_id)

            X = pd.DataFrame(rows)

            # Scale
            if app.state.scaler is not None:
                X = pd.DataFrame(
                    app.state.scaler.transform(X),
                    columns=X.columns
                )

            # Predict
            probs = app.state.model.predict_proba(X)[:, 1]
            risks = assign_risk_levels(probs)

            predictions = []
            for i, (prob, risk) in enumerate(zip(probs, risks)):
                predictions.append(PredictionResponse(
                    customer_id=ids[i],
                    churn_probability=round(float(prob), 4),
                    risk_level=risk,
                    model_type=app.state.metadata.get('model_type', 'unknown'),
                ))

            return BatchResponse(predictions=predictions, total=len(predictions))

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/scores")
    def get_saved_scores():
        """Read the most recent churn_scores.csv if available."""
        csv_path = OUTPUT_DIR / "churn_scores.csv"
        if not csv_path.exists():
            raise HTTPException(
                status_code=404,
                detail="No scores file found. Run CLI scoring first."
            )
        df = pd.read_csv(csv_path)
        return df.to_dict('records')

    @app.get("/model/features")
    def get_model_features():
        """Return the list of features the model expects.
        NOTE: All features must be numeric floats.
              customer_tier must be sent as customer_tier_encoded (Bronze=1, Silver=2, Gold=3, Platinum=4).
        """
        return {
            'feature_names': app.state.feature_names,
            'n_features': len(app.state.feature_names),
            'notes': 'All values must be numeric. customer_tier_encoded: Bronze=1, Silver=2, Gold=3, Platinum=4.',
        }

    return app


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7: CLI PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def run_scoring_pipeline(
    source: str = 'csv',
    db_url: Optional[str] = None,
    csv_path: Optional[str] = None,
    model_path: Optional[str] = None,
    output_mode: str = 'csv',
    top_n: int = 10,
    client_id: Optional[str] = None,
    allow_missing_features: bool = False,
    ignore_drift: bool = False,
) -> Dict[str, Any]:
    """
    End-to-end batch scoring pipeline (CLI mode).

    Steps:
        1. Load trained model
        2. Load customer data (DB or CSV)
        3. Prepare features
        4. Scale and predict
        5. Assign risk levels
        6. Build score table
        7. Save outputs (CSV / DB / report)

    Returns:
        Dict with summary and output paths
    """
    log.info("=" * 70)
    log.info("  ANALYST AGENT — CHURN SCORING PIPELINE")
    log.info("=" * 70)

    # 1. Load model — when running per-tenant, pass client_id so
    # auto-discovery picks the right per-tenant model file.
    bundle = load_model_bundle(model_path, client_id=client_id)
    model = bundle['model']
    scaler = bundle['scaler']
    feature_names = bundle['feature_names']
    metadata = bundle['metadata']
    imputation_values = metadata.get('imputation_values', {})  # audit #1

    # 1b. Drift check (audit issue #3) — compare the live client_config
    # against the snapshot saved in metadata at training time. Aborts on
    # label-defining drift unless --ignore-drift is set.
    if source == 'db':
        effective_db_url = db_url or os.getenv('DB_URL')
        check_feature_freeze_drift(
            metadata,
            db_url=effective_db_url,
            client_id=client_id,
            ignore_drift=ignore_drift,
        )

    # 2. Load data
    if source == 'db':
        if not db_url:
            db_url = os.getenv('DB_URL')
        if not db_url:
            raise ValueError("Database URL required. Use --db-url or set DB_URL in .env")
        df = load_customers_from_db(db_url, client_id=client_id)
    else:
        if csv_path is None:
            # Try customer_features.csv first (full data with IDs), then feature_matrix.csv
            full_path = OUTPUT_DIR / 'customer_features.csv'
            matrix_path = OUTPUT_DIR / 'feature_matrix.csv'
            if full_path.exists():
                csv_path = str(full_path)
            elif matrix_path.exists():
                csv_path = str(matrix_path)
            else:
                raise FileNotFoundError("No CSV found. Run compute_rfm.py first.")
        df = load_customers_from_csv(csv_path)

    # 3. Prepare features (audit fixes #1, #2)
    X_score, customer_info = prepare_features_for_scoring(
        df, feature_names,
        imputation_values=imputation_values,
        allow_missing_features=allow_missing_features,
    )

    # 4. Scale
    X_scaled = scale_for_scoring(X_score, scaler)

    # 5. Predict (raw model output)
    raw_probabilities = predict_churn_probability(model, X_scaled)

    # 5b. Apply business-tier weighting (Platinum/Gold get boosted)
    probabilities = apply_tier_weighting(raw_probabilities, df)

    # 6. Risk levels (based on tier-adjusted probabilities). Audit
    # issue #7: thresholds are resolved per-client when client_config
    # carries risk_high_threshold / risk_medium_threshold; otherwise
    # fall back to the global RISK_THRESHOLDS defaults.
    effective_db_url = db_url or os.getenv('DB_URL')
    risk_thresholds = load_risk_thresholds(
        db_url=effective_db_url, client_id=client_id,
    )
    risk_levels = assign_risk_levels(probabilities, thresholds=risk_thresholds)

    # 6b. Compute per-customer churn drivers (top 3 features). Audit
    # issue #9: pass probabilities + the medium cutoff so LOW-risk
    # customers get None drivers instead of a misleading top-3 list.
    drivers_df = compute_churn_drivers(
        model, X_scaled, top_n=3,
        probabilities=probabilities,
        low_risk_cutoff=risk_thresholds['medium'],
    )

    # 7. Score table
    score_df = build_score_table(customer_info, probabilities, risk_levels, df, drivers_df)

    # 8. Outputs
    output_files = {}

    if output_mode in ('csv', 'both', 'all'):
        csv_out = save_scores_csv(score_df)
        output_files['csv'] = str(csv_out)

    if output_mode in ('json', 'all'):
        json_out = save_scores_json(score_df)
        output_files['json'] = str(json_out)

    if output_mode in ('db', 'both', 'all'):
        if not db_url:
            log.warning("  No db_url — skipping database save")
        else:
            # Audit issue #4: build a traceable version string from the
            # training metadata so every row in churn_scores can be tied
            # back to the trained model that produced it.
            metrics = metadata.get('metrics', {})
            training_date = (metadata.get('training_date') or 'unknown')[:10]
            auc = metrics.get('auc_roc')
            auc_str = f"{auc:.3f}" if isinstance(auc, (int, float)) else 'na'
            model_version = (
                f"{metadata.get('model_type', 'unknown')}_"
                f"{training_date}_auc{auc_str}"
            )
            rows = save_scores_to_db(score_df, db_url, model_version=model_version)
            output_files['db_rows'] = rows

    # 9. Risk report
    summary = generate_risk_report(score_df)
    report_path = save_risk_report(summary)
    output_files['report'] = str(report_path)

    print_risk_report(summary)

    log.info("=" * 70)
    log.info("  SCORING COMPLETE")
    log.info("  Outputs: %s", output_files)
    log.info("=" * 70)

    return {'summary': summary, 'output_files': output_files, 'score_df': score_df}


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8: MAIN
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Churn Prediction — Scoring & API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Score all customers from CSV:
  python -m analyst_agent.ml.predict --mode cli --source csv

  # Score from database:
  python -m analyst_agent.ml.predict --mode cli --source db --db-url postgresql://user:pass@localhost/db

  # Start API server:
  python -m analyst_agent.ml.predict --mode api --port 8000
        """
    )
    parser.add_argument('--mode', choices=['cli', 'api'], default='cli',
                        help='Run mode: cli (batch scoring) or api (FastAPI server)')
    parser.add_argument('--source', choices=['csv', 'db'], default='csv',
                        help='Data source for CLI mode (default: csv)')
    parser.add_argument('--csv-path', type=str, default=None,
                        help='Path to CSV file (auto-discovers if not set)')
    parser.add_argument('--db-url', type=str, default=None,
                        help='PostgreSQL URL (also reads DB_URL from .env)')
    parser.add_argument('--model-path', type=str, default=None,
                        help='Path to .joblib model (auto-discovers best if not set)')
    parser.add_argument('--output', choices=['csv', 'json', 'db', 'both', 'all'], default='csv',
                        help='Output destination for CLI mode (default: csv). "both"=csv+db, "all"=csv+json+db')
    parser.add_argument('--port', type=int, default=DEFAULT_API_PORT,
                        help='API server port (default: 8000)')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='API server host (default: 0.0.0.0)')
    parser.add_argument('--client-id', type=str, default=None,
                        help='Filter data by client_id (e.g., CLT-002)')
    parser.add_argument('--allow-missing-features', action='store_true',
                        help='Fall back to zero-filling features the model '
                             'expects but the input data lacks. NOT '
                             'recommended for production. Audit issue #2.')
    parser.add_argument('--ignore-drift', action='store_true',
                        help='Skip the abort when client_config has drifted '
                             'from the training-time snapshot. Predictions '
                             'will be biased — use only for debugging. '
                             'Audit issue #3.')
    return parser.parse_args()


def main():
    """Entry point — routes to CLI pipeline or API server."""
    args = parse_args()
    load_dotenv()

    if args.mode == 'api':
        # ── API Mode ──────────────────────────────────────────────────
        log.info("Starting API server...")
        bundle = load_model_bundle(args.model_path)
        app = create_api_app(bundle)

        try:
            import uvicorn
            log.info("  Server running at http://%s:%d", args.host, args.port)
            log.info("  Docs at http://%s:%d/docs", args.host, args.port)
            uvicorn.run(app, host=args.host, port=args.port, log_level="info")
        except ImportError:
            log.error("uvicorn not installed. Run: pip install uvicorn")
            sys.exit(1)

    else:
        # ── CLI Mode ──────────────────────────────────────────────────
        db_url = args.db_url or os.getenv('DB_URL')
        result = run_scoring_pipeline(
            source=args.source,
            db_url=db_url,
            csv_path=args.csv_path,
            model_path=args.model_path,
            output_mode=args.output,
            client_id=args.client_id,
            allow_missing_features=args.allow_missing_features,
            ignore_drift=args.ignore_drift,
        )
        sys.exit(0 if result.get('summary') else 1)


if __name__ == '__main__':
    _force_utf8_stdout()
    main()
