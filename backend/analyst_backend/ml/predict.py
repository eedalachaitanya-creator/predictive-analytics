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

# Risk level thresholds on churn_probability
RISK_THRESHOLDS = {
    'high': 0.65,     # >= 0.65 → HIGH risk
    'medium': 0.35,   # >= 0.35 → MEDIUM risk
                       # < 0.35  → LOW risk
}

# Business tier weights — applied AFTER model scoring to prioritize
# high-value customers. Losing a Platinum customer costs far more than
# losing a Bronze customer, so we boost their churn probability to
# ensure they surface as HIGH risk sooner.
#
# Example: Platinum customer with 0.50 raw probability → 0.50 × 1.25 = 0.625
#          This pushes them from MEDIUM into HIGH risk tier (threshold 0.65
#          is close), making the business act faster on valuable customers.
#
# Why these specific values?
#   Platinum × 1.25 → aggressive boost (most revenue at stake)
#   Gold     × 1.15 → moderate boost
#   Silver   × 1.00 → no change (baseline)
#   Bronze   × 0.90 → slight reduction (lower business impact)
TIER_WEIGHTS = {
    'Platinum': 1.25,
    'Gold':     1.15,
    'Silver':   1.00,
    'Bronze':   0.90,
}
TIER_WEIGHT_DEFAULT = 1.00  # Used when tier is missing or unknown

# Default model preference order (best AUC-ROC first)
MODEL_PREFERENCE = ['random_forest', 'xgboost', 'logistic_regression']

# API default port
DEFAULT_API_PORT = 8000


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: MODEL LOADING
# ═══════════════════════════════════════════════════════════════════════════

def discover_best_model() -> Path:
    """
    Auto-discover the best available model in MODEL_DIR.

    Searches for .joblib files and picks based on MODEL_PREFERENCE order.
    Falls back to the most recently modified model if no preference matches.

    Returns:
        Path to the best model file
    """
    log.info("Auto-discovering best model in %s...", MODEL_DIR)
    model_files = list(MODEL_DIR.glob("churn_model_*.joblib"))

    if not model_files:
        raise FileNotFoundError(f"No model files found in {MODEL_DIR}")

    # Try preference order first
    for preferred in MODEL_PREFERENCE:
        for f in model_files:
            if preferred in f.name:
                log.info("  Found preferred model: %s", f.name)
                return f

    # Fallback: most recent
    best = max(model_files, key=lambda p: p.stat().st_mtime)
    log.info("  Using most recent model: %s", best.name)
    return best


def load_model_bundle(model_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load a trained model bundle (.joblib) containing model, scaler,
    feature names, and training metadata.

    Args:
        model_path: Explicit path to .joblib file, or None to auto-discover

    Returns:
        Dict with keys: 'model', 'scaler', 'feature_names', 'metadata'
    """
    if model_path is None:
        path = discover_best_model()
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


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_customers_from_db(db_url: str, client_id: str = None) -> pd.DataFrame:
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

    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("  Connected successfully")

    if client_id:
        log.info("  Filtering by client_id=%s", client_id)
        df = pd.read_sql(text("SELECT * FROM mv_customer_features WHERE client_id = :cid"), engine, params={"cid": client_id})
    else:
        df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)
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
    feature_names: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare customer data for model scoring.

    Steps:
        1. Extract customer identifiers (client_id, customer_id)
        2. Encode customer_tier → ordinal (matching train_model.py)
        3. Align columns to the exact feature list the model expects
        4. Fill missing features with 0
        5. Handle NaN values

    Args:
        df: Raw customer DataFrame
        feature_names: The exact list of features the model was trained on

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
    available = [f for f in feature_names if f in df_work.columns]
    missing = [f for f in feature_names if f not in df_work.columns]

    if missing:
        log.warning("  Missing %d features (filling with 0): %s", len(missing), missing)
        for feat in missing:
            df_work[feat] = 0

    X_score = df_work[feature_names].copy()

    # Handle NaN
    nan_count = X_score.isna().sum().sum()
    if nan_count > 0:
        log.warning("  Found %d NaN values — filling with 0", nan_count)
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
    Apply business-tier weighting to raw churn probabilities.

    WHY: The ML model treats all customers equally — a Platinum customer
    with 50% churn probability and a Bronze customer with 50% are scored
    the same. But from a BUSINESS perspective, losing a Platinum customer
    is far more costly. This function boosts high-tier probabilities so
    they surface as HIGH risk sooner.

    HOW: Multiply raw probability by the tier weight, then clip to [0, 1].
    The weights are defined in TIER_WEIGHTS (Section 0).

    Args:
        probabilities: Raw model-output churn probabilities (0 to 1)
        original_df: Full customer DataFrame (must have 'customer_tier' column)

    Returns:
        Adjusted probabilities (same length, clipped to [0, 1])
    """
    if 'customer_tier' not in original_df.columns:
        log.warning("  No customer_tier column found — skipping tier weighting")
        return probabilities

    log.info("Applying business-tier weighting...")

    # Map each customer's tier to its weight
    tier_series = original_df['customer_tier'].fillna('Unknown')
    weights = tier_series.map(TIER_WEIGHTS).fillna(TIER_WEIGHT_DEFAULT).values

    # Multiply raw probability by tier weight
    adjusted = probabilities * weights

    # Clip to valid probability range [0, 1]
    adjusted = np.clip(adjusted, 0.0, 1.0)

    # Log the impact of tier weighting
    changed_count = int((adjusted != probabilities).sum())
    if changed_count > 0:
        log.info("  Tier weighting adjusted %d / %d customers:", changed_count, len(probabilities))
        for tier in ['Platinum', 'Gold', 'Silver', 'Bronze']:
            mask = tier_series == tier
            if mask.any():
                tier_count = int(mask.sum())
                avg_before = float(probabilities[mask].mean())
                avg_after = float(adjusted[mask].mean())
                log.info("    %s (%d): avg probability %.4f → %.4f (×%.2f)",
                         tier, tier_count, avg_before, avg_after, TIER_WEIGHTS.get(tier, 1.0))

    return adjusted


def compute_churn_drivers(
    model: Any,
    X_score: pd.DataFrame,
    top_n: int = 3
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
        model: Trained classifier (XGBoost, RandomForest, LogReg, ...)
        X_score: Scaled feature matrix (customers × features)
        top_n: Number of top drivers to extract (default: 3)

    Returns:
        DataFrame with columns: driver_1, driver_2, ..., driver_N.
        Entries are None when no feature pushed the customer toward churn.
    """
    log.info("Computing per-customer churn drivers (SHAP)...")

    feature_names = list(X_score.columns)
    n_customers = len(X_score)

    # ── 1. Compute signed SHAP contributions (positive → pushes toward churn) ──
    shap_values = None
    try:
        # Prefer XGBoost's built-in TreeSHAP — no extra dependency required.
        try:
            import xgboost as xgb
        except ImportError:
            xgb = None

        if xgb is not None and isinstance(model, xgb.XGBClassifier):
            booster = model.get_booster()
            dmatrix = xgb.DMatrix(X_score.values, feature_names=feature_names)
            # pred_contribs shape: (n_rows, n_features + 1); last column is bias
            contribs = booster.predict(dmatrix, pred_contribs=True)
            shap_values = contribs[:, :-1]
        else:
            # Fallback for RandomForest / GradientBoosting / LogReg: use `shap`.
            import shap as _shap
            if hasattr(model, 'feature_importances_'):
                explainer = _shap.TreeExplainer(model)
            elif hasattr(model, 'coef_'):
                explainer = _shap.LinearExplainer(model, X_score)
            else:
                raise RuntimeError(
                    "Unsupported model type for SHAP — needs tree_importances_ or coef_"
                )
            sv = explainer.shap_values(X_score)
            # Binary classifiers in sklearn return a list [neg_class, pos_class]
            shap_values = sv[1] if isinstance(sv, list) else sv
    except Exception as e:
        log.warning(
            "  SHAP computation failed (%s) — drivers will be empty. "
            "Install the 'shap' package for non-XGBoost models.", e
        )
        return pd.DataFrame({
            f'driver_{i+1}': [None] * n_customers for i in range(top_n)
        })

    def _clean_feature_name(name: str) -> str:
        """Convert snake_case feature name to readable label."""
        return name.replace('_', ' ').replace('usd', 'USD').replace('pct', '%').title()

    # ── 2. For each customer, keep only features that pushed TOWARD churn ──
    driver_cols = {f'driver_{i+1}': [] for i in range(top_n)}

    for row_idx in range(n_customers):
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
    score_df['churn_probability'] = np.round(probabilities, 4)
    score_df['risk_level'] = risk_levels

    # Add context columns for the strategist agent
    context_cols = [
        'customer_tier', 'total_spend_usd', 'total_orders',
        'avg_order_value_usd', 'avg_rating', 'days_since_last_order',
        'is_high_value', 'rfm_total_score',
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


def save_scores_to_db(score_df: pd.DataFrame, db_url: str) -> int:
    """
    Save scoring results to PostgreSQL churn_scores table.

    The teammate's schema has churn_scores with columns:
        score_id (serial PK), client_id, customer_id, scored_at,
        churn_probability, risk_tier, churn_label_simulated,
        model_version, batch_run_id

    We map our DataFrame columns to match this schema, then clear
    only the client(s) in this batch (NOT TRUNCATE) to preserve both
    the FK from retention_interventions AND other clients' scores.
    """
    log.info("Saving scores to database...")
    from sqlalchemy import create_engine, text, inspect
    from datetime import datetime

    engine = create_engine(db_url, pool_pre_ping=True)
    inspector = inspect(engine)

    if 'churn_scores' in inspector.get_table_names():
        # Table exists with teammate's schema — clear only this run's
        # clients, then insert matching rows.
        #
        # WHY per-client DELETE instead of TRUNCATE:
        #   The schema is multi-tenant. TRUNCATE wipes every client's
        #   scores, so scoring CLT-001 alone would wipe CLT-002's predictions.
        #   We restrict the delete to clients present in the current batch.
        clients_in_batch = score_df['client_id'].dropna().unique().tolist()
        log.info("  Clearing existing churn_scores for %d client(s): %s",
                 len(clients_in_batch), clients_in_batch)

        with engine.connect() as conn:
            if clients_in_batch:
                conn.execute(
                    text("DELETE FROM churn_scores WHERE client_id = ANY(:cids)"),
                    {"cids": clients_in_batch},
                )
            else:
                log.warning("  No client_id values in score_df — skipping delete")
            conn.commit()

        # Map our columns to the teammate's schema
        db_df = pd.DataFrame()
        db_df['client_id'] = score_df['client_id']
        db_df['customer_id'] = score_df['customer_id']
        db_df['scored_at'] = datetime.now()
        db_df['churn_probability'] = score_df['churn_probability']
        # Map risk_level → risk_tier (teammate uses risk_tier)
        db_df['risk_tier'] = score_df['risk_level'].map({
            'HIGH': 'HIGH', 'MEDIUM': 'MEDIUM', 'LOW': 'LOW'
        }).fillna('LOW')
        db_df['churn_label_simulated'] = (score_df['churn_probability'] >= 0.5)
        # Add churn drivers (top 3 features contributing to this customer's score)
        db_df['driver_1'] = score_df.get('driver_1')
        db_df['driver_2'] = score_df.get('driver_2')
        db_df['driver_3'] = score_df.get('driver_3')
        db_df['model_version'] = 'v1.0'
        db_df['batch_run_id'] = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        db_df.to_sql('churn_scores', engine, if_exists='append', index=False)
    else:
        # Table doesn't exist — create fresh with our data
        score_df.to_sql('churn_scores', engine, if_exists='replace', index=False)

    engine.dispose()

    log.info("  Inserted %d rows into churn_scores", len(score_df))
    return len(score_df)


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
        print(f"    {level:8s}  {info['count']:4d}  ({info['pct']:5.1f}%)  {bar}")
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
    client_id: str = None,
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

    # 1. Load model
    bundle = load_model_bundle(model_path)
    model = bundle['model']
    scaler = bundle['scaler']
    feature_names = bundle['feature_names']

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

    # 3. Prepare features
    X_score, customer_info = prepare_features_for_scoring(df, feature_names)

    # 4. Scale
    X_scaled = scale_for_scoring(X_score, scaler)

    # 5. Predict (raw model output)
    raw_probabilities = predict_churn_probability(model, X_scaled)

    # 5b. Apply business-tier weighting (Platinum/Gold get boosted)
    probabilities = apply_tier_weighting(raw_probabilities, df)

    # 6. Risk levels (based on tier-adjusted probabilities)
    risk_levels = assign_risk_levels(probabilities)

    # 6b. Compute per-customer churn drivers (top 3 features)
    drivers_df = compute_churn_drivers(model, X_scaled, top_n=3)

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
            rows = save_scores_to_db(score_df, db_url)
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
        )
        sys.exit(0 if result.get('summary') else 1)


if __name__ == '__main__':
    main()
