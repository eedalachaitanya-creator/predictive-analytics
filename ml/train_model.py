"""
train_model.py — Analyst Agent | ML Model Training Pipeline
===========================================================
Fully modularized churn prediction model training pipeline.

Reads feature_matrix.csv or directly from PostgreSQL mv_customer_features,
cleans features (zero-variance, multicollinearity), trains models with
stratified K-fold cross-validation, tunes hyperparameters, evaluates
performance, and saves trained models + reports.

ARCHITECTURE:
    Section 0 — Configuration (constants, paths, defaults)
    Section 1 — Data Loading (CSV / PostgreSQL)
    Section 2 — Feature Cleaning (zero-variance, multicollinearity)
    Section 3 — Preprocessing (scaling, imbalance, feature selection)
    Section 4 — Model Definitions (XGBoost, Random Forest, Logistic Regression)
    Section 5 — Cross-Validation & Hyperparameter Tuning
    Section 6 — Evaluation & Metrics
    Section 7 — Visualization (ROC, confusion matrix, feature importance)
    Section 8 — Reporting (text report, model comparison)
    Section 9 — Model Persistence (save / load)
    Section 10 — Main Pipeline (orchestration)

Every step is its own function — features, preprocessing, and models can
be added/modified later without changing the overall structure.

Usage:
    # From project root (new_walmart/):
    python -m analyst_agent.ml.train_model --source csv
    python -m analyst_agent.ml.train_model --source csv --model-type all
    python -m analyst_agent.ml.train_model --source db
    python -m analyst_agent.ml.train_model --source csv --model-type xgboost --tune

Output files (saved to ml/output/ and ml/models/):
    - churn_model_[type]_[timestamp].joblib          → Trained model + scaler + metadata
    - training_report_[timestamp].txt                → Comprehensive training report
    - model_comparison_[timestamp].txt               → Side-by-side model comparison (if --model-type all)
    - roc_curve_[model_type].png                     → ROC curve plot
    - confusion_matrix_[model_type].png              → Confusion matrix heatmap
    - feature_importance_[model_type].png             → Top features bar chart
    - cross_val_scores_[model_type].png              → K-fold score distribution

Requirements:
    pip install scikit-learn xgboost joblib pandas numpy matplotlib seaborn psycopg2-binary sqlalchemy python-dotenv
    # Optional: pip install imbalanced-learn shap
"""

import os
import sys
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
log = logging.getLogger("train_model")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 0: CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

MODEL_DIR = Path(__file__).parent / "models"
REPORT_DIR = Path(__file__).parent / "output"
PLOT_DIR = REPORT_DIR / "plots"
MODEL_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)
PLOT_DIR.mkdir(exist_ok=True)

RANDOM_STATE = 42
TEST_SIZE = 0.2
N_FOLDS = 5
TARGET_COL = 'churn_label'

# Columns that are NOT features (identifiers, dates, metadata)
NON_FEATURE_COLS = [
    'client_id', 'customer_id', 'first_order_date', 'last_order_date',
    'last_review_date', 'computed_at',
]

# Ordinal encoding for customer tier
TIER_ORDER = {'Bronze': 1, 'Silver': 2, 'Gold': 3, 'Platinum': 4}

# ── LEAKY FEATURES ──────────────────────────────────────────────────────────
# These features directly encode or are derived from the churn label definition:
#   churn_label = 1 WHEN days_since_last_order > churn_window_days (90 days)
#
# Three types of leakage found:
#   1. Direct: days_since_last_order IS the churn formula input
#   2. Derived: rfm_recency_score, rfm_total_score are computed from #1
#   3. Time-window: 90-day features are 100% zero for churned customers
#      because churn = no orders in 90 days → spend/orders in last 90 = 0
#      180-day features are partially leaky (20% zero for churned)
#
# Keeping ANY of these gives inflated scores (up to 1.0) because the model
# is reading the answer, not predicting future churn.
LEAKY_COLS = [
    'days_since_last_order',       # Directly used to compute churn_label
    'rfm_recency_score',           # Derived from days_since_last_order via NTILE
    'rfm_total_score',             # Sum that includes rfm_recency_score
    'orders_last_90d',             # 100% zero for churned (= churn definition)
    'spend_last_90d_usd',          # 100% zero for churned (= churn definition)
    'orders_last_180d',            # Partially leaky (20% zero for churned)
    'spend_last_180d_usd',         # Partially leaky (20% zero for churned)
]

# Threshold for dropping highly correlated features
HIGH_CORR_THRESHOLD = 0.90

# Minimum variance threshold — features with variance below this are dropped
MIN_VARIANCE_THRESHOLD = 0.001

# Plot colors (matching compute_rfm.py palette)
COLOR_GREEN = '#70AD47'
COLOR_ORANGE = '#ED7D31'
COLOR_BLUE = '#2E75B6'
COLOR_RED = '#C00000'
COLOR_GRAY = '#808080'

# Default hyperparameter search spaces for tuning
XGBOOST_PARAM_GRID = {
    'n_estimators': [50, 100, 200, 300],
    'max_depth': [3, 5, 7, 10],
    'learning_rate': [0.01, 0.05, 0.1, 0.2],
    'subsample': [0.7, 0.8, 0.9, 1.0],
    'colsample_bytree': [0.7, 0.8, 0.9, 1.0],
    'min_child_weight': [1, 3, 5],
    'gamma': [0, 0.1, 0.3],
}

RF_PARAM_GRID = {
    'n_estimators': [50, 100, 200, 300],
    'max_depth': [5, 10, 15, 20, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 5],
    'max_features': ['sqrt', 'log2', None],
}

LR_PARAM_GRID = {
    'C': [0.001, 0.01, 0.1, 1, 10, 100],
    'penalty': ['l1', 'l2'],
    'solver': ['liblinear', 'saga'],
    'max_iter': [500, 1000, 2000],
}


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Load feature matrix from CSV file.

    Args:
        csv_path: Path to the CSV file (relative or absolute)

    Returns:
        DataFrame with all columns from CSV
    """
    log.info("Loading data from CSV: %s", csv_path)
    if not os.path.exists(csv_path):
        log.error("CSV file not found: %s", csv_path)
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    log.info("  Loaded %d rows x %d columns", df.shape[0], df.shape[1])
    return df


def load_from_database(db_url: str, client_id: str = None) -> pd.DataFrame:
    """
    Load feature matrix directly from PostgreSQL mv_customer_features.

    Args:
        db_url: PostgreSQL connection string
        client_id: If provided, only load this client's data

    Returns:
        DataFrame with all columns from materialized view
    """
    log.info("Connecting to database...")
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  Connected successfully")
    except Exception as e:
        log.error("  Failed to connect: %s", e)
        raise

    if client_id:
        log.info("  Loading from mv_customer_features WHERE client_id=%s", client_id)
        df = pd.read_sql(text("SELECT * FROM mv_customer_features WHERE client_id = :cid"), engine, params={"cid": client_id})
    else:
        log.info("  Loading from mv_customer_features (all clients)")
        df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)
    log.info("  Loaded %d rows x %d columns", df.shape[0], df.shape[1])
    return df


def prepare_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Convert raw DataFrame into (X, y) for modeling.

    Steps:
        1. Encode customer_tier → ordinal numeric (Bronze=1 ... Platinum=4)
        2. Drop non-feature columns (IDs, dates, metadata)
        3. Separate target from features
        4. Keep only numeric columns
        5. Fill remaining NaN with 0

    Returns:
        (X, y) — feature matrix and target series
    """
    log.info("Preparing features...")
    df_copy = df.copy()

    # Encode customer_tier if present (raw string form from database)
    if 'customer_tier' in df_copy.columns:
        df_copy['customer_tier_encoded'] = (
            df_copy['customer_tier'].map(TIER_ORDER).fillna(1).astype(int)
        )
        df_copy = df_copy.drop(columns=['customer_tier'])
        log.info("  Encoded customer_tier → customer_tier_encoded (1-4)")

    # Drop non-feature columns
    drop_cols = [c for c in NON_FEATURE_COLS if c in df_copy.columns]
    if drop_cols:
        df_copy = df_copy.drop(columns=drop_cols)
        log.info("  Dropped non-feature columns: %s", drop_cols)

    # Separate target
    if TARGET_COL not in df_copy.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found in data")
    y = df_copy[TARGET_COL].copy()
    X = df_copy.drop(columns=[TARGET_COL])

    # Keep only numeric
    X = X.select_dtypes(include=[np.number])

    # Fill NaN
    nan_count = X.isna().sum().sum()
    if nan_count > 0:
        log.warning("  Found %d NaN values — filling with 0", nan_count)
        X = X.fillna(0)

    log.info("  Feature matrix: %d rows x %d features", X.shape[0], X.shape[1])
    log.info("  Target distribution: %s", y.value_counts().to_dict())
    return X, y


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: FEATURE CLEANING
# ═══════════════════════════════════════════════════════════════════════════

def drop_leaky_features(X: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Remove features that leak the churn label.

    churn_label is defined as days_since_last_order > churn_window_days,
    so days_since_last_order (and features derived from it like
    rfm_recency_score, rfm_total_score) contain the answer.
    Keeping them gives 100% accuracy but zero real predictive power.

    Returns:
        (X_cleaned, dropped_columns)
    """
    log.info("Removing leaky features...")
    leaky_found = [c for c in LEAKY_COLS if c in X.columns]
    if leaky_found:
        log.info("  Dropping %d leaky features: %s", len(leaky_found), leaky_found)
        X = X.drop(columns=leaky_found)
    else:
        log.info("  No leaky features found in data")
    return X, leaky_found


def drop_zero_variance_features(X: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Remove features with zero or near-zero variance.

    These columns carry no predictive information. Based on EDA:
    orders_last_30d, spend_last_30d_usd, unique_categories_purchased,
    and all subscription features were identified as zero-variance.

    Returns:
        (X_cleaned, dropped_columns)
    """
    log.info("Checking for zero/near-zero variance features...")
    variances = X.var()
    low_var_cols = variances[variances < MIN_VARIANCE_THRESHOLD].index.tolist()

    if low_var_cols:
        log.info("  Dropping %d zero-variance features: %s", len(low_var_cols), low_var_cols)
        X = X.drop(columns=low_var_cols)
    else:
        log.info("  No zero-variance features found")

    return X, low_var_cols


def drop_highly_correlated_features(
    X: pd.DataFrame,
    threshold: float = HIGH_CORR_THRESHOLD
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Remove redundant features using pairwise correlation.

    When two features have |r| > threshold, the one with lower
    average correlation to the target-adjacent features is dropped.
    This reduces multicollinearity without losing signal.

    Args:
        X: Feature matrix
        threshold: Correlation cutoff (default 0.90)

    Returns:
        (X_cleaned, dropped_columns)
    """
    log.info("Checking for highly correlated features (threshold=%.2f)...", threshold)
    corr_matrix = X.corr().abs()

    # Upper triangle only (avoid double counting)
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))

    # Find columns to drop — for each correlated pair, drop the one with
    # higher mean correlation to all other features (more redundant)
    to_drop = set()
    for col in upper.columns:
        correlated_cols = upper.index[upper[col] > threshold].tolist()
        for corr_col in correlated_cols:
            if col not in to_drop and corr_col not in to_drop:
                # Keep the one with lower average correlation to others
                avg_corr_col = corr_matrix[col].drop([col, corr_col], errors='ignore').mean()
                avg_corr_other = corr_matrix[corr_col].drop([col, corr_col], errors='ignore').mean()
                drop_candidate = corr_col if avg_corr_other >= avg_corr_col else col
                to_drop.add(drop_candidate)

    dropped = sorted(to_drop)
    if dropped:
        log.info("  Dropping %d highly correlated features: %s", len(dropped), dropped)
        X = X.drop(columns=dropped)
    else:
        log.info("  No highly correlated features found")

    log.info("  Remaining features: %d", X.shape[1])
    return X, dropped


def clean_features(X: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """
    Run all feature cleaning steps in sequence.

    Returns:
        (X_cleaned, cleaning_log) where cleaning_log records what was removed
    """
    log.info("=" * 60)
    log.info("FEATURE CLEANING")
    log.info("=" * 60)
    cleaning_log = {}
    total_removed = 0

    X, leaky_dropped = drop_leaky_features(X)
    cleaning_log['leaky_dropped'] = leaky_dropped
    total_removed += len(leaky_dropped)

    X, zero_var_dropped = drop_zero_variance_features(X)
    cleaning_log['zero_variance_dropped'] = zero_var_dropped
    total_removed += len(zero_var_dropped)

    X, corr_dropped = drop_highly_correlated_features(X)
    cleaning_log['high_correlation_dropped'] = corr_dropped
    total_removed += len(corr_dropped)

    log.info("  Final feature count: %d (started with %d, removed %d)",
             X.shape[1],
             X.shape[1] + total_removed,
             total_removed)
    return X, cleaning_log


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: PREPROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def split_data(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = TEST_SIZE
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Stratified train/test split.

    Args:
        X: Feature matrix
        y: Target series
        test_size: Fraction for test set (default 0.2)

    Returns:
        (X_train, X_test, y_train, y_test)
    """
    from sklearn.model_selection import train_test_split
    log.info("Splitting data (test_size=%.2f, stratified)...", test_size)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=RANDOM_STATE, stratify=y
    )
    log.info("  Train: %d rows | Test: %d rows", len(X_train), len(X_test))
    log.info("  Train distribution: %s", y_train.value_counts().to_dict())
    log.info("  Test distribution:  %s", y_test.value_counts().to_dict())
    return X_train, X_test, y_train, y_test


def scale_features(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    method: str = 'standard'
) -> Tuple[pd.DataFrame, pd.DataFrame, Any]:
    """
    Scale features using the specified method. Fit on train, transform both.

    Args:
        method: 'standard' (mean=0, std=1), 'minmax' (0-1), or 'none'

    Returns:
        (X_train_scaled, X_test_scaled, scaler_object)
    """
    log.info("Scaling features (method=%s)...", method)

    if method == 'none':
        log.info("  Skipping scaling")
        return X_train, X_test, None

    from sklearn.preprocessing import StandardScaler, MinMaxScaler

    if method == 'standard':
        scaler = StandardScaler()
    elif method == 'minmax':
        scaler = MinMaxScaler()
    else:
        raise ValueError(f"Unknown scaling method: {method}")

    X_train_scaled = pd.DataFrame(
        scaler.fit_transform(X_train),
        columns=X_train.columns,
        index=X_train.index
    )
    X_test_scaled = pd.DataFrame(
        scaler.transform(X_test),
        columns=X_test.columns,
        index=X_test.index
    )
    log.info("  Scaling applied")
    return X_train_scaled, X_test_scaled, scaler


def handle_class_imbalance(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    method: str = 'none'
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Handle class imbalance in training data.

    NOTE: EDA showed ratio=0.88 (well-balanced), so default is 'none'.
    Override with --imbalance-method smote if needed for other datasets.

    Args:
        method: 'none', 'smote', or 'class_weight' (applied at model level)

    Returns:
        (X_resampled, y_resampled)
    """
    log.info("Class imbalance handling (method=%s)...", method)
    log.info("  Original: %s", y_train.value_counts().to_dict())

    if method in ('none', 'class_weight'):
        log.info("  No resampling applied")
        return X_train, y_train

    if method == 'smote':
        try:
            from imblearn.over_sampling import SMOTE
            smote = SMOTE(random_state=RANDOM_STATE)
            X_res, y_res = smote.fit_resample(X_train, y_train)
            # Ensure DataFrame/Series return (older imblearn returns numpy arrays)
            if not isinstance(X_res, pd.DataFrame):
                X_res = pd.DataFrame(X_res, columns=X_train.columns)
            if not isinstance(y_res, pd.Series):
                y_res = pd.Series(y_res, name=y_train.name)
            log.info("  SMOTE applied: %s", y_res.value_counts().to_dict())
            return X_res, y_res
        except ImportError:
            log.warning("  imbalanced-learn not installed — skipping SMOTE")
            return X_train, y_train

    raise ValueError(f"Unknown imbalance method: {method}")


def select_features_by_importance(
    X: pd.DataFrame,
    y: pd.Series,
    method: str = 'all',
    top_n: int = 20
) -> List[str]:
    """
    Select a subset of features using the specified method.

    Args:
        method: 'all' (keep everything), 'importance' (RF-based), or 'correlation'
        top_n: How many features to keep

    Returns:
        List of selected feature names
    """
    log.info("Feature selection (method=%s, top_n=%d)...", method, top_n)

    if method == 'all':
        log.info("  Keeping all %d features", X.shape[1])
        return list(X.columns)

    if method == 'importance':
        from sklearn.ensemble import RandomForestClassifier
        rf = RandomForestClassifier(n_estimators=100, random_state=RANDOM_STATE, n_jobs=-1)
        rf.fit(X, y)
        importances = pd.Series(rf.feature_importances_, index=X.columns).sort_values(ascending=False)
        selected = importances.head(top_n).index.tolist()
        log.info("  Selected %d features by RF importance", len(selected))
        log.info("  Top 5: %s", selected[:5])
        return selected

    if method == 'correlation':
        correlations = X.corrwith(y).abs().sort_values(ascending=False).dropna()
        selected = correlations.head(top_n).index.tolist()
        log.info("  Selected %d features by correlation", len(selected))
        log.info("  Top 5: %s", selected[:5])
        return selected

    raise ValueError(f"Unknown feature selection method: {method}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: MODEL DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

def build_xgboost(class_weight_ratio: Optional[float] = None, **params) -> Any:
    """
    Create an XGBoost classifier instance.

    Args:
        class_weight_ratio: If set, used as scale_pos_weight
        **params: Override any XGBoost hyperparameter

    Returns:
        Unfitted XGBClassifier
    """
    try:
        import xgboost as xgb
    except ImportError:
        log.error("XGBoost not installed. Run: pip install xgboost")
        raise

    defaults = {
        'n_estimators': 100,
        'max_depth': 6,
        'learning_rate': 0.1,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'random_state': RANDOM_STATE,
        'n_jobs': -1,
    }
    # eval_metric supported in XGBoost >= 1.3
    try:
        defaults['eval_metric'] = 'logloss'
    except Exception:
        pass
    if class_weight_ratio is not None:
        defaults['scale_pos_weight'] = class_weight_ratio
    defaults.update(params)
    return xgb.XGBClassifier(**defaults)


def build_random_forest(class_weight: Optional[str] = None, **params) -> Any:
    """
    Create a Random Forest classifier instance.

    Args:
        class_weight: 'balanced' or None
        **params: Override any RF hyperparameter

    Returns:
        Unfitted RandomForestClassifier
    """
    from sklearn.ensemble import RandomForestClassifier
    defaults = {
        'n_estimators': 100,
        'max_depth': 15,
        'min_samples_split': 10,
        'min_samples_leaf': 5,
        'random_state': RANDOM_STATE,
        'n_jobs': -1,
    }
    if class_weight:
        defaults['class_weight'] = class_weight
    defaults.update(params)
    return RandomForestClassifier(**defaults)


def build_logistic_regression(class_weight: Optional[str] = None, **params) -> Any:
    """
    Create a Logistic Regression classifier instance.

    Args:
        class_weight: 'balanced' or None
        **params: Override any LR hyperparameter

    Returns:
        Unfitted LogisticRegression
    """
    from sklearn.linear_model import LogisticRegression
    defaults = {
        'max_iter': 1000,
        'random_state': RANDOM_STATE,
        'solver': 'liblinear',
    }
    if class_weight:
        defaults['class_weight'] = class_weight
    defaults.update(params)
    return LogisticRegression(**defaults)


def build_model(model_type: str, class_weight: Optional[str] = None, **params) -> Any:
    """
    Factory function — build any supported model by name.

    Args:
        model_type: 'xgboost', 'random_forest', or 'logistic_regression'
        class_weight: 'balanced' or None (for RF/LR); converted to ratio for XGBoost
        **params: Passed to the specific builder

    Returns:
        Unfitted model instance
    """
    if model_type == 'xgboost':
        return build_xgboost(**params)
    elif model_type == 'random_forest':
        return build_random_forest(class_weight=class_weight, **params)
    elif model_type == 'logistic_regression':
        return build_logistic_regression(class_weight=class_weight, **params)
    else:
        raise ValueError(f"Unknown model type: {model_type}")


def train_single_model(
    model: Any,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_type: str = 'unknown'
) -> Any:
    """
    Fit a model on training data.

    Args:
        model: Unfitted model instance
        X_train: Training features
        y_train: Training target

    Returns:
        Fitted model
    """
    log.info("Training %s model...", model_type)
    if model_type == 'xgboost':
        model.fit(X_train, y_train, verbose=False)
    else:
        model.fit(X_train, y_train)
    log.info("  %s trained successfully", model_type)
    return model


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: CROSS-VALIDATION & HYPERPARAMETER TUNING
# ═══════════════════════════════════════════════════════════════════════════

def cross_validate_model(
    model: Any,
    X: pd.DataFrame,
    y: pd.Series,
    n_folds: int = N_FOLDS,
    scoring: str = 'roc_auc'
) -> Dict[str, Any]:
    """
    Run stratified K-fold cross-validation.

    More reliable than a single train/test split — especially important
    with only 199 samples. Reports mean and std of each metric.

    Args:
        model: Unfitted model instance
        X: Full feature matrix (pre-split)
        y: Full target
        n_folds: Number of folds (default 5)
        scoring: Primary metric

    Returns:
        Dict with per-fold and aggregate scores
    """
    from sklearn.model_selection import StratifiedKFold, cross_validate

    log.info("Running %d-fold cross-validation (scoring=%s)...", n_folds, scoring)

    cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    scoring_metrics = ['accuracy', 'precision', 'recall', 'f1', 'roc_auc']
    cv_results = cross_validate(
        model, X, y, cv=cv, scoring=scoring_metrics,
        return_train_score=True, n_jobs=-1
    )

    results = {}
    for metric in scoring_metrics:
        test_scores = cv_results[f'test_{metric}']
        train_scores = cv_results[f'train_{metric}']
        results[metric] = {
            'test_mean': test_scores.mean(),
            'test_std': test_scores.std(),
            'test_folds': test_scores.tolist(),
            'train_mean': train_scores.mean(),
            'train_std': train_scores.std(),
        }
        log.info("  %s: test=%.4f (±%.4f) | train=%.4f (±%.4f)",
                 metric,
                 test_scores.mean(), test_scores.std(),
                 train_scores.mean(), train_scores.std())

        # Check for overfitting: large gap between train and test
        gap = train_scores.mean() - test_scores.mean()
        if gap > 0.10:
            log.warning("  ⚠ Possible overfitting on %s (train-test gap: %.4f)", metric, gap)

    return results


def tune_hyperparameters(
    model_type: str,
    X: pd.DataFrame,
    y: pd.Series,
    n_folds: int = N_FOLDS,
    n_iter: int = 30,
    scoring: str = 'roc_auc'
) -> Tuple[Any, Dict[str, Any]]:
    """
    Randomized hyperparameter search with stratified K-fold.

    Args:
        model_type: 'xgboost', 'random_forest', or 'logistic_regression'
        X: Feature matrix
        y: Target
        n_folds: CV folds
        n_iter: Number of random parameter combinations to try
        scoring: Optimization metric

    Returns:
        (best_model, best_params)
    """
    from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold

    log.info("Tuning hyperparameters for %s (%d iterations, %d-fold CV)...",
             model_type, n_iter, n_folds)

    # Get base model and param grid
    base_model = build_model(model_type)
    if model_type == 'xgboost':
        param_grid = XGBOOST_PARAM_GRID
    elif model_type == 'random_forest':
        param_grid = RF_PARAM_GRID
    elif model_type == 'logistic_regression':
        param_grid = LR_PARAM_GRID
    else:
        raise ValueError(f"No param grid for model type: {model_type}")

    cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    search = RandomizedSearchCV(
        estimator=base_model,
        param_distributions=param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring=scoring,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbose=0,
        refit=True,
    )

    search.fit(X, y)

    log.info("  Best %s score: %.4f", scoring, search.best_score_)
    log.info("  Best parameters: %s", search.best_params_)

    return search.best_estimator_, search.best_params_


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6: EVALUATION & METRICS
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_model(
    model: Any,
    X_test: pd.DataFrame,
    y_test: pd.Series
) -> Dict[str, Any]:
    """
    Evaluate a fitted model on the test set.

    Returns:
        Dict with accuracy, precision, recall, f1, auc_roc,
        confusion_matrix, classification_report, y_pred, y_pred_proba
    """
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, confusion_matrix, classification_report
    )

    log.info("Evaluating model on test set...")
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        'accuracy': accuracy_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred, zero_division=0),
        'recall': recall_score(y_test, y_pred, zero_division=0),
        'f1': f1_score(y_test, y_pred, zero_division=0),
        'auc_roc': roc_auc_score(y_test, y_pred_proba),
        'confusion_matrix': confusion_matrix(y_test, y_pred),
        'classification_report': classification_report(y_test, y_pred),
        'y_pred': y_pred,
        'y_pred_proba': y_pred_proba,
    }

    log.info("  Accuracy:  %.4f", metrics['accuracy'])
    log.info("  Precision: %.4f", metrics['precision'])
    log.info("  Recall:    %.4f", metrics['recall'])
    log.info("  F1 Score:  %.4f", metrics['f1'])
    log.info("  AUC-ROC:   %.4f", metrics['auc_roc'])
    return metrics


def get_feature_importances(
    model: Any,
    feature_names: List[str],
    model_type: str
) -> pd.DataFrame:
    """
    Extract feature importances from a trained model.

    Returns:
        DataFrame with columns ['feature', 'importance'], sorted descending
    """
    log.info("Extracting feature importances (%s)...", model_type)

    if model_type in ('xgboost', 'random_forest'):
        importances = model.feature_importances_
    elif model_type == 'logistic_regression':
        importances = np.abs(model.coef_[0])
    else:
        log.warning("  Feature importance not available for %s", model_type)
        return pd.DataFrame(columns=['feature', 'importance'])

    df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False).reset_index(drop=True)

    log.info("  Top 5 features: %s", list(df.head(5)['feature']))
    return df


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7: VISUALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def plot_roc_curve(
    y_test: pd.Series,
    y_pred_proba: np.ndarray,
    model_name: str,
    output_dir: Path
) -> Optional[Path]:
    """Plot and save ROC curve with AUC annotation."""
    log.info("Plotting ROC curve (%s)...", model_name)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from sklearn.metrics import roc_curve, auc

        fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
        roc_auc = auc(fpr, tpr)

        fig, ax = plt.subplots(figsize=(8, 6))
        ax.plot(fpr, tpr, color=COLOR_BLUE, lw=2.5,
                label=f'ROC Curve (AUC = {roc_auc:.3f})')
        ax.plot([0, 1], [0, 1], color=COLOR_GRAY, lw=1.5,
                linestyle='--', label='Random Classifier')
        ax.set_xlabel('False Positive Rate', fontsize=11)
        ax.set_ylabel('True Positive Rate', fontsize=11)
        ax.set_title(f'ROC Curve — {model_name}', fontsize=13, fontweight='bold')
        ax.legend(loc='lower right', fontsize=10)
        ax.grid(True, alpha=0.3)

        output_path = output_dir / f"roc_curve_{model_name}.png"
        fig.savefig(output_path, dpi=200, bbox_inches='tight')
        plt.close()
        log.info("  Saved → %s", output_path)
        return output_path
    except ImportError:
        log.warning("  matplotlib not installed — skipping")
        return None


def plot_confusion_matrix(
    y_test: pd.Series,
    y_pred: np.ndarray,
    model_name: str,
    output_dir: Path
) -> Optional[Path]:
    """Plot and save confusion matrix heatmap."""
    log.info("Plotting confusion matrix (%s)...", model_name)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import seaborn as sns
        from sklearn.metrics import confusion_matrix

        cm = confusion_matrix(y_test, y_pred)

        fig, ax = plt.subplots(figsize=(7, 6))
        sns.heatmap(
            cm, annot=True, fmt='d', cmap='Blues', cbar=True,
            xticklabels=['Active', 'Churned'],
            yticklabels=['Active', 'Churned'],
            ax=ax, cbar_kws={'label': 'Count'},
            annot_kws={'size': 16}
        )
        ax.set_xlabel('Predicted', fontsize=12)
        ax.set_ylabel('Actual', fontsize=12)
        ax.set_title(f'Confusion Matrix — {model_name}', fontsize=13, fontweight='bold')

        output_path = output_dir / f"confusion_matrix_{model_name}.png"
        fig.savefig(output_path, dpi=200, bbox_inches='tight')
        plt.close()
        log.info("  Saved → %s", output_path)
        return output_path
    except ImportError:
        log.warning("  matplotlib/seaborn not installed — skipping")
        return None


def plot_feature_importance(
    importance_df: pd.DataFrame,
    model_name: str,
    output_dir: Path,
    top_n: int = 20
) -> Optional[Path]:
    """Plot horizontal bar chart of top feature importances."""
    log.info("Plotting feature importance (%s, top %d)...", model_name, top_n)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        plot_df = importance_df.head(top_n).iloc[::-1]  # Reverse for horizontal bar

        fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.35)))
        colors = [COLOR_BLUE if i >= len(plot_df) - 5 else COLOR_ORANGE
                  for i in range(len(plot_df))]
        ax.barh(plot_df['feature'], plot_df['importance'], color=colors)
        ax.set_xlabel('Importance Score', fontsize=11)
        ax.set_ylabel('Feature', fontsize=11)
        ax.set_title(f'Top {top_n} Features — {model_name}', fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='x')

        output_path = output_dir / f"feature_importance_{model_name}.png"
        fig.savefig(output_path, dpi=200, bbox_inches='tight')
        plt.close()
        log.info("  Saved → %s", output_path)
        return output_path
    except ImportError:
        log.warning("  matplotlib not installed — skipping")
        return None


def plot_cross_val_scores(
    cv_results: Dict[str, Any],
    model_name: str,
    output_dir: Path
) -> Optional[Path]:
    """Plot boxplot of cross-validation fold scores for each metric."""
    log.info("Plotting cross-validation scores (%s)...", model_name)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        metrics_to_plot = ['accuracy', 'precision', 'recall', 'f1', 'roc_auc']
        available = [m for m in metrics_to_plot if m in cv_results]

        if not available:
            log.warning("  No CV metrics to plot")
            return None

        fig, ax = plt.subplots(figsize=(10, 6))
        data = [cv_results[m]['test_folds'] for m in available]
        bp = ax.boxplot(data, labels=available, patch_artist=True)

        for box in bp['boxes']:
            box.set_facecolor(COLOR_BLUE)
            box.set_alpha(0.6)

        ax.set_ylabel('Score', fontsize=11)
        ax.set_title(f'{N_FOLDS}-Fold Cross-Validation — {model_name}',
                     fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_ylim(0, 1.05)

        # Add mean markers
        means = [cv_results[m]['test_mean'] for m in available]
        ax.scatter(range(1, len(available) + 1), means,
                   color=COLOR_ORANGE, marker='D', s=80, zorder=5, label='Mean')
        ax.legend(loc='lower left')

        output_path = output_dir / f"cross_val_scores_{model_name}.png"
        fig.savefig(output_path, dpi=200, bbox_inches='tight')
        plt.close()
        log.info("  Saved → %s", output_path)
        return output_path
    except ImportError:
        log.warning("  matplotlib not installed — skipping")
        return None


def generate_all_plots(
    y_test: pd.Series,
    metrics: Dict[str, Any],
    importance_df: pd.DataFrame,
    cv_results: Optional[Dict[str, Any]],
    model_name: str,
    output_dir: Path
) -> List[Path]:
    """
    Generate all visualization plots for a trained model.

    Returns:
        List of saved file paths
    """
    log.info("Generating all plots for %s...", model_name)
    plots = []

    path = plot_roc_curve(y_test, metrics['y_pred_proba'], model_name, output_dir)
    if path:
        plots.append(path)

    path = plot_confusion_matrix(y_test, metrics['y_pred'], model_name, output_dir)
    if path:
        plots.append(path)

    if not importance_df.empty:
        path = plot_feature_importance(importance_df, model_name, output_dir)
        if path:
            plots.append(path)

    if cv_results:
        path = plot_cross_val_scores(cv_results, model_name, output_dir)
        if path:
            plots.append(path)

    log.info("  Generated %d plots", len(plots))
    return plots


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8: REPORTING
# ═══════════════════════════════════════════════════════════════════════════

def generate_training_report(
    model_name: str,
    metrics: Dict[str, Any],
    cv_results: Optional[Dict[str, Any]],
    importance_df: pd.DataFrame,
    cleaning_log: Dict[str, List[str]],
    feature_names: List[str],
    data_shape: Tuple[int, int],
    output_dir: Path
) -> Path:
    """
    Generate a comprehensive text report of model training results.

    Returns:
        Path to saved report file
    """
    log.info("Generating training report (%s)...", model_name)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    lines = []
    lines.append("=" * 75)
    lines.append("  CHURN PREDICTION — MODEL TRAINING REPORT")
    lines.append("=" * 75)
    lines.append(f"  Model:          {model_name}")
    lines.append(f"  Generated:      {timestamp}")
    lines.append(f"  Dataset:        {data_shape[0]} rows x {data_shape[1]} original features")
    lines.append(f"  Final features: {len(feature_names)}")
    lines.append(f"  Test size:      {TEST_SIZE * 100:.0f}%")
    lines.append(f"  Random state:   {RANDOM_STATE}")
    lines.append("")

    # Feature cleaning summary
    lines.append("-" * 75)
    lines.append("  FEATURE CLEANING SUMMARY")
    lines.append("-" * 75)
    lk = cleaning_log.get('leaky_dropped', [])
    zv = cleaning_log.get('zero_variance_dropped', [])
    hc = cleaning_log.get('high_correlation_dropped', [])
    lines.append(f"  Leaky features removed ({len(lk)}):")
    for f in lk:
        lines.append(f"    - {f}")
    lines.append(f"  Zero-variance features removed ({len(zv)}):")
    for f in zv:
        lines.append(f"    - {f}")
    lines.append(f"  Highly correlated features removed ({len(hc)}):")
    for f in hc:
        lines.append(f"    - {f}")
    lines.append(f"  Features retained: {len(feature_names)}")
    lines.append("")

    # Test set metrics
    lines.append("-" * 75)
    lines.append("  TEST SET PERFORMANCE")
    lines.append("-" * 75)
    lines.append(f"  Accuracy:    {metrics['accuracy']:.4f}")
    lines.append(f"  Precision:   {metrics['precision']:.4f}")
    lines.append(f"  Recall:      {metrics['recall']:.4f}")
    lines.append(f"  F1 Score:    {metrics['f1']:.4f}")
    lines.append(f"  AUC-ROC:     {metrics['auc_roc']:.4f}")
    lines.append("")

    # Confusion matrix
    cm = metrics['confusion_matrix']
    lines.append("  Confusion Matrix:")
    lines.append(f"                    Predicted Active  Predicted Churned")
    lines.append(f"    Actual Active       {cm[0][0]:>5d}             {cm[0][1]:>5d}")
    lines.append(f"    Actual Churned      {cm[1][0]:>5d}             {cm[1][1]:>5d}")
    lines.append("")

    # Cross-validation results
    if cv_results:
        lines.append("-" * 75)
        lines.append(f"  {N_FOLDS}-FOLD CROSS-VALIDATION RESULTS")
        lines.append("-" * 75)
        for metric_name, scores in cv_results.items():
            lines.append(f"  {metric_name:12s}  test: {scores['test_mean']:.4f} (±{scores['test_std']:.4f})  "
                         f"train: {scores['train_mean']:.4f} (±{scores['train_std']:.4f})")
        lines.append("")

    # Classification report
    lines.append("-" * 75)
    lines.append("  DETAILED CLASSIFICATION REPORT")
    lines.append("-" * 75)
    lines.append(metrics['classification_report'])
    lines.append("")

    # Feature importance
    if not importance_df.empty:
        lines.append("-" * 75)
        lines.append("  TOP 15 FEATURE IMPORTANCES")
        lines.append("-" * 75)
        for _, row in importance_df.head(15).iterrows():
            bar = "█" * int(row['importance'] * 50)
            lines.append(f"  {row['feature']:35s}  {row['importance']:.4f}  {bar}")
        lines.append("")

    lines.append("=" * 75)
    lines.append("  Generated by Analyst Agent | Churn Prediction Pipeline")
    lines.append("=" * 75)

    report_text = "\n".join(lines)
    report_path = output_dir / f"training_report_{model_name}.txt"
    with open(report_path, 'w') as f:
        f.write(report_text)

    log.info("  Saved → %s", report_path)
    return report_path


def generate_comparison_report(
    all_results: Dict[str, Dict[str, Any]],
    output_dir: Path
) -> Path:
    """
    Compare multiple models side-by-side and identify the best.

    Returns:
        Path to saved comparison report
    """
    log.info("Generating model comparison report...")

    rows = []
    for name, data in all_results.items():
        m = data['metrics']
        rows.append({
            'Model': name,
            'Accuracy': m['accuracy'],
            'Precision': m['precision'],
            'Recall': m['recall'],
            'F1': m['f1'],
            'AUC-ROC': m['auc_roc'],
        })

    df = pd.DataFrame(rows).sort_values('AUC-ROC', ascending=False)
    best_name = df.iloc[0]['Model']

    lines = []
    lines.append("=" * 75)
    lines.append("  MODEL COMPARISON REPORT")
    lines.append("=" * 75)
    lines.append("")
    lines.append(df.to_string(index=False))
    lines.append("")
    lines.append(f"  ★ Best model: {best_name} (AUC-ROC: {df.iloc[0]['AUC-ROC']:.4f})")
    lines.append("=" * 75)

    report_text = "\n".join(lines)
    report_path = output_dir / "model_comparison.txt"
    with open(report_path, 'w') as f:
        f.write(report_text)

    log.info("  Best model: %s", best_name)
    log.info("  Saved → %s", report_path)
    return report_path


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 9: MODEL PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════════

def save_model(
    model: Any,
    scaler: Any,
    feature_names: List[str],
    metadata: Dict[str, Any],
    output_path: Path
) -> Path:
    """
    Save model + scaler + feature list + metadata as a single joblib bundle.

    This bundle contains everything needed to make predictions on new data.
    """
    log.info("Saving model → %s", output_path)
    package = {
        'model': model,
        'scaler': scaler,
        'feature_names': feature_names,
        'metadata': metadata,
    }
    joblib.dump(package, output_path)
    log.info("  Model saved successfully")
    return output_path


def load_saved_model(model_path: Path) -> Tuple[Any, Any, List[str], Dict[str, Any]]:
    """
    Load a saved model bundle.

    Returns:
        (model, scaler, feature_names, metadata)
    """
    log.info("Loading model from %s", model_path)
    package = joblib.load(model_path)
    log.info("  Model type: %s", package['metadata'].get('model_type', 'unknown'))
    log.info("  Training date: %s", package['metadata'].get('training_date', 'unknown'))
    log.info("  Features: %d", len(package['feature_names']))
    return (
        package['model'],
        package.get('scaler'),
        package['feature_names'],
        package['metadata'],
    )


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 10: MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Churn Prediction Model Training Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m analyst_agent.ml.train_model --source csv
  python -m analyst_agent.ml.train_model --source csv --model-type all
  python -m analyst_agent.ml.train_model --source csv --model-type xgboost --tune
  python -m analyst_agent.ml.train_model --source db --db-url postgresql://user:pass@localhost/db
        """
    )
    parser.add_argument('--source', choices=['csv', 'db'], default='csv',
                        help='Data source (default: csv)')
    parser.add_argument('--csv-path', type=str,
                        default=str(REPORT_DIR / 'feature_matrix.csv'),
                        help='Path to CSV file')
    parser.add_argument('--db-url', type=str,
                        help='PostgreSQL URL (also reads DB_URL from .env)')
    parser.add_argument('--model-type',
                        choices=['xgboost', 'random_forest', 'logistic_regression', 'all'],
                        default='xgboost',
                        help='Model to train (default: xgboost)')
    parser.add_argument('--imbalance-method', choices=['smote', 'class_weight', 'none'],
                        default='none',
                        help='Class imbalance handling (default: none — data is balanced)')
    parser.add_argument('--scale-method', choices=['standard', 'minmax', 'none'],
                        default='standard',
                        help='Feature scaling (default: standard)')
    parser.add_argument('--feature-selection', choices=['importance', 'correlation', 'all'],
                        default='all',
                        help='Feature selection after cleaning (default: all)')
    parser.add_argument('--top-features', type=int, default=20,
                        help='Features to keep if using importance/correlation selection')
    parser.add_argument('--skip-cleaning', action='store_true',
                        help='Skip zero-variance and multicollinearity removal')
    parser.add_argument('--tune', action='store_true',
                        help='Run hyperparameter tuning (RandomizedSearchCV)')
    parser.add_argument('--tune-iter', type=int, default=30,
                        help='Number of tuning iterations (default: 30)')
    parser.add_argument('--no-cv', action='store_true',
                        help='Skip cross-validation')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating plots')
    parser.add_argument('--client-id', type=str, default=None,
                        help='Filter data by client_id (e.g., CLT-002)')
    return parser.parse_args()


def run_pipeline_for_model(
    model_type: str,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    X_full: pd.DataFrame,
    y_full: pd.Series,
    feature_names: List[str],
    cleaning_log: Dict[str, List[str]],
    original_shape: Tuple[int, int],
    scaler: Any,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    """
    Train, evaluate, and save a single model. This is the inner loop
    called once per model type.

    Returns:
        Dict with 'model', 'metrics', 'cv_results', 'importance_df', 'report_path'
    """
    log.info("")
    log.info("=" * 75)
    log.info("  TRAINING: %s", model_type.upper())
    log.info("=" * 75)

    # ─ Hyperparameter tuning or default build
    if args.tune:
        model, best_params = tune_hyperparameters(
            model_type, X_train, y_train,
            n_iter=args.tune_iter
        )
        log.info("  Best params: %s", best_params)
    else:
        model = build_model(model_type)
        model = train_single_model(model, X_train, y_train, model_type)

    # ─ Cross-validation (on full data with fresh model)
    # Uses Pipeline with scaler so LR gets properly scaled data inside each fold
    cv_results = None
    if not args.no_cv:
        cv_model = build_model(model_type)
        if model_type == 'logistic_regression' and args.scale_method != 'none':
            from sklearn.pipeline import Pipeline
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            scaler_cls = StandardScaler if args.scale_method == 'standard' else MinMaxScaler
            cv_pipeline = Pipeline([('scaler', scaler_cls()), ('model', cv_model)])
            cv_results = cross_validate_model(cv_pipeline, X_full, y_full)
        else:
            cv_results = cross_validate_model(cv_model, X_full, y_full)

    # ─ Evaluate on held-out test set
    metrics = evaluate_model(model, X_test, y_test)

    # ─ Feature importance
    importance_df = get_feature_importances(model, feature_names, model_type)

    # ─ Plots
    if not args.no_plots:
        generate_all_plots(y_test, metrics, importance_df, cv_results, model_type, PLOT_DIR)

    # ─ Training report
    report_path = generate_training_report(
        model_type, metrics, cv_results, importance_df,
        cleaning_log, feature_names, original_shape, REPORT_DIR
    )

    # ─ Save model
    metadata = {
        'model_type': model_type,
        'training_date': datetime.now().isoformat(),
        'test_size': TEST_SIZE,
        'random_state': RANDOM_STATE,
        'n_features': len(feature_names),
        'feature_names': feature_names,
        'cleaning_log': cleaning_log,
        'tuned': args.tune,
        'metrics': {
            'accuracy': float(metrics['accuracy']),
            'precision': float(metrics['precision']),
            'recall': float(metrics['recall']),
            'f1': float(metrics['f1']),
            'auc_roc': float(metrics['auc_roc']),
        }
    }
    model_path = MODEL_DIR / f"churn_model_{model_type}.joblib"
    save_model(model, scaler, feature_names, metadata, model_path)

    return {
        'model': model,
        'metrics': metrics,
        'cv_results': cv_results,
        'importance_df': importance_df,
        'report_path': report_path,
        'model_path': model_path,
    }


def main():
    """Main entry point — orchestrates the full training pipeline."""
    args = parse_args()
    load_dotenv()

    log.info("=" * 75)
    log.info("  ANALYST AGENT — CHURN PREDICTION MODEL TRAINING")
    log.info("=" * 75)

    # ── Step 1: Load data ──────────────────────────────────────────────────
    if args.source == 'csv':
        df = load_from_csv(args.csv_path)
    else:
        db_url = args.db_url or os.getenv('DB_URL')
        if not db_url:
            log.error("Database URL required. Use --db-url or set DB_URL in .env")
            sys.exit(1)
        df = load_from_database(db_url, client_id=args.client_id)

    original_shape = df.shape

    # ── Step 2: Prepare features ───────────────────────────────────────────
    X, y = prepare_features(df)

    # ── Step 3: Clean features ─────────────────────────────────────────────
    cleaning_log = {'zero_variance_dropped': [], 'high_correlation_dropped': []}
    if not args.skip_cleaning:
        X, cleaning_log = clean_features(X)

    # ── Step 4: Feature selection ──────────────────────────────────────────
    selected = select_features_by_importance(X, y, method=args.feature_selection,
                                             top_n=args.top_features)
    X = X[selected]
    feature_names = list(X.columns)

    # Keep full data for cross-validation
    X_full = X.copy()
    y_full = y.copy()

    # ── Step 5: Train/test split ───────────────────────────────────────────
    X_train, X_test, y_train, y_test = split_data(X, y)

    # ── Step 6: Handle class imbalance ─────────────────────────────────────
    X_train, y_train = handle_class_imbalance(X_train, y_train, method=args.imbalance_method)

    # ── Step 7: Scale features ─────────────────────────────────────────────
    X_train, X_test, scaler = scale_features(X_train, X_test, method=args.scale_method)

    # Also scale full data for CV (fit fresh scaler inside CV, so just pass unscaled)
    # CV uses X_full/y_full which are unscaled — sklearn CV handles this internally

    # ── Step 8: Train model(s) ─────────────────────────────────────────────
    models_to_train = (
        ['xgboost', 'random_forest', 'logistic_regression']
        if args.model_type == 'all'
        else [args.model_type]
    )

    all_results = {}
    for model_type in models_to_train:
        try:
            result = run_pipeline_for_model(
                model_type=model_type,
                X_train=X_train, X_test=X_test,
                y_train=y_train, y_test=y_test,
                X_full=X_full, y_full=y_full,
                feature_names=feature_names,
                cleaning_log=cleaning_log,
                original_shape=original_shape,
                scaler=scaler,
                args=args,
            )
            all_results[model_type] = result
        except Exception as e:
            log.error("Failed to train %s: %s", model_type, e)
            if args.model_type != 'all':
                raise

    # ── Step 9: Compare models ─────────────────────────────────────────────
    if len(all_results) > 1:
        generate_comparison_report(
            {name: res for name, res in all_results.items()},
            REPORT_DIR
        )

    # ── Done ───────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 75)
    log.info("  TRAINING COMPLETE")
    log.info("  Models saved to:  %s", MODEL_DIR)
    log.info("  Reports saved to: %s", REPORT_DIR)
    log.info("  Plots saved to:   %s", PLOT_DIR)
    log.info("=" * 75)


if __name__ == '__main__':
    main()
