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
    Section 4 — Model Definitions (XGBoost, Random Forest)
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

# ── GRAY-ZONE EXCLUSION (Step 3, 2026-04-22) ────────────────────────────────
# The churn label has a hard cutoff: churn_label = 1 WHEN DSLO >= 90.
# Customers with DSLO very close to 90 days carry ambiguous labels —
# a DSLO=85 ("active") and a DSLO=95 ("churned") customer behave almost
# identically in every other feature, yet receive opposite labels. Training
# on these boundary customers forces the model to fit arbitrary label flips
# and injects noise into the learned decision boundary.
#
# We exclude DSLO in [85, 95) from the TRAINING set only. The test set,
# cross-validation set, and live prediction input keep all customers, so
# evaluation metrics reflect real-world performance.
#
# ── Window history (2026-04-22) ─────────────────────────────────────────────
# First attempt used [75, 105) (±15 around the cutoff). That window removed
# too many borderline training rows, so the model never saw "ambiguous"
# patterns and polarized to near-0 or near-1 at inference. The dashboard
# showed 128 HIGH at 99.4–99.5% with only 6 MEDIUM between them — a clear
# bimodal polarization fingerprint. Narrowed to ±5 to keep DSLO 75-84 and
# 95-105 in the training set so the model still sees the full
# probability continuum while we strip only the most-ambiguous boundary.
GRAY_ZONE_LOWER = 85   # Customers with DSLO >= this are ambiguous
GRAY_ZONE_UPPER = 95   # Customers with DSLO < this are ambiguous

# ── Login gray-zone (Phase 4 — login-aware churn rule, 2026-04-25) ──────────
# Same idea as the order gray-zone above, applied to the SECOND condition of
# the new two-condition churn label. A customer at exactly
# days_since_last_login = 30 (login_window_days) sits on the boundary; small
# day-to-day variance (one log-in or no log-in this morning) flips their
# label between 0 and 1. ±5 around the default login window mirrors the
# order side and keeps the impact symmetric.
LOGIN_GRAY_ZONE_LOWER = 25   # Customers with DSLI >= this are ambiguous
LOGIN_GRAY_ZONE_UPPER = 35   # Customers with DSLI < this are ambiguous

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
    # ── Tenure-based leaks ──────────────────────────────────────────────
    # In the synthetic data, churned customers were generated with older
    # signup dates (they've been around long enough to churn), so
    # account_age_days and related date fields correlate with the target
    # almost as strongly as recency does. Model global importance for
    # account_age_days was 0.16 (#2 feature) with every top-risk customer
    # showing it as their #1 SHAP driver — classic target leak via tenure.
    'account_age_days',            # Tenure proxy for churn in synthetic data
    'first_order_date',            # Raw date form of account age
    'last_order_date',             # Derived from recency (same concern)
    # ── Algebraic leak via refill cycle ─────────────────────────────────
    # days_overdue_for_refill is computed as:
    #     CURRENT_DATE - (last_purchase_date + avg_refill_days)
    # which is algebraically identical to:
    #     days_since_last_order - avg_refill_days
    # So for any fixed avg_refill_days the feature is a linear shift of
    # DSLO and carries the churn label almost verbatim. Observed effect:
    # Logistic Regression saturated to exact 1.000 for short-history
    # Bronze customers (e.g., Edward Williams, 2 orders) even when all
    # other real-behaviour features were neutral. Drop to force the
    # model to learn from genuine behavioural signal.
    'days_overdue_for_refill',     # = DSLO − avg_refill_days (algebraic leak)
    # ── 90-day behavioral-trend leaks (Step 2 migration, 2026-04-22) ────
    # All 9 of these features are computed on the last-90-day window,
    # which is the SAME window as the churn label
    # (churn_label = 1 WHEN days_since_last_order >= 90). When a customer
    # has no orders in the last 90 days (the churn definition itself),
    # every one of these features collapses to an extreme value — 0,
    # NaN→0, or −100 — acting as a perfect proxy for the label.
    # Observed effect: AUC jumped from 0.877 → 1.000 on both
    # XGBoost and Random Forest immediately after these features
    # appeared in the feature set. Classic target-window leak.
    # Leave the Step-2 migration's MV columns in place for EDA / BI
    # but exclude them from the training feature matrix.
    'avg_order_value_last_90d_usd',   # = 0 when orders_last_90d = 0
    'aov_trend_pct',                  # = −100 when 90d AOV = 0
    'avg_items_per_order_last_90d',   # = 0 when orders_last_90d = 0
    'basket_size_trend_pct',          # = −100 when 90d basket = 0
    'orders_with_discount_last_90d',  # = 0 when orders_last_90d = 0
    'pct_orders_discounted_last_90d', # = 0 / NULL when orders_last_90d = 0
    'discount_rate_last_90d_pct',     # = 0 when spend_last_90d = 0
    'spend_velocity_ratio',           # = 0 when spend_last_90d = 0
    'order_gap_inflation_pct',        # collapses for no-recent-order customers
    # ── Login-aware churn leaks (Phase 2 migration, 2026-04-24) ─────────
    # The churn label is now a TWO-condition rule:
    #   churn_label = 1
    #     WHEN days_since_last_order >= churn_window_days
    #      AND days_since_last_login   >  login_window_days
    # That makes both `last_login_date` and `days_since_last_login` part of
    # the label definition itself, not predictors. Including them in the
    # feature matrix would trivially leak the target. The MV still exposes
    # them so the dashboard / agent can show "logged in N days ago" — they
    # just don't reach the model.
    'last_login_date',                # raw date used to derive label
    'days_since_last_login',          # = ref_date − last_login_date
    # ── Redundancy exclusion (2026-04-25) ───────────────────────────────
    # is_high_value is NOT a target leak — it's redundant with
    # customer_tier_encoded. In quartile mode it differs from the Platinum
    # tier flag only by a 5-percentage-point cutoff (75 vs 80); in custom
    # mode it uses the SAME threshold as Platinum (`custom_platinum_min`),
    # making it literally identical. The two flags ride on the same axis
    # (net spend) and were burning two of our 34 feature slots on one
    # signal. The correlation-drop step at threshold 0.90 was missing it
    # because binary vs ordinal-encoded gave them just enough divergence
    # to sneak through.
    #
    # We DON'T drop the column from the MV (the strategist still consumes
    # `mv.is_high_value` in repositories.py / strategist_router.py), so
    # it must be excluded here at training time only.
    'is_high_value',
]

# ── REVIEW FEATURES (excluded from model) ───────────────────────────────────
# Why we're dropping these:
#   1. In the MV, customers who have never written a review get the sentinel
#      `days_since_last_review = 9999` and `total_reviews = 0`. Because
#      non-reviewing customers overlap heavily with churned customers, the
#      model latches onto these features as a proxy for "has no reviews"
#      rather than learning real transaction-based churn signal.
#   2. For a retail transaction-based churn model, purchase recency /
#      frequency / monetary value should dominate. Review behaviour is noisy
#      (most customers never review at all) and not a leading indicator of
#      future churn — more of a lagging after-the-fact signal.
#   3. Previous dashboards showed review-based features ("Days Since Last
#      Review", "Avg Rating") as the #1 driver for most HIGH-risk customers,
#      crowding out the transaction features we actually want to act on.
#
# If you later want to bring review signal back in, consider:
#   - Only including reviews written within the last N days (not 9999 sentinel)
#   - Using a separate sentiment model on top of churn, not as a feature
REVIEW_COLS = [
    'total_reviews',               # Count of reviews ever written
    'avg_rating',                  # Mean star rating
    'pct_positive_reviews',        # % VADER=positive
    'pct_negative_reviews',        # % VADER=negative
    'days_since_last_review',      # 9999 sentinel for non-reviewers → proxy leak
    # ── Step 1 migration additions (2026-04-22) ─────────────────────────
    # Same 9999-sentinel / all-time proxy problems as the features above:
    #   - days_since_last_negative_review uses 9999 for customers who
    #     never left a negative review (i.e., most customers), so the
    #     feature acts as "has no negative reviews ↔ inactive" rather
    #     than measuring real churn signal.
    #   - avg_sentiment_score is all-time (non-windowed), so silent
    #     customers and happy customers both get neutral/NULL values
    #     that the model misreads as a churn proxy.
    # The other 4 Step-1 review features (reviews_last_90d,
    # avg_sentiment_score_last_90d, negative_reviews_last_90d,
    # low_star_reviews_last_90d) are KEPT in the feature set — they are
    # windowed (no 9999 sentinel, just 0 for non-reviewers) and represent
    # legitimate leading signal.
    'days_since_last_negative_review',  # 9999 sentinel → proxy leak
    'avg_sentiment_score',              # all-time, non-windowed proxy
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

# NOTE: Logistic Regression was removed from the supported model set because
# its sigmoid saturates to exactly 1.000 whenever a leaky feature pushes the
# log-odds above ~25. RandomForest + XGBoost are tree ensembles that cap per-
# leaf frequencies, so they don't saturate the same way and produce better-
# calibrated risk probabilities. The best of the two (by held-out AUC-ROC) is
# the one picked up by discover_best_model() in predict.py.


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

    # ── Impute columns that carry intentional NULLs ─────────────────────────
    # median_days_between_orders is NULL by design for customers with <3
    # orders (see the order_gaps CTE in walmart_crp_universal.sql). Filling
    # with 0 would mislead the model into thinking short-history customers
    # have a "0-day cadence" — the opposite of ignoring the feature. Fill
    # with the *population* median so those rows contribute ~no signal on
    # this feature and the model decides from their other features.
    for col in ('median_days_between_orders', 'order_gap_mean_median_diff'):
        if col in X.columns and X[col].isna().any():
            pop_median = X[col].median()
            n_missing = int(X[col].isna().sum())
            if pd.isna(pop_median):
                # Whole column is NULL. This signals an upstream data issue
                # (e.g., every customer has <3 orders, or the MV column is
                # broken) — imputing to 0 here lets training continue but
                # the column will have zero variance and be dropped by
                # drop_zero_variance_features shortly after. Warn loudly so
                # the issue is visible in production logs.
                log.warning(
                    "  Column '%s' is 100%% NULL (%d rows) — falling back to "
                    "0.0 imputation. Upstream feature extraction may be broken.",
                    col, n_missing,
                )
                pop_median = 0.0
            X[col] = X[col].fillna(pop_median)
            log.info(
                "  Imputed %d NULLs in '%s' with population median = %.2f",
                n_missing, col, float(pop_median),
            )

    # Fill NaN (remaining columns default to 0)
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


def drop_review_features(X: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Remove customer-review-derived features from the feature matrix.

    These are excluded because `days_since_last_review = 9999` (the sentinel
    for customers who never reviewed) and `total_reviews = 0` act as proxies
    for "inactive customer", not as true future-churn signal. See REVIEW_COLS
    definition at the top of this file for full rationale.

    Returns:
        (X_cleaned, dropped_columns)
    """
    log.info("Removing review-derived features...")
    review_found = [c for c in REVIEW_COLS if c in X.columns]
    if review_found:
        log.info("  Dropping %d review features: %s", len(review_found), review_found)
        X = X.drop(columns=review_found)
    else:
        log.info("  No review features found in data")
    return X, review_found


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

    X, review_dropped = drop_review_features(X)
    cleaning_log['review_dropped'] = review_dropped
    total_removed += len(review_dropped)

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


def exclude_gray_zone_from_training(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    df_full: pd.DataFrame,
    lower: int = GRAY_ZONE_LOWER,
    upper: int = GRAY_ZONE_UPPER,
) -> Tuple[pd.DataFrame, pd.Series, Dict[str, Any]]:
    """
    Remove rows with days_since_last_order in [lower, upper) from the
    TRAINING set. These customers sit near the hard 90-day churn cutoff,
    so their binary labels are noisy even though their feature vectors
    look identical to customers on either side of the boundary.

    IMPORTANT: This touches the TRAINING data only. The caller must NOT
    pass X_test / y_test through this function — the test set needs to
    include boundary customers so held-out metrics reflect real-world
    performance (where gray-zone customers definitely exist).

    Because `days_since_last_order` is in LEAKY_COLS and has already been
    dropped from X_train, we look it up from the original DataFrame via
    X_train.index. train_test_split preserves indices, so this works.

    Args:
        X_train: Training feature matrix (after leaky/review drops and split)
        y_train: Training target series
        df_full: The original DataFrame before prepare_features, still
                 containing days_since_last_order
        lower: Lower bound of gray zone (inclusive)
        upper: Upper bound of gray zone (exclusive)

    Returns:
        (X_train_clean, y_train_clean, gray_zone_log)
        where gray_zone_log captures counts for the training report.
    """
    log.info("Excluding gray-zone customers from training "
             "(DSLO in [%d, %d))...", lower, upper)

    if 'days_since_last_order' not in df_full.columns:
        log.warning("  days_since_last_order missing from source df — "
                    "skipping gray-zone exclusion")
        return X_train, y_train, {
            'lower': lower, 'upper': upper,
            'excluded': 0, 'reason': 'dslo_missing',
        }

    dslo = df_full.loc[X_train.index, 'days_since_last_order']
    gray_mask = (dslo >= lower) & (dslo < upper)
    n_excluded = int(gray_mask.sum())
    n_total = len(X_train)

    if n_excluded == 0:
        log.info("  No customers in gray zone — no rows excluded")
    else:
        pct = 100.0 * n_excluded / max(n_total, 1)
        log.info("  %d rows excluded (%.1f%% of train set, %d → %d)",
                 n_excluded, pct, n_total, n_total - n_excluded)

    X_train_clean = X_train.loc[~gray_mask]
    y_train_clean = y_train.loc[~gray_mask]

    log.info("  Train distribution after exclusion: %s",
             y_train_clean.value_counts().to_dict())

    gray_zone_log = {
        'lower': lower,
        'upper': upper,
        'excluded': n_excluded,
        'train_before': n_total,
        'train_after': len(X_train_clean),
    }
    return X_train_clean, y_train_clean, gray_zone_log


def exclude_login_gray_zone_from_training(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    df_full: pd.DataFrame,
    lower: int = LOGIN_GRAY_ZONE_LOWER,
    upper: int = LOGIN_GRAY_ZONE_UPPER,
) -> Tuple[pd.DataFrame, pd.Series, Dict[str, Any]]:
    """
    Mirror of exclude_gray_zone_from_training, applied to the LOGIN window
    instead of the order window.

    Background (Phase 4 churn-label work, 2026-04-25):
        The new two-condition churn label is:
              churn = (days_since_last_order >= churn_window_days)
                  AND (days_since_last_login   > login_window_days)
        The order gray-zone strips boundary customers around the first
        condition. This function strips them around the second one.

        A customer at days_since_last_login ≈ 30 has a label that flips
        easily on day-to-day noise — one extra app open this morning and
        their second condition flips. Excluding [25, 35) from the training
        set lets the model learn from clear-signal customers while keeping
        boundary cases in the test set so held-out metrics still reflect
        production reality.

    TRAINING SET ONLY. Test set is left intact, same convention as the
    order gray-zone function above.

    Both functions can be called sequentially (we drop a customer if
    EITHER gray zone applies to them). They use independent masks because
    a customer in only one gray zone is still ambiguous.

    Args:
        X_train, y_train: Training matrix + target after split
        df_full: Original DataFrame (still has days_since_last_login,
                 which is in LEAKY_COLS and dropped from X_train)
        lower / upper: Login gray-zone bounds (inclusive / exclusive)

    Returns:
        (X_train_clean, y_train_clean, gray_zone_log)
    """
    log.info("Excluding login gray-zone customers from training "
             "(DSLI in [%d, %d))...", lower, upper)

    if 'days_since_last_login' not in df_full.columns:
        log.warning("  days_since_last_login missing from source df — "
                    "skipping login gray-zone exclusion. This is expected "
                    "for tenants that haven't supplied login data yet; the "
                    "single-condition churn rule applies to them and the "
                    "login gray-zone is irrelevant.")
        return X_train, y_train, {
            'lower': lower, 'upper': upper,
            'excluded': 0, 'reason': 'dsli_missing',
        }

    dsli = df_full.loc[X_train.index, 'days_since_last_login']
    gray_mask = (dsli >= lower) & (dsli < upper)
    n_excluded = int(gray_mask.sum())
    n_total = len(X_train)

    if n_excluded == 0:
        log.info("  No customers in login gray zone — no rows excluded")
    else:
        pct = 100.0 * n_excluded / max(n_total, 1)
        log.info("  %d rows excluded (%.1f%% of train set, %d → %d)",
                 n_excluded, pct, n_total, n_total - n_excluded)

    X_train_clean = X_train.loc[~gray_mask]
    y_train_clean = y_train.loc[~gray_mask]

    log.info("  Train distribution after login gray-zone: %s",
             y_train_clean.value_counts().to_dict())

    gray_zone_log = {
        'lower': lower,
        'upper': upper,
        'excluded': n_excluded,
        'train_before': n_total,
        'train_after': len(X_train_clean),
    }
    return X_train_clean, y_train_clean, gray_zone_log


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

    # 2026-04-25 — capacity-trimmed defaults for ~500-row dataset.
    #
    # Previously max_depth=6, learning_rate=0.1, no regularization, no
    # min_child_weight. With 100 boosting rounds × depth-6 trees, the
    # model had room for ~6,400 leaves on a training fold of ~450 rows.
    # Result: CV showed train AUC=0.96 vs test AUC=0.69 — classic
    # memorization gap. Diagnosis assumed no data leak (verified —
    # CV runs on X_full snapshot pre-SMOTE/pre-scaling).
    #
    # New defaults trade a small amount of training-fold AUC for a
    # large reduction in the train-test gap:
    #   - max_depth 6 → 3:        2^3=8 leaves max per tree (vs 64)
    #   - learning_rate 0.1 → 0.05: slower fit, less overshoot
    #   - min_child_weight 1 → 5:  leaves need ≥5 samples worth of hessian
    #   - reg_alpha 0 → 1.0:       L1 — drops uninformative features
    #   - reg_lambda 1 → 5.0:      L2 — shrinks leaf weights toward 0
    #   - subsample 0.8 → 0.7:     more row noise per tree
    #   - colsample 0.8 → 0.7:     more column noise per tree
    #
    # When --tune is passed the RandomizedSearch grid (TUNE_PARAM_GRID
    # in this file) is consulted instead; these defaults only matter
    # for the un-tuned path — which is what the pipeline currently
    # uses.
    defaults = {
        'n_estimators': 100,
        'max_depth': 3,
        'learning_rate': 0.05,
        'min_child_weight': 5,
        'reg_alpha': 1.0,
        'reg_lambda': 5.0,
        'subsample': 0.7,
        'colsample_bytree': 0.7,
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
    # 2026-04-25 — capacity-trimmed defaults for ~500-row dataset.
    #
    # Previously max_depth=15, min_samples_leaf=5, n_estimators=100. With
    # 450 training rows per CV fold, depth-15 trees grew until every
    # leaf had effectively one sample — train fold AUC = 0.92, test fold
    # AUC = 0.71, gap = 0.21 (overfitting flag tripped).
    #
    # New defaults shrink the trees and bag harder:
    #   - max_depth 15 → 6:        log2(500)≈9, depth 6 is plenty
    #   - min_samples_leaf 5 → 10:  doubles the floor
    #   - min_samples_split 10 → 20: doubles the floor
    #   - n_estimators 100 → 200:   more shallow trees beat fewer deep
    #   - max_features 'sqrt'       (sklearn default but explicit)
    #
    # Same RandomizedSearch grid caveat as XGBoost: --tune overrides
    # these via TUNE_PARAM_GRID. Note the RF tune grid still allows
    # max_depth=None — that should be tightened separately if --tune
    # ever lands in the pipeline default.
    defaults = {
        'n_estimators': 200,
        'max_depth': 6,
        'min_samples_split': 20,
        'min_samples_leaf': 10,
        'max_features': 'sqrt',
        'random_state': RANDOM_STATE,
        'n_jobs': -1,
    }
    if class_weight:
        defaults['class_weight'] = class_weight
    defaults.update(params)
    return RandomForestClassifier(**defaults)


def build_model(model_type: str, class_weight: Optional[str] = None, **params) -> Any:
    """
    Factory function — build any supported model by name.

    Args:
        model_type: 'xgboost' or 'random_forest'
        class_weight: 'balanced' or None (for RF); converted to ratio for XGBoost
        **params: Passed to the specific builder

    Returns:
        Unfitted model instance
    """
    if model_type == 'xgboost':
        return build_xgboost(**params)
    elif model_type == 'random_forest':
        return build_random_forest(class_weight=class_weight, **params)
    else:
        raise ValueError(
            f"Unknown model type: {model_type} "
            "(supported: 'xgboost', 'random_forest')"
        )


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
        model_type: 'xgboost' or 'random_forest'
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
    rv = cleaning_log.get('review_dropped', [])
    zv = cleaning_log.get('zero_variance_dropped', [])
    hc = cleaning_log.get('high_correlation_dropped', [])
    lines.append(f"  Leaky features removed ({len(lk)}):")
    for f in lk:
        lines.append(f"    - {f}")
    lines.append(f"  Review features removed ({len(rv)}):")
    for f in rv:
        lines.append(f"    - {f}")
    lines.append(f"  Zero-variance features removed ({len(zv)}):")
    for f in zv:
        lines.append(f"    - {f}")
    lines.append(f"  Highly correlated features removed ({len(hc)}):")
    for f in hc:
        lines.append(f"    - {f}")
    lines.append(f"  Features retained: {len(feature_names)}")
    lines.append("")

    # Gray-zone exclusion summary (Step 3 — order window)
    gz = cleaning_log.get('gray_zone', {})
    lines.append("-" * 75)
    lines.append("  GRAY-ZONE EXCLUSION — ORDER WINDOW (TRAINING ONLY)")
    lines.append("-" * 75)
    if gz.get('skipped'):
        lines.append("  Skipped (--skip-gray-zone flag)")
    elif gz.get('reason') == 'dslo_missing':
        lines.append("  Skipped: days_since_last_order column unavailable")
    elif gz:
        lines.append(f"  Gray zone window: DSLO in [{gz.get('lower')}, {gz.get('upper')})")
        lines.append(f"  Excluded from train: {gz.get('excluded', 0)} rows")
        lines.append(f"  Train size: {gz.get('train_before', 0)} → {gz.get('train_after', 0)}")
        lines.append("  (Test set keeps all customers for honest evaluation)")
    else:
        lines.append("  No order gray-zone info recorded")
    lines.append("")

    # Gray-zone exclusion summary (Phase 4 — login window, 2026-04-25)
    lgz = cleaning_log.get('login_gray_zone', {})
    lines.append("-" * 75)
    lines.append("  GRAY-ZONE EXCLUSION — LOGIN WINDOW (TRAINING ONLY)")
    lines.append("-" * 75)
    if lgz.get('skipped'):
        lines.append("  Skipped (--skip-login-gray-zone flag)")
    elif lgz.get('reason') == 'dsli_missing':
        lines.append("  Skipped: days_since_last_login column unavailable "
                     "(tenant has not supplied login data yet)")
    elif lgz:
        lines.append(f"  Gray zone window: DSLI in [{lgz.get('lower')}, {lgz.get('upper')})")
        lines.append(f"  Excluded from train: {lgz.get('excluded', 0)} rows")
        lines.append(f"  Train size: {lgz.get('train_before', 0)} → {lgz.get('train_after', 0)}")
        lines.append("  (Test set keeps all customers for honest evaluation)")
    else:
        lines.append("  No login gray-zone info recorded")
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
                        choices=['xgboost', 'random_forest', 'all'],
                        default='all',
                        help='Model to train (default: all — trains both and '
                             'picks the AUC-ROC winner for prediction)')
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
    # Gray-zone exclusion (Step 3): drop ambiguous boundary customers from
    # training only. Defaults to [75, 105) around the 90-day churn cutoff.
    parser.add_argument('--skip-gray-zone', action='store_true',
                        help='Disable gray-zone exclusion (train on all customers)')
    parser.add_argument('--gray-zone-lower', type=int, default=GRAY_ZONE_LOWER,
                        help=f'Lower DSLO bound of gray zone '
                             f'(default: {GRAY_ZONE_LOWER})')
    parser.add_argument('--gray-zone-upper', type=int, default=GRAY_ZONE_UPPER,
                        help=f'Upper DSLO bound of gray zone '
                             f'(default: {GRAY_ZONE_UPPER})')
    # Login gray-zone (Phase 4 — login-aware churn rule, 2026-04-25):
    # mirrors the order gray-zone above for the second condition of the
    # two-condition churn label. Default ±5 around the 30-day login window.
    parser.add_argument('--skip-login-gray-zone', action='store_true',
                        help='Disable login gray-zone exclusion (train on all customers)')
    parser.add_argument('--login-gray-zone-lower', type=int, default=LOGIN_GRAY_ZONE_LOWER,
                        help=f'Lower DSLI bound of login gray zone '
                             f'(default: {LOGIN_GRAY_ZONE_LOWER})')
    parser.add_argument('--login-gray-zone-upper', type=int, default=LOGIN_GRAY_ZONE_UPPER,
                        help=f'Upper DSLI bound of login gray zone '
                             f'(default: {LOGIN_GRAY_ZONE_UPPER})')
    # Probability calibration (audit 2026-04-24 issue #7): wrap the base
    # tree model with CalibratedClassifierCV(method='isotonic', cv=5) so
    # predict_proba returns well-calibrated probabilities instead of the
    # 0/1-saturated output that tree ensembles produce by default.
    parser.add_argument('--skip-calibration', action='store_true',
                        help='Skip probability calibration (deploy raw '
                             'tree-model predict_proba — less accurate risk '
                             'scores but faster training)')
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

    # ─ Hyperparameter tuning or default build (builds BASE model)
    # `base_model` is the uncalibrated tree model. It's what CV scores, what
    # feature importances are extracted from, and what gets wrapped with
    # isotonic calibration below to form the final deployed model.
    if args.tune:
        base_model, best_params = tune_hyperparameters(
            model_type, X_train, y_train,
            n_iter=args.tune_iter
        )
        log.info("  Best params: %s", best_params)
    else:
        base_model = build_model(model_type)
        base_model = train_single_model(base_model, X_train, y_train, model_type)

    # ─ Cross-validation (same hyperparameters as the deployed model)
    # XGBoost + RandomForest are both scale-invariant trees, so no inner
    # scaler pipeline is required — the same features that went into the
    # held-out fit work unchanged inside each CV fold.
    #
    # Previously `cv_model = build_model(model_type)` spun up a fresh
    # DEFAULT model, so when --tune was on, the CV scores in the report
    # described an untuned model while the test-set scores described the
    # tuned one — two different configurations being compared side-by-side.
    # `clone()` returns an unfit copy of the actual model with identical
    # hyperparameters, so CV now measures the real deployed model.
    # (Audit 2026-04-24 issue #3.)
    #
    # CV is deliberately run on the UNCALIBRATED base model so the numbers
    # remain comparable to earlier runs and describe the raw learner's
    # discrimination. Calibration is a monotonic transform of predict_proba
    # and does not change ROC/AUC — the CV AUC is identical either way.
    from sklearn.base import clone
    cv_results = None
    if not args.no_cv:
        cv_model = clone(base_model)
        cv_results = cross_validate_model(cv_model, X_full, y_full)

    # ─ Probability calibration (audit 2026-04-24 issue #7)
    # Tree ensembles saturate predict_proba near 0 and 1, which is why the
    # dashboard showed almost no customers in the MEDIUM risk band (the
    # 0.35–0.65 cutoffs never triggered). Isotonic regression calibration
    # with cv=5 reshapes the probability distribution so predict_proba
    # values are meaningful — the risk score "0.52" now corresponds to a
    # roughly 52% chance of churn, not a 0/1 artifact of tree voting.
    #
    # Mechanics: CalibratedClassifierCV with cv=5 refits the base estimator
    # on 5 internal folds of X_train/y_train, then learns a monotonic
    # isotonic mapping from each base model's raw output to the observed
    # class frequencies. At predict time it averages the 5 calibrated
    # probabilities. The resulting estimator exposes predict_proba /
    # predict exactly like the base, so no changes are needed in predict.py
    # for the probability path. SHAP in predict.py does need to unwrap the
    # calibrated wrapper — that's handled separately there.
    if not args.skip_calibration:
        from sklearn.calibration import CalibratedClassifierCV
        log.info("Calibrating probabilities (isotonic, cv=5)...")
        model = CalibratedClassifierCV(
            clone(base_model),   # unfit clone; CalibratedClassifierCV refits
            method='isotonic',
            cv=5,
        )
        model.fit(X_train, y_train)
        log.info("  Calibration complete — predict_proba now calibrated")
    else:
        log.info("Probability calibration disabled via --skip-calibration")
        model = base_model

    # ─ Evaluate on held-out test set (uses CALIBRATED model)
    metrics = evaluate_model(model, X_test, y_test)

    # ─ Feature importance (uses UNCALIBRATED base — CalibratedClassifierCV
    # has no feature_importances_ attribute; the importances from the base
    # model are a faithful summary of what the calibrated ensemble uses)
    importance_df = get_feature_importances(base_model, feature_names, model_type)

    # ─ Save model FIRST (before plots/reports — so model is persisted even if plots crash)
    metadata = {
        'model_type': model_type,
        'training_date': datetime.now().isoformat(),
        'test_size': TEST_SIZE,
        'random_state': RANDOM_STATE,
        'n_features': len(feature_names),
        'feature_names': feature_names,
        'cleaning_log': cleaning_log,
        'tuned': args.tune,
        'calibrated': not args.skip_calibration,
        'calibration_method': 'isotonic' if not args.skip_calibration else None,
        'calibration_cv': 5 if not args.skip_calibration else None,
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

    # ─ Plots (non-fatal — model is already saved above)
    if not args.no_plots:
        try:
            generate_all_plots(y_test, metrics, importance_df, cv_results, model_type, PLOT_DIR)
        except Exception as e:
            log.warning("Plot generation failed (non-fatal): %s", e)

    # ─ Training report
    report_path = generate_training_report(
        model_type, metrics, cv_results, importance_df,
        cleaning_log, feature_names, original_shape, REPORT_DIR
    )

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

    # ── Step 4: Train/test split ───────────────────────────────────────────
    # Split FIRST, BEFORE feature selection. Previously feature importance
    # was computed on the full X/y, which meant test-set labels influenced
    # which features ended up in the model and silently inflated held-out
    # AUC. Splitting first keeps the test set out of every downstream
    # decision. (Audit 2026-04-24 issue #1.)
    X_train, X_test, y_train, y_test = split_data(X, y)

    # ── Step 5: Feature selection (training data only) ─────────────────────
    # Feature importance is computed on X_train/y_train only. The same
    # selected column list is then applied to X_test so both sides of the
    # split see identical columns in the same order.
    selected = select_features_by_importance(X_train, y_train,
                                             method=args.feature_selection,
                                             top_n=args.top_features)
    X_train = X_train[selected]
    X_test  = X_test[selected]
    feature_names = list(X_train.columns)

    # Snapshot of training data for cross-validation — pre-gray-zone,
    # pre-SMOTE, pre-scaling. CV now uses TRAINING data only; the held-out
    # test set is reserved for final unbiased evaluation and never appears
    # in CV folds.
    X_full = X_train.copy()
    y_full = y_train.copy()

    # ── Step 5b: Gray-zone exclusion (training only) ───────────────────────
    # Customers with DSLO near the 90-day churn cutoff have ambiguous labels.
    # Remove them from TRAINING only — test set keeps all customers so
    # held-out metrics reflect real-world performance.
    if not args.skip_gray_zone:
        X_train, y_train, gray_zone_log = exclude_gray_zone_from_training(
            X_train, y_train, df,
            lower=args.gray_zone_lower,
            upper=args.gray_zone_upper,
        )
        cleaning_log['gray_zone'] = gray_zone_log
    else:
        log.info("Gray-zone exclusion disabled via --skip-gray-zone")
        cleaning_log['gray_zone'] = {'skipped': True}

    # ── Step 5c: Login gray-zone exclusion (training only) ─────────────────
    # Phase 4 (2026-04-25): same idea as Step 5b but on the second condition
    # of the new two-condition churn rule. A customer at days_since_last_
    # login ≈ 30 has a label that flips on small daily noise. Drop them
    # from training; keep them in test for honest held-out metrics.
    # Runs AFTER the order gray-zone — both can apply, both shrink the
    # training set independently.
    if not args.skip_login_gray_zone:
        X_train, y_train, login_gray_zone_log = exclude_login_gray_zone_from_training(
            X_train, y_train, df,
            lower=args.login_gray_zone_lower,
            upper=args.login_gray_zone_upper,
        )
        cleaning_log['login_gray_zone'] = login_gray_zone_log
    else:
        log.info("Login gray-zone exclusion disabled via --skip-login-gray-zone")
        cleaning_log['login_gray_zone'] = {'skipped': True}

    # ── Step 6: Handle class imbalance ─────────────────────────────────────
    X_train, y_train = handle_class_imbalance(X_train, y_train, method=args.imbalance_method)

    # ── Step 7: Scale features ─────────────────────────────────────────────
    X_train, X_test, scaler = scale_features(X_train, X_test, method=args.scale_method)

    # CV uses X_full/y_full (a snapshot of X_train/y_train taken above,
    # pre-gray-zone, pre-SMOTE, pre-scaling). Tree models are scale-invariant
    # so this works today — when LogReg or any L1/L2 model comes back, wrap
    # scaler+model in a sklearn.Pipeline and pass that to cross_validate.

    # ── Step 8: Train model(s) ─────────────────────────────────────────────
    # Supported models: XGBoost + RandomForest. LogisticRegression was
    # removed because its sigmoid saturated to 1.000 under leaky features
    # and produced poorly-calibrated risk scores. The AUC winner of the
    # two is selected at prediction time by discover_best_model().
    models_to_train = (
        ['xgboost', 'random_forest']
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
