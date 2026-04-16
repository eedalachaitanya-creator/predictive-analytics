"""
compute_rfm.py — Analyst Agent | Feature Extraction & EDA
==========================================================
Connects to PostgreSQL, reads the 52-column materialized view
mv_customer_features, performs Exploratory Data Analysis, and saves
ML-ready datasets.

All thresholds (churn window, tier method, repeat-customer threshold,
reference date) are dynamic and read from client_config at view refresh
time — this script simply consumes the output.

This is the bridge between the database and the ML model.

Usage:
    # Extract features and run EDA:
    python -m ml.compute_rfm --db-url postgresql://user:pass@localhost:5432/walmart_crp

    # Just extract features (skip EDA):
    python -m ml.compute_rfm --db-url postgresql://user:pass@localhost:5432/walmart_crp --no-eda

    # Use .env file for DB connection:
    python -m ml.compute_rfm

Output files (saved to ml/output/):
    - customer_features.csv       → Full feature dataset (all 52 columns)
    - feature_matrix.csv          → ML-ready matrix (numeric features + encoded categoricals)
    - eda_report.txt              → Summary statistics and findings
    - correlation_matrix.csv      → Feature correlations
    - class_balance.csv           → Churn label distribution
    - feature_importance_rfm.csv  → RFM score distribution
    - tier_distribution.csv       → Customer tier breakdown

Requirements:
    pip install pandas psycopg2-binary sqlalchemy python-dotenv matplotlib seaborn
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION SECTION
# ═══════════════════════════════════════════════════════════════════════════

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("compute_rfm")

# Output directory
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Columns to EXCLUDE from the ML feature matrix
# (keys, dates, metadata — the model shouldn't train on these)
NON_FEATURE_COLS = [
    'client_id',
    'customer_id',
    'first_order_date',
    'last_order_date',
    'last_review_date',
    'computed_at',
]

# Categorical columns that need encoding before ML
CATEGORICAL_COLS = [
    'customer_tier',  # Platinum / Gold / Silver / Bronze
]

# Ordered tier mapping for ordinal encoding
TIER_ORDER = {'Bronze': 1, 'Silver': 2, 'Gold': 3, 'Platinum': 4}
TIER_DISPLAY_ORDER = ['Platinum', 'Gold', 'Silver', 'Bronze']

# The target variable (what we're predicting)
TARGET_COL = 'churn_label'

# RFM feature columns
RFM_COLS = ['rfm_recency_score', 'rfm_frequency_score', 'rfm_monetary_score', 'rfm_total_score']

# Spending feature columns for comparison
SPEND_COLS = ['total_spend_usd', 'avg_order_value_usd', 'total_orders', 'days_since_last_order']

# Churn correlation threshold for "high correlation"
HIGH_CORR_THRESHOLD = 0.8

# Color palettes for plots
COLOR_ACTIVE = '#70AD47'
COLOR_CHURNED = '#ED7D31'
COLOR_POSITIVE_CORR = '#ED7D31'
COLOR_NEGATIVE_CORR = '#2E75B6'
TIER_COLORS = ['#A855F7', '#F59E0B', '#94A3B8', '#CD7F32']
REPEAT_COLORS = ['#EF4444', '#22C55E']
RFM_COLORS = ['#2E75B6', '#ED7D31', '#70AD47']


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE SECTION
# ═══════════════════════════════════════════════════════════════════════════

def connect_db(db_url):
    """Create SQLAlchemy engine and verify connection."""
    log.info("Connecting to database...")
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        result.fetchone()
    log.info("  Connected successfully.")
    return engine


def _compute_derived_columns(engine, df):
    """
    Compute columns that the customer_rfm_features table needs but the
    materialized view mv_customer_features does NOT produce.

    Missing columns and how we derive them:
      last_order_status      → query the orders table for each customer's latest order
      avg_orders_per_month   → total_orders / (account_age_days / 30)
      order_frequency_trend  → compare orders_last_90d vs orders in prior 90d window
      spend_trend            → compare spend_last_90d vs spend in prior 90d window
      rfm_segment            → map rfm_total_score to a named segment
      top_category           → query line_items + categories for most-purchased category
      discount_dependency_pct→ (orders_with_discount / total_orders) × 100
      total_items_purchased  → query line_items for total quantity per customer
    """
    log.info("  Computing derived columns for customer_rfm_features ...")

    # ── 1. last_order_status: status of each customer's most recent order ──
    try:
        status_df = pd.read_sql(text("""
            SELECT DISTINCT ON (client_id, customer_id)
                   client_id, customer_id, order_status AS last_order_status
            FROM orders
            ORDER BY client_id, customer_id, order_date DESC
        """), engine)
        df = df.merge(status_df, on=['client_id', 'customer_id'], how='left')
    except Exception as e:
        log.warning("  Could not fetch last_order_status: %s", e)
        df['last_order_status'] = None

    # ── 2. avg_orders_per_month: total_orders / months_active ──
    months_active = (df['account_age_days'].fillna(1) / 30.0).clip(lower=1)
    df['avg_orders_per_month'] = (df['total_orders'] / months_active).round(2)

    # ── 3. order_frequency_trend ──
    #    Compare recent 90d orders vs the prior 90d window (orders_last_180d - orders_last_90d)
    prior_90d_orders = df['orders_last_180d'].fillna(0) - df['orders_last_90d'].fillna(0)
    recent_90d_orders = df['orders_last_90d'].fillna(0)
    df['order_frequency_trend'] = np.where(
        recent_90d_orders > prior_90d_orders, 'Increasing',
        np.where(recent_90d_orders < prior_90d_orders, 'Decreasing', 'Stable')
    )

    # ── 4. spend_trend ──
    #    Compare recent 90d spend vs prior 90d window (spend_last_180d - spend_last_90d)
    prior_90d_spend = df['spend_last_180d_usd'].fillna(0) - df['spend_last_90d_usd'].fillna(0)
    recent_90d_spend = df['spend_last_90d_usd'].fillna(0)
    df['spend_trend'] = np.where(
        recent_90d_spend > prior_90d_spend, 'Increasing',
        np.where(recent_90d_spend < prior_90d_spend, 'Decreasing', 'Stable')
    )

    # ── 5. rfm_segment: use individual R, F, M scores for accurate segmentation ──
    #
    # WHY individual scores matter:
    #   R=1, F=5, M=5 (total=11) → "Can't Lose Them" (was best customer, now gone)
    #   R=5, F=3, M=3 (total=11) → "Potential Loyalist" (recent buyer, nurture them)
    #   Both have total=11 but need completely different business actions.
    #
    # SEGMENTS (from best to worst):
    #   Champions         → High R, High F, High M (best customers, still active)
    #   Loyal Customers   → Medium+ R, High F, High M (consistent buyers)
    #   Can't Lose Them   → Low R, High F, High M (were great, now disappearing!)
    #   Potential Loyalists→ High R, Medium F/M (recent, growing engagement)
    #   At Risk           → Low R, Medium F/M (slowing down, needs intervention)
    #   New Customers     → High R, Low F (just started buying)
    #   Needs Attention   → Medium R, Low-Medium F/M (drifting away)
    #   Hibernating       → Low R, Low F, Low M (inactive, hard to win back)
    #
    def _rfm_segment(row):
        r = row.get('rfm_recency_score', 0)
        f = row.get('rfm_frequency_score', 0)
        m = row.get('rfm_monetary_score', 0)

        if pd.isna(r) or pd.isna(f) or pd.isna(m):
            return 'Unknown'

        r, f, m = int(r), int(f), int(m)

        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal Customers'
        elif r <= 2 and f >= 4 and m >= 4:
            return "Can't Lose Them"
        elif r <= 2 and f >= 3 and m >= 3:
            return 'At Risk'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r >= 4 and f >= 2 and m >= 2:
            return 'Potential Loyalists'
        elif r <= 2 and f <= 2:
            return 'Hibernating'
        else:
            return 'Needs Attention'

    df['rfm_segment'] = df.apply(_rfm_segment, axis=1)

    # ── 6. top_category: most purchased category per customer ──
    try:
        top_cat_df = pd.read_sql(text("""
            SELECT li.client_id, li.customer_id, c.category_name AS top_category
            FROM (
                SELECT client_id, customer_id, product_id,
                       COUNT(*) AS buy_count,
                       ROW_NUMBER() OVER (
                           PARTITION BY client_id, customer_id
                           ORDER BY COUNT(*) DESC
                       ) AS rn
                FROM line_items
                GROUP BY client_id, customer_id, product_id
            ) li
            JOIN products p ON li.product_id = p.product_id
            JOIN categories c ON p.category_id = c.category_id
            WHERE li.rn = 1
        """), engine)
        df = df.merge(top_cat_df, on=['client_id', 'customer_id'], how='left')
    except Exception as e:
        log.warning("  Could not fetch top_category: %s", e)
        df['top_category'] = None

    # ── 7. discount_dependency_pct: % of orders that used a discount ──
    df['discount_dependency_pct'] = (
        (df['orders_with_discount'].fillna(0) / df['total_orders'].clip(lower=1)) * 100
    ).round(2)

    # ── 8. total_items_purchased ──
    try:
        items_df = pd.read_sql(text("""
            SELECT client_id, customer_id, COALESCE(SUM(quantity), 0)::INT AS total_items_purchased
            FROM line_items
            GROUP BY client_id, customer_id
        """), engine)
        df = df.merge(items_df, on=['client_id', 'customer_id'], how='left')
    except Exception as e:
        log.warning("  Could not fetch total_items_purchased: %s", e)
        df['total_items_purchased'] = 0

    log.info("  ✓ Derived columns computed.")
    return df


def save_rfm_to_db(engine, df):
    """
    Save the extracted RFM features into the customer_rfm_features table.

    Steps:
      1. Compute derived columns that the view doesn't produce
      2. Map view columns → table columns
      3. TRUNCATE + bulk INSERT
    """

    # Step 1: Compute the 7 missing derived columns
    df = _compute_derived_columns(engine, df)

    # Step 2: Map mv_customer_features columns → customer_rfm_features columns
    # (only columns that NOW exist after derivation)
    column_map = {
        'client_id':              'client_id',
        'customer_id':            'customer_id',
        'computed_at':            'computed_at',
        'days_since_last_order':  'days_since_last_order',
        'last_order_date':        'last_order_date',
        'last_order_status':      'last_order_status',
        'total_orders':           'total_orders',
        'orders_last_30d':        'orders_last_30d',
        'orders_last_90d':        'orders_last_90d',
        'orders_last_180d':       'orders_last_180d',
        'avg_orders_per_month':   'avg_orders_per_month',
        'order_frequency_trend':  'order_frequency_trend',
        'total_spend_usd':        'total_spend_usd',
        'avg_order_value_usd':    'avg_order_value_usd',
        'spend_last_90d_usd':     'spend_last_90d_usd',
        'spend_last_180d_usd':    'spend_last_180d_usd',
        'ltv_usd':                'ltv_usd',
        'spend_trend':            'spend_trend',
        'rfm_recency_score':      'recency_score',
        'rfm_frequency_score':    'frequency_score',
        'rfm_monetary_score':     'monetary_score',
        'rfm_total_score':        'rfm_total_score',
        'rfm_segment':            'rfm_segment',
        'total_items_purchased':  'total_items_purchased',
        'unique_products_purchased': 'unique_products_bought',
        'top_category':           'top_category',
        'return_rate_pct':        'return_rate_pct',
        'orders_with_discount':   'total_discounts_used',
        'total_discount_usd':     'total_discount_usd',
        'discount_dependency_pct': 'discount_dependency_pct',
        'account_age_days':       'account_age_days',
        'customer_tier':          'customer_tier',
    }

    # Build the subset DataFrame using only columns that exist
    db_df = pd.DataFrame()
    for mv_col, table_col in column_map.items():
        if mv_col in df.columns:
            db_df[table_col] = df[mv_col]

    log.info("  Saving %d rows to customer_rfm_features (%d columns) ...",
             len(db_df), len(db_df.columns))

    # Step 3: TRUNCATE + bulk INSERT
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE customer_rfm_features"))

    db_df.to_sql(
        'customer_rfm_features',
        engine,
        if_exists='append',
        index=False,
        method='multi',
    )

    log.info("  ✓ customer_rfm_features table populated: %d rows", len(db_df))


def extract_features(engine, client_id: str = None):
    """
    Read rows from mv_customer_features into a pandas DataFrame.
    If client_id is provided, only extract that client's data.
    This is the core function — it pulls the 52 columns (keys + features + target).
    """
    if client_id:
        log.info("Extracting features from mv_customer_features for client_id=%s", client_id)
        query = text("SELECT * FROM mv_customer_features WHERE client_id = :cid")
        df = pd.read_sql(query, engine, params={"cid": client_id})
    else:
        log.info("Extracting features from mv_customer_features (ALL clients).")
        query = "SELECT * FROM mv_customer_features;"
        df = pd.read_sql(query, engine)

    log.info("  Extracted %d rows x %d columns", df.shape[0], df.shape[1])
    log.info("  Columns: %s", list(df.columns))

    return df


def get_table_info(engine, client_id: str = None):
    """Get row counts and materialized view refresh timestamp, optionally filtered by client."""
    info = {}
    with engine.connect() as conn:
        if client_id:
            result = conn.execute(text("SELECT COUNT(*) FROM mv_customer_features WHERE client_id = :cid"), {"cid": client_id})
            info['total_customers'] = result.fetchone()[0]

            result = conn.execute(text("SELECT MAX(computed_at) FROM mv_customer_features WHERE client_id = :cid"), {"cid": client_id})
            info['last_refresh'] = result.fetchone()[0]

            result = conn.execute(text("SELECT churn_label, COUNT(*) FROM mv_customer_features WHERE client_id = :cid GROUP BY churn_label ORDER BY churn_label"), {"cid": client_id})
            info['churn_dist'] = dict(result.fetchall())
        else:
            result = conn.execute(text("SELECT COUNT(*) FROM mv_customer_features"))
            info['total_customers'] = result.fetchone()[0]

            result = conn.execute(text("SELECT MAX(computed_at) FROM mv_customer_features"))
            info['last_refresh'] = result.fetchone()[0]

            result = conn.execute(text("SELECT churn_label, COUNT(*) FROM mv_customer_features GROUP BY churn_label ORDER BY churn_label"))
            info['churn_dist'] = dict(result.fetchall())

    return info


# ═══════════════════════════════════════════════════════════════════════════
# FEATURE MATRIX SECTION
# ═══════════════════════════════════════════════════════════════════════════

def prepare_feature_matrix(df):
    """
    Convert the full DataFrame into an ML-ready feature matrix.

    Steps:
    1. Remove non-feature columns (keys, dates, metadata)
    2. Separate features (X) from target (y)
    3. Encode categorical columns (customer_tier → ordinal numeric)
    4. Handle any remaining NaN values
    5. Report feature types and stats
    """
    log.info("Preparing ML-ready feature matrix...")

    # Separate target
    y = df[TARGET_COL].copy()

    # Drop non-feature columns and target
    drop_cols = [c for c in NON_FEATURE_COLS + [TARGET_COL] if c in df.columns]
    X = df.drop(columns=drop_cols)

    # Encode customer_tier as ordinal (Bronze=1, Silver=2, Gold=3, Platinum=4)
    if 'customer_tier' in X.columns:
        X['customer_tier_encoded'] = X['customer_tier'].map(TIER_ORDER).fillna(1).astype(int)
        X = X.drop(columns=['customer_tier'])
        log.info("  Encoded customer_tier → customer_tier_encoded (ordinal 1-4)")

    # Keep only numeric columns (drop any remaining text/date columns)
    X_numeric = X.select_dtypes(include=[np.number])

    # Fill any NaN with 0 (shouldn't happen due to COALESCE in SQL, but just in case)
    nan_count = X_numeric.isna().sum().sum()
    if nan_count > 0:
        log.warning("  Found %d NaN values — filling with 0", nan_count)
        X_numeric = X_numeric.fillna(0)

    log.info("  Feature matrix: %d customers x %d features", X_numeric.shape[0], X_numeric.shape[1])
    log.info("  Target distribution: %s", y.value_counts().to_dict())

    return X_numeric, y


# ═══════════════════════════════════════════════════════════════════════════
# EDA ANALYSIS SECTION
# ═══════════════════════════════════════════════════════════════════════════

def analyze_class_balance(df, y):
    """Analyze churn label distribution and class imbalance."""
    churn_counts = y.value_counts()
    churn_pct = y.value_counts(normalize=True) * 100
    imbalance_ratio = churn_counts.min() / churn_counts.max()

    # Save class balance
    class_df = pd.DataFrame({
        'churn_label': churn_counts.index,
        'count': churn_counts.values,
        'percentage': churn_pct.values
    })
    class_df.to_csv(OUTPUT_DIR / "class_balance.csv", index=False)

    return {
        'churn_counts': churn_counts,
        'churn_pct': churn_pct,
        'imbalance_ratio': imbalance_ratio,
    }


def analyze_feature_stats(X):
    """Generate feature statistics."""
    stats = X.describe().T
    stats['missing'] = X.isna().sum()
    stats['zeros'] = (X == 0).sum()
    stats['zero_pct'] = ((X == 0).sum() / len(X) * 100).round(1)
    return stats


def analyze_churn_correlations(X, y):
    """Analyze feature correlations with churn label."""
    X_with_target = X.copy()
    X_with_target['churn_label'] = y.values

    corr_matrix = X_with_target.corr()
    churn_corr = corr_matrix['churn_label'].drop('churn_label').dropna().sort_values(ascending=False)

    # Save full correlation matrix
    corr_matrix.to_csv(OUTPUT_DIR / "correlation_matrix.csv")

    return churn_corr, corr_matrix


def analyze_high_correlations(X):
    """Find highly correlated feature pairs (multicollinearity)."""
    feature_corr = X.corr()
    high_corr_pairs = []
    for i in range(len(feature_corr.columns)):
        for j in range(i + 1, len(feature_corr.columns)):
            corr_val = feature_corr.iloc[i, j]
            if abs(corr_val) > HIGH_CORR_THRESHOLD:
                high_corr_pairs.append((
                    feature_corr.columns[i],
                    feature_corr.columns[j],
                    corr_val
                ))

    high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    return high_corr_pairs


def analyze_rfm_distribution(X):
    """Analyze RFM score distributions."""
    rfm_available = [c for c in RFM_COLS if c in X.columns]

    # Save RFM distribution
    if rfm_available:
        rfm_df = X[rfm_available].copy()
        rfm_df.to_csv(OUTPUT_DIR / "feature_importance_rfm.csv", index=False)

    return rfm_available


def analyze_order_gaps(X):
    """Analyze mean vs median order gap patterns."""
    gap_analysis = {}

    if 'avg_days_between_orders' in X.columns and 'median_days_between_orders' in X.columns:
        gap_diff = X['order_gap_mean_median_diff'] if 'order_gap_mean_median_diff' in X.columns else \
                   abs(X['avg_days_between_orders'] - X['median_days_between_orders'])

        gap_analysis['avg_gap_diff'] = gap_diff.mean()
        gap_analysis['max_gap_diff'] = gap_diff.max()
        gap_analysis['high_gap_count'] = (gap_diff > 10).sum()
        gap_analysis['high_gap_pct'] = (gap_diff > 10).mean() * 100

    return gap_analysis


def analyze_subscriptions(X):
    """Analyze subscription product features."""
    sub_analysis = {}

    if 'subscription_product_count' in X.columns:
        sub_customers = (X['subscription_product_count'] > 0).sum()
        sub_analysis['sub_customer_count'] = sub_customers
        sub_analysis['sub_customer_pct'] = sub_customers / len(X) * 100

        if 'days_overdue_for_refill' in X.columns and sub_customers > 0:
            overdue = X[X['subscription_product_count'] > 0]['days_overdue_for_refill']
            sub_analysis['avg_overdue_days'] = overdue.mean()
            sub_analysis['overdue_gt30_count'] = (overdue > 30).sum()

        if 'missed_refill_count' in X.columns and sub_customers > 0:
            missed = X[X['subscription_product_count'] > 0]['missed_refill_count']
            sub_analysis['avg_missed_refills'] = missed.mean()

    return sub_analysis


def analyze_tier_distribution(df):
    """Analyze customer tier distribution and churn by tier."""
    tier_analysis = {}

    if 'customer_tier' in df.columns:
        tier_counts = df['customer_tier'].value_counts()
        tier_pct = df['customer_tier'].value_counts(normalize=True) * 100

        tier_analysis['tier_counts'] = tier_counts
        tier_analysis['tier_pct'] = tier_pct

        # Churn rate by tier
        tier_churn = df.groupby('customer_tier')['churn_label'].mean() * 100
        tier_analysis['tier_churn'] = tier_churn

        # Save tier distribution
        tier_df = pd.DataFrame({
            'customer_tier': tier_counts.index,
            'count': tier_counts.values,
            'percentage': tier_pct.values
        })
        tier_df.to_csv(OUTPUT_DIR / "tier_distribution.csv", index=False)

    # High-value analysis
    if 'is_high_value' in df.columns:
        hv_count = df['is_high_value'].sum()
        hv_pct = hv_count / len(df) * 100
        hv_churn = df[df['is_high_value'] == 1]['churn_label'].mean() * 100
        non_hv_churn = df[df['is_high_value'] == 0]['churn_label'].mean() * 100

        tier_analysis['high_value_count'] = int(hv_count)
        tier_analysis['high_value_pct'] = hv_pct
        tier_analysis['high_value_churn'] = hv_churn
        tier_analysis['non_high_value_churn'] = non_hv_churn

    return tier_analysis


def analyze_repeat_customers(df):
    """Analyze repeat vs one-time customer patterns."""
    repeat_analysis = {}

    if 'is_repeat_customer' in df.columns:
        repeat_count = df['is_repeat_customer'].sum()
        repeat_pct = repeat_count / len(df) * 100

        repeat_analysis['repeat_count'] = int(repeat_count)
        repeat_analysis['repeat_pct'] = repeat_pct
        repeat_analysis['onetime_count'] = int(len(df) - repeat_count)
        repeat_analysis['onetime_pct'] = 100 - repeat_pct

        repeat_churn = df[df['is_repeat_customer'] == 1]['churn_label'].mean() * 100
        onetime_churn = df[df['is_repeat_customer'] == 0]['churn_label'].mean() * 100

        repeat_analysis['repeat_churn'] = repeat_churn
        repeat_analysis['onetime_churn'] = onetime_churn

    return repeat_analysis


def analyze_recent_gaps(X):
    """Analyze recent vs overall order gap patterns."""
    recent_analysis = {}

    if 'recent_avg_gap_days' in X.columns:
        recent_gap = X['recent_avg_gap_days']
        recent_analysis['mean_recent_gap'] = recent_gap.mean()
        recent_analysis['median_recent_gap'] = recent_gap.median()
        recent_analysis['max_recent_gap'] = recent_gap.max()

        # Compare recent gap to overall gap
        if 'avg_days_between_orders' in X.columns:
            overall_gap = X['avg_days_between_orders']
            accelerating = (X['recent_avg_gap_days'] > X['avg_days_between_orders']).sum()
            recent_analysis['accelerating_count'] = accelerating
            recent_analysis['accelerating_pct'] = accelerating / len(X) * 100

    return recent_analysis


def generate_insights(churn_corr, high_corr_pairs, imbalance_ratio):
    """Generate key insights and recommendations."""
    insights = {}

    # Strongest churn predictors
    insights['top_positive_churn'] = churn_corr.head(3)
    insights['top_negative_churn'] = churn_corr.tail(3)

    # Recommendations
    recommendations = []
    if imbalance_ratio < 0.3:
        recommendations.append("Use SMOTE oversampling or class_weight='balanced' in model")
    recommendations.append("Use stratified K-fold cross-validation (preserve churn ratio in folds)")
    recommendations.append("Primary metric: AUC-ROC (handles class imbalance well)")
    recommendations.append("Secondary metrics: Precision, Recall, F1-score")
    if high_corr_pairs:
        recommendations.append(f"Consider dropping {len(high_corr_pairs)} highly correlated features")
    recommendations.append("customer_tier_encoded preserves ordinal ranking — suitable for tree models and regression")

    insights['recommendations'] = recommendations

    return insights


def run_eda(df, X, y):
    """
    Orchestrate all EDA analyses and generate report.
    """
    log.info("\nRunning Exploratory Data Analysis...")

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("EXPLORATORY DATA ANALYSIS REPORT")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 70)

    # 1. Dataset Overview
    report_lines.append("\n1. DATASET OVERVIEW")
    report_lines.append("-" * 40)
    report_lines.append(f"   Total customers:     {df.shape[0]}")
    report_lines.append(f"   Total columns:        {df.shape[1]}")
    report_lines.append(f"   Numeric features:     {X.shape[1]}")
    report_lines.append(f"   Date columns:         {len(df.select_dtypes(include=['datetime64']).columns)}")

    # 2. Class Balance
    report_lines.append("\n2. CLASS BALANCE (CHURN LABEL)")
    report_lines.append("-" * 40)
    class_bal = analyze_class_balance(df, y)
    churn_counts = class_bal['churn_counts']
    churn_pct = class_bal['churn_pct']
    imbalance_ratio = class_bal['imbalance_ratio']

    for label in sorted(churn_counts.index):
        status = "Active" if label == 0 else "Churned"
        report_lines.append(
            f"   {status} (label={label}):  {churn_counts[label]:>5} customers  ({churn_pct[label]:.1f}%)"
        )

    report_lines.append(f"\n   Imbalance ratio: {imbalance_ratio:.2f}")
    if imbalance_ratio < 0.3:
        report_lines.append("   WARNING: Highly imbalanced! Consider SMOTE or class weights during training.")
    elif imbalance_ratio < 0.5:
        report_lines.append("   NOTE: Moderately imbalanced. Use stratified sampling for train/test split.")
    else:
        report_lines.append("   OK: Reasonably balanced dataset.")

    # 3. Feature Statistics
    report_lines.append("\n3. FEATURE STATISTICS")
    report_lines.append("-" * 40)
    stats = analyze_feature_stats(X)

    report_lines.append(f"\n   {'Feature':<35} {'Mean':>10} {'Std':>10} {'Min':>10} {'Max':>10} {'Zeros%':>8}")
    report_lines.append("   " + "-" * 83)
    for feat in stats.index:
        report_lines.append(
            f"   {feat:<35} {stats.loc[feat,'mean']:>10.2f} {stats.loc[feat,'std']:>10.2f} "
            f"{stats.loc[feat,'min']:>10.2f} {stats.loc[feat,'max']:>10.2f} {stats.loc[feat,'zero_pct']:>7.1f}%"
        )

    # 4. Correlation Analysis
    report_lines.append("\n4. CORRELATION WITH CHURN LABEL")
    report_lines.append("-" * 40)
    churn_corr, corr_matrix = analyze_churn_correlations(X, y)

    report_lines.append("\n   Top 10 POSITIVELY correlated with churn (higher = more churn):")
    for feat, corr in churn_corr.head(10).items():
        bar = "+" * int(abs(corr) * 20) if not np.isnan(corr) else ""
        report_lines.append(f"   {feat:<35} {corr:>+.3f}  {bar}")

    report_lines.append("\n   Top 10 NEGATIVELY correlated with churn (higher = less churn):")
    for feat, corr in churn_corr.tail(10).items():
        bar = "-" * int(abs(corr) * 20) if not np.isnan(corr) else ""
        report_lines.append(f"   {feat:<35} {corr:>+.3f}  {bar}")

    # 5. High Correlation Between Features
    report_lines.append("\n5. HIGHLY CORRELATED FEATURE PAIRS (|r| > 0.8)")
    report_lines.append("-" * 40)
    report_lines.append("   (Consider dropping one from each pair to reduce multicollinearity)")
    high_corr_pairs = analyze_high_correlations(X)

    if high_corr_pairs:
        for f1, f2, corr in high_corr_pairs:
            report_lines.append(f"   {f1:<30} <-> {f2:<30} r={corr:+.3f}")
    else:
        report_lines.append("   No highly correlated pairs found.")

    # 6. RFM Score Distribution
    report_lines.append("\n6. RFM SCORE DISTRIBUTION")
    report_lines.append("-" * 40)
    rfm_available = analyze_rfm_distribution(X)

    for col in RFM_COLS:
        if col in X.columns:
            report_lines.append(f"\n   {col}:")
            report_lines.append(f"     Mean: {X[col].mean():.2f}  |  Median: {X[col].median():.1f}  |  Std: {X[col].std():.2f}")
            if col != 'rfm_total_score':
                dist = X[col].value_counts().sort_index()
                for score, count in dist.items():
                    pct = count / len(X) * 100
                    bar = "#" * int(pct)
                    report_lines.append(f"     Score {int(score)}: {count:>4} customers ({pct:.1f}%)  {bar}")

    # 7. Order Gap Analysis
    report_lines.append("\n7. ORDER GAP: MEAN vs MEDIAN ANALYSIS")
    report_lines.append("-" * 40)
    gap_analysis = analyze_order_gaps(X)

    if gap_analysis:
        report_lines.append(f"   Avg mean-median diff:  {gap_analysis.get('avg_gap_diff', 0):.1f} days")
        report_lines.append(f"   Max mean-median diff:  {gap_analysis.get('max_gap_diff', 0):.1f} days")
        report_lines.append(
            f"   Customers with diff > 10 days:  {gap_analysis.get('high_gap_count', 0)} "
            f"({gap_analysis.get('high_gap_pct', 0):.1f}%)"
        )
        report_lines.append("   (Large diff = erratic buying pattern = potential churn signal)")

    # 8. Subscription Analysis
    report_lines.append("\n8. SUBSCRIPTION FEATURES")
    report_lines.append("-" * 40)
    sub_analysis = analyze_subscriptions(X)

    if sub_analysis:
        report_lines.append(
            f"   Customers with subscription products: {sub_analysis.get('sub_customer_count', 0)} "
            f"({sub_analysis.get('sub_customer_pct', 0):.1f}%)"
        )

        if 'avg_overdue_days' in sub_analysis:
            report_lines.append(f"   Avg days overdue (subscribers only): {sub_analysis['avg_overdue_days']:.1f}")
            report_lines.append(f"   Subscribers overdue > 30 days: {sub_analysis.get('overdue_gt30_count', 0)}")

        if 'avg_missed_refills' in sub_analysis:
            report_lines.append(f"   Avg missed refills (subscribers): {sub_analysis['avg_missed_refills']:.1f}")

    # 9. Tier Distribution
    report_lines.append("\n9. CUSTOMER TIER DISTRIBUTION")
    report_lines.append("-" * 40)
    tier_analysis = analyze_tier_distribution(df)

    if 'tier_counts' in tier_analysis:
        tier_counts = tier_analysis['tier_counts']
        tier_pct = tier_analysis['tier_pct']
        for tier in TIER_DISPLAY_ORDER:
            if tier in tier_counts.index:
                bar = "#" * int(tier_pct[tier])
                report_lines.append(
                    f"   {tier:<10} {tier_counts[tier]:>5} customers ({tier_pct[tier]:>5.1f}%)  {bar}"
                )

        # Churn rate by tier
        if 'tier_churn' in tier_analysis:
            report_lines.append("\n   Churn rate by tier:")
            tier_churn = tier_analysis['tier_churn']
            for tier in TIER_DISPLAY_ORDER:
                if tier in tier_churn.index:
                    report_lines.append(f"     {tier:<10} {tier_churn[tier]:>5.1f}% churned")

    # High-value analysis
    if 'high_value_count' in tier_analysis:
        report_lines.append(
            f"\n   High-value customers: {tier_analysis['high_value_count']} ({tier_analysis['high_value_pct']:.1f}%)"
        )
        report_lines.append(f"   Churn rate (high-value):     {tier_analysis['high_value_churn']:.1f}%")
        report_lines.append(f"   Churn rate (non-high-value): {tier_analysis['non_high_value_churn']:.1f}%")

    # 10. Repeat Customer Analysis
    report_lines.append("\n10. REPEAT CUSTOMER ANALYSIS")
    report_lines.append("-" * 40)
    repeat_analysis = analyze_repeat_customers(df)

    if repeat_analysis:
        report_lines.append(f"   Repeat customers:    {repeat_analysis['repeat_count']} ({repeat_analysis['repeat_pct']:.1f}%)")
        report_lines.append(f"   One-time customers:  {repeat_analysis['onetime_count']} ({repeat_analysis['onetime_pct']:.1f}%)")
        report_lines.append(f"\n   Churn rate (repeat):   {repeat_analysis['repeat_churn']:.1f}%")
        report_lines.append(f"   Churn rate (one-time): {repeat_analysis['onetime_churn']:.1f}%")

    # 11. Recent Gap Analysis
    report_lines.append("\n11. RECENT ORDER GAP ANALYSIS")
    report_lines.append("-" * 40)
    recent_analysis = analyze_recent_gaps(X)

    if recent_analysis:
        report_lines.append(f"   Mean recent gap:   {recent_analysis.get('mean_recent_gap', 0):.1f} days")
        report_lines.append(f"   Median recent gap: {recent_analysis.get('median_recent_gap', 0):.1f} days")
        report_lines.append(f"   Max recent gap:    {recent_analysis.get('max_recent_gap', 0):.1f} days")

        if 'accelerating_count' in recent_analysis:
            report_lines.append(
                f"\n   Customers with widening gaps (recent > overall): {recent_analysis['accelerating_count']} "
                f"({recent_analysis['accelerating_pct']:.1f}%)"
            )
            report_lines.append("   (Widening gap = buying less frequently = early churn signal)")

    # 12. Key Insights Summary
    report_lines.append("\n" + "=" * 70)
    report_lines.append("KEY INSIGHTS FOR MODEL TRAINING")
    report_lines.append("=" * 70)

    insights = generate_insights(churn_corr, high_corr_pairs, imbalance_ratio)

    report_lines.append("\n   STRONGEST CHURN SIGNALS (positive correlation = more churn):")
    for feat, corr in insights['top_positive_churn'].items():
        report_lines.append(f"     -> {feat}: r={corr:+.3f}")

    report_lines.append("\n   STRONGEST RETENTION SIGNALS (negative correlation = less churn):")
    for feat, corr in insights['top_negative_churn'].items():
        report_lines.append(f"     -> {feat}: r={corr:+.3f}")

    report_lines.append("\n   RECOMMENDATIONS:")
    for i, rec in enumerate(insights['recommendations'], 1):
        report_lines.append(f"     {i}. {rec}")

    # Save report
    report_text = "\n".join(report_lines)
    (OUTPUT_DIR / "eda_report.txt").write_text(report_text)
    log.info("  EDA report saved to: %s", OUTPUT_DIR / "eda_report.txt")

    return report_text


# ═══════════════════════════════════════════════════════════════════════════
# VISUALIZATION SECTION
# ═══════════════════════════════════════════════════════════════════════════

def plot_churn_distribution(y, plot_dir):
    """Plot churn label distribution."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = [COLOR_ACTIVE, COLOR_CHURNED]
    y.value_counts().sort_index().plot(kind='bar', color=colors, ax=ax, edgecolor='white')
    ax.set_title('Churn Label Distribution', fontsize=14, fontweight='bold')
    ax.set_xlabel('Churn Label (0=Active, 1=Churned)')
    ax.set_ylabel('Number of Customers')
    ax.set_xticklabels(['Active (0)', 'Churned (1)'], rotation=0)
    for i, v in enumerate(y.value_counts().sort_index()):
        ax.text(i, v + 2, str(v), ha='center', fontweight='bold')
    plt.tight_layout()
    fig.savefig(plot_dir / "01_churn_distribution.png", dpi=150)
    plt.close()


def plot_feature_correlations(X, y, plot_dir):
    """Plot top feature correlations with churn."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    X_with_target = X.copy()
    X_with_target['churn_label'] = y.values
    churn_corr = X_with_target.corr()['churn_label'].drop('churn_label').sort_values()

    fig, ax = plt.subplots(figsize=(10, 12))
    churn_corr.plot(kind='barh', color=[(COLOR_POSITIVE_CORR if v > 0 else COLOR_NEGATIVE_CORR) for v in churn_corr.values], ax=ax)
    ax.set_title('Feature Correlation with Churn Label', fontsize=14, fontweight='bold')
    ax.set_xlabel('Correlation Coefficient')
    ax.axvline(x=0, color='black', linewidth=0.5)
    plt.tight_layout()
    fig.savefig(plot_dir / "02_feature_correlation_churn.png", dpi=150)
    plt.close()


def plot_correlation_heatmap(X, y, plot_dir):
    """Plot correlation heatmap for top features."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns

    X_with_target = X.copy()
    X_with_target['churn_label'] = y.values
    churn_corr = X_with_target.corr()['churn_label'].drop('churn_label')

    top_features = churn_corr.abs().nlargest(15).index.tolist()
    top_features.append('churn_label')

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(X_with_target[top_features].corr(), annot=True, fmt='.2f',
                cmap='RdBu_r', center=0, ax=ax, square=True,
                linewidths=0.5, cbar_kws={'shrink': 0.8})
    ax.set_title('Correlation Heatmap (Top 15 Features)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    fig.savefig(plot_dir / "03_correlation_heatmap.png", dpi=150)
    plt.close()


def plot_rfm_distributions(X, plot_dir):
    """Plot RFM score distributions."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    rfm_cols = ['rfm_recency_score', 'rfm_frequency_score', 'rfm_monetary_score']
    available_cols = [c for c in rfm_cols if c in X.columns]

    if not available_cols:
        return

    fig, axes = plt.subplots(1, len(available_cols), figsize=(5 * len(available_cols), 5))
    if len(available_cols) == 1:
        axes = [axes]

    for i, col in enumerate(available_cols):
        X[col].value_counts().sort_index().plot(kind='bar', ax=axes[i], color=RFM_COLORS[i], edgecolor='white')
        axes[i].set_title(col.replace('_', ' ').title(), fontweight='bold')
        axes[i].set_xlabel('Score')
        axes[i].set_ylabel('Count')

    plt.suptitle('RFM Score Distributions', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    fig.savefig(plot_dir / "04_rfm_distributions.png", dpi=150, bbox_inches='tight')
    plt.close()


def plot_active_vs_churned(X, y, plot_dir):
    """Plot spending patterns comparison between active and churned customers."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    available_spend = [c for c in SPEND_COLS if c in X.columns]
    if not available_spend:
        return

    fig, axes = plt.subplots(1, len(available_spend), figsize=(5 * len(available_spend), 5))
    if len(available_spend) == 1:
        axes = [axes]

    for i, col in enumerate(available_spend):
        data_active = X.loc[y == 0, col]
        data_churned = X.loc[y == 1, col]
        bp = axes[i].boxplot([data_active, data_churned], labels=['Active', 'Churned'],
                             patch_artist=True)
        bp['boxes'][0].set_facecolor(COLOR_ACTIVE)
        bp['boxes'][1].set_facecolor(COLOR_CHURNED)
        axes[i].set_title(col.replace('_', ' ').title(), fontweight='bold')
        axes[i].set_ylabel('Value')

    plt.suptitle('Active vs Churned Customer Comparison', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    fig.savefig(plot_dir / "05_active_vs_churned.png", dpi=150, bbox_inches='tight')
    plt.close()


def plot_mean_vs_median_gap(X, y, plot_dir):
    """Plot mean vs median order gap scatter."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    if 'avg_days_between_orders' not in X.columns or 'median_days_between_orders' not in X.columns:
        return

    fig, ax = plt.subplots(figsize=(8, 8))
    scatter_colors = [COLOR_ACTIVE if label == 0 else COLOR_CHURNED for label in y.values]
    ax.scatter(X['avg_days_between_orders'], X['median_days_between_orders'],
              c=scatter_colors, alpha=0.6, edgecolors='white', linewidth=0.5, s=60)
    max_val = max(X['avg_days_between_orders'].max(), X['median_days_between_orders'].max())
    ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Mean = Median line')
    ax.set_xlabel('Mean Order Gap (days)', fontsize=11)
    ax.set_ylabel('Median Order Gap (days)', fontsize=11)
    ax.set_title('Mean vs Median Order Gap\n(Points far from diagonal = erratic buyers)',
                 fontsize=13, fontweight='bold')
    ax.legend(['Mean=Median', 'Active', 'Churned'], loc='upper left')
    plt.tight_layout()
    fig.savefig(plot_dir / "06_mean_vs_median_gap.png", dpi=150)
    plt.close()


def plot_tier_distribution(df, plot_dir):
    """Plot customer tier distribution and churn by tier."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    if 'customer_tier' not in df.columns:
        return

    tier_order = TIER_DISPLAY_ORDER
    tier_data = df[df['customer_tier'].isin(tier_order)]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: tier count
    tier_counts = tier_data['customer_tier'].value_counts().reindex(tier_order)
    tier_counts.plot(kind='bar', ax=axes[0], color=TIER_COLORS, edgecolor='white')
    axes[0].set_title('Customer Tier Distribution', fontweight='bold')
    axes[0].set_xlabel('Tier')
    axes[0].set_ylabel('Customers')
    axes[0].set_xticklabels(tier_order, rotation=0)

    # Right: churn rate by tier
    churn_by_tier = tier_data.groupby('customer_tier')['churn_label'].mean().reindex(tier_order) * 100
    churn_by_tier.plot(kind='bar', ax=axes[1], color=TIER_COLORS, edgecolor='white')
    axes[1].set_title('Churn Rate by Tier', fontweight='bold')
    axes[1].set_xlabel('Tier')
    axes[1].set_ylabel('Churn Rate (%)')
    axes[1].set_xticklabels(tier_order, rotation=0)
    for i, v in enumerate(churn_by_tier.values):
        axes[1].text(i, v + 0.5, f"{v:.1f}%", ha='center', fontweight='bold')

    plt.tight_layout()
    fig.savefig(plot_dir / "07_tier_distribution.png", dpi=150)
    plt.close()


def plot_repeat_vs_onetime(df, plot_dir):
    """Plot repeat vs one-time customer comparison."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    if 'is_repeat_customer' not in df.columns:
        return

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Left: repeat vs one-time count
    repeat_counts = df['is_repeat_customer'].value_counts().sort_index()
    repeat_counts.index = ['One-time', 'Repeat']
    repeat_counts.plot(kind='bar', ax=axes[0], color=REPEAT_COLORS, edgecolor='white')
    axes[0].set_title('Repeat vs One-time Customers', fontweight='bold')
    axes[0].set_ylabel('Customers')
    axes[0].set_xticklabels(['One-time', 'Repeat'], rotation=0)

    # Right: churn rate comparison
    churn_by_repeat = df.groupby('is_repeat_customer')['churn_label'].mean() * 100
    churn_by_repeat.index = ['One-time', 'Repeat']
    churn_by_repeat.plot(kind='bar', ax=axes[1], color=REPEAT_COLORS, edgecolor='white')
    axes[1].set_title('Churn Rate: Repeat vs One-time', fontweight='bold')
    axes[1].set_ylabel('Churn Rate (%)')
    axes[1].set_xticklabels(['One-time', 'Repeat'], rotation=0)
    for i, v in enumerate(churn_by_repeat.values):
        axes[1].text(i, v + 0.5, f"{v:.1f}%", ha='center', fontweight='bold')

    plt.tight_layout()
    fig.savefig(plot_dir / "08_repeat_vs_onetime.png", dpi=150)
    plt.close()


def plot_recent_vs_overall_gap(X, y, plot_dir):
    """Plot recent vs overall order gap comparison."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    if 'recent_avg_gap_days' not in X.columns or 'avg_days_between_orders' not in X.columns:
        return

    fig, ax = plt.subplots(figsize=(8, 8))
    scatter_colors = [COLOR_ACTIVE if label == 0 else COLOR_CHURNED for label in y.values]
    ax.scatter(X['avg_days_between_orders'], X['recent_avg_gap_days'],
              c=scatter_colors, alpha=0.6, edgecolors='white', linewidth=0.5, s=60)
    max_val = max(X['avg_days_between_orders'].max(), X['recent_avg_gap_days'].max())
    ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Equal line')
    ax.set_xlabel('Overall Avg Gap (days)', fontsize=11)
    ax.set_ylabel('Recent Avg Gap (days)', fontsize=11)
    ax.set_title('Recent vs Overall Order Gap\n(Above diagonal = buying less frequently recently)',
                 fontsize=13, fontweight='bold')
    ax.legend(['Equal gap', 'Active', 'Churned'], loc='upper left')
    plt.tight_layout()
    fig.savefig(plot_dir / "09_recent_vs_overall_gap.png", dpi=150)
    plt.close()


def generate_plots(df, X, y):
    """Orchestrate all plot generation."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        log.warning("  matplotlib/seaborn not installed — skipping plots")
        log.warning("  Install with: pip install matplotlib seaborn")
        return

    log.info("  Generating EDA plots...")
    plot_dir = OUTPUT_DIR / "plots"
    plot_dir.mkdir(exist_ok=True)

    plot_churn_distribution(y, plot_dir)
    plot_feature_correlations(X, y, plot_dir)
    plot_correlation_heatmap(X, y, plot_dir)
    plot_rfm_distributions(X, plot_dir)
    plot_active_vs_churned(X, y, plot_dir)
    plot_mean_vs_median_gap(X, y, plot_dir)
    plot_tier_distribution(df, plot_dir)
    plot_repeat_vs_onetime(df, plot_dir)
    plot_recent_vs_overall_gap(X, y, plot_dir)

    log.info("  Plots saved to: %s", plot_dir)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SECTION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """Main orchestration function."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Extract ML features and run EDA")
    parser.add_argument("--db-url", default=os.getenv("DB_URL"),
                        help="PostgreSQL connection string (or set DB_URL env var)")
    parser.add_argument("--client-id", default=None,
                        help="Filter by client_id (e.g., CLT-001). If omitted, all clients.")
    parser.add_argument("--no-eda", action="store_true", help="Skip EDA, only extract features")
    parser.add_argument("--no-plots", action="store_true", help="Skip plot generation")
    args = parser.parse_args()

    if not args.db_url:
        log.error("DB_URL not set. Use --db-url or set DB_URL env var.")
        log.error("Example: postgresql://postgres:password@localhost:5432/walmart_crp")
        sys.exit(1)

    cid = args.client_id
    if cid:
        log.info("Running for client_id=%s", cid)

    # Step 1: Connect and extract
    engine = connect_db(args.db_url)
    info = get_table_info(engine, client_id=cid)

    log.info("Database info:")
    log.info("  Total customers:  %d", info['total_customers'])
    log.info("  Last refresh:     %s", info['last_refresh'])
    log.info("  Churn dist:       %s", info['churn_dist'])

    # Step 2: Extract full dataset
    df = extract_features(engine, client_id=cid)

    # Save full dataset
    df.to_csv(OUTPUT_DIR / "customer_features.csv", index=False)
    log.info("  Full dataset saved: %s", OUTPUT_DIR / "customer_features.csv")

    # Step 2b: Save RFM features to the database table
    save_rfm_to_db(engine, df)

    # Step 3: Prepare ML-ready matrix
    X, y = prepare_feature_matrix(df)

    # Save feature matrix (X) and target (y)
    feature_df = X.copy()
    feature_df['churn_label'] = y.values
    feature_df.to_csv(OUTPUT_DIR / "feature_matrix.csv", index=False)
    log.info("  Feature matrix saved: %s", OUTPUT_DIR / "feature_matrix.csv")

    # Step 4: Run EDA
    if not args.no_eda:
        report = run_eda(df, X, y)
        print("\n" + report)

        if not args.no_plots:
            generate_plots(df, X, y)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("DONE — Output files in: %s", OUTPUT_DIR)
    log.info("=" * 60)
    log.info("  customer_features.csv    → Full dataset (%d rows x %d cols)", df.shape[0], df.shape[1])
    log.info("  feature_matrix.csv       → ML-ready (%d rows x %d features + target)", X.shape[0], X.shape[1])
    if not args.no_eda:
        log.info("  eda_report.txt           → Analysis report")
        log.info("  correlation_matrix.csv   → Feature correlations")
        log.info("  class_balance.csv        → Churn distribution")
        log.info("  feature_importance_rfm.csv → RFM scores")
        if not args.no_plots:
            log.info("  tier_distribution.csv    → Customer tier breakdown")
            log.info("  plots/                   → 9 visualization charts")

    log.info("\nNext step: python -m ml.train_model (coming in Task 1.10-1.11)")


if __name__ == "__main__":
    main()
