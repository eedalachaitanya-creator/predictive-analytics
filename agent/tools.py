"""
tools.py — Analyst Agent | LangGraph Tool Definitions
======================================================
Defines all tools the Analyst Agent can use to answer questions
about customer churn, risk profiles, and retention strategies.

Tools:
    1. query_database     — Run read-only SQL against PostgreSQL
    2. predict_churn      — Score one or more customers with the ML model
    3. get_customer_profile — Fetch a customer's full 360-degree view
    4. get_risk_summary   — Get aggregate churn risk distribution
    5. get_feature_importance — Show which features drive churn predictions
    6. search_at_risk_customers — Find customers matching risk criteria

Each tool is a plain function decorated with @tool so LangGraph
can bind it to the LLM's tool-calling interface.

Requirements:
    pip install langchain-core sqlalchemy joblib pandas numpy python-dotenv
"""

import os
import logging
from typing import Optional
from pathlib import Path

import pandas as pd
import numpy as np
from langchain_core.tools import tool
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("analyst_agent.tools")

# ── Paths ──
BASE_DIR = Path(__file__).parent.parent
ML_DIR = BASE_DIR / "ml"
MODEL_DIR = ML_DIR / "models"
OUTPUT_DIR = ML_DIR / "output"

# ── Database ──
DB_URL = os.getenv("DATABASE_URL", os.getenv("DB_URL", ""))

# ── Model cache (loaded once, reused) ──
_model_cache = {}


# ═══════════════════════════════════════════════════════════════════════════
# HELPER: Database connection
# ═══════════════════════════════════════════════════════════════════════════

def _get_engine():
    """Create a SQLAlchemy engine from environment config."""
    if not DB_URL:
        raise ValueError(
            "No database URL found. Set DATABASE_URL or DB_URL in your .env file."
        )
    return create_engine(DB_URL, pool_pre_ping=True)


def _run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute a read-only SQL query and return results as DataFrame."""
    engine = _get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    engine.dispose()
    return df


# ═══════════════════════════════════════════════════════════════════════════
# HELPER: Model loading
# ═══════════════════════════════════════════════════════════════════════════

def _load_model_bundle():
    """Load the best ML model bundle (cached after first load)."""
    if 'bundle' in _model_cache:
        return _model_cache['bundle']

    import joblib

    # Preference order
    preference = ['random_forest', 'xgboost', 'logistic_regression']
    model_files = list(MODEL_DIR.glob("churn_model_*.joblib"))

    if not model_files:
        raise FileNotFoundError(f"No model files found in {MODEL_DIR}")

    # Pick best available
    best_path = None
    for pref in preference:
        for f in model_files:
            if pref in f.name:
                best_path = f
                break
        if best_path:
            break

    if not best_path:
        best_path = max(model_files, key=lambda p: p.stat().st_mtime)

    bundle = joblib.load(best_path)
    _model_cache['bundle'] = bundle
    log.info("Loaded model: %s", best_path.name)
    return bundle


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 1: query_database
# ═══════════════════════════════════════════════════════════════════════════

@tool
def query_database(sql_query: str) -> str:
    """
    Run a read-only SQL query against the PostgreSQL database.
    Use this to explore customer data, orders, reviews, tickets, and features.

    IMPORTANT RULES:
    - Only SELECT queries are allowed (no INSERT, UPDATE, DELETE, DROP, etc.)
    - Always LIMIT results to 50 rows max unless the user asks for more
    - Key tables: mv_customer_features, customers, orders, line_items,
      customer_reviews, support_tickets, churn_scores

    Args:
        sql_query: A valid PostgreSQL SELECT query

    Returns:
        Query results as a formatted string (or error message)
    """
    # Safety: block write operations
    normalized = sql_query.strip().upper()
    blocked = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE',
               'CREATE', 'GRANT', 'REVOKE', 'EXEC']
    for keyword in blocked:
        if normalized.startswith(keyword):
            return f"ERROR: {keyword} queries are not allowed. Only SELECT queries are permitted."

    try:
        df = _run_query(sql_query)

        if df.empty:
            return "Query returned 0 rows."

        # Format output
        row_count = len(df)
        preview = df.to_string(index=False, max_rows=50)

        if row_count > 50:
            return f"Showing 50 of {row_count} rows:\n\n{preview}\n\n... ({row_count - 50} more rows)"
        return f"{row_count} rows returned:\n\n{preview}"

    except Exception as e:
        return f"SQL Error: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 2: predict_churn
# ═══════════════════════════════════════════════════════════════════════════

TIER_ORDER = {'Bronze': 1, 'Silver': 2, 'Gold': 3, 'Platinum': 4}

RISK_THRESHOLDS = {
    'high': 0.65,
    'medium': 0.35,
}


@tool
def predict_churn(customer_ids: str) -> str:
    """
    Predict churn probability for one or more customers using the trained ML model.
    Fetches customer features from the database, runs prediction, and returns
    churn probability with risk level.

    Args:
        customer_ids: Comma-separated customer IDs (e.g., "WMT-CUST-00001" or
                      "WMT-CUST-00001,WMT-CUST-00050,WMT-CUST-00100")

    Returns:
        Prediction results with churn probability and risk level for each customer
    """
    try:
        bundle = _load_model_bundle()
        model = bundle['model']
        scaler = bundle.get('scaler')
        feature_names = bundle['feature_names']
        metadata = bundle.get('metadata', {})

        # Parse customer IDs
        ids = [cid.strip() for cid in customer_ids.split(',')]

        # Fetch features from database (parameterized)
        placeholders = ', '.join([f":id_{i}" for i in range(len(ids))])
        params = {f"id_{i}": cid for i, cid in enumerate(ids)}
        sql = f"""
            SELECT * FROM mv_customer_features
            WHERE customer_id IN ({placeholders})
        """
        df = _run_query(sql, params)

        if df.empty:
            return f"No customers found matching: {', '.join(ids)}"

        # Prepare features
        df_work = df.copy()

        # Encode customer_tier
        if 'customer_tier' in df_work.columns:
            df_work['customer_tier_encoded'] = (
                df_work['customer_tier'].map(TIER_ORDER).fillna(1).astype(int)
            )

        # Align to model features
        for feat in feature_names:
            if feat not in df_work.columns:
                df_work[feat] = 0

        X = df_work[feature_names].fillna(0)

        # Scale
        if scaler is not None:
            X = pd.DataFrame(scaler.transform(X), columns=X.columns, index=X.index)

        # Predict
        probabilities = model.predict_proba(X)[:, 1]

        # Build results
        results = []
        for i, (_, row) in enumerate(df.iterrows()):
            prob = float(probabilities[i])
            risk = (
                'HIGH' if prob >= RISK_THRESHOLDS['high']
                else 'MEDIUM' if prob >= RISK_THRESHOLDS['medium']
                else 'LOW'
            )
            cid = row.get('customer_id', ids[i] if i < len(ids) else 'unknown')
            tier = row.get('customer_tier', 'N/A')
            spend = row.get('total_spend_usd', 0)
            orders = row.get('total_orders', 0)

            results.append(
                f"  {cid}:\n"
                f"    Churn Probability: {prob:.4f}\n"
                f"    Risk Level: {risk}\n"
                f"    Tier: {tier} | Total Spend: ${spend:,.2f} | Orders: {orders}\n"
            )

        model_type = metadata.get('model_type', type(model).__name__)
        header = f"Churn Predictions (model: {model_type}, {len(feature_names)} features):\n"
        return header + "\n".join(results)

    except Exception as e:
        return f"Prediction error: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 3: get_customer_profile
# ═══════════════════════════════════════════════════════════════════════════

@tool
def get_customer_profile(customer_id: str) -> str:
    """
    Fetch a complete 360-degree profile for a specific customer.
    Includes: demographics, order history, RFM scores, review/ticket signals,
    tier, churn risk, and recent orders.

    Args:
        customer_id: The customer ID (e.g., "WMT-CUST-00001")

    Returns:
        Formatted customer profile with all available information
    """
    try:
        cid = customer_id.strip()
        params = {"cid": cid}

        # 1. Customer features from materialized view
        features_sql = """
            SELECT * FROM mv_customer_features
            WHERE customer_id = :cid
        """
        df_feat = _run_query(features_sql, params)

        if df_feat.empty:
            return f"Customer '{cid}' not found in mv_customer_features."

        row = df_feat.iloc[0]

        # 2. Recent orders
        orders_sql = """
            SELECT order_id, order_date, order_value_usd, discount_usd,
                   order_status, payment_method
            FROM orders
            WHERE customer_id = :cid
            ORDER BY order_date DESC
            LIMIT 5
        """
        df_orders = _run_query(orders_sql, params)

        # 3. Recent reviews
        reviews_sql = """
            SELECT review_id, rating, sentiment, review_date,
                   SUBSTRING(review_text, 1, 80) AS review_snippet
            FROM customer_reviews
            WHERE customer_id = :cid
            ORDER BY review_date DESC
            LIMIT 3
        """
        df_reviews = _run_query(reviews_sql, params)

        # 4. Open tickets
        tickets_sql = """
            SELECT ticket_id, ticket_type, priority, status, opened_date
            FROM support_tickets
            WHERE customer_id = :cid AND LOWER(status) != 'resolved'
            ORDER BY opened_date DESC
            LIMIT 5
        """
        df_tickets = _run_query(tickets_sql, params)

        # Build profile
        profile = []
        profile.append(f"=== CUSTOMER PROFILE: {cid} ===\n")

        # Account info
        profile.append("ACCOUNT:")
        profile.append(f"  Tier: {row.get('customer_tier', 'N/A')}")
        profile.append(f"  Account Age: {row.get('account_age_days', 0)} days")
        profile.append(f"  High Value: {'Yes' if row.get('is_high_value', 0) == 1 else 'No'}")

        # Order metrics
        profile.append("\nORDER HISTORY:")
        profile.append(f"  Total Orders: {row.get('total_orders', 0)}")
        profile.append(f"  Total Spend: ${row.get('total_spend_usd', 0):,.2f}")
        profile.append(f"  Avg Order Value: ${row.get('avg_order_value_usd', 0):,.2f}")
        profile.append(f"  Days Since Last Order: {row.get('days_since_last_order', 'N/A')}")
        profile.append(f"  Median Days Between Orders: {row.get('median_days_between_orders', 0)}")

        # RFM scores
        profile.append("\nRFM SCORES:")
        profile.append(f"  Recency: {row.get('rfm_recency_score', 'N/A')}/5")
        profile.append(f"  Frequency: {row.get('rfm_frequency_score', 'N/A')}/5")
        profile.append(f"  Monetary: {row.get('rfm_monetary_score', 'N/A')}/5")
        profile.append(f"  Total RFM: {row.get('rfm_total_score', 'N/A')}/15")

        # Engagement signals
        profile.append("\nENGAGEMENT:")
        profile.append(f"  Avg Rating: {row.get('avg_rating', 0):.1f}/5")
        profile.append(f"  Total Reviews: {row.get('total_reviews', 0)}")
        profile.append(f"  Total Tickets: {row.get('total_tickets', 0)}")
        profile.append(f"  Open Tickets: {row.get('open_tickets', 0)}")
        profile.append(f"  Return Rate: {row.get('return_rate_pct', 0):.1f}%")

        # Churn label
        churn = row.get('churn_label', 'N/A')
        profile.append(f"\nCHURN STATUS: {'CHURNED' if churn == 1 else 'ACTIVE' if churn == 0 else 'N/A'}")

        # Recent orders
        if not df_orders.empty:
            profile.append("\nRECENT ORDERS (last 5):")
            for _, o in df_orders.iterrows():
                profile.append(
                    f"  {o.get('order_date', 'N/A')} | "
                    f"${o.get('order_value_usd', 0):,.2f} | "
                    f"{o.get('order_status', 'N/A')}"
                )

        # Recent reviews
        if not df_reviews.empty:
            profile.append("\nRECENT REVIEWS:")
            for _, r in df_reviews.iterrows():
                profile.append(
                    f"  [{r.get('rating', 0)}/5] ({r.get('sentiment', 'N/A')}) "
                    f"{r.get('review_snippet', '')}"
                )

        # Open tickets
        if not df_tickets.empty:
            profile.append("\nOPEN TICKETS:")
            for _, t in df_tickets.iterrows():
                profile.append(
                    f"  {t.get('ticket_id', 'N/A')} | {t.get('ticket_type', 'N/A')} | "
                    f"Priority: {t.get('priority', 'N/A')}"
                )

        return "\n".join(profile)

    except Exception as e:
        return f"Error fetching profile: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 4: get_risk_summary
# ═══════════════════════════════════════════════════════════════════════════

@tool
def get_risk_summary() -> str:
    """
    Get an aggregate summary of churn risk across all customers.
    Shows risk distribution (HIGH/MEDIUM/LOW), average churn probability,
    and key statistics from the most recent churn scores.

    Returns:
        Formatted risk summary with distribution and statistics
    """
    try:
        # Try database first
        try:
            sql = """
                SELECT
                    COUNT(*) AS total_customers,
                    ROUND(AVG(churn_probability)::NUMERIC, 4) AS avg_prob,
                    ROUND(MIN(churn_probability)::NUMERIC, 4) AS min_prob,
                    ROUND(MAX(churn_probability)::NUMERIC, 4) AS max_prob,
                    COUNT(CASE WHEN risk_tier = 'HIGH' THEN 1 END) AS high_risk,
                    COUNT(CASE WHEN risk_tier = 'MEDIUM' THEN 1 END) AS medium_risk,
                    COUNT(CASE WHEN risk_tier = 'LOW' THEN 1 END) AS low_risk
                FROM churn_scores
                WHERE scored_at = (SELECT MAX(scored_at) FROM churn_scores)
            """
            df = _run_query(sql)
            if not df.empty and df.iloc[0]['total_customers'] > 0:
                r = df.iloc[0]
                total = int(r['total_customers'])
                return (
                    f"CHURN RISK SUMMARY (from database):\n\n"
                    f"  Total Scored Customers: {total}\n"
                    f"  Avg Churn Probability:  {r['avg_prob']}\n"
                    f"  Min / Max:              {r['min_prob']} / {r['max_prob']}\n\n"
                    f"  Risk Distribution:\n"
                    f"    HIGH:   {int(r['high_risk']):>4d}  ({100*int(r['high_risk'])/total:.1f}%)\n"
                    f"    MEDIUM: {int(r['medium_risk']):>4d}  ({100*int(r['medium_risk'])/total:.1f}%)\n"
                    f"    LOW:    {int(r['low_risk']):>4d}  ({100*int(r['low_risk'])/total:.1f}%)\n"
                )
        except Exception:
            pass

        # Fallback: read from CSV
        csv_path = OUTPUT_DIR / "churn_scores.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            total = len(df)
            high = (df['risk_level'] == 'HIGH').sum()
            medium = (df['risk_level'] == 'MEDIUM').sum()
            low = (df['risk_level'] == 'LOW').sum()
            avg_prob = df['churn_probability'].mean()

            return (
                f"CHURN RISK SUMMARY (from CSV scores):\n\n"
                f"  Total Scored Customers: {total}\n"
                f"  Avg Churn Probability:  {avg_prob:.4f}\n"
                f"  Min / Max:              {df['churn_probability'].min():.4f} / {df['churn_probability'].max():.4f}\n\n"
                f"  Risk Distribution:\n"
                f"    HIGH:   {high:>4d}  ({100*high/total:.1f}%)\n"
                f"    MEDIUM: {medium:>4d}  ({100*medium/total:.1f}%)\n"
                f"    LOW:    {low:>4d}  ({100*low/total:.1f}%)\n"
            )

        return "No churn scores found. Run predict.py first."

    except Exception as e:
        return f"Error: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 5: get_feature_importance
# ═══════════════════════════════════════════════════════════════════════════

@tool
def get_feature_importance() -> str:
    """
    Show the top features that drive churn predictions.
    Uses the currently loaded model's feature importances to explain
    which customer behaviors most strongly predict churn.

    Returns:
        Ranked list of top 15 features with importance scores
    """
    try:
        bundle = _load_model_bundle()
        model = bundle['model']
        feature_names = bundle['feature_names']
        metadata = bundle.get('metadata', {})
        model_type = metadata.get('model_type', 'unknown')

        # Extract importances
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
        elif hasattr(model, 'coef_'):
            importances = np.abs(model.coef_[0])
        else:
            return "Feature importance not available for this model type."

        # Sort
        pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)

        lines = [f"TOP 15 CHURN DRIVERS (model: {model_type}):\n"]
        for i, (feat, imp) in enumerate(pairs[:15], 1):
            bar = "█" * int(imp * 50)
            lines.append(f"  {i:2d}. {feat:<35s} {imp:.4f}  {bar}")

        lines.append(f"\n  Total features: {len(feature_names)}")
        return "\n".join(lines)

    except Exception as e:
        return f"Error: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL 6: search_at_risk_customers
# ═══════════════════════════════════════════════════════════════════════════

@tool
def search_at_risk_customers(
    risk_level: str = "HIGH",
    tier: Optional[str] = None,
    min_spend: Optional[float] = None,
    limit: int = 20
) -> str:
    """
    Search for at-risk customers matching specific criteria.
    Useful for finding high-value customers who are about to churn,
    or filtering by tier, spend, or risk level.

    Args:
        risk_level: Filter by risk level: "HIGH", "MEDIUM", or "LOW" (default: HIGH)
        tier: Optional filter by customer tier: "Platinum", "Gold", "Silver", "Bronze"
        min_spend: Optional minimum total spend in USD
        limit: Max number of results (default: 20, max: 100)

    Returns:
        List of at-risk customers matching the criteria
    """
    try:
        # Try reading from CSV first (always available after CLI scoring)
        csv_path = OUTPUT_DIR / "churn_scores.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)

            # Apply filters
            mask = df['risk_level'] == risk_level.upper()
            if tier:
                mask &= df['customer_tier'] == tier
            if min_spend:
                mask &= df['total_spend_usd'] >= min_spend

            filtered = df[mask].sort_values('churn_probability', ascending=False)

            limit = min(limit, 100)
            filtered = filtered.head(limit)

            if filtered.empty:
                return f"No customers found matching: risk={risk_level}, tier={tier}, min_spend={min_spend}"

            # Format results
            lines = [f"AT-RISK CUSTOMERS ({risk_level}, {len(filtered)} found):\n"]

            display_cols = ['customer_id', 'churn_probability', 'risk_level']
            if 'customer_tier' in filtered.columns:
                display_cols.append('customer_tier')
            if 'total_spend_usd' in filtered.columns:
                display_cols.append('total_spend_usd')
            if 'total_orders' in filtered.columns:
                display_cols.append('total_orders')

            available = [c for c in display_cols if c in filtered.columns]
            lines.append(filtered[available].to_string(index=False))
            return "\n".join(lines)

        return "No churn scores found. Run predict.py --mode cli first."

    except Exception as e:
        return f"Error: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════
# TOOL REGISTRY
# ═══════════════════════════════════════════════════════════════════════════

ALL_TOOLS = [
    query_database,
    predict_churn,
    get_customer_profile,
    get_risk_summary,
    get_feature_importance,
    search_at_risk_customers,
]
