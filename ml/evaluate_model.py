"""
evaluate_model.py — Analyst Agent | Model Evaluation & Reporting
================================================================
Standalone evaluation script that loads saved model bundles (.joblib)
and produces a consolidated evaluation against the test set.

ARCHITECTURE:
    Section 0 — Configuration (paths, constants)
    Section 1 — Data & Model Loading
    Section 2 — Evaluation Metrics (per-model)
    Section 3 — Precision-Recall Curves
    Section 4 — Consolidated Report (text + JSON)
    Section 5 — Main

Outputs (saved to ml/output/):
    - evaluation_report.txt               → Full text evaluation report
    - evaluation_summary.json             → Machine-readable summary (for Strategist Agent)
    - precision_recall_curve_[model].png  → PR curve per model
    - model_comparison_all.png            → Bar chart comparing all models

Usage:
    python -m analyst_agent.ml.evaluate_model
    python -m analyst_agent.ml.evaluate_model --model random_forest
    python -m analyst_agent.ml.evaluate_model --all

Requirements:
    pip install scikit-learn pandas numpy matplotlib seaborn joblib
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
import pandas as pd
import joblib

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("evaluate")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 0: CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
MODEL_DIR = BASE_DIR / "models"
OUTPUT_DIR = BASE_DIR / "output"
PLOT_DIR = OUTPUT_DIR / "plots"

TARGET_COL = 'churn_label'

# Must match train_model.py
LEAKY_COLS = [
    'days_since_last_order',
    'rfm_recency_score',
    'rfm_total_score',
    'orders_last_90d',
    'spend_last_90d_usd',
    'orders_last_180d',
    'spend_last_180d_usd',
]

NON_FEATURE_COLS = [
    'client_id', 'customer_id', 'first_order_date', 'last_order_date',
    'last_review_date', 'computed_at',
]

TIER_ORDER = {'Bronze': 1, 'Silver': 2, 'Gold': 3, 'Platinum': 4}

MODEL_PREFERENCE = ['random_forest', 'xgboost', 'logistic_regression']

TEST_SIZE = 0.20
RANDOM_STATE = 42


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: DATA & MODEL LOADING
# ═══════════════════════════════════════════════════════════════════════════

def discover_models() -> List[Dict[str, Any]]:
    """Find all saved model bundles in MODEL_DIR."""
    model_files = sorted(MODEL_DIR.glob("churn_model_*.joblib"))
    if not model_files:
        raise FileNotFoundError(f"No model files in {MODEL_DIR}")

    bundles = []
    for f in model_files:
        bundle = joblib.load(f)
        model_type = bundle.get('metadata', {}).get('model_type', f.stem)
        bundles.append({
            'path': f,
            'model_type': model_type,
            'model': bundle['model'],
            'scaler': bundle.get('scaler'),
            'feature_names': bundle['feature_names'],
            'metadata': bundle.get('metadata', {}),
        })
        log.info("Loaded: %s (%s)", f.name, model_type)

    return bundles


def load_feature_data() -> pd.DataFrame:
    """Load feature matrix from CSV."""
    matrix_path = OUTPUT_DIR / "feature_matrix.csv"
    full_path = OUTPUT_DIR / "customer_features.csv"

    if matrix_path.exists():
        df = pd.read_csv(matrix_path)
        log.info("Loaded feature_matrix.csv: %d rows x %d cols", df.shape[0], df.shape[1])
        return df
    elif full_path.exists():
        df = pd.read_csv(full_path)
        log.info("Loaded customer_features.csv: %d rows x %d cols", df.shape[0], df.shape[1])
        return df
    else:
        raise FileNotFoundError("No feature CSV found in output/")


def prepare_test_set(
    df: pd.DataFrame,
    feature_names: List[str],
    scaler: Any
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Recreate the exact train/test split used during training.

    Uses the same RANDOM_STATE and TEST_SIZE so the test set is identical
    to what train_model.py used.
    """
    from sklearn.model_selection import train_test_split

    # Encode tier if needed
    if 'customer_tier' in df.columns:
        df['customer_tier_encoded'] = (
            df['customer_tier'].map(TIER_ORDER).fillna(1).astype(int)
        )

    # Extract target
    if TARGET_COL not in df.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found in data")

    y = df[TARGET_COL]

    # Align features
    available = [f for f in feature_names if f in df.columns]
    missing = [f for f in feature_names if f not in df.columns]
    if missing:
        log.warning("Missing %d features (filling 0): %s", len(missing), missing)
        for feat in missing:
            df[feat] = 0

    X = df[feature_names].fillna(0)

    # Recreate same split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )

    # Scale
    if scaler is not None:
        X_test = pd.DataFrame(
            scaler.transform(X_test),
            columns=X_test.columns,
            index=X_test.index
        )

    log.info("Test set: %d samples (%d active, %d churned)",
             len(y_test), (y_test == 0).sum(), (y_test == 1).sum())

    return X_test, y_test


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: EVALUATION METRICS
# ═══════════════════════════════════════════════════════════════════════════

def compute_all_metrics(
    model: Any,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    model_type: str
) -> Dict[str, Any]:
    """
    Compute comprehensive evaluation metrics for one model.

    Returns dict with:
        - Standard metrics (accuracy, precision, recall, F1, AUC-ROC)
        - Per-class precision/recall/F1
        - Confusion matrix
        - Prediction arrays (for plotting)
    """
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, confusion_matrix, classification_report,
        average_precision_score, precision_recall_curve,
        matthews_corrcoef, balanced_accuracy_score,
    )

    log.info("Evaluating %s...", model_type)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    # Precision-recall curve data
    pr_precision, pr_recall, pr_thresholds = precision_recall_curve(y_test, y_proba)

    metrics = {
        'model_type': model_type,
        'accuracy': float(accuracy_score(y_test, y_pred)),
        'balanced_accuracy': float(balanced_accuracy_score(y_test, y_pred)),
        'precision': float(precision_score(y_test, y_pred, zero_division=0)),
        'recall': float(recall_score(y_test, y_pred, zero_division=0)),
        'f1': float(f1_score(y_test, y_pred, zero_division=0)),
        'auc_roc': float(roc_auc_score(y_test, y_proba)),
        'avg_precision': float(average_precision_score(y_test, y_proba)),
        'matthews_corrcoef': float(matthews_corrcoef(y_test, y_pred)),
        'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
        'classification_report': classification_report(y_test, y_pred, output_dict=True),
        'classification_report_text': classification_report(y_test, y_pred),
        # For plotting
        'y_pred': y_pred,
        'y_proba': y_proba,
        'pr_precision': pr_precision,
        'pr_recall': pr_recall,
        'pr_thresholds': pr_thresholds,
    }

    log.info("  Accuracy:       %.4f", metrics['accuracy'])
    log.info("  Precision:      %.4f", metrics['precision'])
    log.info("  Recall:         %.4f", metrics['recall'])
    log.info("  F1:             %.4f", metrics['f1'])
    log.info("  AUC-ROC:        %.4f", metrics['auc_roc'])
    log.info("  Avg Precision:  %.4f", metrics['avg_precision'])
    log.info("  MCC:            %.4f", metrics['matthews_corrcoef'])

    return metrics


def get_feature_importances(
    model: Any,
    feature_names: List[str],
    model_type: str
) -> List[Dict[str, Any]]:
    """Extract top 15 feature importances."""
    if model_type in ('xgboost', 'random_forest'):
        importances = model.feature_importances_
    elif model_type == 'logistic_regression':
        importances = np.abs(model.coef_[0])
    else:
        return []

    pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
    return [{'feature': f, 'importance': round(float(v), 6)} for f, v in pairs[:15]]


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: PRECISION-RECALL CURVES
# ═══════════════════════════════════════════════════════════════════════════

def plot_precision_recall_curve(
    y_test: pd.Series,
    y_proba: np.ndarray,
    model_name: str,
    avg_precision: float,
    output_dir: Path
) -> Path:
    """Plot and save precision-recall curve for one model."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve

    precision, recall, _ = precision_recall_curve(y_test, y_proba)

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(recall, precision, 'b-', linewidth=2,
            label=f'{model_name} (AP={avg_precision:.3f})')
    ax.axhline(y=y_test.mean(), color='gray', linestyle='--', alpha=0.5,
               label=f'Baseline ({y_test.mean():.3f})')
    ax.set_xlabel('Recall', fontsize=12)
    ax.set_ylabel('Precision', fontsize=12)
    ax.set_title(f'Precision-Recall Curve — {model_name}', fontsize=14)
    ax.legend(fontsize=11)
    ax.set_xlim([0.0, 1.05])
    ax.set_ylim([0.0, 1.05])
    ax.grid(True, alpha=0.3)

    out_path = output_dir / f"precision_recall_curve_{model_name}.png"
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    log.info("  Saved PR curve → %s", out_path.name)
    return out_path


def plot_model_comparison_chart(
    all_metrics: List[Dict[str, Any]],
    output_dir: Path
) -> Path:
    """Bar chart comparing all models across key metrics."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    metric_keys = ['accuracy', 'precision', 'recall', 'f1', 'auc_roc', 'avg_precision']
    metric_labels = ['Accuracy', 'Precision', 'Recall', 'F1', 'AUC-ROC', 'Avg Prec']
    model_names = [m['model_type'] for m in all_metrics]

    x = np.arange(len(metric_keys))
    width = 0.25
    offsets = np.linspace(-width, width, len(model_names))

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = ['#2196F3', '#FF9800', '#4CAF50', '#E91E63']
    for i, (m, offset) in enumerate(zip(all_metrics, offsets)):
        values = [m[k] for k in metric_keys]
        bars = ax.bar(x + offset, values, width * 0.9, label=m['model_type'],
                       color=colors[i % len(colors)], alpha=0.85)
        # Value labels on bars
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f'{val:.3f}', ha='center', va='bottom', fontsize=8)

    ax.set_ylabel('Score', fontsize=12)
    ax.set_title('Model Comparison — All Metrics', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(metric_labels, fontsize=11)
    ax.legend(fontsize=10)
    ax.set_ylim(0, 1.1)
    ax.grid(axis='y', alpha=0.3)

    out_path = output_dir / "model_comparison_all.png"
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    log.info("Saved comparison chart → %s", out_path.name)
    return out_path


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: CONSOLIDATED REPORT
# ═══════════════════════════════════════════════════════════════════════════

def generate_evaluation_report(
    all_results: List[Dict[str, Any]],
    best_model: str
) -> str:
    """Generate consolidated text evaluation report."""
    lines = []
    lines.append("=" * 75)
    lines.append("  CHURN MODEL EVALUATION REPORT — CONSOLIDATED")
    lines.append("=" * 75)
    lines.append(f"  Generated:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Models:       {len(all_results)}")
    lines.append(f"  Best model:   {best_model}")
    lines.append(f"  Test size:    {TEST_SIZE * 100:.0f}%")
    lines.append(f"  Random state: {RANDOM_STATE}")
    lines.append("")

    # ── Side-by-side comparison table ──
    lines.append("-" * 75)
    lines.append("  SIDE-BY-SIDE COMPARISON")
    lines.append("-" * 75)

    header = f"  {'Metric':<25s}"
    for r in all_results:
        header += f"  {r['model_type']:>18s}"
    lines.append(header)
    lines.append("  " + "-" * (25 + 20 * len(all_results)))

    for key, label in [
        ('accuracy', 'Accuracy'),
        ('balanced_accuracy', 'Balanced Accuracy'),
        ('precision', 'Precision'),
        ('recall', 'Recall'),
        ('f1', 'F1 Score'),
        ('auc_roc', 'AUC-ROC'),
        ('avg_precision', 'Average Precision'),
        ('matthews_corrcoef', 'Matthews Corr Coef'),
    ]:
        row = f"  {label:<25s}"
        for r in all_results:
            val = r[key]
            row += f"  {val:>18.4f}"
        lines.append(row)

    lines.append("")

    # ── Per-model details ──
    for r in all_results:
        star = " ★" if r['model_type'] == best_model else ""
        lines.append("-" * 75)
        lines.append(f"  {r['model_type'].upper()}{star}")
        lines.append("-" * 75)

        # Confusion matrix
        cm = r['confusion_matrix']
        lines.append("  Confusion Matrix:")
        lines.append(f"                        Predicted Active  Predicted Churned")
        lines.append(f"    Actual Active          {cm[0][0]:>5d}             {cm[0][1]:>5d}")
        lines.append(f"    Actual Churned         {cm[1][0]:>5d}             {cm[1][1]:>5d}")
        lines.append("")

        # Classification report
        lines.append("  Classification Report:")
        for line in r['classification_report_text'].strip().split('\n'):
            lines.append(f"    {line}")
        lines.append("")

        # Feature importance
        if r.get('feature_importances'):
            lines.append("  Top 10 Feature Importances:")
            for i, fi in enumerate(r['feature_importances'][:10], 1):
                bar = "█" * int(fi['importance'] * 50)
                lines.append(f"    {i:2d}. {fi['feature']:<35s} {fi['importance']:.4f}  {bar}")
            lines.append("")

    # ── Recommendation ──
    lines.append("=" * 75)
    lines.append(f"  RECOMMENDATION: Deploy {best_model}")

    best = next(r for r in all_results if r['model_type'] == best_model)
    lines.append(f"  AUC-ROC: {best['auc_roc']:.4f} | F1: {best['f1']:.4f} | "
                 f"Avg Precision: {best['avg_precision']:.4f}")
    lines.append("=" * 75)

    return "\n".join(lines)


def generate_evaluation_json(
    all_results: List[Dict[str, Any]],
    best_model: str
) -> Dict[str, Any]:
    """
    Generate machine-readable evaluation summary (for Strategist Agent).

    Strips numpy arrays and keeps only serializable data.
    """
    models = []
    for r in all_results:
        models.append({
            'model_type': r['model_type'],
            'accuracy': r['accuracy'],
            'balanced_accuracy': r['balanced_accuracy'],
            'precision': r['precision'],
            'recall': r['recall'],
            'f1': r['f1'],
            'auc_roc': r['auc_roc'],
            'avg_precision': r['avg_precision'],
            'matthews_corrcoef': r['matthews_corrcoef'],
            'confusion_matrix': r['confusion_matrix'],
            'per_class_metrics': r['classification_report'],
            'feature_importances': r.get('feature_importances', []),
        })

    return {
        'generated_at': datetime.now().isoformat(),
        'best_model': best_model,
        'test_size': TEST_SIZE,
        'random_state': RANDOM_STATE,
        'models': models,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: MAIN
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Churn Model Evaluation — Consolidated Report',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Evaluate all saved models:
  python -m analyst_agent.ml.evaluate_model --all

  # Evaluate specific model:
  python -m analyst_agent.ml.evaluate_model --model random_forest

  # Skip plots (text + JSON only):
  python -m analyst_agent.ml.evaluate_model --all --no-plots
        """
    )
    parser.add_argument('--all', action='store_true', default=True,
                        help='Evaluate all saved models (default)')
    parser.add_argument('--model', type=str, default=None,
                        help='Evaluate a specific model type (e.g., random_forest)')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating plots')
    parser.add_argument('--client-id', type=str, default=None,
                        help='Client ID (passed through from pipeline, not used by eval)')
    return parser.parse_args()


def main():
    args = parse_args()

    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    log.info("=" * 75)
    log.info("  ANALYST AGENT — MODEL EVALUATION")
    log.info("=" * 75)

    # 1. Load models
    bundles = discover_models()

    # Filter if specific model requested
    if args.model:
        bundles = [b for b in bundles if args.model in b['model_type']]
        if not bundles:
            log.error("No model matching '%s' found", args.model)
            sys.exit(1)

    # 2. Load data
    df = load_feature_data()

    # 3. Evaluate each model
    all_results = []

    for bundle in bundles:
        model_type = bundle['model_type']
        feature_names = bundle['feature_names']
        scaler = bundle['scaler']

        try:
            # Recreate test set (same split as training)
            X_test, y_test = prepare_test_set(df, feature_names, scaler)

            # Compute metrics
            metrics = compute_all_metrics(bundle['model'], X_test, y_test, model_type)

            # Feature importances
            metrics['feature_importances'] = get_feature_importances(
                bundle['model'], feature_names, model_type
            )

            # Precision-recall curve (non-fatal)
            if not args.no_plots:
                try:
                    plot_precision_recall_curve(
                        y_test, metrics['y_proba'], model_type,
                        metrics['avg_precision'], PLOT_DIR
                    )
                except Exception as plot_err:
                    log.warning("PR curve plot failed (non-fatal): %s", plot_err)

            all_results.append(metrics)

        except Exception as e:
            log.error("Failed to evaluate %s: %s", model_type, e)
            continue

    # 4. Determine best model (by AUC-ROC)
    if not all_results:
        log.error("All model evaluations failed. Cannot generate report.")
        sys.exit(1)

    best = max(all_results, key=lambda r: r['auc_roc'])
    best_model = best['model_type']
    log.info("")
    log.info("Best model: %s (AUC-ROC: %.4f)", best_model, best['auc_roc'])

    # 5. Comparison chart (non-fatal)
    if not args.no_plots and len(all_results) > 1:
        try:
            plot_model_comparison_chart(all_results, PLOT_DIR)
        except Exception as plot_err:
            log.warning("Comparison chart failed (non-fatal): %s", plot_err)

    # 6. Text report
    report_text = generate_evaluation_report(all_results, best_model)
    report_path = OUTPUT_DIR / "evaluation_report.txt"
    with open(report_path, 'w') as f:
        f.write(report_text)
    log.info("Saved report → %s", report_path)

    # 7. JSON summary
    summary = generate_evaluation_json(all_results, best_model)
    json_path = OUTPUT_DIR / "evaluation_summary.json"
    with open(json_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    log.info("Saved JSON → %s", json_path)

    # 8. Print report
    print()
    print(report_text)

    log.info("")
    log.info("=" * 75)
    log.info("  EVALUATION COMPLETE")
    log.info("  Report:  %s", report_path)
    log.info("  JSON:    %s", json_path)
    log.info("  Plots:   %s", PLOT_DIR)
    log.info("=" * 75)


if __name__ == '__main__':
    main()
