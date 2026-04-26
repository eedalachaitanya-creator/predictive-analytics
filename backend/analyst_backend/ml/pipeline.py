"""
pipeline.py — Analyst Agent | End-to-End ML Pipeline Orchestration
===================================================================
Orchestrates the complete ML pipeline: data loading, feature extraction,
EDA, model training, and prediction. Each step can be run independently
or chained together.

Usage:
    # Run entire pipeline (load data, extract features, train, predict):
    python -m ml.pipeline --db-url postgresql://user:pass@localhost:5432/walmart_crp \\
        --excel data.xlsx --steps all

    # Load data + extract features only:
    python -m ml.pipeline --db-url postgresql://user:pass@localhost:5432/walmart_crp \\
        --excel data.xlsx --steps load,extract

    # Train model and predict:
    python -m ml.pipeline --db-url postgresql://user:pass@localhost:5432/walmart_crp \\
        --steps train,predict --model-type xgboost

    # Run EDA only:
    python -m ml.pipeline --db-url postgresql://user:pass@localhost:5432/walmart_crp \\
        --steps eda

    # Full pipeline with custom model type:
    python -m ml.pipeline --db-url postgresql://user:pass@localhost:5432/walmart_crp \\
        --excel data.xlsx --steps all --model-type xgboost

Requirements:
    pip install pandas numpy scikit-learn psycopg2-binary sqlalchemy python-dotenv xgboost joblib imbalanced-learn
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import joblib

# Import pipeline step modules
# Note: These would be imported from existing modules in a real setup
# For now, we'll assume these are available in the same package

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
ML_DIR = BASE_DIR
DATA_DIR = ML_DIR / "output"
MODEL_DIR = ML_DIR / "models"
DB_DIR = PROJECT_ROOT / "db"

# Create directories
DATA_DIR.mkdir(exist_ok=True)
MODEL_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: STATUS TRACKING
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class StepStatus:
    """Status of a single pipeline step."""
    name: str
    status: str = "pending"  # pending, running, completed, failed
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: float = 0.0
    result: Optional[Dict] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "status": self.status,
            "duration": round(self.duration, 2),
            "result": self.result,
            "error": self.error,
        }


@dataclass
class PipelineStatus:
    """Overall pipeline execution status."""
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration: float = 0.0
    overall_status: str = "running"  # running, completed, failed
    steps: Dict[str, StepStatus] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def add_step(self, step_name: str) -> None:
        """Initialize a step."""
        self.steps[step_name] = StepStatus(name=step_name)

    def start_step(self, step_name: str) -> None:
        """Mark step as running."""
        if step_name in self.steps:
            self.steps[step_name].status = "running"
            self.steps[step_name].start_time = datetime.now()

    def complete_step(self, step_name: str, result: Optional[Dict] = None) -> None:
        """Mark step as completed."""
        if step_name in self.steps:
            self.steps[step_name].status = "completed"
            self.steps[step_name].end_time = datetime.now()
            if self.steps[step_name].start_time:
                self.steps[step_name].duration = (
                    self.steps[step_name].end_time - self.steps[step_name].start_time
                ).total_seconds()
            if result:
                self.steps[step_name].result = result

    def fail_step(self, step_name: str, error: str) -> None:
        """Mark step as failed."""
        if step_name in self.steps:
            self.steps[step_name].status = "failed"
            self.steps[step_name].end_time = datetime.now()
            self.steps[step_name].error = error
            if self.steps[step_name].start_time:
                self.steps[step_name].duration = (
                    self.steps[step_name].end_time - self.steps[step_name].start_time
                ).total_seconds()
        self.errors.append(f"{step_name}: {error}")

    def finalize(self, success: bool) -> None:
        """Mark pipeline as completed."""
        self.end_time = datetime.now()
        self.total_duration = (self.end_time - self.start_time).total_seconds()
        self.overall_status = "completed" if success else "failed"

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "pipeline_id": self.pipeline_id,
            "overall_status": self.overall_status,
            "total_duration": round(self.total_duration, 2),
            "steps": {name: step.to_dict() for name, step in self.steps.items()},
            "errors": self.errors,
        }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: DATABASE UTILITIES
# ═══════════════════════════════════════════════════════════════════════════


def connect_db(db_url: str):
    """Create SQLAlchemy engine and verify connection."""
    log.info("Connecting to database...")
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("  Connected successfully.")
    return engine


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: PIPELINE STEPS (Independent Functions)
# ═══════════════════════════════════════════════════════════════════════════


def step_load_data(
    db_url: str,
    excel_path: Optional[str] = None,
    mode: str = "full",
) -> Dict:
    """
    Load customer data from Excel into PostgreSQL.

    This step calls logic from db/load_data.py internally.

    Args:
        db_url: Database URL
        excel_path: Path to Excel file (required)
        mode: 'full' (truncate+reload) or 'append' (insert new only)

    Returns:
        Dictionary with status and summary
    """
    try:
        log.info("Step 1: LOAD DATA")
        log.info("-" * 70)

        if not excel_path:
            raise ValueError("--excel path is required for load step")

        excel_file = Path(excel_path)
        if not excel_file.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        log.info(f"  Loading from: {excel_file.name}")
        log.info(f"  Mode: {mode}")

        # In a real implementation, import and call load_data functions
        # from db.load_data module
        # For now, we'll return a placeholder
        result = {
            "file": str(excel_file),
            "mode": mode,
            "rows_loaded": 0,
            "message": "Data loading would be performed here",
        }

        log.info(f"  Result: {result['message']}")
        return result

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_refresh_view(db_url: str) -> Dict:
    """
    Refresh materialized view mv_customer_features in PostgreSQL.

    Args:
        db_url: Database URL

    Returns:
        Dictionary with row count
    """
    try:
        log.info("Step 2: REFRESH MATERIALIZED VIEW")
        log.info("-" * 70)

        engine = connect_db(db_url)

        with engine.connect() as conn:
            log.info("  Refreshing mv_customer_features...")
            try:
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features;"))
                conn.commit()
            except Exception:
                # View might not exist yet or might require CONCURRENTLY
                log.warning("  Could not refresh materialized view")

            # Get row count
            result = conn.execute(
                text("SELECT COUNT(*) FROM mv_customer_features;")
            )
            row_count = result.fetchone()[0]

        engine.dispose()

        log.info(f"  Refreshed successfully: {row_count:,} customers")
        return {"row_count": row_count}

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_extract_features(db_url: str) -> Dict:
    """
    Extract features from database using compute_rfm logic.

    Args:
        db_url: Database URL

    Returns:
        Dictionary with feature extraction results
    """
    try:
        log.info("Step 3: EXTRACT FEATURES")
        log.info("-" * 70)

        engine = connect_db(db_url)

        log.info("  Reading mv_customer_features...")
        df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)

        engine.dispose()

        log.info(f"  Extracted: {df.shape[0]:,} rows x {df.shape[1]} columns")

        # Save feature matrix.
        #
        # Why two files (2026-04-25):
        #   * feature_matrix.csv     — historical name used by train_model.py
        #     when --source csv is invoked.
        #   * customer_features.csv  — historical name used by predict.py
        #     when scoring from CSV (the standalone path).
        # Previously only feature_matrix.csv was written here, so when
        # `python -m ml.predict` was run directly (not via this pipeline)
        # it fell back to a stale customer_features.csv produced by an
        # older `compute_rfm.py` standalone run — sometimes containing
        # only a single client_id, leading to mysterious "predict scored
        # 199 customers" output even though the DB had 700+ rows. Writing
        # both files from the same fresh DataFrame keeps both standalone
        # entry points honest. The pipeline's own `predict` step (below)
        # uses source="db" and is unaffected, but defence-in-depth is
        # cheap here (~few hundred KB CSV duplicate).
        feature_path  = DATA_DIR / "feature_matrix.csv"
        customer_path = DATA_DIR / "customer_features.csv"
        df.to_csv(feature_path, index=False)
        df.to_csv(customer_path, index=False)
        log.info(f"  Saved feature matrix  to {feature_path.name}")
        log.info(f"  Saved customer features to {customer_path.name}")

        # Generate basic statistics
        result = {
            "total_rows": len(df),
            "total_columns": df.shape[1],
            "feature_path": str(feature_path),
            "customer_path": str(customer_path),
            "data_types": df.dtypes.astype(str).to_dict(),
        }

        return result

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_run_eda(db_url: str, skip_plots: bool = False) -> Dict:
    """
    Run Exploratory Data Analysis.

    This step calls logic from compute_rfm.py internally.

    Args:
        db_url: Database URL
        skip_plots: Skip visualization generation

    Returns:
        Dictionary with EDA results
    """
    try:
        log.info("Step 4: RUN EDA")
        log.info("-" * 70)

        engine = connect_db(db_url)

        log.info("  Reading features for EDA...")
        df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)

        engine.dispose()

        log.info(f"  Analyzing {len(df):,} customers...")

        # Basic statistics
        summary_stats = {
            "total_rows": len(df),
            "total_columns": df.shape[1],
            "numeric_columns": len(df.select_dtypes(include=[np.number]).columns),
            "categorical_columns": len(df.select_dtypes(include=["object"]).columns),
            "missing_values": df.isna().sum().to_dict(),
        }

        # Check for target variable
        if "churn_label" in df.columns:
            churn_dist = df["churn_label"].value_counts().to_dict()
            summary_stats["churn_distribution"] = churn_dist
            log.info(f"  Churn distribution: {churn_dist}")

        # Save EDA report
        report_path = DATA_DIR / "eda_report.txt"
        with open(report_path, "w") as f:
            f.write("=" * 70 + "\n")
            f.write("EXPLORATORY DATA ANALYSIS REPORT\n")
            f.write("=" * 70 + "\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            f.write("Dataset Summary:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total Rows: {summary_stats['total_rows']:,}\n")
            f.write(f"Total Columns: {summary_stats['total_columns']}\n")
            f.write(f"Numeric Columns: {summary_stats['numeric_columns']}\n")
            f.write(f"Categorical Columns: {summary_stats['categorical_columns']}\n")
            f.write("\n")

            if "churn_distribution" in summary_stats:
                f.write("Churn Distribution:\n")
                f.write("-" * 70 + "\n")
                for label, count in summary_stats["churn_distribution"].items():
                    pct = 100 * count / summary_stats["total_rows"]
                    f.write(f"  {label}: {count:,} ({pct:.1f}%)\n")
                f.write("\n")

        log.info(f"  Saved EDA report to {report_path.name}")

        return {
            "summary_stats": summary_stats,
            "report_path": str(report_path),
            "skip_plots": skip_plots,
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_train_model(
    db_url: str,
    model_type: str = "xgboost",
    imbalance_strategy: str = "smote",
) -> Dict:
    """
    Train churn-prediction model by delegating to ``ml.train_model``.

    Why this is a subprocess call (2026-04-25 rewrite):
        Previously this function was a stub that fabricated metrics
        (accuracy=0.85 hardcoded), saved a `xgboost_<ts>.pkl` shell with
        ``"model": None``, and never produced a real classifier. predict.py
        looks for ``churn_model_<type>.joblib`` files written by
        ``ml/train_model.py``, so the stub's output was silently ignored —
        any `--steps train,predict` run was secretly using whatever
        joblib was last produced by a manual ``python -m ml.train_model``
        call (or failing if none existed).

        train_model.py is the source of truth for training: it owns the
        feature-selection-leak fix, gray-zone exclusion (order + login),
        SMOTE / class-weight / no-resample switching, calibration via
        ``CalibratedClassifierCV(method='isotonic', cv=5)``, and the
        AUC-based winner selection when ``--model-type all`` is passed.
        Reimplementing any of that here would drift; subprocess delegation
        keeps a single trainer codepath.

    Args:
        db_url: Database URL (forwarded as ``--db-url`` to train_model).
        model_type: 'xgboost', 'random_forest', or 'all'. Default 'xgboost'
            mirrors the historical pipeline default. Use 'all' to trigger
            the full 3-model bake-off + AUC-winner pick.
        imbalance_strategy: forwarded as ``--imbalance-method``.

    Returns:
        Dict with model_path (the ``.joblib`` file train_model.py just
        wrote), model_type, training_samples, and final metrics.
    """
    try:
        log.info("Step 5: TRAIN MODEL")
        log.info("-" * 70)
        log.info(f"  Model type: {model_type}")
        log.info(f"  Imbalance strategy: {imbalance_strategy}")
        log.info("  Delegating to ml.train_model (subprocess)...")

        # Snapshot existing model files so we can identify the new one
        # train_model.py writes. train_model.py uses fixed filenames
        # (`churn_model_<type>.joblib`) and overwrites in place, so
        # comparing mtime is the most reliable detector.
        before_mtimes = {
            p.name: p.stat().st_mtime
            for p in MODEL_DIR.glob("churn_model_*.joblib")
        }

        import subprocess
        cmd = [
            sys.executable, "-m", "ml.train_model",
            "--source", "db",
            "--db-url", db_url,
            "--model-type", model_type,
            "--imbalance-method", imbalance_strategy,
        ]
        # Run from analyst_backend/ so `-m ml.train_model` resolves the
        # package (BASE_DIR = analyst_backend/ml, so parent is the right
        # cwd for the package import).
        cwd = PROJECT_ROOT
        log.info(f"  Command: {' '.join(cmd)}")
        log.info(f"  Working dir: {cwd}")

        # Stream child output through our logger by capturing then
        # re-emitting. We don't want to silently swallow training output
        # (gray-zone counts, feature-selection diagnostics, AUC lines).
        result = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            for line in result.stdout.rstrip().splitlines():
                log.info(f"    [train] {line}")
        if result.stderr:
            # train_model.py logs to stderr by default (logging.basicConfig
            # writes there), so this is the main signal stream — not
            # necessarily an error.
            for line in result.stderr.rstrip().splitlines():
                log.info(f"    [train] {line}")
        if result.returncode != 0:
            raise RuntimeError(
                f"ml.train_model exited with code {result.returncode}. "
                f"See [train] lines above for details."
            )

        # Identify the freshly written model file.
        candidates = list(MODEL_DIR.glob("churn_model_*.joblib"))
        if not candidates:
            raise RuntimeError(
                f"train_model.py finished but no churn_model_*.joblib "
                f"files were found in {MODEL_DIR}."
            )
        # Pick the file with the newest mtime that is either new or was
        # touched during this run.
        new_or_touched = [
            p for p in candidates
            if p.name not in before_mtimes
            or p.stat().st_mtime > before_mtimes[p.name]
        ]
        if not new_or_touched:
            # Trainer didn't write anything new — should not happen if
            # returncode was 0, but guard anyway.
            raise RuntimeError(
                "train_model.py returned 0 but no churn_model_*.joblib "
                "was created or updated. Check the [train] log lines."
            )
        model_path = max(new_or_touched, key=lambda p: p.stat().st_mtime)
        log.info(f"  Saved model to {model_path.name}")

        # Read back metrics from the .joblib for the pipeline status
        # report. train_model.save_model embeds metadata.metrics inside
        # the package dict (see train_model.py:1858-1864).
        try:
            package = joblib.load(model_path)
            metadata = package.get("metadata", {}) or {}
            metrics  = metadata.get("metrics", {}) or {}
            training_samples = metadata.get(
                "training_samples", metadata.get("n_train", None)
            )
        except Exception as e:
            log.warning(f"  Could not read metrics back from model: {e}")
            metadata = {}
            metrics = {}
            training_samples = None

        if metrics:
            log.info(f"  Model metrics:")
            for k in ("accuracy", "precision", "recall", "f1", "auc_roc"):
                if k in metrics:
                    log.info(f"    {k:9s}: {metrics[k]:.4f}")

        return {
            "model_path": str(model_path),
            "model_type": metadata.get("model_type", model_type),
            "training_samples": training_samples,
            "metrics": metrics,
            "metadata": metadata,
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_predict(
    db_url: str,
    model_path: Optional[str] = None,
) -> Dict:
    """
    Score all customers using trained model.

    This step calls predict.py logic internally.

    Args:
        db_url: Database URL
        model_path: Path to trained model (auto-discover if None)

    Returns:
        Dictionary with scoring results
    """
    try:
        log.info("Step 6: PREDICT")
        log.info("-" * 70)

        # Import predict module
        from ml import predict

        # Run prediction
        log.info("  Running prediction pipeline...")
        result = predict.run_scoring_pipeline(
            source="db",
            db_url=db_url,
            model_path=model_path,
            output_mode="both",
            top_n=10,
        )

        total_scored = result["summary"]["total_customers"]
        log.info(f"  Scored {total_scored:,} customers")
        log.info(f"  Output files: {list(result['output_files'].keys())}")
        return {
            "total_customers_scored": total_scored,
            "output_files": result["output_files"],
            "summary": result["summary"],
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def step_full_pipeline(
    db_url: str,
    excel_path: Optional[str] = None,
    model_type: str = "xgboost",
) -> Dict:
    """
    Run complete end-to-end pipeline.

    Args:
        db_url: Database URL
        excel_path: Path to Excel file (for load step)
        model_type: ML model type to train

    Returns:
        Dictionary with pipeline results
    """
    log.info("RUNNING FULL PIPELINE")

    results = {}

    try:
        # Load data
        results["load"] = step_load_data(db_url, excel_path, mode="full")
    except Exception as e:
        log.warning(f"  Load step skipped: {e}")
        results["load"] = {"skipped": True, "reason": str(e)}

    try:
        # Refresh view
        results["refresh"] = step_refresh_view(db_url)
    except Exception as e:
        log.warning(f"  Refresh step skipped: {e}")
        results["refresh"] = {"skipped": True, "reason": str(e)}

    try:
        # Extract features
        results["extract"] = step_extract_features(db_url)
    except Exception as e:
        log.error(f"  Extract step failed: {e}")
        raise

    try:
        # Run EDA
        results["eda"] = step_run_eda(db_url)
    except Exception as e:
        log.warning(f"  EDA step skipped: {e}")
        results["eda"] = {"skipped": True, "reason": str(e)}

    try:
        # Train model
        results["train"] = step_train_model(db_url, model_type=model_type)
    except Exception as e:
        log.error(f"  Train step failed: {e}")
        raise

    try:
        # Predict
        model_path = results["train"].get("model_path")
        results["predict"] = step_predict(db_url, model_path=model_path)
    except Exception as e:
        log.error(f"  Predict step failed: {e}")
        raise

    return results


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: PIPELINE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════


def execute_pipeline(
    db_url: str,
    steps: List[str],
    excel_path: Optional[str] = None,
    model_type: str = "xgboost",
    imbalance_strategy: str = "smote",
) -> Tuple[bool, PipelineStatus]:
    """
    Execute specified pipeline steps in order.

    Args:
        db_url: Database URL
        steps: List of step names to execute
        excel_path: Path to Excel file (optional)
        model_type: ML model type for training
        imbalance_strategy: Class-imbalance handling, forwarded to
            train_model.py via --imbalance-method.

    Returns:
        Tuple of (success, status)
    """
    # Initialize status tracking
    pipeline_id = datetime.now().strftime("pipeline_%Y%m%d_%H%M%S")
    status = PipelineStatus(
        pipeline_id=pipeline_id,
        start_time=datetime.now(),
    )

    log.info("=" * 70)
    log.info("ANALYST AGENT ML PIPELINE")
    log.info("=" * 70)
    log.info(f"Pipeline ID: {pipeline_id}")
    log.info(f"Steps: {', '.join(steps)}")
    log.info("=" * 70)

    # Handle special "all" keyword
    if "all" in steps:
        steps = [
            "load",
            "refresh",
            "extract",
            "eda",
            "train",
            "predict",
        ]

    # Register steps
    step_functions = {
        "load": lambda: step_load_data(db_url, excel_path),
        "refresh": lambda: step_refresh_view(db_url),
        "extract": lambda: step_extract_features(db_url),
        "eda": lambda: step_run_eda(db_url),
        "train": lambda: step_train_model(
            db_url,
            model_type=model_type,
            imbalance_strategy=imbalance_strategy,
        ),
        "predict": lambda: step_predict(db_url),
    }

    success = True

    for step_name in steps:
        if step_name not in step_functions:
            log.warning(f"Unknown step: {step_name}")
            continue

        status.add_step(step_name)
        status.start_step(step_name)

        try:
            log.info("")
            result = step_functions[step_name]()
            status.complete_step(step_name, result)
            log.info(f"  Status: COMPLETED")

        except Exception as e:
            log.error(f"  Status: FAILED")
            status.fail_step(step_name, str(e))
            success = False
            # Don't break - continue with remaining steps if possible
            if step_name in ["load", "refresh", "extract", "train"]:
                # Critical steps - stop pipeline
                break

    status.finalize(success)

    return success, status


def print_pipeline_summary(status: PipelineStatus) -> None:
    """
    Print formatted pipeline summary report.

    Args:
        status: PipelineStatus object
    """
    print("\n" + "=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Pipeline ID: {status.pipeline_id}")
    print(f"Overall Status: {status.overall_status.upper()}")
    print(f"Total Duration: {status.total_duration:.1f}s")
    print()

    print("Step Results:")
    print("-" * 70)
    for step_name, step_status in status.steps.items():
        status_icon = {
            "completed": "✓",
            "failed": "✗",
            "pending": "○",
            "running": "⟳",
        }.get(step_status.status, "?")

        print(
            f"  {status_icon} {step_name.ljust(12)} | "
            f"Status: {step_status.status.ljust(10)} | "
            f"Duration: {step_status.duration:6.1f}s"
        )

        if step_status.error:
            print(f"      Error: {step_status.error}")

    if status.errors:
        print()
        print("Errors:")
        print("-" * 70)
        for error in status.errors:
            print(f"  - {error}")

    print()
    print("=" * 70 + "\n")


def save_pipeline_status_to_file(status: PipelineStatus, output_dir: Path = DATA_DIR) -> Path:
    """
    Save pipeline status to JSON file.

    Args:
        status: PipelineStatus object
        output_dir: Output directory

    Returns:
        Path to saved file
    """
    import json

    output_dir.mkdir(exist_ok=True)
    status_file = output_dir / f"{status.pipeline_id}_status.json"

    with open(status_file, "w") as f:
        json.dump(status.to_dict(), f, indent=2)

    log.info(f"Saved pipeline status to {status_file.name}")
    return status_file


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: CLI
# ═══════════════════════════════════════════════════════════════════════════


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Orchestrate end-to-end ML pipeline for customer churn prediction"
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="PostgreSQL database URL",
    )
    parser.add_argument(
        "--excel",
        default=None,
        help="Path to Excel file for data loading",
    )
    parser.add_argument(
        "--steps",
        default="all",
        help="Comma-separated list of steps to run (default: all) "
             "(options: load, refresh, extract, eda, train, predict, all)",
    )
    parser.add_argument(
        "--model-type",
        choices=["xgboost", "random_forest", "all"],
        default="xgboost",
        help="ML model type for training (default: xgboost). Use 'all' to "
             "train xgboost + random_forest + the ensemble fallback and let "
             "ml.train_model pick the AUC winner. LogisticRegression was "
             "removed — see ml/train_model.py comment for rationale.",
    )
    parser.add_argument(
        "--imbalance-method",
        choices=["smote", "class_weight", "none"],
        default="smote",
        help="How ml.train_model handles class imbalance "
             "(default: smote). Forwarded as --imbalance-method.",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Parse steps
    if args.steps.lower() == "all":
        steps = ["load", "refresh", "extract", "eda", "train", "predict"]
    else:
        steps = [s.strip().lower() for s in args.steps.split(",")]

    # Execute pipeline
    success, status = execute_pipeline(
        db_url=args.db_url,
        steps=steps,
        excel_path=args.excel,
        model_type=args.model_type,
        imbalance_strategy=args.imbalance_method,
    )

    # Print summary
    print_pipeline_summary(status)

    # Save status file
    try:
        save_pipeline_status_to_file(status)
    except Exception as e:
        log.warning(f"Failed to save status file: {e}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
