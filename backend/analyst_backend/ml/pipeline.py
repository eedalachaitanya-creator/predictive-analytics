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
from contextlib import contextmanager
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


@contextmanager
def connect_db(db_url: str):
    """
    Yield a verified SQLAlchemy engine that is always disposed on exit.

    Audit issue #5 (2026-04-28) — previously this returned a bare engine
    and every caller was responsible for calling .dispose() afterward.
    Several call sites disposed only on the success path, so any
    exception between connect_db() and dispose() leaked the connection
    pool. As a context manager, dispose runs in `finally` regardless of
    how the with-block exits.

    Usage:
        with connect_db(db_url) as engine:
            with engine.connect() as conn:
                rows = conn.execute(text("SELECT ...")).fetchall()
    """
    log.info("Connecting to database...")
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  Connected successfully.")
        yield engine
    finally:
        engine.dispose()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: PIPELINE STEPS (Independent Functions)
# ═══════════════════════════════════════════════════════════════════════════


def step_load_data(
    db_url: str,
    excel_path: Optional[str] = None,
    mode: str = "full",
) -> Dict:
    """
    Loud-fail stub — there is no in-process Excel→DB loader on this code
    path. Audit issue #1 (2026-04-28).

    The original implementation here was a placeholder that returned
    `{"rows_loaded": 0, "message": "Data loading would be performed
    here"}` and let `execute_pipeline` mark the step COMPLETED.
    Operators running `--steps load --excel data.xlsx` got a green
    pipeline status with no data actually loaded; downstream steps then
    ran on stale features.

    Real loading happens via:
      * The UI: backend/analyst_backend/app/upload_router.py (preferred —
        validates rows, surfaces per-row errors, audit-logs the upload).
      * The CLI: backend/analyst_backend/db/load_data.py functions called
        directly from a custom script that handles workbook iteration.

    Wiring db/load_data.py into a single in-process call here would
    duplicate hundreds of lines of orchestration and validation that
    upload_router.py already does. We refuse instead so the failure is
    visible and the operator goes through the right entry point.
    """
    log.info("Step 1: LOAD DATA")
    log.info("-" * 70)
    raise NotImplementedError(
        "ml.pipeline does not load Excel files in-process. Use the UI "
        "Upload page (upload_router.py) for validated batch loading, "
        "or call db/load_data.py functions from a custom script. The "
        f"placeholder that previously accepted --excel={excel_path!r} "
        "and reported success without doing anything has been removed."
    )


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

        # Audit issue #2 + #5 (2026-04-28):
        #   - Connection pool now disposed via context manager.
        #   - Catch only the "view does not exist" case (first-run on a
        #     fresh DB). Every other exception propagates so callers
        #     can't silently score against stale features.
        from sqlalchemy.exc import ProgrammingError

        refreshed = False
        with connect_db(db_url) as engine:
            with engine.connect() as conn:
                log.info("  Refreshing mv_customer_features...")
                try:
                    conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features;"))
                    conn.commit()
                    refreshed = True
                except ProgrammingError as e:
                    if 'does not exist' in str(e).lower():
                        log.warning(
                            "  mv_customer_features does not exist yet — "
                            "skipping refresh (first run on a fresh DB?)"
                        )
                    else:
                        # Re-raise: permission denied, lock conflict, etc.
                        raise

                # Get row count (may be zero if the view was just created
                # on this run; will raise if the view truly doesn't exist).
                result = conn.execute(
                    text("SELECT COUNT(*) FROM mv_customer_features;")
                )
                row_count = result.fetchone()[0]

        if refreshed:
            log.info(f"  Refreshed successfully: {row_count:,} customers")
        else:
            log.info(f"  View not refreshed; current row count: {row_count:,}")
        return {"row_count": row_count, "refreshed": refreshed}

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

        # Audit issue #5: connection pool disposed via context manager.
        with connect_db(db_url) as engine:
            log.info("  Reading mv_customer_features...")
            df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)

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

        # Audit issue #10 (2026-04-28): the CSVs above contain rows from
        # ALL tenants concatenated. If anyone runs `predict.py --source
        # csv` against these files in a multi-tenant deployment, every
        # tenant's rows get scored by whichever model was loaded — the
        # auto-discovery in predict.py picks ONE model and applies it
        # uniformly. Warn loudly when the export crosses tenants so
        # CSV-mode invocations are visible.
        if 'client_id' in df.columns:
            n_tenants = int(df['client_id'].nunique())
            if n_tenants > 1:
                log.warning(
                    "  CSVs above contain rows from %d tenants. CSV-mode "
                    "scoring (predict.py --source csv) cannot per-tenant "
                    "filter; use --source db --client-id <CLT-XXX> for "
                    "correct per-tenant scoring.",
                    n_tenants,
                )

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


def step_run_eda(db_url: str) -> Dict:
    """
    Run Exploratory Data Analysis.

    Currently this writes a text report (eda_report.txt) with row /
    column counts and the churn-label distribution. Audit issue #9
    (2026-04-28): the previous signature included `skip_plots: bool =
    False` but the function never produced any plots, so the parameter
    was decorative. Removed to stop the API from lying about what it
    does. If/when plotting is added back, the parameter can come back.

    Args:
        db_url: Database URL

    Returns:
        Dictionary with EDA results
    """
    try:
        log.info("Step 4: RUN EDA")
        log.info("-" * 70)

        # Audit issue #5: connection pool disposed via context manager.
        with connect_db(db_url) as engine:
            log.info("  Reading features for EDA...")
            df = pd.read_sql("SELECT * FROM mv_customer_features;", engine)

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
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def _discover_client_ids(db_url: str) -> List[str]:
    """
    Return the sorted list of client_ids that have rows in mv_customer_features.

    Per-tenant training (Option A — 2026-04-27 rewrite) iterates over this
    list and trains one model per client, so each model sees only the labels
    that match its own client_config (churn_window_days, login_window_days)
    instead of being confused by mixed labels from a global pool.
    """
    # Audit issue #5: connection pool disposed via context manager.
    with connect_db(db_url) as engine:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT client_id FROM mv_customer_features ORDER BY client_id"
            )).fetchall()
    return [r[0] for r in rows]


def step_train_model(
    db_url: str,
    model_type: str = "xgboost",
    imbalance_strategy: str = "none",
) -> Dict:
    """
    Train one churn-prediction model PER CLIENT_ID.

    Why per-tenant (2026-04-27 rewrite — Option A):
        Each client_config row carries its own churn_window_days and
        login_window_days. The materialized view computes labels using the
        per-tenant rule (CLT-001 = 90 days, CLT-002 = 80 days,
        CLT-006 = 75 days, etc.). When we previously trained one global
        model on all 700 customers, we were feeding it labels generated by
        three different rules, which the model could not reconcile because
        client_id is not a feature. The model averaged the three thresholds
        and produced wrong predictions for at least two of the tenants.

        This rewrite trains one model per client_id. Each tenant's model
        sees only its own customers' (features, label) pairs computed by
        its own rule. Trade-off: each tenant has only ~175 customers so
        per-model overfit risk is higher. Mitigated by the existing
        capacity-trim defaults (max_depth=3, L1/L2 reg) and the held-out
        test split.

    Why this is a subprocess call:
        train_model.py is the source of truth for training. Reimplementing
        any of its logic (feature-selection-leak fix, gray-zone exclusion,
        SMOTE, calibration, AUC-winner pick) inside pipeline.py would drift.
        We delegate by shelling out, once per client_id.

    Args:
        db_url: Database URL (forwarded as --db-url to train_model).
        model_type: 'xgboost', 'random_forest', or 'all'. Forwarded as-is.
        imbalance_strategy: forwarded as --imbalance-method.

    Returns:
        Dict keyed by client_id with each entry's model_path, model_type,
        training_samples, and metrics. Plus an aggregate summary at top level.
    """
    try:
        log.info("Step 5: TRAIN MODEL (per-tenant)")
        log.info("-" * 70)
        log.info(f"  Model type: {model_type}")
        log.info(f"  Imbalance strategy: {imbalance_strategy}")

        # Discover which client_ids exist in the MV. We train one model
        # per client.
        client_ids = _discover_client_ids(db_url)
        log.info(f"  Discovered {len(client_ids)} client(s): {client_ids}")

        if not client_ids:
            raise RuntimeError(
                "No client_ids found in mv_customer_features. Did you forget "
                "to refresh the MV (--steps refresh) or upload data?"
            )

        # Snapshot existing per-client model files so we can identify the
        # new ones written by each subprocess. Filenames are now
        # `churn_model_<type>_<client_id>.joblib` after the train_model.py
        # 2026-04-27 update.
        before_mtimes = {
            p.name: p.stat().st_mtime
            for p in MODEL_DIR.glob("churn_model_*.joblib")
        }

        import subprocess
        cwd = PROJECT_ROOT

        per_client_results: Dict[str, Dict] = {}

        for cid in client_ids:
            log.info("")
            log.info(f"  ── Training model for client_id={cid} ──")
            cmd = [
                sys.executable, "-m", "ml.train_model",
                "--source", "db",
                "--db-url", db_url,
                "--client-id", cid,                         # ← per-tenant filter
                "--model-type", model_type,
                "--imbalance-method", imbalance_strategy,
            ]
            log.info(f"  Command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                cwd=str(cwd),
                capture_output=True,
                text=True,
                check=False,
            )
            # Stream subprocess output through our logger so AUC, gray-zone,
            # and feature-selection diagnostics are not silently swallowed.
            if result.stdout:
                for line in result.stdout.rstrip().splitlines():
                    log.info(f"    [train {cid}] {line}")
            # Audit issue #6: route stderr at ERROR level when the
            # subprocess failed so the actual traceback isn't buried in
            # INFO output. Successful runs may also emit some stderr
            # (sklearn warnings) — keep that at INFO so it doesn't trip
            # log-level dashboards.
            if result.stderr:
                stderr_level = log.error if result.returncode != 0 else log.info
                for line in result.stderr.rstrip().splitlines():
                    stderr_level(f"    [train {cid}] {line}")
            if result.returncode != 0:
                # Don't bail on the whole pipeline if one client fails (e.g.,
                # CLT with too few customers for a stratified split). Log,
                # record, and continue to the next.
                log.error(
                    f"  ml.train_model failed for client_id={cid} "
                    f"(exit code {result.returncode}). Skipping this client."
                )
                per_client_results[cid] = {"error": f"exit code {result.returncode}"}
                continue

            # Identify the freshly written per-client model file(s).
            # When --model-type all is used, two files are written per
            # client (xgboost + random_forest); we pick the higher-AUC one.
            candidates = list(MODEL_DIR.glob(f"churn_model_*_{cid}.joblib"))
            new_or_touched = [
                p for p in candidates
                if p.name not in before_mtimes
                or p.stat().st_mtime > before_mtimes[p.name]
            ]
            if not new_or_touched:
                log.warning(
                    f"  No new model file written for {cid}. Skipping."
                )
                per_client_results[cid] = {"error": "no model file written"}
                continue

            # Among the freshly-written per-client files, pick the one
            # with highest AUC by reading metadata from each .joblib.
            best_path = None
            best_auc = -1.0
            for p in new_or_touched:
                try:
                    pkg = joblib.load(p)
                    auc = pkg.get("metadata", {}).get("metrics", {}).get("auc_roc", 0.0)
                    if auc > best_auc:
                        best_auc = auc
                        best_path = p
                except Exception as e:
                    log.warning(f"  Could not read AUC from {p.name}: {e}")

            if best_path is None:
                # Fall back to most-recently-modified
                best_path = max(new_or_touched, key=lambda p: p.stat().st_mtime)

            try:
                pkg = joblib.load(best_path)
                metadata = pkg.get("metadata", {}) or {}
                metrics  = metadata.get("metrics", {}) or {}
            except Exception as e:
                log.warning(f"  Could not read metrics from {best_path.name}: {e}")
                metadata = {}
                metrics = {}

            log.info(
                f"  ✓ {cid}: winner = {best_path.name} "
                f"(AUC-ROC = {metrics.get('auc_roc', 0.0):.4f})"
            )
            # Also update before_mtimes so subsequent loop iterations
            # don't re-pick this file.
            for p in new_or_touched:
                before_mtimes[p.name] = p.stat().st_mtime

            per_client_results[cid] = {
                "model_path": str(best_path),
                "model_type": metadata.get("model_type", model_type),
                "training_samples": metadata.get("n_train"),
                "metrics": metrics,
            }

        # Top-level aggregate so the pipeline status report has a single
        # number for "did training succeed across all clients".
        succeeded = [c for c, r in per_client_results.items() if "error" not in r]
        failed    = [c for c, r in per_client_results.items() if "error" in r]
        log.info("")
        log.info(f"  Per-tenant training summary: {len(succeeded)} succeeded, "
                 f"{len(failed)} failed")

        return {
            "client_count": len(client_ids),
            "succeeded_clients": succeeded,
            "failed_clients": failed,
            "per_client": per_client_results,
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


def _pick_best_model_for_client(client_id: str) -> Optional[Path]:
    """
    For a given client_id, find the highest-AUC per-tenant model file.

    Looks at all `churn_model_*_<client_id>.joblib` files, reads each one's
    metadata.metrics.auc_roc, and returns the path of the highest-AUC one.
    Returns None if no per-client models exist (caller should skip or
    fall back).
    """
    candidates = list(MODEL_DIR.glob(f"churn_model_*_{client_id}.joblib"))
    if not candidates:
        return None

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
            log.warning(f"  Could not read AUC from {p.name}: {e}")

    return best_path


def step_predict(
    db_url: str,
    model_path: Optional[str] = None,
) -> Dict:
    """
    Score all customers using PER-TENANT trained models.

    Why per-tenant prediction (2026-04-27 rewrite):
        After step_train_model now produces one model per client_id, we
        must use the right model for each tenant when scoring. A CLT-001
        customer's churn probability should be computed by the CLT-001
        model (which was trained on CLT-001 labels and CLT-001 patterns),
        not by whichever model file was most recently saved.

        We loop over the same client_ids found in mv_customer_features,
        pick each one's AUC-winning per-tenant model, and call
        predict.run_scoring_pipeline filtered to that tenant.

    The model_path arg is kept for backward compatibility with single-tenant
    legacy calls (when None, we auto-discover per-client). When set, it
    overrides discovery — useful for scripts that want to test a specific
    model file.

    Args:
        db_url: Database URL
        model_path: Override the auto-discovered per-client model. When
            provided, the same model is used for every client. Default
            None → discover per-client.

    Returns:
        Dict with per-client scoring summaries plus a top-level aggregate.
    """
    try:
        log.info("Step 6: PREDICT (per-tenant)")
        log.info("-" * 70)

        # Import predict module
        from ml import predict

        # Discover client_ids the same way step_train_model does, so the
        # train and predict loops cover the same set.
        client_ids = _discover_client_ids(db_url)
        log.info(f"  Discovered {len(client_ids)} client(s) to score: {client_ids}")

        if not client_ids:
            raise RuntimeError(
                "No client_ids found in mv_customer_features. Did you forget "
                "to refresh the MV?"
            )

        # Audit issue #12 (2026-04-28): warn loudly when a caller passes
        # model_path AND there is more than one tenant — the same model
        # file will be used to score every tenant's customers, which
        # is almost never what they want in a per-tenant deployment.
        if model_path and len(client_ids) > 1:
            log.warning(
                "  model_path override is set: ALL %d tenants will be "
                "scored by the same model (%s). Per-tenant model "
                "discovery is bypassed. Pass model_path=None for "
                "correct per-tenant scoring.",
                len(client_ids), model_path,
            )

        per_client_summaries: Dict[str, Dict] = {}
        total_scored_all = 0
        all_output_files = {}

        for cid in client_ids:
            log.info("")
            log.info(f"  ── Scoring client_id={cid} ──")

            # Pick the right model for this tenant.
            if model_path:
                # Caller-provided override (rare, but kept for back-compat).
                tenant_model_path = model_path
                log.info(f"  Using override model_path: {tenant_model_path}")
            else:
                best = _pick_best_model_for_client(cid)
                if best is None:
                    log.warning(
                        f"  No per-tenant model found for {cid} "
                        f"(looked for churn_model_*_{cid}.joblib). "
                        f"Skipping. Did training fail for this tenant?"
                    )
                    per_client_summaries[cid] = {"error": "no model"}
                    continue
                tenant_model_path = str(best)
                try:
                    pkg = joblib.load(best)
                    auc = pkg.get("metadata", {}).get("metrics", {}).get("auc_roc", 0.0)
                    log.info(
                        f"  Selected model: {best.name} (AUC-ROC = {auc:.4f})"
                    )
                except Exception:
                    log.info(f"  Selected model: {best.name}")

            try:
                result = predict.run_scoring_pipeline(
                    source="db",
                    db_url=db_url,
                    model_path=tenant_model_path,
                    output_mode="both",
                    top_n=10,
                    client_id=cid,                    # ← scope to this tenant
                )
                # The predict.summary uses 'total_customers' — keep that key.
                scored = result["summary"].get("total_customers", 0)
                total_scored_all += scored
                per_client_summaries[cid] = {
                    "scored": scored,
                    "model_path": tenant_model_path,
                    "summary": result["summary"],
                    "output_files": result["output_files"],
                }
                # Merge output files (csv/json/db_rows). We only keep the
                # last-written paths since each tenant writes to the same
                # output files (predict.py does an UPSERT-style replace
                # filtered by client_id, so DB rows accumulate correctly
                # across tenants but CSV/JSON files are last-tenant-wins).
                all_output_files.update(result["output_files"])
                log.info(f"  ✓ {cid}: scored {scored} customers")
            except Exception as e:
                log.error(f"  Scoring failed for {cid}: {e}")
                per_client_summaries[cid] = {"error": str(e)}
                continue

        succeeded = [c for c, r in per_client_summaries.items() if "error" not in r]
        failed    = [c for c, r in per_client_summaries.items() if "error" in r]
        log.info("")
        log.info(
            f"  Per-tenant scoring summary: {len(succeeded)} succeeded, "
            f"{len(failed)} failed | total customers scored: {total_scored_all}"
        )

        return {
            "total_customers_scored": total_scored_all,
            "client_count": len(client_ids),
            "succeeded_clients": succeeded,
            "failed_clients": failed,
            "per_client": per_client_summaries,
            "output_files": all_output_files,
        }

    except Exception as e:
        log.error(f"  Failed: {e}")
        raise


# step_full_pipeline was removed 2026-04-28 (audit issues #3 + #7).
# It was unreachable — execute_pipeline below is the single end-to-end
# entry point — and it carried two bugs that nothing was catching:
#   1. It read `results["train"].get("model_path")` even though the
#      per-tenant rewrite stopped putting that key in the train result.
#      The .get() returned None silently and step_predict's fallback
#      auto-discovery worked by accident.
#   2. Each step was wrapped in try/except that downgraded failures to
#      {"skipped": True, "reason": ...} regardless of severity, which
#      diverged from execute_pipeline's stricter policy of aborting on
#      load/refresh/extract/train failures. Two divergent error-handling
#      contracts for the same set of steps.
# If anyone needs an in-process "run everything" helper, call
# execute_pipeline(steps=["load", "refresh", "extract", "eda",
# "train", "predict"]) — that's the canonical path.


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: PIPELINE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════


def execute_pipeline(
    db_url: str,
    steps: List[str],
    excel_path: Optional[str] = None,
    model_type: str = "xgboost",
    imbalance_strategy: str = "none",
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
    # Initialize status tracking. Audit issue #8: include microseconds
    # and the process id so two pipeline runs starting in the same
    # second (CI cron + UI trigger overlap) don't collide on the same
    # pipeline_id and overwrite each other's status JSON.
    pipeline_id = (
        datetime.now().strftime("pipeline_%Y%m%d_%H%M%S_%f")
        + f"_pid{os.getpid()}"
    )
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
    Log formatted pipeline summary report.

    Audit issue #11 (2026-04-28): switched from raw print() to log.info
    so the summary lands in the same stream as the rest of the pipeline
    log. Operators redirecting logs to a file no longer see the summary
    appear only on stdout.

    Args:
        status: PipelineStatus object
    """
    log.info("")
    log.info("=" * 70)
    log.info("PIPELINE EXECUTION SUMMARY")
    log.info("=" * 70)
    log.info(f"Pipeline ID: {status.pipeline_id}")
    log.info(f"Overall Status: {status.overall_status.upper()}")
    log.info(f"Total Duration: {status.total_duration:.1f}s")
    log.info("")

    log.info("Step Results:")
    log.info("-" * 70)
    for step_name, step_status in status.steps.items():
        status_icon = {
            "completed": "✓",
            "failed": "✗",
            "pending": "○",
            "running": "⟳",
        }.get(step_status.status, "?")

        log.info(
            f"  {status_icon} {step_name.ljust(12)} | "
            f"Status: {step_status.status.ljust(10)} | "
            f"Duration: {step_status.duration:6.1f}s"
        )

        if step_status.error:
            log.info(f"      Error: {step_status.error}")

    if status.errors:
        log.info("")
        log.info("Errors:")
        log.info("-" * 70)
        for error in status.errors:
            log.info(f"  - {error}")

    log.info("")
    log.info("=" * 70)


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
        default="none",
        help="How ml.train_model handles class imbalance "
             "(default: none — matches train_model.py's own default; the "
             "dataset is well-balanced at ratio≈0.88). Use 'smote' only "
             "if you confirm severe imbalance for a specific tenant. "
             "Forwarded as --imbalance-method. Audit issue #4.",
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
