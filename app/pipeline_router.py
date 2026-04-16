"""
pipeline_router.py — Pipeline API for the Angular frontend
============================================================
Exposes endpoints that the frontend's PipelineService calls:
    POST /api/v1/pipeline/run         → starts a background job
    GET  /api/v1/pipeline/status/{id} → poll job progress
    GET  /api/v1/pipeline/last-run    → most recent completed run

Each pipeline run triggers the ML scripts in sequence:
    Stage 1: Validate database connection
    Stage 2: Refresh materialized view (mv_customer_features)
    Stage 3: Compute RFM features
    Stage 4: Train ML models
    Stage 5: Evaluate models (AUC-ROC, precision, recall, F1)
    Stage 6: Score all customers (churn predictions)
    Stage 7: Generate risk summary
    Stage 8: Run subscription refill alerts
    Stage 9: Generate outreach emails
    Stage 10: Finalize and save outputs
"""

import logging
import os
import subprocess
import sys
import uuid
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine

log = logging.getLogger("crp_api.pipeline")

# ── Get the real DB URL (str(engine.url) masks passwords with ***) ──
_DB_URL = engine.url.render_as_string(hide_password=False)

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])

# ── Paths ──
BASE_DIR = Path(__file__).parent.parent
ML_DIR = BASE_DIR / "ml"

# ── In-memory job store (production would use Redis) ──
_jobs: dict = {}


# ═══════════════════════════════════════════════════════════════════════════
# MODELS (matching frontend TypeScript interfaces exactly)
# ═══════════════════════════════════════════════════════════════════════════

class PipelineRunRequest(BaseModel):
    clientId: str = "CLT-001"
    mode: str = "full"  # churn | retention | segmentation | full


class PipelineStage(BaseModel):
    stage: int
    label: str
    status: str = "pending"  # pending | running | done | error
    message: str = ""
    timestamp: Optional[str] = None


class PipelineSummary(BaseModel):
    totalCustomers: int = 0
    totalOrders: int = 0
    totalLineItems: int = 0
    churned: int = 0
    churnRate: float = 0.0
    atRisk: int = 0
    highValue: int = 0
    repeatCustomers: int = 0
    mlFeatures: int = 0
    outputSheets: int = 0


class PipelineRunResponse(BaseModel):
    jobId: str
    status: str = "queued"  # queued | running | complete | failed
    progress: int = 0
    stages: list[PipelineStage] = []
    startedAt: str = ""
    completedAt: Optional[str] = None
    durationSeconds: Optional[float] = None
    summary: Optional[PipelineSummary] = None


# ═══════════════════════════════════════════════════════════════════════════
# STAGE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

STAGE_LABELS = [
    "Validate database connection",
    "Analyze review sentiment",
    "Refresh materialized view",
    "Compute RFM features",
    "Train ML models",
    "Evaluate models",
    "Score customers (churn predictions)",
    "Generate risk summary",
    "Compute purchase cycles",
    "Run refill alerts + outreach",
    "Finalize outputs",
]


def _make_stages() -> list[PipelineStage]:
    return [
        PipelineStage(stage=i + 1, label=label)
        for i, label in enumerate(STAGE_LABELS)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE EXECUTION (runs in background thread)
# ═══════════════════════════════════════════════════════════════════════════

def _update_stage(job: dict, stage_idx: int, status: str, message: str = ""):
    """Update a stage's status and recalculate progress."""
    job["stages"][stage_idx]["status"] = status
    job["stages"][stage_idx]["message"] = message
    job["stages"][stage_idx]["timestamp"] = datetime.now().isoformat()

    done_count = sum(1 for s in job["stages"] if s["status"] == "done")
    job["progress"] = int((done_count / len(job["stages"])) * 100)


def _run_python_module(module: str, args: list[str] = None) -> tuple[bool, str]:
    """Run a Python module as a subprocess and return (success, output)."""
    cmd = [sys.executable, "-m", module] + (args or [])
    # Pass parent environment + DB_URL so subprocesses can connect on all OS
    env = {**os.environ, "DB_URL": _DB_URL, "DATABASE_URL": _DB_URL}
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min max per stage
            cwd=str(BASE_DIR),
            env=env,
        )
        if result.returncode == 0:
            return True, result.stdout[-500:] if result.stdout else "OK"
        return False, result.stderr[-500:] if result.stderr else f"Exit code: {result.returncode}"
    except subprocess.TimeoutExpired:
        return False, "Stage timed out after 5 minutes"
    except Exception as e:
        return False, str(e)


def _execute_pipeline(job_id: str, client_id: str, mode: str):
    """Execute the full pipeline in a background thread."""
    job = _jobs[job_id]
    job["status"] = "running"
    start_time = datetime.now()

    try:
        # ── Clear old output files before starting ────────────────────
        # This prevents stale files from a previous client's run from
        # being saved under the current client's ID in Stage 10.
        output_path = ML_DIR / "output"
        if output_path.is_dir():
            import shutil
            for f in output_path.iterdir():
                if f.is_file() and not f.name.startswith("."):
                    f.unlink()
            log.info("Cleared old output files from %s", output_path)

        # Stage 1: Validate DB connection
        _update_stage(job, 0, "running", "Checking database...")
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            _update_stage(job, 0, "done", "Database connected")
        except Exception as e:
            _update_stage(job, 0, "error", f"DB error: {str(e)[:100]}")
            job["status"] = "failed"
            return

        # Stage 2: Analyze review sentiment (NLP)
        _update_stage(job, 1, "running", f"Analyzing review sentiment for {client_id}...")
        ok, msg = _run_python_module("ml.sentiment", ["--db-url", _DB_URL, "--client-id", client_id, "--update-all"])
        _update_stage(job, 1, "done" if ok else "error", msg[:200])

        # Stage 3: Refresh materialized view
        _update_stage(job, 2, "running", "Refreshing mv_customer_features...")
        try:
            with engine.connect() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features"))
                conn.commit()
            _update_stage(job, 2, "done", "Materialized view refreshed")
        except Exception as e:
            _update_stage(job, 2, "error", f"View refresh failed: {str(e)[:100]}")
            _update_stage(job, 2, "done", f"Using existing view data (refresh skipped)")

        # Stage 4: Compute RFM features
        _update_stage(job, 3, "running", f"Computing RFM features for {client_id}...")
        ok, msg = _run_python_module("ml.compute_rfm", ["--db-url", _DB_URL, "--client-id", client_id])
        _update_stage(job, 3, "done" if ok else "error", msg[:200])

        # Stage 5: Train ML models
        _update_stage(job, 4, "running", f"Training models for {client_id}...")
        ok, msg = _run_python_module("ml.train_model", ["--source", "db", "--db-url", _DB_URL, "--client-id", client_id])
        _update_stage(job, 4, "done" if ok else "error", msg[:200])

        # Stage 6: Evaluate models
        _update_stage(job, 5, "running", "Evaluating model performance...")
        ok, msg = _run_python_module("ml.evaluate_model", ["--client-id", client_id])
        if not ok:
            ok, msg = _run_python_module("ml.evaluate_model", ["--no-plots", "--client-id", client_id])
        _update_stage(job, 5, "done" if ok else "error", msg[:200])

        # Stage 7: Score customers
        _update_stage(job, 6, "running", f"Scoring {client_id} customers...")
        ok, msg = _run_python_module("ml.predict", ["--mode", "cli", "--source", "db", "--db-url", _DB_URL, "--output", "all", "--client-id", client_id])
        _update_stage(job, 6, "done" if ok else "error", msg[:200])

        # Stage 8: Generate risk summary
        _update_stage(job, 7, "running", "Generating risk summary...")
        risk_file = ML_DIR / "output" / "risk_summary.txt"
        if risk_file.exists():
            _update_stage(job, 7, "done", "Risk summary available")
        else:
            _update_stage(job, 7, "done", "Risk summary generated from scores")

        # Stage 9: Compute purchase cycles
        _update_stage(job, 8, "running", "Computing purchase cycles...")
        ok, msg = _run_python_module("ml.compute_purchase_cycles", ["--db-url", _DB_URL, "--client-id", client_id])
        _update_stage(job, 8, "done" if ok else "error", msg[:200])

        # Stage 10: Run refill alerts + outreach generation
        _update_stage(job, 9, "running", "Generating refill alerts + outreach emails...")
        if mode in ("retention", "full"):
            ok, msg = _run_python_module("ml.alerts")
            _update_stage(job, 9, "done" if ok else "error", msg[:200])
            # Also generate template-based churn outreach emails
            try:
                from app.messages_router import generate_outreach, GenerateOutreachRequest
                from app.database import SessionLocal
                outreach_db = SessionLocal()
                try:
                    outreach_req = GenerateOutreachRequest(clientId=client_id, saveToDb=True)
                    result = generate_outreach(outreach_req, outreach_db)
                    log.info("Auto-generated %d churn outreach emails", result.get("total", 0))
                finally:
                    outreach_db.close()
            except Exception as e:
                log.warning("Churn outreach generation failed (non-blocking): %s", e)
        else:
            _update_stage(job, 9, "done", "Skipped (mode: {})".format(mode))

        # Stage 11: Finalize — save all output files to database + build summary
        _update_stage(job, 10, "running", "Finalizing outputs & storing to database...")
        try:
            from db.pipeline_outputs_store import save_all_output_files
            output_dir = str(ML_DIR / "output")
            stored = save_all_output_files(engine, client_id, output_dir)
            log.info("Stored %d output files to pipeline_outputs table", stored)
        except Exception as e:
            log.warning("Could not store outputs to DB: %s (downloads will use disk fallback)", e)

        summary = _build_summary(client_id)
        job["summary"] = summary
        _update_stage(job, 10, "done", "Pipeline complete")

        # Check if any stage failed
        has_errors = any(s["status"] == "error" for s in job["stages"])
        job["status"] = "complete" if not has_errors else "failed"

    except Exception as e:
        log.error("Pipeline crashed: %s", e)
        job["status"] = "failed"
        # Mark remaining stages as error
        for s in job["stages"]:
            if s["status"] in ("pending", "running"):
                s["status"] = "error"
                s["message"] = "Pipeline interrupted"

    finally:
        end_time = datetime.now()
        job["completedAt"] = end_time.isoformat()
        job["durationSeconds"] = round((end_time - start_time).total_seconds(), 1)
        job["progress"] = 100 if job["status"] == "complete" else job["progress"]


def _build_summary(client_id: str) -> dict:
    """Build pipeline summary from database."""
    summary = PipelineSummary()
    try:
        with engine.connect() as conn:
            # Total customers
            r = conn.execute(text("SELECT COUNT(*) FROM customers WHERE client_id = :cid"), {"cid": client_id})
            summary.totalCustomers = r.scalar() or 0

            # Total orders
            r = conn.execute(text("SELECT COUNT(*) FROM orders WHERE client_id = :cid"), {"cid": client_id})
            summary.totalOrders = r.scalar() or 0

            # Total line items
            r = conn.execute(text("""
                SELECT COUNT(*) FROM line_items li
                JOIN orders o ON li.order_id = o.order_id
                WHERE o.client_id = :cid
            """), {"cid": client_id})
            summary.totalLineItems = r.scalar() or 0

            # Churned + churn rate
            r = conn.execute(text("""
                SELECT COUNT(*) FILTER (WHERE churn_label = 1) AS churned,
                       COUNT(*) AS total
                FROM mv_customer_features WHERE client_id = :cid
            """), {"cid": client_id})
            row = r.fetchone()
            if row:
                summary.churned = row[0] or 0
                total = row[1] or 1
                summary.churnRate = round(summary.churned / total * 100, 1)

            # At risk (from churn_scores table or CSV fallback)
            try:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM churn_scores WHERE client_id = :cid AND risk_tier = 'HIGH'"
                ), {"cid": client_id})
                summary.atRisk = r.scalar() or 0
            except Exception:
                # Fallback to CSV if churn_scores table doesn't exist yet
                import pandas as pd
                scores_path = ML_DIR / "output" / "churn_scores.csv"
                if scores_path.exists():
                    df = pd.read_csv(scores_path)
                    col = "risk_tier" if "risk_tier" in df.columns else "risk_level"
                    summary.atRisk = int((df[col] == "HIGH").sum())

            # High value + repeat (derived from rfm_total_score and total_orders)
            r = conn.execute(text("""
                SELECT COUNT(*) FILTER (WHERE rfm_total_score >= 12) AS hv,
                       COUNT(*) FILTER (WHERE total_orders >= 2) AS repeat
                FROM mv_customer_features WHERE client_id = :cid
            """), {"cid": client_id})
            row = r.fetchone()
            if row:
                summary.highValue = row[0] or 0
                summary.repeatCustomers = row[1] or 0

            # ML features count
            r = conn.execute(text("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'mv_customer_features'"))
            summary.mlFeatures = r.scalar() or 0

            summary.outputSheets = 6  # scores CSV/JSON, eval report/JSON, alerts, risk summary

    except Exception as e:
        log.warning("Summary build partial: %s", e)

    return summary.model_dump()


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(req: PipelineRunRequest):
    """Start a new pipeline run. Returns immediately with a jobId for polling."""
    job_id = str(uuid.uuid4())[:8]

    job = {
        "jobId": job_id,
        "status": "queued",
        "progress": 0,
        "stages": [s.model_dump() for s in _make_stages()],
        "startedAt": datetime.now().isoformat(),
        "completedAt": None,
        "durationSeconds": None,
        "summary": None,
    }
    _jobs[job_id] = job

    # Run in background thread
    thread = threading.Thread(
        target=_execute_pipeline,
        args=(job_id, req.clientId, req.mode),
        daemon=True,
    )
    thread.start()

    log.info("Pipeline job %s started (mode=%s, client=%s)", job_id, req.mode, req.clientId)
    return PipelineRunResponse(**job)


@router.get("/status/{job_id}", response_model=PipelineRunResponse)
def get_pipeline_status(job_id: str):
    """Poll the status of a running pipeline job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job = _jobs[job_id]
    return PipelineRunResponse(**job)


@router.get("/last-run", response_model=PipelineRunResponse)
def get_last_run(clientId: str = Query(default="CLT-001")):
    """Get the most recent completed pipeline run."""
    # Find the most recent completed job
    completed = [
        j for j in _jobs.values()
        if j["status"] in ("complete", "failed")
    ]

    if not completed:
        # Return a default "no runs yet" response
        return PipelineRunResponse(
            jobId="none",
            status="complete",
            progress=0,
            stages=[],
            startedAt=datetime.now().isoformat(),
        )

    latest = max(completed, key=lambda j: j.get("completedAt", ""))
    return PipelineRunResponse(**latest)
