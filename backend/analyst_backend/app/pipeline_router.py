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

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Header
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.audit_logger import log_audit_event
from app.auth_router import _find_user_by_token, get_current_user

log = logging.getLogger("crp_api.pipeline")

# ── Get the real DB URL (str(engine.url) masks passwords with ***) ──
_DB_URL = engine.url.render_as_string(hide_password=False)

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"], dependencies=[Depends(get_current_user)])  # audit-2026-04-29: router-level auth

# ── Paths ──
BASE_DIR = Path(__file__).parent.parent
ML_DIR = BASE_DIR / "ml"

# ── In-memory job store (production would use Redis) ──
_jobs: dict = {}

# Audit fix (2026-04-29): bumped from 300s → 900s. The 5-minute cap
# was tight for 700-customer datasets running --model-type all (XGBoost
# + RandomForest + isotonic calibration with cv=5). Configurable via
# the ML_STAGE_TIMEOUT_SECS env var so ops can override without a code
# change.
_STAGE_TIMEOUT_SECS = int(os.environ.get("ML_STAGE_TIMEOUT_SECS", "900"))

# Cap to bound _jobs growth across long uptimes. Old completed/failed
# jobs are evicted FIFO once we exceed this. 200 keeps a few weeks of
# history at typical "a handful of runs per day" cadence.
_JOBS_MAX_SIZE = 200


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
    "Validate database connection",         # 0
    "Analyze review sentiment",             # 1
    "Ingest external signals",              # 2  (STAGE_INGEST — gated by EXTERNAL_SIGNALS_SYNTHETIC)
    "Classify signal emotion",              # 3  (STAGE_EMOTION)
    "Refresh materialized view",            # 4
    "Compute RFM features",                 # 5
    "Train ML models",                      # 6
    "Evaluate models",                      # 7
    "Score customers (churn predictions)",  # 8
    "Temporal churn modeling (forward 90-day)",  # 9
    "Generate risk summary",                # 10
    "Compute purchase cycles",              # 11
    "Run refill alerts + outreach",         # 12
    "Finalize outputs",                     # 13
]

# Friendly named constants for the two new stages (indices into STAGE_LABELS above)
STAGE_INGEST = 2
STAGE_EMOTION = 3


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
    """Run a Python module as a subprocess and return (success, output).

    Audit fix 2026-04-29: when the subprocess exits non-zero, every line
    of its stderr is logged at ERROR level via the uvicorn logger.
    Previously the stderr was captured silently and only surfaced in
    the job-status JSON's stage `message` field (truncated to 200
    chars) — which the UI doesn't render as an error block. Operators
    tailing the uvicorn console got zero signal that a stage failed.
    Now the actual traceback shows up directly in the server log, with
    the `[module]` prefix so it's traceable to which subprocess.
    """
    cmd = [sys.executable, "-m", module] + (args or [])
    # Pass parent environment + DB_URL so subprocesses can connect on all OS
    env = {**os.environ, "DB_URL": _DB_URL, "DATABASE_URL": _DB_URL}
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_STAGE_TIMEOUT_SECS,  # configurable via env (default 900s)
            cwd=str(BASE_DIR),
            env=env,
        )
        if result.returncode == 0:
            return True, result.stdout[-500:] if result.stdout else "OK"

        # Subprocess failed — surface the full stderr at ERROR level so
        # it's immediately visible in the uvicorn console. Cap to last
        # 100 lines so we don't spam the log if a runaway script
        # produced megabytes of output.
        log.error(
            "Subprocess [%s] exited %d — see stderr below",
            module, result.returncode,
        )
        if result.stderr:
            for line in result.stderr.rstrip().splitlines()[-100:]:
                log.error("  [%s] %s", module, line)
        else:
            log.error("  [%s] (no stderr captured)", module)

        return False, result.stderr[-500:] if result.stderr else f"Exit code: {result.returncode}"
    except subprocess.TimeoutExpired:
        log.error("Subprocess [%s] timed out after %ds", module, _STAGE_TIMEOUT_SECS)
        return False, f"Stage timed out after {_STAGE_TIMEOUT_SECS}s"
    except Exception as e:
        log.error("Subprocess [%s] launch failed: %s", module, e)
        return False, str(e)


def _execute_pipeline(job_id: str, client_id: str, mode: str):
    """Execute the full pipeline in a background thread."""
    job = _jobs[job_id]
    job["status"] = "running"
    start_time = datetime.now()

    try:
        # ── Clear old output + model files before starting ────────────
        # Output files (csv/json/txt reports) aren't per-tenant-named, so
        # wiping them all at the start is fine — Stage 7+ rewrites them
        # for the current tenant in this run.
        #
        # Model files ARE per-tenant after the train_model.py 2026-04-27
        # rewrite (churn_model_<type>_<client_id>.joblib). Audit fix
        # (2026-04-29): scope the model cleanup to the current
        # client_id so concurrent runs / sequential per-tenant runs
        # don't destroy each other's saved models.
        output_path = ML_DIR / "output"
        if output_path.is_dir():
            for f in output_path.iterdir():
                if f.is_file() and not f.name.startswith("."):
                    f.unlink()
            log.info("Cleared old output files from %s", output_path)

        models_path = ML_DIR / "models"
        if models_path.is_dir():
            # Match per-tenant filenames first; fall back to legacy
            # un-suffixed names (churn_model_xgboost.joblib) for old
            # bundles that haven't been retrained per-tenant yet.
            patterns = [
                f"churn_model_*_{client_id}.joblib",
                f"churn_model_{client_id}.joblib",
            ]
            removed = 0
            for pat in patterns:
                for f in models_path.glob(pat):
                    if f.is_file():
                        f.unlink()
                        removed += 1
            log.info(
                "Cleared %d old model file(s) for client_id=%s "
                "(other tenants' models preserved)",
                removed, client_id,
            )

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

        # Stage 3 (STAGE_INGEST): Ingest external signals — gated by env flag so
        # synthetic ingestion never runs against real production tenants.
        if os.getenv("EXTERNAL_SIGNALS_SYNTHETIC") == "1":
            _update_stage(job, STAGE_INGEST, "running", "Ingesting external signals…")
            ok, msg = _run_python_module(
                "ml.connectors.ingest",
                ["--db-url", _DB_URL, "--client-id", client_id],
            )
            _update_stage(job, STAGE_INGEST, "done" if ok else "error", msg[:200])
        else:
            _update_stage(job, STAGE_INGEST, "skipped", "synthetic ingest disabled")

        # Stage 4 (STAGE_EMOTION): Classify emotion on unscored signal rows.
        # Non-fatal: a classifier failure marks the stage error but does NOT
        # abort the pipeline — downstream temporal features degrade gracefully
        # to zero emotion columns rather than blocking churn scoring.
        _update_stage(job, STAGE_EMOTION, "running", "Classifying signal emotion…")
        ok, msg = _run_python_module(
            "ml.emotion_classifier",
            ["--db-url", _DB_URL, "--client-id", client_id, "--update-unscored"],
        )
        _update_stage(job, STAGE_EMOTION, "done" if ok else "error", msg[:200])

        # Stage 5: Refresh materialized view
        # Audit fix (2026-04-29): the previous implementation marked the
        # stage 'error' and IMMEDIATELY overwrote it with 'done', silently
        # swallowing real refresh failures (permission denied, lock
        # conflict, schema mismatch). Now we narrow the lenient branch
        # to the only case it was meant to handle — the view truly
        # not existing yet on a brand-new DB — and let every other
        # error mark the stage as failed so downstream stages don't run
        # against stale features.
        from sqlalchemy.exc import ProgrammingError
        _update_stage(job, 4, "running", "Refreshing mv_customer_features...")
        try:
            with engine.connect() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features"))
                conn.commit()
            _update_stage(job, 4, "done", "Materialized view refreshed")
        except ProgrammingError as e:
            if 'does not exist' in str(e).lower():
                _update_stage(
                    job, 4, "done",
                    "MV does not exist yet (first-run on a fresh DB)",
                )
            else:
                _update_stage(job, 4, "error", f"View refresh failed: {str(e)[:120]}")
                job["status"] = "failed"
                return
        except Exception as e:
            _update_stage(job, 4, "error", f"View refresh failed: {str(e)[:120]}")
            job["status"] = "failed"
            return

        # Stage 6: Compute RFM features
        # --no-plots: the Downloads/report UI was removed, so the matplotlib figures
        # these ML stages used to render are never served. Skipping them saves the
        # plot rendering + the matplotlib/seaborn import in the subprocess, and drops
        # the old plots->no-plots fallback (which doubled a stage's runtime on failure).
        _update_stage(job, 5, "running", f"Computing RFM features for {client_id}...")
        ok, msg = _run_python_module("ml.compute_rfm", ["--db-url", _DB_URL, "--no-plots", "--client-id", client_id])
        _update_stage(job, 5, "done" if ok else "error", msg[:200])

        # Stage 7: Train ML models
        # --model-type all trains BOTH XGBoost and Random Forest on the
        # same feature set, then train_model.py picks the AUC-winner per
        # tenant (per-tenant filenames since the 2026-04-27 rewrite).
        # Logistic Regression was removed from train_model.py because its
        # sigmoid saturated to exactly 1.000 under leaky features —
        # see ml/train_model.py around line 280 for the rationale.
        # Without --model-type all, only XGBoost would train (the argparse
        # default) and the AUC selection degenerates to "pick the only
        # model trained."
        _update_stage(job, 6, "running", f"Training models for {client_id}...")
        ok, msg = _run_python_module("ml.train_model", ["--source", "db", "--db-url", _DB_URL, "--client-id", client_id, "--model-type", "all", "--no-plots"])
        _update_stage(job, 6, "done" if ok else "error", msg[:200])

        # Stage 8: Evaluate models
        _update_stage(job, 7, "running", "Evaluating model performance...")
        ok, msg = _run_python_module("ml.evaluate_model", ["--client-id", client_id, "--no-plots"])
        _update_stage(job, 7, "done" if ok else "error", msg[:200])

        # Stage 9: Score customers (LEGACY model → baseline churn_scores)
        _update_stage(job, 8, "running", f"Scoring {client_id} customers...")
        ok, msg = _run_python_module("ml.predict", ["--mode", "cli", "--source", "db", "--db-url", _DB_URL, "--output", "all", "--client-id", client_id])
        _update_stage(job, 8, "done" if ok else "error", msg[:200])

        # Stage 10: Temporal churn modeling (forward 90-day)
        # The legacy model above already wrote baseline scores into
        # churn_scores (Stage 9). This stage builds point-in-time snapshots,
        # trains the leakage-free temporal model, and — on success — OVERWRITES
        # churn_scores with its forward-90-day predictions (what the dashboard
        # then shows). If the tenant lacks enough history or the leakage gate
        # hard-fails, ml.temporal_pipeline falls back gracefully: it ALWAYS
        # exits 0 and prints `MODE=temporal|fallback`, leaving the baseline
        # Stage-9 scores untouched. So this stage can never fail the pipeline
        # or leave churn_scores in a broken state.
        _update_stage(job, 9, "running", f"Temporal churn modeling for {client_id}...")
        ok, msg = _run_python_module("ml.temporal_pipeline", ["--client-id", client_id, "--db-url", _DB_URL])
        if ok and "MODE=temporal" in msg:
            _update_stage(job, 9, "done", f"Temporal predictions applied — {msg.strip()[:160]}")
        elif ok:
            _update_stage(job, 9, "done", f"Baseline model kept (temporal fallback) — {msg.strip()[:160]}")
        else:
            # Unexpected non-zero exit: keep the Stage-9 baseline scores; do not
            # fail the run (the dashboard still has valid legacy predictions).
            _update_stage(job, 9, "done", f"Temporal stage skipped, baseline scores kept — {msg[:140]}")

        # Stage 11: Generate risk summary
        _update_stage(job, 10, "running", "Generating risk summary...")
        risk_file = ML_DIR / "output" / "risk_summary.txt"
        if risk_file.exists():
            _update_stage(job, 10, "done", "Risk summary available")
        else:
            _update_stage(job, 10, "done", "Risk summary generated from scores")

        # Stage 12: Compute purchase cycles
        _update_stage(job, 11, "running", "Computing purchase cycles...")
        ok, msg = _run_python_module("ml.compute_purchase_cycles", ["--db-url", _DB_URL, "--client-id", client_id])
        _update_stage(job, 11, "done" if ok else "error", msg[:200])

        # Stage 13: Run refill alerts + outreach generation
        _update_stage(job, 12, "running", "Generating refill alerts + outreach emails...")
        if mode in ("retention", "full"):
            ok, msg = _run_python_module("ml.alerts")
            _update_stage(job, 12, "done" if ok else "error", msg[:200])
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
            _update_stage(job, 12, "done", "Skipped (mode: {})".format(mode))

        # Stage 14: Finalize — save all output files to database + build summary
        _update_stage(job, 13, "running", "Finalizing outputs & storing to database...")
        try:
            from db.pipeline_outputs_store import save_all_output_files
            output_dir = str(ML_DIR / "output")
            stored = save_all_output_files(engine, client_id, output_dir)
            log.info("Stored %d output files to pipeline_outputs table", stored)
        except Exception as e:
            log.warning("Could not store outputs to DB: %s (downloads will use disk fallback)", e)

        summary = _build_summary(client_id)
        job["summary"] = summary
        _update_stage(job, 13, "done", "Pipeline complete")

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

            # High value + repeat. Audit fix (2026-04-29): high-value
            # now keys on `customer_tier = 'Platinum'` to match the
            # Dashboard's High Value tile. The previous heuristic
            # `rfm_total_score >= 12` was retired when is_high_value
            # was dropped from the MV (2026-04-25).
            r = conn.execute(text("""
                SELECT COUNT(*) FILTER (WHERE customer_tier = 'Platinum') AS hv,
                       COUNT(*) FILTER (WHERE total_orders >= 2)          AS repeat
                FROM mv_customer_features WHERE client_id = :cid
            """), {"cid": client_id})
            row = r.fetchone()
            if row:
                summary.highValue = row[0] or 0
                summary.repeatCustomers = row[1] or 0

            # ML features count.
            # We query pg_catalog (pg_class + pg_attribute) instead of
            # information_schema.columns because information_schema follows the
            # ANSI SQL standard, which doesn't recognize MATERIALIZED VIEWs
            # (a Postgres-only extension). mv_customer_features IS a materialized
            # view, so the old information_schema query always returned 0.
            #   relkind filter: 'm' = materialized view, 'r' = table, 'v' = view
            #   attnum > 0     : skip system columns (ctid, oid, etc.)
            #   NOT attisdropped: skip columns that were dropped but not VACUUMed
            r = conn.execute(text("""
                SELECT COUNT(*)
                FROM pg_attribute a
                JOIN pg_class c      ON a.attrelid = c.oid
                JOIN pg_namespace n  ON c.relnamespace = n.oid
                WHERE n.nspname = 'public'
                  AND c.relname = 'mv_customer_features'
                  AND c.relkind IN ('m', 'r', 'v')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            """))
            summary.mlFeatures = r.scalar() or 0

            summary.outputSheets = 6  # scores CSV/JSON, eval report/JSON, alerts, risk summary

    except Exception as e:
        log.warning("Summary build partial: %s", e)

    return summary.model_dump()


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(
    req: PipelineRunRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
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

    # Audit fix (2026-04-29): cap _jobs growth. Once we exceed the cap,
    # evict the oldest *completed/failed* job. Running/queued jobs are
    # never evicted so an active poll never returns 404 for an
    # in-progress run. Python dicts preserve insertion order (3.7+),
    # so iterating keys gives oldest-first.
    if len(_jobs) > _JOBS_MAX_SIZE:
        for old_id in list(_jobs.keys()):
            if _jobs[old_id]["status"] in ("complete", "failed"):
                del _jobs[old_id]
                if len(_jobs) <= _JOBS_MAX_SIZE:
                    break

    # Run in background thread
    thread = threading.Thread(
        target=_execute_pipeline,
        args=(job_id, req.clientId, req.mode),
        daemon=True,
    )
    thread.start()

    log.info("Pipeline job %s started (mode=%s, client=%s)", job_id, req.mode, req.clientId)

    # Audit: record who kicked off this job. We extract the user from the
    # bearer token if present; background/automated runs will have no token
    # and simply land in the log with user_email=None.
    caller = None
    if authorization and authorization.lower().startswith("bearer "):
        caller = _find_user_by_token(authorization.split(None, 1)[1].strip())
    log_audit_event(
        request,
        action_type="pipeline_run",
        details=f"JOB-{job_id} started · mode={req.mode}",
        client_id=req.clientId,
        user_id=caller["id"] if caller else None,
        user_email=caller["email"] if caller else None,
        outcome="success",
    )

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
