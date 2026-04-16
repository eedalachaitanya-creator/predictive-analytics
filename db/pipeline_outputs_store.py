"""
pipeline_outputs_store.py — Store/retrieve ML pipeline output files in PostgreSQL
=================================================================================
Instead of serving downloads from local disk (ml/output/), this module
stores every downloadable file as a row in the `pipeline_outputs` table.

This means:
  - Teammates who receive a pg_dump get ALL reports included
  - No need to share loose CSV/JSON/TXT files separately
  - The frontend downloads page works identically (same API)

Table schema:
    pipeline_outputs (
        id              SERIAL PRIMARY KEY,
        client_id       VARCHAR(20),
        filename        VARCHAR(255) NOT NULL,
        title           VARCHAR(255),
        icon            VARCHAR(10),
        description     TEXT,
        category        VARCHAR(50),
        mime_type       VARCHAR(100),
        file_size       INT,
        file_content    BYTEA,           -- the actual file bytes
        pipeline_run_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(client_id, filename)      -- one file per client, upsert on re-run
    )

Usage:
    from db.pipeline_outputs_store import save_output_file, get_output_file, list_output_files

    # Save a file (called at end of pipeline)
    save_output_file(engine, "CLT-001", "churn_scores.csv", content_bytes, metadata)

    # Retrieve a file (called by downloads_router)
    row = get_output_file(engine, "CLT-001", "churn_scores.csv")
    # row = { filename, title, file_content (bytes), mime_type, file_size, ... }

    # List all files (called by downloads_router)
    files = list_output_files(engine, "CLT-001")
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

log = logging.getLogger("pipeline_outputs_store")


# ── Table creation DDL ──────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_outputs (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(20) NOT NULL DEFAULT 'CLT-001',
    filename        VARCHAR(255) NOT NULL,
    title           VARCHAR(255),
    icon            VARCHAR(10) DEFAULT '📄',
    description     TEXT,
    category        VARCHAR(50) DEFAULT 'other',
    mime_type       VARCHAR(100) DEFAULT 'application/octet-stream',
    file_size       INT DEFAULT 0,
    file_content    BYTEA NOT NULL,
    pipeline_run_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_id, filename)
);
"""

# ── File metadata catalogue (same as downloads_router.py) ───────────────
FILE_CATALOGUE = {
    "churn_scores.csv": {
        "title": "Churn Prediction Scores (CSV)",
        "icon": "🔮",
        "desc": "Per-customer churn probability, risk tier (HIGH/MEDIUM/LOW), "
                "top 3 churn drivers, model version",
        "category": "predictions",
        "mime": "text/csv",
    },
    "churn_scores.json": {
        "title": "Churn Prediction Scores (JSON)",
        "icon": "🔮",
        "desc": "Same churn scores in JSON format with metadata wrapper — "
                "generated_at, total_customers, scores array",
        "category": "predictions",
        "mime": "application/json",
    },
    "risk_summary.txt": {
        "title": "Risk Summary Report",
        "icon": "⚠️",
        "desc": "Human-readable risk distribution — total customers by tier, "
                "top 10 highest-risk customers with scores",
        "category": "reports",
        "mime": "text/plain",
    },
    "feature_matrix.csv": {
        "title": "ML Feature Matrix (CSV)",
        "icon": "🧠",
        "desc": "ML-ready feature matrix used for model training — "
                "all computed features per customer, binary churn_label target",
        "category": "features",
        "mime": "text/csv",
    },
    "customer_features.csv": {
        "title": "Full Customer Features (CSV)",
        "icon": "📊",
        "desc": "Complete 52-column feature dump from materialized view — "
                "RFM scores, spend metrics, review signals, support signals",
        "category": "features",
        "mime": "text/csv",
    },
    "evaluation_report.txt": {
        "title": "Model Evaluation Report",
        "icon": "🔍",
        "desc": "AUC-ROC, precision, recall, F1 scores for all trained models — "
                "Random Forest, XGBoost, Logistic Regression comparison",
        "category": "reports",
        "mime": "text/plain",
    },
    "evaluation_summary.json": {
        "title": "Evaluation Summary (JSON)",
        "icon": "🔍",
        "desc": "Machine-readable model evaluation metrics — "
                "used by the strategist agent for model selection",
        "category": "reports",
        "mime": "application/json",
    },
    "eda_report.txt": {
        "title": "EDA Report",
        "icon": "📋",
        "desc": "Exploratory Data Analysis — data quality, distributions, "
                "null rates, feature statistics, class balance",
        "category": "reports",
        "mime": "text/plain",
    },
    "feature_importance_rfm.csv": {
        "title": "Feature Importance Rankings",
        "icon": "📈",
        "desc": "Feature importance scores from the trained model — "
                "which features drive churn predictions most",
        "category": "features",
        "mime": "text/csv",
    },
    "correlation_matrix.csv": {
        "title": "Correlation Matrix",
        "icon": "📐",
        "desc": "Pairwise correlation between all ML features — "
                "helps identify multicollinearity and feature relationships",
        "category": "features",
        "mime": "text/csv",
    },
    "tier_distribution.csv": {
        "title": "Customer Tier Distribution",
        "icon": "🏅",
        "desc": "Count of customers per RFM tier — "
                "Platinum, Gold, Silver, Bronze breakdown",
        "category": "reports",
        "mime": "text/csv",
    },
    "class_balance.csv": {
        "title": "Class Balance Report",
        "icon": "⚖️",
        "desc": "Churned vs active customer counts — "
                "class imbalance check for ML training",
        "category": "reports",
        "mime": "text/csv",
    },
}

# Default MIME types by extension
MIME_BY_EXT = {
    ".csv": "text/csv",
    ".json": "application/json",
    ".txt": "text/plain",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
}


def ensure_table(engine):
    """Create the pipeline_outputs table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
    log.info("pipeline_outputs table ready.")


def save_output_file(engine, client_id: str, filename: str, content: bytes,
                     metadata: dict = None):
    """
    Save (or update) a single output file to the database.

    Args:
        engine:    SQLAlchemy engine
        client_id: e.g. "CLT-001"
        filename:  e.g. "churn_scores.csv"
        content:   raw file bytes
        metadata:  optional dict with title, icon, desc, category, mime
    """
    meta = metadata or {}

    # Look up catalogue for defaults
    cat = FILE_CATALOGUE.get(filename, {})
    ext = os.path.splitext(filename)[1].lower()

    title = meta.get("title", cat.get("title", filename))
    icon = meta.get("icon", cat.get("icon", "📄"))
    desc = meta.get("desc", cat.get("desc", ""))
    category = meta.get("category", cat.get("category", "other"))
    mime_type = meta.get("mime", cat.get("mime", MIME_BY_EXT.get(ext, "application/octet-stream")))

    sql = text("""
        INSERT INTO pipeline_outputs
            (client_id, filename, title, icon, description, category,
             mime_type, file_size, file_content, pipeline_run_at)
        VALUES
            (:client_id, :filename, :title, :icon, :description, :category,
             :mime_type, :file_size, :file_content, NOW())
        ON CONFLICT (client_id, filename) DO UPDATE SET
            title = EXCLUDED.title,
            icon = EXCLUDED.icon,
            description = EXCLUDED.description,
            category = EXCLUDED.category,
            mime_type = EXCLUDED.mime_type,
            file_size = EXCLUDED.file_size,
            file_content = EXCLUDED.file_content,
            pipeline_run_at = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "client_id": client_id,
            "filename": filename,
            "title": title,
            "icon": icon,
            "description": desc,
            "category": category,
            "mime_type": mime_type,
            "file_size": len(content),
            "file_content": content,
        })

    log.info("  Stored %s (%s, %d bytes)", filename, category, len(content))


def save_all_output_files(engine, client_id: str, output_dir: str):
    """
    Scan the ml/output/ directory and save ALL files to the database.
    Called once at the end of the pipeline (Stage 10: Finalize).

    This is the key function — it takes every file the pipeline generated
    on disk and copies it into the pipeline_outputs table.
    """
    ensure_table(engine)

    output_path = Path(output_dir)
    if not output_path.is_dir():
        log.warning("Output directory not found: %s", output_dir)
        return 0

    count = 0
    for filepath in sorted(output_path.iterdir()):
        # Skip directories (like plots/) and hidden files
        if filepath.is_dir() or filepath.name.startswith("."):
            continue

        try:
            content = filepath.read_bytes()
            save_output_file(engine, client_id, filepath.name, content)
            count += 1
        except Exception as e:
            log.warning("  Failed to store %s: %s", filepath.name, e)

    log.info("Stored %d output files in pipeline_outputs table.", count)
    return count


def get_output_file(engine, client_id: str, filename: str) -> dict | None:
    """
    Retrieve a single file from the database.

    Returns:
        dict with keys: filename, title, icon, description, category,
                        mime_type, file_size, file_content (bytes),
                        pipeline_run_at
        or None if not found.
    """
    sql = text("""
        SELECT filename, title, icon, description, category,
               mime_type, file_size, file_content, pipeline_run_at
        FROM pipeline_outputs
        WHERE client_id = :client_id AND filename = :filename
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, {"client_id": client_id, "filename": filename}).mappings().first()

    if not row:
        return None

    return dict(row)


def list_output_files(engine, client_id: str) -> list[dict]:
    """
    List all stored output files for a client (without the actual file content).

    Returns:
        list of dicts with keys: filename, title, icon, description, category,
                                  mime_type, file_size, pipeline_run_at
    """
    ensure_table(engine)

    sql = text("""
        SELECT filename, title, icon, description, category,
               mime_type, file_size, pipeline_run_at
        FROM pipeline_outputs
        WHERE client_id = :client_id
        ORDER BY category, filename
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"client_id": client_id}).mappings().all()

    return [dict(r) for r in rows]
