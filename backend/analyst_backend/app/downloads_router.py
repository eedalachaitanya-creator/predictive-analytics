"""
downloads_router.py — File download endpoints for ML pipeline outputs
=====================================================================
GET  /api/v1/downloads              — List all available download files
GET  /api/v1/downloads/{filename}   — Download a specific file
GET  /api/v1/downloads/zip/all      — Download all reports as a single ZIP

PRIMARY SOURCE: PostgreSQL `pipeline_outputs` table (so teammates
                who receive a pg_dump get all reports automatically).
FALLBACK:       Local disk ml/output/ (for backward compatibility).
"""

import os
import io
import zipfile
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse, Response

from app.database import engine
from db.pipeline_outputs_store import (
    list_output_files,
    get_output_file,
    ensure_table,
)

router = APIRouter(prefix="/api/v1", tags=["downloads"])
log = logging.getLogger("downloads")

# ── Disk fallback directory ────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "ml", "output")
OUTPUT_DIR = os.path.abspath(OUTPUT_DIR)


# ── Helpers ─────────────────────────────────────────────────────────────

def _human_size(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024*1024):.1f} MB"


def _get_disk_file_info(filename: str) -> dict | None:
    """Get file info from local disk (fallback)."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    if not os.path.isfile(filepath):
        return None

    stat = os.stat(filepath)
    return {
        "filename": filename,
        "title": filename,
        "icon": "📄",
        "desc": "",
        "category": "other",
        "size": _human_size(stat.st_size),
        "sizeBytes": stat.st_size,
        "lastModified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "ready": True,
        "source": "disk",
    }


# ── Endpoints ───────────────────────────────────────────────────────────

@router.get("/downloads")
def list_downloads(clientId: str = Query("CLT-001")):
    """
    List all available download files.
    Primary: reads from pipeline_outputs table in the database.
    Fallback: reads from ml/output/ directory on disk.
    """
    files = []

    # ── Try database first ──
    try:
        db_files = list_output_files(engine, clientId)
        if db_files:
            for row in db_files:
                files.append({
                    "filename": row["filename"],
                    "title": row.get("title", row["filename"]),
                    "icon": row.get("icon", "📄"),
                    "desc": row.get("description", ""),
                    "category": row.get("category", "other"),
                    "size": _human_size(row.get("file_size", 0)),
                    "sizeBytes": row.get("file_size", 0),
                    "lastModified": row["pipeline_run_at"].isoformat()
                        if row.get("pipeline_run_at") else None,
                    "ready": True,
                    "source": "database",
                })
            log.info("Listed %d files from database.", len(files))
    except Exception as e:
        log.warning("Could not read from pipeline_outputs table: %s. Falling back to disk.", e)

    # ── Fallback to disk if database had nothing ──
    if not files and os.path.isdir(OUTPUT_DIR):
        log.info("Falling back to disk for file listing.")
        for f in sorted(os.listdir(OUTPUT_DIR)):
            if f.startswith(".") or os.path.isdir(os.path.join(OUTPUT_DIR, f)):
                continue
            info = _get_disk_file_info(f)
            if info:
                files.append(info)

    # Get last pipeline run time
    last_run = None
    if files:
        modified_dates = [f["lastModified"] for f in files if f.get("lastModified")]
        if modified_dates:
            last_run = max(modified_dates)

    return {
        "files": files,
        "totalFiles": len(files),
        "lastPipelineRun": last_run,
    }


@router.get("/downloads/zip/all")
def download_all_as_zip(clientId: str = Query("CLT-001")):
    """
    Package all output files into a single ZIP and stream it.
    Primary: reads file content from database.
    Fallback: reads from disk.
    """
    zip_buffer = io.BytesIO()
    file_count = 0

    # ── Try database first ──
    try:
        db_files = list_output_files(engine, clientId)
        if db_files:
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for row in db_files:
                    full = get_output_file(engine, clientId, row["filename"])
                    if full and full.get("file_content"):
                        zf.writestr(row["filename"], full["file_content"])
                        file_count += 1
    except Exception as e:
        log.warning("Could not read files from DB for ZIP: %s. Falling back to disk.", e)
        file_count = 0

    # ── Fallback to disk ──
    if file_count == 0:
        if not os.path.isdir(OUTPUT_DIR):
            raise HTTPException(status_code=404, detail="No output files found. Run the pipeline first.")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, filenames in os.walk(OUTPUT_DIR):
                for f in filenames:
                    if f.startswith("."):
                        continue
                    filepath = os.path.join(root, f)
                    arcname = os.path.relpath(filepath, OUTPUT_DIR)
                    zf.write(filepath, arcname)
                    file_count += 1

    if file_count == 0:
        raise HTTPException(status_code=404, detail="No output files found. Run the pipeline first.")

    zip_buffer.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="CRP_ML_Reports_{timestamp}.zip"',
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


@router.get("/downloads/{filename}")
def download_file(filename: str, clientId: str = Query("CLT-001")):
    """
    Download a specific file.
    Primary: serves from database (pipeline_outputs table).
    Fallback: serves from disk (ml/output/).
    """
    safe_name = os.path.basename(filename)

    # ── Try database first ──
    try:
        row = get_output_file(engine, clientId, safe_name)
        if row and row.get("file_content"):
            content = row["file_content"]
            # Handle memoryview (psycopg2 returns BYTEA as memoryview)
            if isinstance(content, memoryview):
                content = bytes(content)

            return Response(
                content=content,
                media_type=row.get("mime_type", "application/octet-stream"),
                headers={
                    "Content-Disposition": f'attachment; filename="{safe_name}"',
                    "Access-Control-Expose-Headers": "Content-Disposition",
                },
            )
    except Exception as e:
        log.warning("Could not serve %s from DB: %s. Falling back to disk.", safe_name, e)

    # ── Fallback to disk ──
    filepath = os.path.join(OUTPUT_DIR, safe_name)
    if not os.path.isfile(filepath):
        raise HTTPException(
            status_code=404,
            detail=f"File '{safe_name}' not found. Run the ML pipeline first to generate outputs.",
        )

    ext = os.path.splitext(safe_name)[1].lower()
    media_types = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".txt": "text/plain",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
    }

    return FileResponse(
        filepath,
        media_type=media_types.get(ext, "application/octet-stream"),
        filename=safe_name,
        headers={"Access-Control-Expose-Headers": "Content-Disposition"},
    )
