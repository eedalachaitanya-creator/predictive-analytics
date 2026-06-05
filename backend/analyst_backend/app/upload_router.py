"""
upload_router.py — CSV/Excel file upload endpoints (BATCH FLOW)
===============================================================
POST   /api/v1/uploads/{masterType}  — Upload a file into the pending batch
GET    /api/v1/uploads               — List staged files in the pending batch
GET    /api/v1/uploads/batch         — Pending batch summary for a client
DELETE /api/v1/uploads/{masterType}  — Remove a file from the pending batch
POST   /api/v1/uploads/commit        — Validate + move staging → real tables
POST   /api/v1/uploads/discard       — Throw away the pending batch

HOW IT WORKS (NEW BATCH FLOW):
1. User picks a CSV/Excel file on the Upload page
2. Frontend sends it as multipart/form-data to POST /uploads/{masterType}
3. Backend reads + validates the file with pandas
4. File saved to disk (audit trail) + rows inserted into staging_<table>
   with a batch_id that groups them with other files the user uploads
5. User uploads more files — in any order, optional types can be skipped
   (re-uploading same master_type REPLACES its staging rows — last wins)
6. When done, user clicks "Commit Batch":
   - POST /uploads/commit runs pre-flight FK checks against staging + real
   - If clean, opens a transaction with SET CONSTRAINTS ALL DEFERRED
   - Moves all staging rows into real tables (DEFERRED FKs check at COMMIT)
   - Refreshes materialized view, clears staging, marks batch committed
7. If the user changes their mind: POST /uploads/discard clears the batch
"""

import os
import io
import re
import uuid
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from fastapi import APIRouter, File, Form, UploadFile, Query, HTTPException, Header, Request, Depends
from typing import Optional

from app.database import engine
from app.auth_router import _find_user_by_token, get_current_user
from app.audit_logger import log_audit_event


# Audit fix 2026-04-29 (#1): centralized tenant access check for every
# endpoint in this router. Same pattern used in chat_router. super_admin
# (clientAccess == ['*']) bypasses the per-tenant check. Anyone else
# must have the requested client_id in their clientAccess list.
def _require_client_access(user: dict, client_id: str) -> None:
    if user.get("role") == "super_admin" or "*" in (user.get("clientAccess") or []):
        return
    if client_id not in (user.get("clientAccess") or []):
        raise HTTPException(
            status_code=403,
            detail=f"You do not have access to client {client_id}",
        )


# Audit fix 2026-04-29 (#2): sanitize filenames before they hit disk.
# os.path.join doesn't reject `../` segments, so a raw UploadFile.filename
# can escape UPLOAD_DIR and write arbitrary paths. We strip everything
# that isn't a basic ASCII letter/digit/dash/underscore/dot, and then
# take basename() as belt-and-braces. Audit-relevant filenames get
# normalised to a safe slug; the original value is logged separately.
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]")


def _sanitize_filename(raw: str) -> str:
    """Return a path-traversal-safe filename derived from a user-supplied one."""
    if not raw:
        return "upload.dat"
    # basename() drops any leading path segments before the final /,
    # then we kill any remaining unsafe character.
    cleaned = _SAFE_FILENAME_RE.sub("_", os.path.basename(str(raw)))
    # Reject empty / dot-only results that would still be problematic.
    if not cleaned or cleaned in (".", ".."):
        return "upload.dat"
    return cleaned[:200]  # cap length so we can't blow path-name limits


# Audit fix 2026-04-29 (#3): cap file size at 50 MB to prevent OOM via
# pathological multi-GB uploads. The Excel files our pipeline expects
# are well under 5 MB; 50 MB is a generous ceiling. Override via env
# var if a tenant ever needs it raised.
MAX_UPLOAD_BYTES = int(os.environ.get("CRP_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))

router = APIRouter(prefix="/api/v1", tags=["uploads"])
log = logging.getLogger("upload")

# ── Valid master types ───────────────────────────────────────────────────────
VALID_MASTER_TYPES = {
    "customer", "order", "line_items",
    "product", "price", "vendor_map",
    "category", "sub_category", "sub_sub_category",
    "brand", "vendor",
    "customer_reviews", "support_tickets",
}

# Directory to save uploaded files
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploads")
try:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
except OSError:
    UPLOAD_DIR = "/tmp/crp_uploads"
    os.makedirs(UPLOAD_DIR, exist_ok=True)


# ── Master type → database table + expected columns ─────────────────────
# Maps each upload type to: (table_name, list_of_db_columns)
# The column list defines the ORDER columns should appear in the DB.
# If the CSV has different column names, we rename them to match.
MASTER_TYPE_TO_TABLE = {
    "customer": (
        "customers",
        # 16 columns — `last_login_date` was added 2026-04-24 for the
        # login-aware churn rule (migration 2026_04_24_last_login_date.sql).
        # Optional in the uploaded file; missing values land NULL.
        ["client_id", "customer_id", "customer_email", "customer_name",
         "customer_phone", "account_created_date", "registration_channel",
         "country_code", "state", "city", "zip_code", "shipping_address",
         "preferred_device", "email_opt_in", "sms_opt_in",
         "last_login_date"],
    ),
    "order": (
        "orders",
        ["client_id", "order_id", "customer_id", "order_date", "order_status",
         "order_value_usd", "discount_usd", "coupon_code", "payment_method",
         "order_item_count"],
    ),
    "line_items": (
        "line_items",
        ["client_id", "line_item_id", "order_id", "customer_id", "product_id",
         "quantity", "unit_price_usd", "final_line_total_usd",
         "item_discount_usd", "item_status"],
    ),
    "product": (
        "products",
        ["client_id", "product_id", "sku", "product_name", "category_id",
         "sub_category_id", "sub_sub_category_id", "brand_id",
         "product_price_id", "rating", "active", "not_available"],
    ),
    "price": (
        "product_prices",
        ["client_id", "price_id", "product_id", "qty_range_label", "qty_min",
         "qty_max", "unit_price_usd", "cost_price_usd"],
    ),
    "vendor_map": (
        "product_vendor_mapping",
        ["client_id", "pv_id", "product_id", "brand_id", "vendor_id"],
    ),
    "category": (
        "categories",
        ["client_id", "category_id", "category_name"],
    ),
    "sub_category": (
        "sub_categories",
        ["client_id", "sub_category_id", "sub_category_name", "category_id"],
    ),
    "sub_sub_category": (
        "sub_sub_categories",
        ["client_id", "sub_sub_category_id", "sub_sub_category_name",
         "sub_category_id", "category_id"],
    ),
    "brand": (
        "brands",
        ["client_id", "brand_id", "brand_name", "brand_description",
         "vendor_id", "active", "not_available", "category_hint"],
    ),
    "vendor": (
        "vendors",
        ["client_id", "vendor_id", "vendor_name", "vendor_description",
         "vendor_contact_no", "vendor_address", "vendor_email"],
    ),
    "customer_reviews": (
        "customer_reviews",
        ["client_id", "review_id", "customer_id", "product_id", "order_id",
         "rating", "review_text", "review_date", "sentiment"],
    ),
    "support_tickets": (
        "support_tickets",
        ["client_id", "ticket_id", "customer_id", "ticket_type", "priority",
         "status", "channel", "opened_date", "resolved_date", "resolution_time_hrs"],
    ),
}

# ── Required-column rules per master type ────────────────────────────────────
# Columns that MUST carry at least one populated value in the uploaded file.
# `client_id` is intentionally omitted because the upload pipeline injects it
# at staging time — it's never present in the customer's Excel/CSV.
#
# This is the source of truth for the "file has rows but every required
# field is blank" guard. Keeping it here (not importing validation_router's
# VALIDATION_CONFIG) avoids a cross-module cycle and lets the two checkers
# evolve independently.
REQUIRED_COLS_PER_MASTER = {
    # last_login_date added 2026-04-24 — required because it drives the
    # login-aware churn label. Uploading a customer master with this
    # column entirely blank gets rejected at staging time, matching
    # validation_router's required_cols list.
    "customer":         ["customer_id", "customer_email", "customer_name", "account_created_date", "last_login_date"],
    "order":            ["order_id", "customer_id", "order_date", "order_value_usd"],
    "line_items":       ["line_item_id", "order_id", "customer_id", "product_id", "quantity", "unit_price_usd"],
    "product":          ["product_id", "product_name", "category_id"],
    "price":            ["price_id", "product_id", "unit_price_usd"],
    "vendor_map":       ["pv_id", "product_id", "vendor_id"],
    "category":         ["category_id", "category_name"],
    "sub_category":     ["sub_category_id", "sub_category_name", "category_id"],
    "sub_sub_category": ["sub_sub_category_id", "sub_sub_category_name", "sub_category_id"],
    "brand":            ["brand_id", "brand_name"],
    "vendor":           ["vendor_id", "vendor_name"],
    "customer_reviews": ["review_id", "customer_id", "product_id", "rating"],
    "support_tickets":  ["ticket_id", "customer_id", "ticket_type"],
}


def _assert_required_columns_present(df: pd.DataFrame, master_type: str, filename: str) -> None:
    """Reject files where any REQUIRED column is missing from the header row.

    Why this exists separately from _assert_file_has_real_data:
        Previously the only schema check between upload and staging was the
        "all matching columns are blank" guard, which bails only when EVERY
        required column happens to be present-but-empty. A file missing a
        single required column (e.g. customer master without last_login_date)
        passed this guard and then silently landed NULL in the DB, leaking
        through to the model. (Reported 2026-04-24 — last_login_date became
        required for the login-aware churn rule but uploads without it kept
        succeeding.)

        This new check runs BEFORE the blank-data guard and rejects with a
        specific, actionable message naming the missing column(s).

    Normalization matches _load_df_to_staging's column-name handling
    (strip + lower + space-to-underscore) so a user's "Last Login Date"
    column header is correctly recognized as last_login_date.
    """
    required = REQUIRED_COLS_PER_MASTER.get(master_type, [])
    if not required:
        return  # master type has no required cols; nothing to assert

    file_cols_normalized = {
        str(c).strip().lower().replace(" ", "_") for c in df.columns
    }
    missing = [c for c in required if c not in file_cols_normalized]

    if missing:
        # 2026-04-27: keep the detailed missing-column list in the SERVER
        # LOG (so we can debug from the backend console), but show the
        # SAME generic message to the user as the "wrong file" path
        # below. Two different rejection messages were confusing — users
        # didn't know whether they uploaded the wrong file vs the right
        # file with bad columns. Both cases mean the same thing from
        # their perspective: "this file doesn't match what we asked for."
        log.warning(
            "Upload rejected for '%s' — missing required column(s): %s | "
            "File had: %s",
            master_type, missing, sorted(file_cols_normalized),
        )
        raise HTTPException(
            status_code=400,
            detail="Wrong file uploaded. Please check you uploaded the correct file.",
        )


def _assert_file_has_real_data(df: pd.DataFrame, master_type: str, filename: str) -> None:
    """Reject files that have rows but every required column is 100% blank.

    `df.empty` already covers the "zero rows" case upstream; this catches the
    second-degree empty file — e.g. a template Excel where every data row is
    just placeholders or whitespace. Treats NaN AND empty / whitespace-only
    strings as blank.

    Assumes _assert_required_columns_present has already run, so any required
    column is guaranteed to be in df.columns by this point.
    """
    required = REQUIRED_COLS_PER_MASTER.get(master_type, [])
    if not required:
        return  # master type has no required cols; nothing to assert

    # Build the list of required columns that are actually present in the
    # uploaded file. After _assert_required_columns_present runs upstream,
    # this list equals `required`; the comprehension is kept defensively in
    # case a future caller skips the presence check.
    present = [c for c in required if c in df.columns]
    if not present:
        return  # schema mismatch is a different error class

    for col in present:
        series = df[col]
        # .notna() catches NaN / NaT; the string cast + strip filter catches
        # literal "" and whitespace-only cells that pandas parsed as strings.
        has_value = series.notna() & (series.astype(str).str.strip() != "")
        if has_value.any():
            return  # at least one real value found — file is fine

    raise HTTPException(
        status_code=400,
        detail=(
            f"{filename} has rows but every required column is blank "
            f"({', '.join(present)}). Please upload a file with actual data."
        ),
    )


# Tables that affect the materialized view — refresh after commit if these were loaded
MV_TRIGGER_TABLES = {"customers", "orders", "line_items", "products",
                     "categories", "brands", "product_prices",
                     "customer_reviews", "support_tickets"}


# ── Conflict behavior per master type ────────────────────────────────────────
# When committing staging → real, a row's primary key may already exist (user
# re-uploading the same file, or a prior commit that was partially re-applied).
# Without explicit ON CONFLICT handling every such row raises UniqueViolation
# and rolls back the entire batch — which is brittle and makes retries
# impossible.
#
# Strategy:
#   - Catalog / dimension tables → UPSERT ('update'): re-uploads refresh the
#     non-PK columns, so corrected data overwrites old.
#   - Transactional / fact tables → SKIP ('nothing'): we never silently
#     overwrite a historical order, line_item, review, or ticket. A duplicate
#     PK is simply ignored and the rest of the batch goes through.
#
# Format: master_type → (pk_columns_tuple, action)
COMMIT_CONFLICT = {
    # Dimension / catalog (UPSERT — safe to refresh)
    "category":         (("client_id", "category_id"),         "update"),
    "sub_category":     (("client_id", "sub_category_id"),     "update"),
    "sub_sub_category": (("client_id", "sub_sub_category_id"), "update"),
    "brand":            (("client_id", "brand_id"),            "update"),
    "vendor":           (("client_id", "vendor_id"),           "update"),
    "product":          (("client_id", "product_id"),          "update"),
    "price":            (("client_id", "price_id"),            "update"),
    "vendor_map":       (("client_id", "pv_id"),               "update"),
    "customer":         (("client_id", "customer_id"),         "update"),
    # Fact / transactional (DO NOTHING — don't overwrite history)
    "order":            (("client_id", "order_id"),            "nothing"),
    "line_items":       (("client_id", "line_item_id"),        "nothing"),
    "customer_reviews": (("client_id", "review_id"),           "nothing"),
    "support_tickets":  (("client_id", "ticket_id"),           "nothing"),
}


# ── Commit order ─────────────────────────────────────────────────────────────
# When committing a batch, we INSERT from staging → real in this order.
# With DEFERRABLE FKs, order doesn't affect correctness (all checks happen at
# COMMIT), but parents-before-children is still more efficient.
COMMIT_ORDER = [
    "category", "vendor",
    "sub_category", "brand",
    "sub_sub_category",
    "product",
    "price", "vendor_map",
    "customer",
    "order",
    "line_items",
    "customer_reviews", "support_tickets",
]


# ── Pre-flight FK validation rules ───────────────────────────────────────────
# Each tuple: (staging_table, staging_col, parent_real_table, parent_real_col,
#              parent_staging_table, fk_name)
# For every staging row where staging_col IS NOT NULL, the referenced key
# must exist EITHER in the parent's real table OR in the parent's staging
# table for the same client_id and batch_id.
CATALOG_FK_CHECKS = [
    ("staging_sub_categories",          "category_id",         "categories",          "category_id",         "staging_categories",         "sub_categories_category_fk"),
    ("staging_sub_sub_categories",      "sub_category_id",     "sub_categories",      "sub_category_id",     "staging_sub_categories",     "sub_sub_categories_sub_category_fk"),
    ("staging_sub_sub_categories",      "category_id",         "categories",          "category_id",         "staging_categories",         "sub_sub_categories_category_fk"),
    ("staging_brands",                  "vendor_id",           "vendors",             "vendor_id",           "staging_vendors",            "brands_vendor_fk"),
    ("staging_products",                "category_id",         "categories",          "category_id",         "staging_categories",         "products_category_fk"),
    ("staging_products",                "sub_category_id",     "sub_categories",      "sub_category_id",     "staging_sub_categories",     "products_sub_category_fk"),
    ("staging_products",                "sub_sub_category_id", "sub_sub_categories",  "sub_sub_category_id", "staging_sub_sub_categories", "products_sub_sub_category_fk"),
    ("staging_products",                "brand_id",            "brands",              "brand_id",            "staging_brands",             "products_brand_fk"),
    ("staging_products",                "product_price_id",    "product_prices",      "price_id",            "staging_product_prices",     "fk_products_price"),
    ("staging_product_prices",          "product_id",          "products",            "product_id",          "staging_products",           "product_prices_product_fk"),
    ("staging_product_vendor_mapping",  "product_id",          "products",            "product_id",          "staging_products",           "pvm_product_fk"),
    ("staging_product_vendor_mapping",  "brand_id",            "brands",              "brand_id",            "staging_brands",             "pvm_brand_fk"),
    ("staging_product_vendor_mapping",  "vendor_id",           "vendors",             "vendor_id",           "staging_vendors",            "pvm_vendor_fk"),
    ("staging_line_items",              "product_id",          "products",            "product_id",          "staging_products",           "line_items_product_fk"),
    ("staging_customer_reviews",        "product_id",          "products",            "product_id",          "staging_products",           "customer_reviews_product_fk"),
]


# ── Batch tracking helpers ────────────────────────────────────────────────────
def _get_pending_batch_id(conn, client_id: str) -> Optional[str]:
    """Return UUID string of the client's pending batch, or None if none exists."""
    result = conn.execute(
        text("SELECT batch_id::text FROM upload_batches "
             "WHERE client_id = :cid AND status = 'pending'"),
        {"cid": client_id},
    ).fetchone()
    return result[0] if result else None


def _get_or_create_pending_batch(conn, client_id: str) -> str:
    """Return pending batch UUID, creating one if none exists."""
    existing = _get_pending_batch_id(conn, client_id)
    if existing:
        return existing

    new_id = str(uuid.uuid4())
    conn.execute(
        text("INSERT INTO upload_batches (batch_id, client_id) VALUES (:bid, :cid)"),
        {"bid": new_id, "cid": client_id},
    )
    log.info("Created new pending batch %s for client %s", new_id, client_id)
    return new_id


def _staging_table_for(master_type: str) -> str:
    """Return the staging table name for a given master type (e.g. 'customer' → 'staging_customers')."""
    real_table, _ = MASTER_TYPE_TO_TABLE[master_type]
    return f"staging_{real_table}"


def _count_batch_staging_rows(conn, batch_id: str, client_id: str) -> int:
    """
    Return the total number of staging rows for a batch across ALL staging
    tables. Used to detect orphaned pending batches (batch_id exists but every
    staging table is empty), which can happen when a user uploads a file then
    hits Remove — the staging rows go away but the upload_batches row stays
    with status='pending'.
    """
    total = 0
    for master_type in COMMIT_ORDER:
        real_table, _ = MASTER_TYPE_TO_TABLE[master_type]
        staging_table = f"staging_{real_table}"
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM {staging_table} "
                 f"WHERE batch_id = :bid AND client_id = :cid"),
            {"bid": batch_id, "cid": client_id},
        ).fetchone()
        total += (row[0] if row else 0)
    return total


def _cleanup_empty_batch(conn, batch_id: str, client_id: str) -> bool:
    """
    If `batch_id` has zero staging rows across every staging table, delete
    the upload_batches row so a fresh upload starts with a fresh batch.
    Returns True if the batch was cleaned up, False if it still has rows.
    """
    remaining = _count_batch_staging_rows(conn, batch_id, client_id)
    if remaining == 0:
        conn.execute(
            text("DELETE FROM upload_batches WHERE batch_id = :bid AND status = 'pending'"),
            {"bid": batch_id},
        )
        log.info("Cleaned up empty pending batch %s for client %s", batch_id, client_id)
        return True
    return False


def _humanize_staging_error(exc: Exception, master_type: str = "") -> str:
    """Translate a raw DB staging exception into a SHORT, user-friendly message.

    A staging INSERT failure used to surface the full SQLAlchemy exception — the
    SQL statement plus every bound parameter set — straight into the UI, which is
    intimidating and not actionable. The caller already logs the full error
    (exc_info=True); here we return only a friendly, actionable sentence and NEVER
    leak the SQL or parameters. Covers the common Postgres staging failures and
    falls back to a generic "wrong file" message.
    """
    import re

    orig = getattr(exc, "orig", None) or exc            # unwrap SQLAlchemy → psycopg2
    name = type(orig).__name__
    pgcode = getattr(orig, "pgcode", "") or ""
    detail = str(orig)
    m = re.search(r'column "([^"]+)"', detail)
    col = f" (column '{m.group(1)}')" if m else ""
    label = f" for {master_type.replace('_', ' ').title()}" if master_type else ""

    if name in ("DatatypeMismatch", "InvalidTextRepresentation") or pgcode in ("42804", "22P02"):
        return (f"Wrong file or wrong format{label}{col}: a value doesn't match the expected "
                "type — for example text or true/false where a number is expected. Please "
                "check you uploaded the correct file for this slot.")
    if name == "UniqueViolation" or pgcode == "23505":
        return "Some rows were already uploaded (duplicate IDs); the existing data was kept."
    if name == "ForeignKeyViolation" or pgcode == "23503":
        return ("This file references IDs that don't exist yet — upload the parent files first "
                "(for example categories, brands and vendors before products).")
    if name == "NotNullViolation" or pgcode == "23502":
        return f"A required column{col} is empty in one or more rows. Please fill it in and retry."
    if name in ("NumericValueOutOfRange", "StringDataRightTruncation") or pgcode in ("22003", "22001"):
        return f"A value{col} is too large or too long for its column. Please check the file."
    if name == "UndefinedColumn" or pgcode == "42703":
        return ("The file has a column that doesn't belong in this slot. Please check you "
                "uploaded the correct file.")
    return ("Wrong file uploaded, or the file has an unexpected format. Please check you "
            "uploaded the correct file for this slot.")


def _load_df_to_staging(df: pd.DataFrame, master_type: str, client_id: str) -> dict:
    """
    Insert a pandas DataFrame into the STAGING table for this master type.

    REPLACE SEMANTICS: if the same master_type was already uploaded in the
    current pending batch, those staging rows are deleted first. Last
    upload wins — so the user can fix a typo and re-upload.

    The real tables are NOT touched here. Data moves from staging to real
    only when the user calls POST /uploads/commit.

    Returns: {staged, batch_id, staging_table, rows_staged} on success,
             {staged: False, reason} on failure.
    """
    if master_type not in MASTER_TYPE_TO_TABLE:
        return {"staged": False, "reason": f"No table mapping for {master_type}"}

    real_table, expected_cols = MASTER_TYPE_TO_TABLE[master_type]
    staging_table = f"staging_{real_table}"

    # ── Normalize column names in the uploaded DataFrame ────────────────
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Always stamp client_id from the authenticated form field — NEVER trust
    # whatever client_id the CSV was saved with.
    #
    # Why: this is a multi-tenant system. A client admin (say, Sams Club) might
    # re-use a CSV that was originally exported with client_id='CLT-001'
    # (Walmart) baked into every row. The old code only injected client_id if
    # the column was missing; if it was present with CLT-001, the rows went to
    # staging with client_id='CLT-001' but batch_id for CLT-004. The downstream
    # DELETE / preflight / INSERT all filter by WHERE client_id = :cid so
    # nothing actually got committed — the upload silently did nothing.
    #
    # The fix: if the expected schema has client_id at all, overwrite whatever
    # the file had. The form/token is the source of truth for tenancy.
    if "client_id" in expected_cols:
        if "client_id" in df.columns:
            mismatched = (df["client_id"].astype(str) != str(client_id)).sum()
            if mismatched:
                log.warning(
                    "Overriding client_id in %d/%d uploaded rows "
                    "(file had a different client_id than target %s)",
                    int(mismatched), len(df), client_id,
                )
            df["client_id"] = client_id
        else:
            df.insert(0, "client_id", client_id)

    # Keep only columns that exist in both the DataFrame and the expected list
    available_cols = [c for c in expected_cols if c in df.columns]
    log.info(
        "Staging prep: %s → %s | expected %d cols, found %d matching | df cols: %s",
        master_type, staging_table, len(expected_cols), len(available_cols), list(df.columns)[:5]
    )
    if not available_cols:
        return {
            "staged": False,
            "reason": f"No matching columns. Expected: {expected_cols}, got: {list(df.columns)}",
        }

    df_to_load = df[available_cols].copy()

    # ── Drop rows where the business PK is null ─────────────────────────
    # Use the first non-client_id column as the PK for null checking.
    # (Previous code used available_cols[0] which was always 'client_id'
    #  when client_id is in expected_cols — making the dropna a no-op.)
    pk_col = next((c for c in available_cols if c != "client_id"), available_cols[0])
    df_to_load = df_to_load.dropna(subset=[pk_col])

    if df_to_load.empty:
        return {"staged": False, "reason": f"All rows had null {pk_col}"}

    # ── Clean up data types before insert ─────────────────────────────
    # Boolean columns: convert NaN/float to proper True/False/None
    # This fixes: "column email_opt_in is boolean but expression is text"
    for col in df_to_load.columns:
        if df_to_load[col].dtype == 'float64':
            unique_vals = set(df_to_load[col].dropna().unique())
            if unique_vals <= {0.0, 1.0, True, False}:
                df_to_load[col] = df_to_load[col].apply(
                    lambda x: None if pd.isna(x) else bool(x)
                )

    # ── Build the INSERT statement targeting the staging table ──────────
    # Staging tables have the same columns as real tables PLUS batch_id
    # and staging_row_id (BIGSERIAL, auto-filled). We only need to supply
    # the real columns + batch_id; staging_row_id is generated by Postgres.
    cols_with_batch = available_cols + ["batch_id"]
    col_list = ", ".join(cols_with_batch)
    placeholders = ", ".join([f":{c}" for c in cols_with_batch])
    insert_sql = text(
        f"INSERT INTO {staging_table} ({col_list}) VALUES ({placeholders})"
    )

    rows_staged = 0

    try:
        # engine.begin() wraps everything in a single transaction — either
        # ALL rows land in staging, or none do (no half-uploads).
        with engine.begin() as conn:
            # 1. Get or create the client's pending batch
            batch_id = _get_or_create_pending_batch(conn, client_id)

            # 2. REPLACE: wipe any existing rows for this batch + master_type
            #    so re-uploads overwrite instead of accumulating duplicates.
            delete_result = conn.execute(
                text(f"DELETE FROM {staging_table} "
                     f"WHERE batch_id = :bid AND client_id = :cid"),
                {"bid": batch_id, "cid": client_id},
            )
            rows_replaced = delete_result.rowcount

            # 3. Convert DataFrame to records, clean NaN → None, stamp batch_id
            records = []
            for record in df_to_load.to_dict("records"):
                clean = {}
                for k, v in record.items():
                    if isinstance(v, float) and pd.isna(v):
                        clean[k] = None
                    elif v is None:
                        clean[k] = None
                    else:
                        clean[k] = v
                clean["batch_id"] = batch_id
                records.append(clean)

            # 4. Insert in chunks of 500 rows to keep memory/round-trips reasonable
            batch_size = 500
            for i in range(0, len(records), batch_size):
                chunk = records[i : i + batch_size]
                result = conn.execute(insert_sql, chunk)
                rows_staged += result.rowcount

        log.info(
            "Staging load OK: %s → %s | batch=%s | %d rows staged (replaced %d)",
            master_type, staging_table, batch_id, rows_staged, rows_replaced,
        )

        return {
            "staged": True,
            "staging_table": staging_table,
            "batch_id": batch_id,
            "rows_staged": rows_staged,
            "rows_replaced": rows_replaced,
        }

    except Exception as e:
        # Full technical error (SQL + bound params) goes to the SERVER LOG only.
        log.error("Staging load FAILED for %s: %s", staging_table, e, exc_info=True)
        # The UI gets a short, actionable message — never the raw psycopg2 dump.
        return {"staged": False, "reason": _humanize_staging_error(e, master_type)}


def _is_decorative_row(df: pd.DataFrame) -> bool:
    """
    Check if a DataFrame's column headers look like a decorative/title row
    rather than actual data column names.

    A decorative row typically has:
    - Most columns named "Unnamed: X" (because they were empty in Excel)
    - At least one column with a long string (>30 chars) or containing
      pipe "|" characters (used for decorative descriptions/separators)
    """
    unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed"))
    has_title_col = any(
        len(str(c)) > 30 or "|" in str(c)
        for c in df.columns
    )
    return has_title_col and unnamed_count >= len(df.columns) // 2


def _read_file_to_df(file_bytes: bytes, filename: str, master_type: str = None) -> pd.DataFrame:
    """
    Read CSV or Excel file bytes into a pandas DataFrame.

    SMART HEADER DETECTION (up to 5 rows):
    ──────────────────────────────────────
    Some Excel files have decorative title/description rows before the
    actual column headers. For example:

        Row 1: "👤 Customer Master | 100 rows | ..."     ← decorative
        Row 2: "client_id", "customer_id", ...            ← real headers
        Row 3: "CLT-002", "CUST-001", ...                 ← data

    Or even TWO decorative rows (like the price master):

        Row 1: "💲 Product Price Master | ..."            ← decorative
        Row 2: "qty_min / qty_max are unit counts | ..."  ← also decorative
        Row 3: "client_id", "price_id", "product_id", ... ← real headers
        Row 4: data

    We scan up to 5 rows looking for the first non-decorative row.
    If none found within 5 rows, we reject the file with an error.

    COLUMN VALIDATION:
    After finding headers, we check if ANY of the expected DB columns
    match the file's columns. If zero match (excluding auto-injected
    client_id), the file is wrong for this master type → error.
    """
    lower = filename.lower()
    if lower.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
    elif lower.endswith((".xlsx", ".xls")):
        # ── Scan up to 5 rows to find real column headers ──────────
        header_row = 0
        found_headers = False

        for try_header in range(5):
            try:
                test_df = pd.read_excel(io.BytesIO(file_bytes), header=try_header)
            except Exception:
                break  # ran out of rows

            if test_df.empty and try_header > 0:
                break  # no more data rows

            if _is_decorative_row(test_df):
                header_row = try_header + 1
                log.info("Row %d in %s is decorative — skipping", try_header + 1, filename)
                continue
            else:
                found_headers = True
                break

        if not found_headers and header_row >= 5:
            raise HTTPException(
                status_code=400,
                detail=f"Could not find column headers in the first 5 rows of {filename}. "
                       f"Please make sure the file has proper column headers "
                       f"(e.g., 'vendor_id', 'vendor_name', etc.).",
            )

        if header_row > 0:
            log.info("Detected title row in %s — using row %d as headers", filename, header_row + 1)

        df = pd.read_excel(io.BytesIO(file_bytes), header=header_row)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {filename}. Use .csv, .xlsx, or .xls",
        )

    # ── Column validation: check that at least some columns match ──────
    if master_type and master_type in MASTER_TYPE_TO_TABLE:
        _, expected_cols = MASTER_TYPE_TO_TABLE[master_type]
        # Normalize file columns the same way _load_df_to_staging does
        normalized_cols = [c.strip().lower().replace(" ", "_") for c in df.columns]
        # Don't count client_id — it gets auto-injected anyway
        expected_without_cid = [c for c in expected_cols if c != "client_id"]
        matching = [c for c in expected_without_cid if c in normalized_cols]

        if len(matching) == 0:
            # User-facing message stays short and friendly — the wall of
            # column names that used to appear here was intimidating and
            # not actionable for the typical client user. Detailed
            # expected-vs-found is logged server-side so devs can still
            # diagnose mis-uploads from the logs without exposing internal
            # column names in the UI. (2026-04-24)
            log.warning(
                "Wrong-file upload rejected for '%s'. Expected: %s | Found: %s",
                master_type, expected_without_cid, normalized_cols,
            )
            raise HTTPException(
                status_code=400,
                detail="Wrong file uploaded. Please check you uploaded the correct file.",
            )
        log.info("Column validation for %s: %d/%d expected columns matched",
                 master_type, len(matching), len(expected_without_cid))

    return df


# ── Commit / discard endpoints (declared BEFORE the /{master_type} wildcard) ──
# FastAPI matches routes in declaration order. If /uploads/{master_type} were
# declared first, a POST to /uploads/commit would be routed there with
# master_type="commit" and get rejected. Declaring literals first fixes that.

@router.post("/uploads/commit")
def commit_batch(
    request: Request,
    clientId: str = Query(...),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Validate + commit the pending batch to real tables.

    Two phases:
      1. Pre-flight FK check (reads staging + real, fails fast with details)
      2. Transactional commit with DEFERRABLE FKs (atomic — all or nothing)

    If anything fails, staging data is preserved and the user can fix and
    retry. On success, staging is cleared and the batch is marked committed.
    """
    _require_client_access(user, clientId)
    with engine.begin() as conn:
        batch_id = _get_pending_batch_id(conn, clientId)
        if not batch_id:
            raise HTTPException(
                status_code=400,
                detail="No pending batch to commit. Upload at least one file first.",
            )

        # Phase 1: FK validation
        violations = _preflight_fk_check(conn, batch_id, clientId)
        if violations:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "FK validation failed. Fix these missing parents and retry commit.",
                    "violations": violations,
                },
            )

        # Phase 2: deferred-FK transactional insert
        # engine.begin() above already started a transaction.
        conn.execute(text("SET CONSTRAINTS ALL DEFERRED"))
        result = _commit_batch(conn, batch_id, clientId)

    # Refresh the materialized view OUTSIDE the commit transaction — it's
    # a heavy operation and shouldn't hold the commit lock longer than needed.
    if result["mvNeedsRefresh"]:
        try:
            with engine.begin() as mv_conn:
                mv_conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features"))
                log.info("Refreshed mv_customer_features after commit of batch %s", batch_id)
        except Exception as e:
            log.error("MV refresh after commit failed: %s", e, exc_info=True)
            # Don't fail the whole commit — MV can be refreshed manually.
            result["mvRefreshWarning"] = str(e)

    # Audit fix #4: record the successful commit so the audit log can
    # reconstruct who promoted which batch into the real tables.
    total_rows = sum(result["perTypeRows"].values())
    log_audit_event(
        request,
        action_type="batch_commit",
        details=(
            f"batch={batch_id} · {total_rows:,} rows committed across "
            f"{len([k for k, v in result['perTypeRows'].items() if v > 0])} table(s)"
        ),
        client_id=clientId,
        user_id=user.get("id"),
        user_email=user.get("email"),
        outcome="success",
    )

    return {
        "committed": True,
        "batchId": batch_id,
        "rowsCommitted": result["perTypeRows"],
        "mvRefreshed": result["mvNeedsRefresh"],
    }


@router.post("/uploads/discard")
def discard_batch(
    request: Request,
    clientId: str = Query(...),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Throw away the pending batch. Deletes all its staging rows and marks
    the batch 'discarded'. Uploaded files on disk are also deleted.
    """
    _require_client_access(user, clientId)
    with engine.begin() as conn:
        batch_id = _get_pending_batch_id(conn, clientId)
        if not batch_id:
            return {"discarded": False, "reason": "No pending batch"}

        total_deleted = 0
        for master_type in COMMIT_ORDER:
            real_table, _ = MASTER_TYPE_TO_TABLE[master_type]
            staging_table = f"staging_{real_table}"
            result = conn.execute(
                text(f"DELETE FROM {staging_table} WHERE batch_id = :bid"),
                {"bid": batch_id},
            )
            total_deleted += result.rowcount

        conn.execute(
            text("UPDATE upload_batches SET status = 'discarded', "
                 "committed_at = NOW() WHERE batch_id = :bid"),
            {"bid": batch_id},
        )

    # Best-effort: remove this client's uploaded files from disk
    try:
        for f in os.listdir(UPLOAD_DIR):
            if f.startswith(f"{clientId}_"):
                os.remove(os.path.join(UPLOAD_DIR, f))
    except Exception:
        pass

    # Audit fix #4: every batch discard leaves a tamper-resistant trail.
    log_audit_event(
        request,
        action_type="batch_discard",
        details=f"batch={batch_id} · {total_deleted:,} staging rows deleted",
        client_id=clientId,
        user_id=user.get("id"),
        user_email=user.get("email"),
        outcome="success",
    )

    return {
        "discarded": True,
        "batchId": batch_id,
        "rowsDeleted": total_deleted,
    }


@router.post("/uploads/{master_type}")
async def upload_file(
    master_type: str,
    request: Request,
    file: UploadFile = File(...),
    clientId: str = Form(None),             # NOW OPTIONAL — auto-detected from token
    masterType: str = Form(None),
    authorization: Optional[str] = Header(default=None),
    user: dict = Depends(get_current_user),  # Audit fix #1 — require auth
):
    """
    Upload a CSV or Excel file for a specific master type.

    HOW client_id AUTO-DETECTION WORKS:
    ───────────────────────────────────
    1. If clientId is provided in the form → use it (backward compatible)
    2. If NOT provided → look at the logged-in user's token
       a. Get user's clientAccess list (e.g., ["CLT-002"])
       b. If user has access to exactly ONE client → use that automatically
       c. If user has access to MULTIPLE clients → require clientId in form
       d. If user is super_admin with "*" access → require clientId in form

    This means: a Costco user just uploads their Excel file.
    The system knows they're Costco (CLT-002) and tags every row automatically.
    They NEVER need to add a client_id column to their Excel sheets!
    """
    # Validate master type
    if master_type not in VALID_MASTER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown master type: {master_type}. "
                   f"Valid types: {sorted(VALID_MASTER_TYPES)}",
        )

    # ── Auto-detect client_id if not provided ─────────────────────────
    # `user` is now provided by Depends(get_current_user) above (audit #1
    # — auth was previously skipped when clientId was supplied in the
    # form, so anyone could upload to any tenant).
    if not clientId:
        client_access = user.get("clientAccess", [])

        if "*" in client_access:
            # Super admins must specify which client they're uploading for
            raise HTTPException(
                status_code=400,
                detail="You have access to all clients. Please specify clientId in the upload form.",
            )
        elif len(client_access) == 1:
            # User has access to exactly one client → auto-detect!
            clientId = client_access[0]
            log.info("Auto-detected client_id=%s from user %s", clientId, user.get("email"))
        elif len(client_access) > 1:
            # User has access to multiple clients → they need to pick one
            raise HTTPException(
                status_code=400,
                detail=f"You have access to multiple clients: {client_access}. "
                       f"Please specify clientId in the upload form.",
            )
        else:
            raise HTTPException(status_code=403, detail="You don't have access to any clients")
    else:
        # Caller supplied a clientId — verify they actually have access
        # to it (audit #1 — previously trusted the form value blindly).
        _require_client_access(user, clientId)

    # ── File size guard (audit fix #3) ────────────────────────────────
    # Reject before reading the body into memory. Some clients send
    # Content-Length; FastAPI exposes it as file.size when set.
    declared_size = getattr(file, "size", None)
    if declared_size is not None and declared_size > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large: {declared_size:,} bytes exceeds the "
                   f"{MAX_UPLOAD_BYTES:,}-byte cap. Override with the "
                   f"CRP_MAX_UPLOAD_BYTES env var if your dataset legitimately "
                   f"needs more.",
        )

    # Read the file. Even if the client lied about Content-Length, we
    # check the actual buffered size again before staging.
    file_bytes = await file.read()
    file_size = len(file_bytes)
    if file_size > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large: {file_size:,} bytes exceeds the "
                   f"{MAX_UPLOAD_BYTES:,}-byte cap.",
        )
    # Audit fix #2: sanitize the user-supplied filename before we do
    # ANYTHING with it on disk. Path traversal via `../` would otherwise
    # let a malicious upload write outside UPLOAD_DIR.
    raw_filename = file.filename or f"{master_type}.csv"
    filename = _sanitize_filename(raw_filename)
    if filename != raw_filename:
        log.info(
            "Sanitized upload filename %r → %r (client=%s, master_type=%s)",
            raw_filename, filename, clientId, master_type,
        )

    # Validate by reading with pandas
    try:
        df = _read_file_to_df(file_bytes, filename, master_type=master_type)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=400, detail="File is empty — no rows found")

    # First-line schema check: every required column must be in the header
    # row. Catches "right file type but missing a column" — e.g. customer
    # master uploaded without last_login_date. Raises 400 with the missing
    # column name(s) if so. Must run BEFORE _assert_file_has_real_data,
    # which assumes required columns exist in df.columns.
    _assert_required_columns_present(df, master_type, filename)

    # Guard against files that have rows but every required column is blank
    # (e.g. template Excel where every cell is a placeholder). Raises 400 if so.
    _assert_file_has_real_data(df, master_type, filename)

    row_count = len(df)
    columns = list(df.columns)

    # Save file to disk
    try:
        safe_filename = f"{clientId}_{master_type}_{filename}"
        save_path = os.path.join(UPLOAD_DIR, safe_filename)
        with open(save_path, "wb") as f:
            f.write(file_bytes)
        log.info(f"Saved {safe_filename} ({row_count} rows, {len(columns)} columns)")
    except Exception as e:
        log.warning(f"Could not save file to disk: {e}")

    # ── LOAD into staging (NOT real tables yet) ───────────────────────
    # The rows land in staging_<table> with a batch_id. They move into the
    # real tables only when the user calls POST /uploads/commit.
    db_result = _load_df_to_staging(df, master_type, clientId)

    if not db_result.get("staged"):
        # Staging failed (bad columns, DB error, etc.). Surface the reason.
        raise HTTPException(
            status_code=400,
            detail=f"Could not stage file: {db_result.get('reason', 'unknown error')}",
        )

    # ── Audit the successful upload ─────────────────────────────────
    # The UI's "File Upload" row in the audit log comes from here.
    # `user` is the authenticated identity from Depends — no need to
    # re-resolve from the token (the previous code did, but now that
    # auth is required up front the user is always present).
    log_audit_event(
        request,
        action_type="file_upload",
        details=f"{master_type} · {filename} · {row_count:,} rows",
        client_id=clientId,
        user_id=user.get("id"),
        user_email=user.get("email"),
        outcome="success",
    )

    return {
        "masterType": master_type,
        "fileName": filename,
        "rowCount": row_count,
        "columns": columns,
        "uploadedAt": datetime.now().isoformat(),
        "status": "staged",
        "batchId": db_result["batch_id"],
        "stagingTable": db_result["staging_table"],
        "rowsStaged": db_result["rows_staged"],
        "rowsReplaced": db_result["rows_replaced"],
    }


@router.get("/uploads")
def list_uploads(
    clientId: str = Query(...),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    List staged files in the client's PENDING batch.

    Queries the database (not an in-memory dict), so results are correct
    even across app restarts. For each master_type that has rows in its
    staging table for the pending batch, returns a row with the count.

    If there is no pending batch, returns an empty list.
    """
    _require_client_access(user, clientId)
    with engine.begin() as conn:
        batch_id = _get_pending_batch_id(conn, clientId)
        if not batch_id:
            return []

        uploads = []
        for master_type, (real_table, _) in MASTER_TYPE_TO_TABLE.items():
            staging_table = f"staging_{real_table}"
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {staging_table} "
                     f"WHERE batch_id = :bid AND client_id = :cid"),
                {"bid": batch_id, "cid": clientId},
            ).fetchone()
            row_count = result[0] if result else 0
            if row_count > 0:
                uploads.append({
                    "masterType": master_type,
                    "stagingTable": staging_table,
                    "rowCount": row_count,
                    "batchId": batch_id,
                    "status": "staged",
                })
        return uploads


@router.get("/uploads/batch")
def get_pending_batch(
    clientId: str = Query(...),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Return the pending batch summary for a client (or null if none).

    Use this on the Upload page to show "You have N files staged. Ready
    to commit?" before the user clicks the Commit button.
    """
    _require_client_access(user, clientId)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT batch_id::text, created_at, status "
                 "FROM upload_batches "
                 "WHERE client_id = :cid AND status = 'pending'"),
            {"cid": clientId},
        ).fetchone()
        if not row:
            return {"pendingBatch": None}

        batch_id, created_at, status = row

        # Count rows per staging table
        per_type = []
        total_rows = 0
        for master_type, (real_table, _) in MASTER_TYPE_TO_TABLE.items():
            staging_table = f"staging_{real_table}"
            count_row = conn.execute(
                text(f"SELECT COUNT(*) FROM {staging_table} "
                     f"WHERE batch_id = :bid AND client_id = :cid"),
                {"bid": batch_id, "cid": clientId},
            ).fetchone()
            n = count_row[0] if count_row else 0
            if n > 0:
                per_type.append({"masterType": master_type, "rowCount": n})
                total_rows += n

        return {
            "pendingBatch": {
                "batchId": batch_id,
                "createdAt": created_at.isoformat() if created_at else None,
                "status": status,
                "totalRows": total_rows,
                "files": per_type,
            }
        }


@router.delete("/uploads/{master_type}")
def delete_upload(
    master_type: str,
    request: Request,
    clientId: str = Query(...),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Remove a single master_type's rows from the pending batch.

    Useful if the user staged the wrong file and wants to redo just that
    one. If no pending batch exists, this is a no-op.
    """
    _require_client_access(user, clientId)
    if master_type not in VALID_MASTER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown master type: {master_type}",
        )

    real_table, _ = MASTER_TYPE_TO_TABLE[master_type]
    staging_table = f"staging_{real_table}"

    with engine.begin() as conn:
        batch_id = _get_pending_batch_id(conn, clientId)
        if not batch_id:
            return {"deleted": 0, "reason": "No pending batch for this client"}

        result = conn.execute(
            text(f"DELETE FROM {staging_table} "
                 f"WHERE batch_id = :bid AND client_id = :cid"),
            {"bid": batch_id, "cid": clientId},
        )
        deleted = result.rowcount

        # If removing this master_type left the batch with zero staging rows
        # across all tables, delete the upload_batches row too. Otherwise
        # /uploads/batch would keep returning a ghost "0-file" pending batch.
        batch_cleaned = _cleanup_empty_batch(conn, batch_id, clientId)

    # Also remove the saved file from disk (best-effort)
    try:
        for f in os.listdir(UPLOAD_DIR):
            if f.startswith(f"{clientId}_{master_type}_"):
                os.remove(os.path.join(UPLOAD_DIR, f))
    except Exception:
        pass

    # Audit fix #4: log the staging-row delete so we can trace
    # "user X removed Y master from pending batch Z" later.
    log_audit_event(
        request,
        action_type="batch_file_remove",
        details=f"batch={batch_id} · {master_type} · {deleted:,} rows removed",
        client_id=clientId,
        user_id=user.get("id"),
        user_email=user.get("email"),
        outcome="success",
    )

    return {
        "deleted": deleted,
        "masterType": master_type,
        "batchId": batch_id,
        "batchCleaned": batch_cleaned,
    }


# ── Commit / discard endpoints ───────────────────────────────────────────────

def _preflight_fk_check(conn, batch_id: str, client_id: str) -> list[dict]:
    """
    Check every FK rule in CATALOG_FK_CHECKS.

    For each rule, a staging row is valid if its foreign key value exists
    EITHER in the real parent table (already committed) OR in the parent's
    own staging table for the SAME batch (being committed together).

    Returns a list of violation dicts. Empty list = all clean.
    """
    violations = []

    for (child_staging, child_col, parent_real, parent_real_col,
         parent_staging, fk_name) in CATALOG_FK_CHECKS:

        # Find staging rows whose FK value is missing from both the real
        # parent table AND the parent's staging rows for this batch.
        query = text(f"""
            SELECT s.{child_col} AS missing_key, COUNT(*) AS row_count
            FROM {child_staging} s
            WHERE s.batch_id = :bid
              AND s.client_id = :cid
              AND s.{child_col} IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {parent_real} p
                  WHERE p.client_id = :cid
                    AND p.{parent_real_col} = s.{child_col}
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {parent_staging} ps
                  WHERE ps.batch_id = :bid
                    AND ps.client_id = :cid
                    AND ps.{parent_real_col} = s.{child_col}
              )
            GROUP BY s.{child_col}
            LIMIT 10
        """)
        rows = conn.execute(query, {"bid": batch_id, "cid": client_id}).fetchall()
        for missing_key, row_count in rows:
            violations.append({
                "fk": fk_name,
                "stagingTable": child_staging,
                "column": child_col,
                "missingKey": missing_key,
                "rowCount": row_count,
                "parentTable": parent_real,
            })

    return violations


def _commit_batch(conn, batch_id: str, client_id: str) -> dict:
    """
    Move rows from staging → real tables in COMMIT_ORDER.

    The caller must have already opened a transaction and run
    SET CONSTRAINTS ALL DEFERRED — so FK checks on transactional tables
    (orders, line_items, etc.) happen at COMMIT, not after each INSERT.

    Each INSERT uses ON CONFLICT handling driven by COMMIT_CONFLICT:
      - dimension tables get UPSERT so re-uploads refresh non-PK columns
      - fact tables get DO NOTHING so historical records aren't overwritten
    This makes commit replay-safe: a batch that partially conflicts with
    existing data no longer blows up the whole transaction.
    """
    per_type_rows = {}
    mv_needs_refresh = False

    for master_type in COMMIT_ORDER:
        real_table, expected_cols = MASTER_TYPE_TO_TABLE[master_type]
        staging_table = f"staging_{real_table}"
        pk_cols, action = COMMIT_CONFLICT[master_type]

        # Only insert columns that exist in the real table (skip batch_id, staging_row_id)
        col_list = ", ".join(expected_cols)
        pk_list = ", ".join(pk_cols)

        # Build ON CONFLICT clause based on strategy for this master_type.
        if action == "update":
            # Update every non-PK column to the staging value (EXCLUDED.col).
            non_pk_cols = [c for c in expected_cols if c not in pk_cols]
            if non_pk_cols:
                set_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in non_pk_cols
                )
                conflict_sql = f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}"
            else:
                # Table has no non-PK columns — nothing to update, just skip.
                conflict_sql = f"ON CONFLICT ({pk_list}) DO NOTHING"
        else:  # 'nothing'
            conflict_sql = f"ON CONFLICT ({pk_list}) DO NOTHING"

        insert_sql = text(f"""
            INSERT INTO {real_table} ({col_list})
            SELECT {col_list}
            FROM {staging_table}
            WHERE batch_id = :bid AND client_id = :cid
            {conflict_sql}
        """)
        result = conn.execute(insert_sql, {"bid": batch_id, "cid": client_id})
        # rowcount counts affected rows (inserts + updates for UPSERT; inserts
        # only for DO NOTHING). Use it as the "rows committed" metric.
        affected = result.rowcount
        per_type_rows[master_type] = affected

        if affected > 0 and real_table in MV_TRIGGER_TABLES:
            mv_needs_refresh = True

    # Clear this batch's staging rows (other pending batches — none for this
    # client — are unaffected because we filter by batch_id)
    for master_type in COMMIT_ORDER:
        real_table, _ = MASTER_TYPE_TO_TABLE[master_type]
        staging_table = f"staging_{real_table}"
        conn.execute(
            text(f"DELETE FROM {staging_table} WHERE batch_id = :bid"),
            {"bid": batch_id},
        )

    # Mark the batch committed
    conn.execute(
        text("UPDATE upload_batches SET status = 'committed', "
             "committed_at = NOW() WHERE batch_id = :bid"),
        {"bid": batch_id},
    )

    return {"perTypeRows": per_type_rows, "mvNeedsRefresh": mv_needs_refresh}


# NOTE: The @router.post("/uploads/commit") and @router.post("/uploads/discard")
# endpoint decorators used to live here. They were moved UP to appear before
# @router.post("/uploads/{master_type}") because FastAPI matches routes in
# declaration order — the parameterized path was catching /uploads/commit
# requests and treating "commit" as a master_type. The helper functions
# _preflight_fk_check() and _commit_batch() remain above; Python resolves
# them at call time, so their position doesn't matter.
