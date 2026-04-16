"""
upload_router.py — CSV/Excel file upload endpoints
===================================================
POST   /api/v1/uploads/{masterType}  — Upload a CSV/Excel file
GET    /api/v1/uploads               — List uploaded files for a client
DELETE /api/v1/uploads/{masterType}  — Remove an uploaded file

HOW IT WORKS:
1. User picks a CSV or Excel file on the Upload page
2. Frontend sends it as multipart/form-data to POST /uploads/{masterType}
3. Backend reads the file with pandas to validate it and count rows/columns
4. File is saved to disk in the uploads/ directory
5. Data is AUTOMATICALLY loaded into the correct PostgreSQL table
6. If customer/order/line_items data is loaded, the materialized view is refreshed
7. Returns the file name, row count, column list, and DB insert status
"""

import os
import io
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from fastapi import APIRouter, File, Form, UploadFile, Query, HTTPException, Header
from typing import Optional

from app.database import engine
from app.auth_router import _find_user_by_token

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

# ── In-memory upload tracking ────────────────────────────────────────────────
# Key: (clientId, masterType) → upload info dict
_upload_registry: dict[tuple[str, str], dict] = {}

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
        ["client_id", "customer_id", "customer_email", "customer_name",
         "customer_phone", "account_created_date", "registration_channel",
         "country_code", "state", "city", "zip_code", "shipping_address",
         "preferred_device", "email_opt_in", "sms_opt_in"],
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
         "qty_max", "unit_price_usd"],
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

# Tables that affect the materialized view — refresh after loading these
MV_TRIGGER_TABLES = {"customers", "orders", "line_items", "products",
                     "categories", "brands", "product_prices",
                     "customer_reviews", "support_tickets"}


def _load_df_to_database(df: pd.DataFrame, master_type: str, client_id: str) -> dict:
    """
    Insert a pandas DataFrame into the correct PostgreSQL table.
    Uses INSERT ... ON CONFLICT DO NOTHING so duplicates are safely skipped.
    Returns a dict with rows_inserted, rows_skipped, and whether the MV was refreshed.
    """
    if master_type not in MASTER_TYPE_TO_TABLE:
        return {"db_loaded": False, "reason": f"No table mapping for {master_type}"}

    table_name, expected_cols = MASTER_TYPE_TO_TABLE[master_type]

    # ── Normalize column names in the uploaded DataFrame ────────────────
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # If client_id column is expected but missing, inject it from the form field
    if "client_id" in expected_cols and "client_id" not in df.columns:
        df.insert(0, "client_id", client_id)

    # Keep only columns that exist in both the DataFrame and the expected list
    available_cols = [c for c in expected_cols if c in df.columns]
    log.info(
        "DB load prep: %s → %s | expected %d cols, found %d matching | df cols: %s",
        master_type, table_name, len(expected_cols), len(available_cols), list(df.columns)[:5]
    )
    if not available_cols:
        return {
            "db_loaded": False,
            "reason": f"No matching columns. Expected: {expected_cols}, got: {list(df.columns)}",
        }

    df_to_load = df[available_cols].copy()

    # ── Drop rows where primary key columns are null ────────────────────
    pk_col = available_cols[0]  # first column is typically the PK
    df_to_load = df_to_load.dropna(subset=[pk_col])

    if df_to_load.empty:
        return {"db_loaded": False, "reason": "All rows had null primary keys"}

    # ── Clean up data types before insert ─────────────────────────────
    # Boolean columns: convert NaN/float to proper True/False/None
    # This fixes: "column email_opt_in is boolean but expression is text"
    for col in df_to_load.columns:
        # If the column has mixed types with NaN, convert NaN to None
        if df_to_load[col].dtype == 'float64':
            # Check if this looks like a boolean column (only 0, 1, NaN values)
            unique_vals = set(df_to_load[col].dropna().unique())
            if unique_vals <= {0.0, 1.0, True, False}:
                df_to_load[col] = df_to_load[col].apply(
                    lambda x: None if pd.isna(x) else bool(x)
                )

    # ── Insert into database ────────────────────────────────────────────
    col_list = ", ".join(available_cols)
    placeholders = ", ".join([f":{c}" for c in available_cols])
    insert_sql = text(
        f"INSERT INTO {table_name} ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    rows_inserted = 0
    rows_skipped = 0

    try:
        with engine.connect() as conn:
            # Insert in batches of 500 rows
            batch_size = 500
            # Convert NaN to None for all columns
            records = []
            for record in df_to_load.to_dict("records"):
                clean = {}
                for k, v in record.items():
                    if pd.isna(v) if isinstance(v, float) else v is None:
                        clean[k] = None
                    else:
                        clean[k] = v
                records.append(clean)

            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                result = conn.execute(insert_sql, batch)
                rows_inserted += result.rowcount
                rows_skipped += len(batch) - result.rowcount

            # ── Refresh materialized view if this table affects it ───
            mv_refreshed = False
            if table_name in MV_TRIGGER_TABLES:
                try:
                    conn.execute(text("REFRESH MATERIALIZED VIEW mv_customer_features"))
                    mv_refreshed = True
                    log.info("Materialized view refreshed after %s load", table_name)
                except Exception as e:
                    log.warning("Could not refresh materialized view: %s", e)

            conn.commit()

        log.info(
            "DB load complete: %s → %s | %d inserted, %d skipped (duplicates)",
            master_type, table_name, rows_inserted, rows_skipped,
        )

        return {
            "db_loaded": True,
            "table": table_name,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "mv_refreshed": mv_refreshed,
        }

    except Exception as e:
        log.error("DB load FAILED for %s: %s", table_name, e, exc_info=True)
        return {"db_loaded": False, "reason": str(e)}


def _read_file_to_df(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Read CSV or Excel file bytes into a pandas DataFrame.

    SMART HEADER DETECTION:
    Some Excel files (like the Costco dataset) have a decorative title row
    in row 1, with the actual column headers in row 2. For example:
        Row 1: "👤 Customer Master | 100 rows | One row per customer..."
        Row 2: "client_id", "customer_id", "customer_email", ...
        Row 3: actual data

    We detect this by checking if the first row looks like a title
    (single long string, other cells are empty). If so, we skip row 1
    and use row 2 as headers.
    """
    lower = filename.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes))
    elif lower.endswith((".xlsx", ".xls")):
        # First, try reading normally (row 1 = headers)
        df = pd.read_excel(io.BytesIO(file_bytes), header=0)

        # Check if row 1 looks like a decorative title row:
        # - First column name is a long string (>30 chars) or contains emoji/pipe
        # - Most other column names are "Unnamed"
        first_col = str(df.columns[0]) if len(df.columns) > 0 else ""
        unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed"))
        is_title_row = (
            (len(first_col) > 30 or "|" in first_col)
            and unnamed_count >= len(df.columns) // 2
        )

        if is_title_row:
            log.info("Detected title row in %s — using row 2 as headers", filename)
            df = pd.read_excel(io.BytesIO(file_bytes), header=1)

        return df
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {filename}. Use .csv, .xlsx, or .xls",
        )


@router.post("/uploads/{master_type}")
async def upload_file(
    master_type: str,
    file: UploadFile = File(...),
    clientId: str = Form(None),             # NOW OPTIONAL — auto-detected from token
    masterType: str = Form(None),
    authorization: Optional[str] = Header(default=None),
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
    if not clientId:
        if not authorization:
            raise HTTPException(
                status_code=400,
                detail="Either provide clientId in the form, or include an Authorization header",
            )
        token = authorization.replace("Bearer ", "")
        user = _find_user_by_token(token)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

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
            log.info("Auto-detected client_id=%s from user %s", clientId, user["email"])
        elif len(client_access) > 1:
            # User has access to multiple clients → they need to pick one
            raise HTTPException(
                status_code=400,
                detail=f"You have access to multiple clients: {client_access}. "
                       f"Please specify clientId in the upload form.",
            )
        else:
            raise HTTPException(status_code=403, detail="You don't have access to any clients")

    # Read the file
    file_bytes = await file.read()
    file_size = len(file_bytes)
    filename = file.filename or f"{master_type}.csv"

    # Validate by reading with pandas
    try:
        df = _read_file_to_df(file_bytes, filename)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=400, detail="File is empty — no rows found")

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

    # ── AUTO-LOAD into database ───────────────────────────────────────
    db_result = _load_df_to_database(df, master_type, clientId)

    # Track the upload
    upload_info = {
        "masterType": master_type,
        "fileName": filename,
        "fileSize": file_size,
        "rowCount": row_count,
        "columns": columns,
        "uploadedAt": datetime.now().isoformat(),
        "status": "success" if db_result.get("db_loaded") else "uploaded_only",
        "dbResult": db_result,
    }
    _upload_registry[(clientId, master_type)] = upload_info

    return {
        "masterType": master_type,
        "fileName": filename,
        "rowCount": row_count,
        "columns": columns,
        "uploadedAt": upload_info["uploadedAt"],
        "dbResult": db_result,
    }


@router.get("/uploads")
def list_uploads(clientId: str = Query(...)):
    """List all uploaded files for a specific client."""
    uploads = []
    for (cid, mtype), info in _upload_registry.items():
        if cid == clientId:
            uploads.append({
                "masterType": info["masterType"],
                "fileName": info["fileName"],
                "fileSize": info["fileSize"],
                "rowCount": info["rowCount"],
                "uploadedAt": info["uploadedAt"],
                "status": info["status"],
            })
    return uploads


@router.delete("/uploads/{master_type}")
def delete_upload(
    master_type: str,
    clientId: str = Query(...),
):
    """Remove an uploaded file record."""
    key = (clientId, master_type)
    if key in _upload_registry:
        # Remove from tracking
        del _upload_registry[key]
        # Try to delete the file from disk
        try:
            for f in os.listdir(UPLOAD_DIR):
                if f.startswith(f"{clientId}_{master_type}_"):
                    os.remove(os.path.join(UPLOAD_DIR, f))
        except Exception:
            pass
    return {}
