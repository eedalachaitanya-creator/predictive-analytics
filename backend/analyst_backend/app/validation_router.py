"""
validation_router.py — Data Quality Validation Endpoints
=========================================================
GET  /api/v1/validation              — Run validation on all uploaded masters
GET  /api/v1/validation/{masterType} — Column-level detail for one master

HOW IT WORKS:
1. Queries each database table to count rows, missing values, duplicate PKs
2. For column-level detail, inspects information_schema and computes per-column stats
3. Returns structured JSON the frontend renders as validation dashboards

The validation runs LIVE against the actual database — not cached or static.
"""

import logging
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text

from app.database import engine
from app.auth_router import get_current_user

router = APIRouter(prefix="/api/v1", tags=["validation"])
log = logging.getLogger("validation")


# Audit fix 2026-04-29 (#1): tenant-access check identical to the
# pattern used in chat_router and upload_router. Without it, the
# validation endpoints leaked PII (customer emails, names, addresses)
# 50 chars at a time via the column-detail "sample" field for any
# tenant the caller named in the query string — no auth required.
def _require_client_access(user: dict, client_id: str) -> None:
    if user.get("role") == "super_admin" or "*" in (user.get("clientAccess") or []):
        return
    if client_id not in (user.get("clientAccess") or []):
        raise HTTPException(
            status_code=403,
            detail=f"You do not have access to client {client_id}",
        )


# ── Validation config per master type ──────────────────────────────────────
# Each entry defines: table name, display label, group, primary key column,
# required columns (nulls here = warnings), date columns, and whether
# the table has a client_id column for filtering.
VALIDATION_CONFIG = {
    "customer": {
        "table": "customers",
        "label": "👤 Customer Master",
        "group": "Transaction",
        "pk": "customer_id",
        "has_client_id": True,
        # last_login_date added 2026-04-24 — it's now part of the
        # two-condition churn label definition (see migration
        # 2026_04_24_login_aware_churn_phase2.sql). A customer master
        # without this column means churn_label falls back to the
        # legacy single-condition rule via COALESCE(...,9999), which
        # silently weakens the model. Marking it required surfaces
        # the gap on the Validation page instead of letting it pass.
        "required_cols": [
            "client_id", "customer_id", "customer_email", "customer_name",
            "account_created_date", "last_login_date",
        ],
        "date_cols": ["account_created_date", "last_login_date"],
    },
    "order": {
        "table": "orders",
        "label": "📦 Order Master",
        "group": "Transaction",
        "pk": "order_id",
        "has_client_id": True,
        "required_cols": [
            "client_id", "order_id", "customer_id", "order_date",
            "order_value_usd",
        ],
        "date_cols": ["order_date"],
    },
    "line_items": {
        "table": "line_items",
        "label": "🛍️ Line Items Master",
        "group": "Transaction",
        "pk": "line_item_id",
        "has_client_id": True,
        "required_cols": [
            "client_id", "line_item_id", "order_id", "customer_id",
            "product_id", "quantity", "unit_price_usd",
        ],
        "date_cols": [],
    },
    "product": {
        "table": "products",
        "label": "📋 Product Master",
        "group": "Product",
        "pk": "product_id",
        "has_client_id": True,
        "required_cols": ["product_id", "product_name", "category_id"],
        "date_cols": [],
    },
    "price": {
        "table": "product_prices",
        "label": "💲 Product Price Master",
        "group": "Product",
        "pk": "price_id",
        "has_client_id": True,
        "required_cols": ["price_id", "product_id", "unit_price_usd"],
        "date_cols": [],
    },
    "vendor_map": {
        "table": "product_vendor_mapping",
        "label": "🔗 Product-Vendor Map",
        "group": "Product",
        "pk": "pv_id",
        "has_client_id": True,
        "required_cols": ["pv_id", "product_id", "vendor_id"],
        "date_cols": [],
    },
    "category": {
        "table": "categories",
        "label": "📂 Category Master",
        "group": "Hierarchy",
        "pk": "category_id",
        "has_client_id": True,
        "required_cols": ["category_id", "category_name"],
        "date_cols": [],
    },
    "sub_category": {
        "table": "sub_categories",
        "label": "📁 Sub-Category Master",
        "group": "Hierarchy",
        "pk": "sub_category_id",
        "has_client_id": True,
        "required_cols": ["sub_category_id", "sub_category_name", "category_id"],
        "date_cols": [],
    },
    "sub_sub_category": {
        "table": "sub_sub_categories",
        "label": "🗂️ Sub-Sub-Category Master",
        "group": "Hierarchy",
        "pk": "sub_sub_category_id",
        "has_client_id": True,
        "required_cols": [
            "sub_sub_category_id", "sub_sub_category_name", "sub_category_id",
        ],
        "date_cols": [],
    },
    "brand": {
        "table": "brands",
        "label": "🏷️ Brand Master",
        "group": "Brand/Vendor",
        "pk": "brand_id",
        "has_client_id": True,
        "required_cols": ["brand_id", "brand_name"],
        "date_cols": [],
    },
    "vendor": {
        "table": "vendors",
        "label": "🏭 Vendor Master",
        "group": "Brand/Vendor",
        "pk": "vendor_id",
        "has_client_id": True,
        "required_cols": ["vendor_id", "vendor_name"],
        "date_cols": [],
    },
    "review": {
        "table": "customer_reviews",
        "label": "⭐ Customer Reviews",
        "group": "Feedback",
        "pk": "review_id",
        "has_client_id": True,
        "required_cols": ["review_id", "customer_id", "product_id", "rating"],
        "date_cols": ["review_date"],
    },
    "support_ticket": {
        "table": "support_tickets",
        "label": "🎫 Support Tickets",
        "group": "Feedback",
        "pk": "ticket_id",
        "has_client_id": True,
        "required_cols": ["ticket_id", "customer_id", "ticket_type"],
        "date_cols": ["opened_date"],
    },
    "login_event": {
        "table": "login_events",
        "label": "🔑 Login Events",
        "group": "Engagement",
        "pk": "login_id",
        "has_client_id": True,
        "required_cols": ["login_id", "customer_id", "login_at"],
        "date_cols": ["login_at"],
    },
}


def _safe_query(conn, sql_str: str, params: dict | None = None):
    """Execute a query and return the scalar result, defaulting to 0 on error.

    Audit fix 2026-04-29 (#3): failures now log at ERROR (was WARNING)
    with the FULL SQL and params. Previously a missing table or syntax
    error silently returned 0, making the validation page show clean
    "OK" rows for tables that don't exist. Returning 0 is still the
    fallback so the page renders, but operators now see the failure
    in production logs at a visible level.
    """
    try:
        result = conn.execute(text(sql_str), params or {})
        return result.scalar() or 0
    except Exception as e:
        log.error(
            "Validation query failed | SQL: %s | params: %s | error: %s",
            sql_str.replace("\n", " ").strip(), params, e,
        )
        return 0


@router.get("/validation")
def get_validation(
    clientId: str = Query(default="CLT-001"),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Run live validation checks on all database tables for a given client.
    Returns summary stats + per-file validation results.
    """
    _require_client_access(user, clientId)
    results = []

    with engine.connect() as conn:
        for n, (master_type, cfg) in enumerate(VALIDATION_CONFIG.items(), 1):
            table = cfg["table"]
            has_cid = cfg["has_client_id"]

            # ── Row count ──────────────────────────────────────────────
            if has_cid:
                row_count = _safe_query(
                    conn,
                    f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid",
                    {"cid": clientId},
                )
            else:
                row_count = _safe_query(conn, f"SELECT COUNT(*) FROM {table}")

            # ── Column count (computed even for empty tables so the row in
            # the summary still shows how wide the schema is) ─────────
            col_count = _safe_query(
                conn,
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = :tbl AND table_schema = 'public'",
                {"tbl": table},
            )

            # ── Tables with zero rows are surfaced as a 'warn' row
            # instead of silently disappearing — so the super admin can
            # see at a glance which masters still need data uploaded.
            if row_count == 0:
                results.append({
                    "n":              n,
                    "masterType":     master_type,
                    "name":           cfg["label"],
                    "group":          cfg["group"],
                    "rows":           0,
                    "cols":           col_count,
                    "missing":        0,
                    "missingDetails": [],
                    "dup":            0,
                    "dateErrors":     0,
                    "status":         "warn",
                    "empty":          True,
                })
                continue

            # ── Missing values in required columns ─────────────────────
            missing = 0
            missing_details = []
            for col in cfg.get("required_cols", []):
                if has_cid:
                    null_count = _safe_query(
                        conn,
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {col} IS NULL AND client_id = :cid",
                        {"cid": clientId},
                    )
                else:
                    null_count = _safe_query(
                        conn,
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL",
                    )
                if null_count > 0:
                    missing += null_count
                    missing_details.append({"column": col, "nullCount": null_count})

            # ── Duplicate primary keys ─────────────────────────────────
            pk = cfg["pk"]
            if has_cid:
                dup_count = _safe_query(
                    conn,
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT {pk} FROM {table} "
                    f"  WHERE client_id = :cid "
                    f"  GROUP BY {pk} HAVING COUNT(*) > 1"
                    f") dupes",
                    {"cid": clientId},
                )
            else:
                dup_count = _safe_query(
                    conn,
                    f"SELECT COUNT(*) FROM ("
                    f"  SELECT {pk} FROM {table} "
                    f"  GROUP BY {pk} HAVING COUNT(*) > 1"
                    f") dupes",
                )

            # ── Date column null checks ────────────────────────────────
            date_errors = 0
            for dcol in cfg.get("date_cols", []):
                if has_cid:
                    date_nulls = _safe_query(
                        conn,
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {dcol} IS NULL AND client_id = :cid",
                        {"cid": clientId},
                    )
                else:
                    date_nulls = _safe_query(
                        conn,
                        f"SELECT COUNT(*) FROM {table} WHERE {dcol} IS NULL",
                    )
                date_errors += date_nulls

            # ── Determine status ───────────────────────────────────────
            status = "ok"
            if missing > 0 or date_errors > 0:
                status = "warn"
            if dup_count > 0:
                status = "error"

            results.append({
                "n": n,
                "masterType": master_type,
                "name": cfg["label"],
                "group": cfg["group"],
                "rows": row_count,
                "cols": col_count,
                "missing": missing,
                "missingDetails": missing_details,
                "dup": dup_count,
                "dateErrors": date_errors,
                "status": status,
            })

    # ── Summary stats ──────────────────────────────────────────────────
    # `uploaded` counts only masters that actually have rows — empty
    # tables now appear as warn rows in `results` but shouldn't be
    # counted as "Tables with Data".
    uploaded = sum(1 for r in results if not r.get("empty"))
    passed = sum(1 for r in results if r["status"] == "ok")
    warnings = sum(1 for r in results if r["status"] == "warn")
    errors = sum(1 for r in results if r["status"] == "error")

    return {
        "summary": {
            "totalMasters": len(VALIDATION_CONFIG),
            "uploaded": uploaded,
            "passed": passed,
            "warnings": warnings,
            "errors": errors,
        },
        "files": results,
    }


@router.get("/validation/{master_type}")
def get_validation_detail(
    master_type: str,
    clientId: str = Query(default="CLT-001"),
    user: dict = Depends(get_current_user),  # Audit fix #1
):
    """
    Column-level validation detail for a specific master type.
    Returns each column's data type, non-null count, unique count, sample value,
    whether it's required, and status (ok/warn).
    """
    _require_client_access(user, clientId)
    if master_type not in VALIDATION_CONFIG:
        raise HTTPException(status_code=404, detail=f"Unknown master type: {master_type}")

    cfg = VALIDATION_CONFIG[master_type]
    table = cfg["table"]
    has_cid = cfg["has_client_id"]
    required_set = set(cfg.get("required_cols", []))

    columns = []

    with engine.connect() as conn:
        # ── Get total row count ────────────────────────────────────────
        if has_cid:
            total_rows = _safe_query(
                conn,
                f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid",
                {"cid": clientId},
            )
        else:
            total_rows = _safe_query(conn, f"SELECT COUNT(*) FROM {table}")

        if total_rows == 0:
            return {"masterType": master_type, "label": cfg["label"], "totalRows": 0, "columns": []}

        # ── Get column names + data types from information_schema ──────
        col_rows = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :tbl AND table_schema = 'public'
            ORDER BY ordinal_position
        """), {"tbl": table}).fetchall()

        for col_name, data_type in col_rows:
            # ── Non-null count ─────────────────────────────────────────
            if has_cid:
                non_null = _safe_query(
                    conn,
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE {col_name} IS NOT NULL AND client_id = :cid",
                    {"cid": clientId},
                )
            else:
                non_null = _safe_query(
                    conn,
                    f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NOT NULL",
                )

            # ── Unique count ───────────────────────────────────────────
            if has_cid:
                unique = _safe_query(
                    conn,
                    f"SELECT COUNT(DISTINCT {col_name}) FROM {table} "
                    f"WHERE client_id = :cid",
                    {"cid": clientId},
                )
            else:
                unique = _safe_query(
                    conn,
                    f"SELECT COUNT(DISTINCT {col_name}) FROM {table}",
                )

            # ── Sample value ───────────────────────────────────────────
            try:
                if has_cid:
                    sample_row = conn.execute(text(
                        f"SELECT {col_name} FROM {table} "
                        f"WHERE {col_name} IS NOT NULL AND client_id = :cid "
                        f"LIMIT 1"
                    ), {"cid": clientId}).fetchone()
                else:
                    sample_row = conn.execute(text(
                        f"SELECT {col_name} FROM {table} "
                        f"WHERE {col_name} IS NOT NULL LIMIT 1"
                    )).fetchone()
                sample = str(sample_row[0]) if sample_row else ""
            except Exception:
                sample = ""

            # ── Map PostgreSQL types to simpler labels ─────────────────
            type_map = {
                "character varying": "string",
                "text": "string",
                "integer": "int",
                "bigint": "int",
                "numeric": "decimal",
                "double precision": "float",
                "real": "float",
                "boolean": "bool",
                "date": "date",
                "timestamp without time zone": "datetime",
                "timestamp with time zone": "datetime",
            }
            simple_type = type_map.get(data_type, data_type)

            # ── Status ─────────────────────────────────────────────────
            is_required = col_name in required_set
            null_count = total_rows - non_null
            status = "ok"
            if is_required and null_count > 0:
                status = "warn"

            columns.append({
                "col": col_name,
                "type": simple_type,
                "nonNull": f"{non_null}/{total_rows}",
                "unique": unique,
                "sample": sample[:50],  # truncate long values
                "req": is_required,
                "nullCount": null_count,
                "status": status,
            })

    return {
        "masterType": master_type,
        "label": cfg["label"],
        "totalRows": total_rows,
        "columns": columns,
    }


@router.get("/validation/{master_type}/rows")
def get_validation_table_rows(
    master_type: str,
    clientId: str = Query(...),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_user),
):
    """Actual rows of a master table — the View modal's DATA tab.

    Returns the generic ``{columns, rows, total, offset, limit}`` shape the other
    data-viewer popups use, tenant-scoped + server-paginated. ``table`` and the
    ORDER BY come from VALIDATION_CONFIG (a trusted whitelist), never user input,
    so the f-string interpolation is injection-safe.
    """
    _require_client_access(user, clientId)
    if master_type not in VALIDATION_CONFIG:
        raise HTTPException(status_code=404, detail=f"Unknown master type: {master_type}")
    cfg = VALIDATION_CONFIG[master_type]
    table = cfg["table"]
    order_by = cfg["pk"]
    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE client_id = :cid"),
            {"cid": clientId},
        ).scalar() or 0
        result = conn.execute(
            text(f"SELECT * FROM {table} WHERE client_id = :cid "
                 f"ORDER BY {order_by} LIMIT :limit OFFSET :offset"),
            {"cid": clientId, "limit": limit, "offset": offset},
        )
        columns = list(result.keys())
        rows = [dict(zip(columns, r)) for r in result.fetchall()]
    return {
        "masterType": master_type,
        "label": cfg["label"],
        "table": table,
        "columns": columns,
        "rows": rows,
        "total": int(total),
        "offset": offset,
        "limit": limit,
    }
