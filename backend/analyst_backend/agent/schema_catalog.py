"""
schema_catalog.py — curated schema exposure for schema-aware text-to-SQL.

Lets the analyst agent answer questions across the tenant's BUSINESS tables, not
just the handful named in the system prompt. It:
  * defines the security boundary (DENY_TABLES — auth/system/internal),
  * lists allowed business tables (those carrying client_id, minus deny/staging),
  * renders a compact catalog for the system prompt,
  * describes a table's columns on demand (describe_tables),
  * detects references to denied tables in a SQL string (query_database's guard).

Allowed = base table / materialized view that has a `client_id` column, is not in
DENY_TABLES, and is not a staging_* table. That keeps every allowed table
tenant-filterable (query_database mandates WHERE client_id = :client_id) and
auto-excludes global reference tables that can't be tenant-scoped.
"""
import re
import logging

from sqlalchemy import text

log = logging.getLogger("crp_api.schema_catalog")

# Auth/system/internal tables the agent must NEVER read. Several of these DO
# carry client_id (audit_log, chat_messages, llm_cost_log, ...), so this explicit
# deny list — not the client_id heuristic — is the real security boundary.
DENY_TABLES = frozenset({
    "users", "active_tokens", "audit_log", "chat_messages", "client_config",
    "llm_cost_log", "ml_temporal_snapshots", "pipeline_outputs",
    "rag_documents", "upload_batches",
})

# Terse one-line purposes for high-signal tables (others fall back to name +
# columns). Kept short — this goes into the system prompt.
TABLE_PURPOSES = {
    "customers": "Customer master (tier, status, signup, demographics).",
    "orders": "One row per order (date, value, status, payment, discount).",
    "line_items": "Order line items (product, qty, unit price) per order.",
    "churn_scores": "Latest ML churn_probability + risk_tier per customer per run.",
    "customer_rfm_features": "RFM recency/frequency/monetary scores + segment.",
    "customer_purchase_cycles": "Per-customer purchase cadence / cycle metrics.",
    "customer_reviews": "Free-text customer reviews with rating + sentiment.",
    "support_tickets": "Support tickets (type, priority, status, channel, timings).",
    "products": "Product catalog (name, category, brand, price).",
    "categories": "Top-level product categories.",
    "sub_categories": "Second-level product categories.",
    "sub_sub_categories": "Third-level product categories.",
    "brands": "Brand reference + descriptions.",
    "vendors": "Vendor / supplier reference.",
    "product_vendor_mapping": "Which vendor supplies which product.",
    "product_prices": "Product price points (by source / time).",
    "retention_interventions": "Retention offers/interventions sent to customers.",
    "outreach_messages": "Outreach messages sent (channel, text, trigger reason).",
    "message_templates": "Reusable outreach message templates.",
    "value_propositions": "Per-segment value-proposition messaging.",
    "pricing_recommendations": "Strategist price recommendations + reasoning.",
    "customer_price_context": "Per-customer pricing context for recommendations.",
    "price_history": "Competitor/product price history (Scout).",
    "price_alerts": "Price-change alerts (Scout).",
    "entities": "Scraped competitor entities (Scout).",
    "entity_listings": "Competitor product listings (Scout).",
    "product_features": "Extracted product features (Scout).",
    "product_results": "Scout product match results.",
    "websites": "Tracked competitor websites (Scout).",
    "mv_customer_features": "Materialized 360 feature view per customer (ML input).",
}

_allowed_cache = None
_catalog_cache = None


def _allowed_tables(engine) -> set:
    """Tables/views with a client_id column, minus deny + staging. Cached."""
    global _allowed_cache
    if _allowed_cache is not None:
        return _allowed_cache
    sql = """
        SELECT DISTINCT c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema = 'public'
          AND c.column_name = 'client_id'
          AND t.table_type IN ('BASE TABLE', 'VIEW')
    """
    with engine.connect() as cx:
        rows = cx.execute(text(sql)).scalars().all()
    allowed = {r for r in rows if r not in DENY_TABLES and not r.startswith("staging_")}
    # Materialized views aren't in information_schema.tables — add the MV the
    # ML tools rely on, if it exists.
    try:
        with engine.connect() as cx:
            mv = cx.execute(text(
                "SELECT 1 FROM pg_matviews WHERE matviewname='mv_customer_features'"
            )).first()
        if mv:
            allowed.add("mv_customer_features")
    except Exception:
        pass
    _allowed_cache = allowed
    return allowed


def is_allowed(engine, table: str) -> bool:
    return table in _allowed_tables(engine)


def compact_catalog(engine) -> str:
    """One line per allowed table (name + terse purpose) for the system prompt."""
    global _catalog_cache
    if _catalog_cache is not None:
        return _catalog_cache
    lines = []
    for t in sorted(_allowed_tables(engine)):
        purpose = TABLE_PURPOSES.get(t, "")
        lines.append(f"- {t}: {purpose}" if purpose else f"- {t}")
    _catalog_cache = "\n".join(lines)
    return _catalog_cache


def describe_tables(engine, names) -> str:
    """Columns + types for the named ALLOWED tables. Denied/unknown tables yield
    a refusal and never leak their columns."""
    out = []
    for raw in names:
        name = str(raw).strip().strip('"').lower()
        if not is_allowed(engine, name):
            out.append(
                f"Table '{name}' is not available for querying (restricted or unknown)."
            )
            continue
        with engine.connect() as cx:
            cols = cx.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:t
                ORDER BY ordinal_position
            """), {"t": name}).mappings().all()
        if not cols:
            out.append(f"Table '{name}' is not available for querying.")
            continue
        col_lines = ", ".join(f"{c['column_name']} ({c['data_type']})" for c in cols)
        out.append(
            f"{name} [tenant-scoped: filter WHERE client_id = :client_id]\n"
            f"  columns: {col_lines}"
        )
    return "\n\n".join(out)


def denied_tables_in_sql(sql: str) -> list:
    """Denied table names referenced in a SQL string (whole-word, case-insensitive).
    Heuristic but effective — catches FROM/JOIN of users, audit_log, etc."""
    low = (sql or "").lower()
    return sorted(t for t in DENY_TABLES if re.search(rf"\b{re.escape(t)}\b", low))
