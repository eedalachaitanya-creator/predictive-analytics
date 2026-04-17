#!/usr/bin/env bash
# ============================================================
# setup_postgresql.sh — One-shot PostgreSQL setup for
# Customer Retention Platform (Analyst Agent)
# ============================================================
# Usage:
#   chmod +x setup_postgresql.sh
#   ./setup_postgresql.sh
# ============================================================

set -euo pipefail

DB_NAME="walmart_crp"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
SCHEMA_FILE="$(dirname "$0")/schema_postgresql.sql"
EXCEL_FILE="../../walmart_raw_data_template_v5.xlsx"

echo "=============================================="
echo " Customer Retention Platform — DB Setup"
echo "=============================================="

# 1. Create database (ignore if exists)
echo ""
echo "[1/4] Creating database '$DB_NAME'..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -tc \
    "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" \
    | grep -q 1 \
    || psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" \
       -c "CREATE DATABASE $DB_NAME;"
echo "      ✓ Database ready"

# 2. Apply schema
echo ""
echo "[2/4] Applying schema..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -f "$SCHEMA_FILE"
echo "      ✓ Schema applied (18 tables + indexes + views)"

# 3. Load data from Excel
echo ""
echo "[3/4] Loading data from $EXCEL_FILE ..."
python3 "$(dirname "$0")/load_data.py" \
    --excel "$EXCEL_FILE" \
    --db-url "postgresql://$DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo "      ✓ Data loaded"

# 4. Verify
echo ""
echo "[4/4] Verifying row counts..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "
SELECT table_name, (xpath('/row/cnt/text()',
    query_to_xml(format('SELECT COUNT(*) AS cnt FROM %I', table_name), false, true, '')))[1]::text::int AS row_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY row_count DESC;
"

echo ""
echo "=============================================="
echo " ✅ PostgreSQL setup complete!"
echo " DB: postgresql://$DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo " Next step: run the RFM feature pipeline"
echo "   python3 ../pipeline/compute_rfm_features.py"
echo "=============================================="
