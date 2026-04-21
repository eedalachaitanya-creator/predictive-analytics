-- ============================================================
-- migration_teammate_pricing_tables.sql
--
-- Adds three tables from your teammate's predictive_analysis_final dump
-- that are not yet in our project:
--
--   1. pricing_recommendations     — per-run pricing engine output
--                                    (one row per product per run)
--   2. customer_price_context      — per-customer price suggestion
--                                    (links a customer to a pricing run)
--   3. llm_cost_log                — per-call LLM cost tracking
--                                    (Groq/OpenAI/etc.)
--
-- Everything else in the teammate's dump (48 tables) already exists in
-- our project via earlier migrations, so this file is the ONLY delta.
--
-- WHY keep them:
--   Our Strategist Agent (retention pricing) and Cost Tracking page are
--   built against these tables. Without them, Strategist runs fail with
--   "relation does not exist" and Cost Tracking has no log to read.
--
-- WHEN TO RUN:
--   Once, on the live walmart_crp database. Idempotent — every CREATE
--   uses IF NOT EXISTS.
--
-- HOW TO RUN (pgAdmin 4):
--   1. Open pgAdmin 4 → Databases → walmart_crp.
--   2. Right-click walmart_crp → Query Tool.
--   3. Paste this file's contents → Execute (F5).
--   4. Re-run steps 5 and 6 below to confirm the new tables exist.
-- ============================================================

-- 1. pricing_recommendations ----------------------------------------------
-- One row per (run_id, product_name). Holds the price the engine
-- suggests plus the competitor / cost context it used to decide.
CREATE TABLE IF NOT EXISTS pricing_recommendations (
    recommendation_id   BIGSERIAL PRIMARY KEY,
    run_id              UUID            NOT NULL,
    client_id           VARCHAR(50)     NOT NULL,
    product_name        TEXT            NOT NULL,
    suggested_price     NUMERIC(12,2),
    pre_retention_price NUMERIC(12,2),
    floor_price         NUMERIC(12,2),
    target_price        NUMERIC(12,2),
    our_cost            NUMERIC(12,2),
    raw_cogs            NUMERIC(12,2),
    competitor_min      NUMERIC(12,2),
    competitor_avg      NUMERIC(12,2),
    competitor_max      NUMERIC(12,2),
    competitor_median   NUMERIC(12,2),
    strategy            VARCHAR(30),
    confidence          VARCHAR(10),
    margin_percent      NUMERIC(6,2),
    market_trend        VARCHAR(10),
    flag                VARCHAR(50),
    reasoning           TEXT,
    platform_breakdown  JSONB,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pricing_rec_client_run
    ON pricing_recommendations (client_id, run_id);

CREATE INDEX IF NOT EXISTS idx_pricing_rec_product
    ON pricing_recommendations (client_id, product_name);


-- 2. customer_price_context -----------------------------------------------
-- Joins a customer to the price the engine would offer them for a given
-- product, tagged with churn context. UNIQUE (customer_id, product_name)
-- means one live suggestion per customer per product — the newest run
-- overwrites the previous.
CREATE TABLE IF NOT EXISTS customer_price_context (
    context_id           BIGSERIAL PRIMARY KEY,
    customer_id          VARCHAR(100)  NOT NULL,
    client_id            VARCHAR(50)   NOT NULL,
    product_name         TEXT          NOT NULL,
    strategy             VARCHAR(30),
    suggested_price      NUMERIC(12,2),
    pre_retention_price  NUMERIC(12,2),
    discount_pct_applied NUMERIC(6,2),
    churn_probability    NUMERIC(5,4),
    risk_tier            VARCHAR(10),
    run_id               UUID,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_customer_price_context UNIQUE (customer_id, product_name)
);

CREATE INDEX IF NOT EXISTS idx_cpc_client_customers
    ON customer_price_context (client_id, customer_id);


-- 3. llm_cost_log ---------------------------------------------------------
-- Append-only log of every LLM call (Groq, OpenAI, etc.) with token
-- counts and cost. The Cost Tracking page reads from this table; the
-- over_budget flag lets us highlight runs that exceeded the per-client
-- budget from client_config.
CREATE TABLE IF NOT EXISTS llm_cost_log (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(20)    NOT NULL,
    call_type       VARCHAR(50)    NOT NULL,
    model           VARCHAR(100)   NOT NULL,
    input_tokens    INTEGER        NOT NULL DEFAULT 0,
    output_tokens   INTEGER        NOT NULL DEFAULT 0,
    total_tokens    INTEGER        NOT NULL DEFAULT 0,
    input_cost_usd  NUMERIC(12,8)  NOT NULL DEFAULT 0,
    output_cost_usd NUMERIC(12,8)  NOT NULL DEFAULT 0,
    total_cost_usd  NUMERIC(12,8)  NOT NULL DEFAULT 0,
    over_budget     BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_cost_log_client_created
    ON llm_cost_log (client_id, created_at DESC);


-- Friendly comments for pgAdmin's table browser
COMMENT ON TABLE pricing_recommendations
    IS 'Strategist Agent — pricing engine output (one row per run per product)';
COMMENT ON TABLE customer_price_context
    IS 'Strategist Agent — per-customer price suggestion from latest run';
COMMENT ON TABLE llm_cost_log
    IS 'Cost Tracking — per-call LLM usage + dollar cost';


-- 4. Sanity check: list the three tables and their column counts
SELECT  c.relname                                AS table_name,
        COUNT(a.attname)                         AS columns,
        pg_size_pretty(pg_relation_size(c.oid))  AS size
FROM    pg_class     c
JOIN    pg_attribute a  ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE   c.relname IN ('pricing_recommendations',
                      'customer_price_context',
                      'llm_cost_log')
GROUP BY c.relname, c.oid
ORDER BY c.relname;
