-- migration_rag_documents.sql
-- RAG vector store for the Analyst Agent's semantic retrieval (Phase 1).
-- Run as a role allowed to CREATE EXTENSION (the app self-ensures this on
-- startup too, but managed deploys often restrict CREATE EXTENSION to a DBA).
--
-- Idempotent: safe to run repeatedly.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_documents (
    id           BIGSERIAL PRIMARY KEY,
    client_id    VARCHAR(20)  NOT NULL REFERENCES client_config(client_id),
    source_type  VARCHAR(40)  NOT NULL,          -- e.g. 'customer_review'
    source_id    VARCHAR(80)  NOT NULL,          -- natural id (review_id)
    content      TEXT         NOT NULL,
    content_hash CHAR(64)     NOT NULL,          -- sha256(content) for idempotency
    embedding    vector(1536) NOT NULL,          -- OpenAI text-embedding-3-small
    metadata     JSONB        DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (client_id, source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_rag_tenant ON rag_documents (client_id, source_type);
CREATE INDEX IF NOT EXISTS idx_rag_vec ON rag_documents
    USING hnsw (embedding vector_cosine_ops);
