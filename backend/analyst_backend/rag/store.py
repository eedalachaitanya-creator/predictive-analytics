"""
store.py — pgvector-backed document store for RAG.

Owns the `rag_documents` schema and the low-level upsert / search SQL. Vectors
are passed as bracketed string literals cast with `::vector`, so NO `pgvector`
Python package is required — only the Postgres `vector` extension (which is
available on the cluster). This keeps the dependency surface minimal and reuses
the existing SQLAlchemy/psycopg2 engine.

Tenancy: every read is filtered by `client_id`. The same column carries a FK to
client_config(client_id), identical to chat_messages, so a typo tenant id can't
be written and the LLM can never reach another tenant's chunks.
"""
import logging
from sqlalchemy import text

log = logging.getLogger("crp_api.rag.store")

EMBED_DIM = 1536

ENSURE_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_documents (
    id           BIGSERIAL PRIMARY KEY,
    client_id    VARCHAR(20)  NOT NULL REFERENCES client_config(client_id),
    source_type  VARCHAR(40)  NOT NULL,
    source_id    VARCHAR(80)  NOT NULL,
    content      TEXT         NOT NULL,
    content_hash CHAR(64)     NOT NULL,
    embedding    vector(1536) NOT NULL,
    metadata     JSONB        DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (client_id, source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_rag_tenant ON rag_documents (client_id, source_type);
CREATE INDEX IF NOT EXISTS idx_rag_vec ON rag_documents
    USING hnsw (embedding vector_cosine_ops);
"""


def ensure_schema(engine) -> None:
    """Idempotently create the extension, table, and indexes."""
    with engine.begin() as cx:
        cx.execute(text(ENSURE_SQL))


def to_pgvector(values) -> str:
    """Render a float sequence as a pgvector literal, e.g. '[0.1,0.2,...]'."""
    return "[" + ",".join(repr(float(v)) for v in values) + "]"


def upsert_document(cx, *, client_id, source_type, source_id, content,
                    content_hash, embedding, metadata_json="{}") -> None:
    """Insert or update one document chunk. `cx` is an open transaction conn so
    callers can batch many upserts in one transaction. `embedding` is a float
    sequence."""
    cx.execute(text("""
        INSERT INTO rag_documents
            (client_id, source_type, source_id, content, content_hash,
             embedding, metadata)
        VALUES
            (:cid, :st, :sid, :content, :hash, CAST(:emb AS vector), CAST(:meta AS jsonb))
        ON CONFLICT (client_id, source_type, source_id) DO UPDATE SET
            content      = EXCLUDED.content,
            content_hash = EXCLUDED.content_hash,
            embedding    = EXCLUDED.embedding,
            metadata     = EXCLUDED.metadata,
            created_at   = NOW()
    """), {
        "cid": client_id, "st": source_type, "sid": source_id,
        "content": content, "hash": content_hash,
        "emb": to_pgvector(embedding), "meta": metadata_json,
    })


def existing_hashes(engine, client_id, source_type) -> dict:
    """Return {source_id: content_hash} already stored for this tenant+source,
    so the embedder can skip rows whose content is unchanged (idempotency)."""
    with engine.connect() as cx:
        rows = cx.execute(text("""
            SELECT source_id, content_hash
            FROM rag_documents
            WHERE client_id = :cid AND source_type = :st
        """), {"cid": client_id, "st": source_type}).mappings().all()
    return {r["source_id"]: (r["content_hash"] or "").strip() for r in rows}


def search(engine, client_id, query_vec, k=6, source_types=None, dedup=True) -> list:
    """Cosine-distance ANN search, scoped to one tenant. Returns dicts with
    content, source_type, source_id, metadata, distance (lower = closer).

    dedup=True (default) collapses identical-content chunks (the synthetic
    review data repeats text), keeping the closest of each distinct content and
    still returning up to k chunks — by over-fetching candidates first.
    """
    k = int(k)
    fetch = min(max(k * 5, k), 200) if dedup else k
    params = {"cid": client_id, "qv": to_pgvector(query_vec), "lim": fetch}
    src_clause = ""
    if source_types:
        src_clause = "AND source_type = ANY(:src)"
        params["src"] = list(source_types)
    sql = f"""
        SELECT source_type, source_id, content, metadata,
               (embedding <=> CAST(:qv AS vector)) AS distance
        FROM rag_documents
        WHERE client_id = :cid {src_clause}
        ORDER BY embedding <=> CAST(:qv AS vector)
        LIMIT :lim
    """
    with engine.connect() as cx:
        rows = cx.execute(text(sql), params).mappings().all()
    if not dedup:
        return [dict(r) for r in rows[:k]]
    seen, out = set(), []
    for r in rows:
        key = (r["content"] or "").strip()
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(r))
        if len(out) >= k:
            break
    return out
