"""
embedder.py — RAG indexing pipeline.

`reindex(client_id, source_type)` pulls a tenant's rows from a registered source
table, builds the text content + a sha256 content-hash, SKIPS rows whose hash is
already stored (idempotency), embeds the rest in batches, and upserts them into
rag_documents.

Phase 1 registers one source: `customer_review`. Adding a corpus = one entry in
SOURCES (fetch SQL scoped by :cid, a content builder, a metadata builder).

Runnable as a CLI:
    python -m rag.embedder --client CLT-002 --source customer_review
"""
import json
import hashlib
import logging

from sqlalchemy import text

from rag import store

log = logging.getLogger("crp_api.rag.embedder")


# ── Source registry ─────────────────────────────────────────────────────────

def _review_content(r) -> str:
    return (r.get("review_text") or "").strip()


def _review_metadata(r) -> dict:
    return {
        "rating": r.get("rating"),
        "sentiment": r.get("sentiment"),
        "review_date": str(r.get("review_date")) if r.get("review_date") else None,
        "customer_id": r.get("customer_id"),
    }


def _outreach_content(r) -> str:
    return (r.get("message_text") or "").strip()


def _outreach_metadata(r) -> dict:
    return {
        "message_type": r.get("message_type"),
        "trigger_reason": r.get("trigger_reason"),
        "channel": r.get("channel"),
        "outcome": r.get("outcome"),
        "responded": r.get("responded"),
        "customer_id": r.get("customer_id"),
        "sent_at": str(r.get("sent_at")) if r.get("sent_at") else None,
    }


def _brand_content(r) -> str:
    name = (r.get("brand_name") or "").strip()
    desc = (r.get("brand_description") or "").strip()
    return f"{name} — {desc}" if (name and desc) else (desc or name)


def _brand_metadata(r) -> dict:
    return {
        "brand_name": r.get("brand_name"),
        "category_hint": r.get("category_hint"),
        "active": r.get("active"),
    }


SOURCES = {
    "customer_review": {
        "fetch_sql": """
            SELECT review_id, review_text, rating, sentiment, review_date, customer_id
            FROM customer_reviews
            WHERE client_id = :cid
              AND review_text IS NOT NULL
              AND length(trim(review_text)) > 0
        """,
        "source_id": "review_id",
        "content": _review_content,
        "metadata": _review_metadata,
    },
    "outreach_message": {
        "fetch_sql": """
            SELECT message_id, message_text, message_type, trigger_reason,
                   channel, outcome, responded, customer_id, sent_at
            FROM outreach_messages
            WHERE client_id = :cid
              AND message_text IS NOT NULL
              AND length(trim(message_text)) > 0
        """,
        "source_id": "message_id",
        "content": _outreach_content,
        "metadata": _outreach_metadata,
    },
    "brand": {
        "fetch_sql": """
            SELECT brand_id, brand_name, brand_description, category_hint, active
            FROM brands
            WHERE client_id = :cid
              AND brand_description IS NOT NULL
              AND length(trim(brand_description)) > 0
        """,
        "source_id": "brand_id",
        "content": _brand_content,
        "metadata": _brand_metadata,
    },
}


# ── Embedding ────────────────────────────────────────────────────────────────

def _default_embed_documents(texts):
    from agent.llm import build_embeddings
    return build_embeddings().embed_documents(list(texts))


# ── Pipeline ─────────────────────────────────────────────────────────────────

def reindex(client_id, source_type, *, embed_fn=None, batch_size=100) -> dict:
    """Index/refresh one tenant's documents for a source. Idempotent: rows whose
    content hash is already stored are skipped (not re-embedded)."""
    if source_type not in SOURCES:
        raise ValueError(f"Unknown RAG source_type: {source_type!r}. "
                         f"Known: {list(SOURCES)}")
    cfg = SOURCES[source_type]

    from app.database import engine
    store.ensure_schema(engine)

    with engine.connect() as cx:
        rows = cx.execute(text(cfg["fetch_sql"]), {"cid": client_id}).mappings().all()

    existing = store.existing_hashes(engine, client_id, source_type)

    pending = []  # (source_id, content, content_hash, metadata_json)
    skipped = 0
    for r in rows:
        content = cfg["content"](r)
        if not content:
            continue
        source_id = str(r[cfg["source_id"]])
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if existing.get(source_id) == content_hash:
            skipped += 1
            continue
        meta_json = json.dumps(cfg["metadata"](r), default=str)
        pending.append((source_id, content, content_hash, meta_json))

    embedder = embed_fn or _default_embed_documents
    embedded = 0
    for i in range(0, len(pending), batch_size):
        batch = pending[i:i + batch_size]
        vectors = embedder([c for (_, c, _, _) in batch])
        with engine.begin() as cx:
            for (source_id, content, content_hash, meta_json), vec in zip(batch, vectors):
                store.upsert_document(
                    cx, client_id=client_id, source_type=source_type,
                    source_id=source_id, content=content,
                    content_hash=content_hash, embedding=vec,
                    metadata_json=meta_json,
                )
        embedded += len(batch)

    stats = {"fetched": len(rows), "embedded": embedded,
             "skipped": skipped, "total": embedded + skipped}
    log.info("reindex client=%s source=%s -> %s", client_id, source_type, stats)
    return stats


def _cli():
    import argparse
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser(description="Reindex a tenant's RAG documents.")
    ap.add_argument("--client", required=True, help="client_id, e.g. CLT-001")
    ap.add_argument("--source", default="customer_review", choices=list(SOURCES))
    args = ap.parse_args()
    stats = reindex(args.client, args.source)
    print(f"Reindexed {args.client}/{args.source}: {stats}")


if __name__ == "__main__":
    _cli()
