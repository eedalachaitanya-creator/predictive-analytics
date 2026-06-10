"""
retriever.py — query-time semantic search for the Analyst Agent's RAG tool.

Embeds the user's query and returns the top-k chunks from rag_documents, STRICTLY
scoped to the caller's tenant. The embedding function is injectable so unit tests
run offline; in production it uses OpenAI text-embedding-3-small via agent.llm.
"""
from rag import store


def _default_embed(query: str):
    from agent.llm import build_embeddings
    return build_embeddings().embed_query(query)


def search_documents(client_id, query, k=6, source_types=None, *, embed_fn=None):
    """Return up to k tenant-scoped chunks most similar to `query`.

    Args:
        client_id:    REQUIRED authenticated tenant — refuses to run without it.
        query:        natural-language query string.
        k:            max chunks to return.
        source_types: optional list to restrict which corpora to search.
        embed_fn:     optional callable(query)->vector for tests; defaults to OpenAI.
    """
    if not client_id:
        raise ValueError("search_documents requires a client_id for tenant scoping.")
    vec = (embed_fn or _default_embed)(query)
    from app.database import engine
    return store.search(engine, client_id, vec, k=k, source_types=source_types)
