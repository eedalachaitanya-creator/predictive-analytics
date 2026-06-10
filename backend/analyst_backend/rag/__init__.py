"""rag/ — Retrieval-Augmented Generation for the CRP Analyst Agent.

store.py     — pgvector-backed document store (schema + upsert + search SQL)
retriever.py — query-time tenant-scoped semantic search
embedder.py  — indexing pipeline (source rows -> embeddings -> rag_documents)
"""
