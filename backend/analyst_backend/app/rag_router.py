"""
rag_router.py — admin endpoint to (re)build the RAG vector index.

POST /api/v1/rag/reindex  (super_admin only)
    Body: {clientId, sourceType?} -> embeds that tenant's source rows into
    rag_documents. It is slow and spends OpenAI embedding tokens, so it is gated
    to super_admin. Retrieval (search_customer_feedback) stays tenant-scoped
    regardless of who triggers the reindex.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth_router import get_current_user

log = logging.getLogger("crp_api.rag")

router = APIRouter(prefix="/api/v1/rag", tags=["rag"])


class ReindexRequest(BaseModel):
    clientId: str
    sourceType: str = "customer_review"


def _require_super_admin(user: dict) -> None:
    if user.get("role") == "super_admin" or "*" in (user.get("clientAccess") or []):
        return
    raise HTTPException(status_code=403, detail="RAG reindex is super_admin only.")


@router.post("/reindex")
def reindex_endpoint(req: ReindexRequest, user: dict = Depends(get_current_user)):
    """Rebuild the vector index for one tenant + source. super_admin only."""
    _require_super_admin(user)
    from rag.embedder import reindex, SOURCES
    if req.sourceType not in SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown sourceType {req.sourceType!r}. Known: {list(SOURCES)}",
        )
    try:
        stats = reindex(req.clientId, req.sourceType)
    except Exception as e:
        log.error("Reindex failed (client=%s source=%s): %s",
                  req.clientId, req.sourceType, e)
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok", "clientId": req.clientId,
            "sourceType": req.sourceType, "stats": stats}
