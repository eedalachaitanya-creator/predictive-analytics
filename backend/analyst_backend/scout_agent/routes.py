"""
scout_agent/routes.py — FastAPI router for the Scout agent endpoints.

Mount this in main.py with:

    from agent.routes import router as agent_router
    app.include_router(agent_router, prefix="/agent", tags=["agent"])

Endpoints
─────────
    POST /agent/chat              — send a message, get a response
    DELETE /agent/session/{id}    — clear a session's memory
    GET  /agent/history/{id}      — retrieve conversation history
    GET  /agent/sessions          — list all active session IDs (debug)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from scout_agent.scout_agent import session_manager

router = APIRouter()


# ── Request / Response models ──────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"        # pass a UUID per browser tab / user


class ChatResponse(BaseModel):
    session_id: str
    message: str
    response: str


# ── Routes ─────────────────────────────────────────────────────────────

@router.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest):
    """
    Send a message to the Scout agent.

    Body:
        {
            "message": "Where is the Dyson V15 cheapest?",
            "session_id": "user-abc123"   // optional, defaults to "default"
        }

    The agent maintains per-session conversation memory. Pass the same
    session_id across requests to preserve context within a conversation.
    """
    if not payload.message.strip():
        raise HTTPException(400, "message cannot be empty.")

    agent = session_manager.get_or_create(payload.session_id)
    response = agent.chat(payload.message)

    return ChatResponse(
        session_id=payload.session_id,
        message=payload.message,
        response=response,
    )


@router.delete("/session/{session_id}")
def delete_session(session_id: str):
    """
    Clear a session's conversation memory.
    Call this when the user starts a new conversation or logs out.
    """
    session_manager.delete(session_id)
    return {"status": "cleared", "session_id": session_id}


@router.get("/history/{session_id}")
def get_history(session_id: str):
    """
    Retrieve the conversation history for a session.
    Returns a list of { role: "user" | "assistant", content: str } objects.
    """
    agent = session_manager.get_or_create(session_id)
    history = agent.get_history()
    return {
        "session_id": session_id,
        "messages": history,
        "count": len(history),
    }


@router.get("/sessions")
def list_sessions():
    """Debug endpoint — list all active session IDs."""
    return {
        "active_sessions": session_manager.active_sessions(),
        "count": len(session_manager.active_sessions()),
    }