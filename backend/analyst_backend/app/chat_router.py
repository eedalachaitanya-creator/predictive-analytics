"""
chat_router.py — Agent Chat API for the Angular frontend
==========================================================
Endpoints:
    POST /api/v1/chat/ask                  — Send a question, get agent response
    GET  /api/v1/chat/history              — Get conversation history
    POST /api/v1/chat/clear                — Clear conversation history
    GET  /api/v1/chat/suggestions          — Get example questions

The agent uses LangGraph + Groq to answer churn analytics questions
with access to 6 tools (SQL queries, ML predictions, customer profiles, etc.)

Chat history is stored in PostgreSQL so teammates can see past queries
when they import the database dump.
"""

import logging
import uuid
import threading
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine

log = logging.getLogger("crp_api.chat")

router = APIRouter(prefix="/api/v1/chat", tags=["chat"])


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE TABLE
# ═══════════════════════════════════════════════════════════════════════════

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chat_messages (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(20) NOT NULL DEFAULT 'CLT-001',
    conversation_id VARCHAR(50) NOT NULL,
    role            VARCHAR(10) NOT NULL CHECK (role IN ('user', 'assistant')),
    content         TEXT NOT NULL,
    tokens_used     INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_conv ON chat_messages(conversation_id, created_at);
CREATE INDEX IF NOT EXISTS idx_chat_client ON chat_messages(client_id, created_at DESC);
"""


def _ensure_table():
    """Create chat_messages table if it doesn't exist."""
    try:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE_SQL))
    except Exception as e:
        log.warning("Could not create chat_messages table: %s", e)


# Create table on module load
_ensure_table()


# ═══════════════════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    question: str
    conversationId: str | None = None
    clientId: str = "CLT-001"


class ChatMessage(BaseModel):
    id: int | None = None
    role: str
    content: str
    timestamp: str | None = None


class ChatResponse(BaseModel):
    answer: str
    conversationId: str
    timestamp: str
    messages: list[ChatMessage] = []


# ═══════════════════════════════════════════════════════════════════════════
# HELPER — SAVE MESSAGE TO DB
# ═══════════════════════════════════════════════════════════════════════════

def _save_message(client_id: str, conversation_id: str, role: str, content: str):
    """Save a single message to the database."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO chat_messages (client_id, conversation_id, role, content)
                VALUES (:client_id, :conversation_id, :role, :content)
            """), {
                "client_id": client_id,
                "conversation_id": conversation_id,
                "role": role,
                "content": content,
            })
    except Exception as e:
        log.warning("Failed to save chat message: %s", e)


def _get_history(client_id: str, conversation_id: str) -> list[dict]:
    """Get conversation history from the database."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, role, content, created_at
                FROM chat_messages
                WHERE client_id = :client_id AND conversation_id = :conversation_id
                ORDER BY created_at ASC
            """), {
                "client_id": client_id,
                "conversation_id": conversation_id,
            }).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _get_langchain_history(client_id: str, conversation_id: str):
    """Convert DB history to LangChain message objects for the agent."""
    from langchain_core.messages import HumanMessage, AIMessage

    messages = []
    rows = _get_history(client_id, conversation_id)
    for row in rows:
        if row["role"] == "user":
            messages.append(HumanMessage(content=row["content"]))
        else:
            messages.append(AIMessage(content=row["content"]))
    return messages


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/ask", response_model=ChatResponse)
def ask_agent_endpoint(req: ChatRequest):
    """
    Send a question to the Analyst Agent and get a response.

    The agent has access to 6 tools:
      - query_database (SQL)
      - predict_churn (ML model)
      - get_customer_profile (360-degree view)
      - get_risk_summary (aggregate stats)
      - get_feature_importance (churn drivers)
      - search_at_risk_customers (filter/search)
    """
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    # Generate or reuse conversation ID
    conv_id = req.conversationId or f"conv-{uuid.uuid4().hex[:12]}"

    # Save user message
    _save_message(req.clientId, conv_id, "user", req.question)

    # Get prior conversation history for context
    history = _get_langchain_history(req.clientId, conv_id)

    # Invoke the agent
    # Pass the authenticated clientId so every tool the agent calls is
    # automatically scoped to this tenant — prevents cross-client data leaks
    # (e.g. Costco user seeing Walmart high-risk customers).
    try:
        from agent.graph import ask_agent
        answer = ask_agent(
            req.question,
            history=history[:-1],  # exclude the just-added user msg
            client_id=req.clientId,
        )
    except Exception as e:
        log.error("Agent error: %s", e)
        answer = f"I encountered an error processing your question: {str(e)[:200]}. Please try rephrasing."

    # Save assistant response
    _save_message(req.clientId, conv_id, "assistant", answer)

    # Get updated history
    db_history = _get_history(req.clientId, conv_id)
    messages = [
        ChatMessage(
            id=row["id"],
            role=row["role"],
            content=row["content"],
            timestamp=row["created_at"].isoformat() if row.get("created_at") else None,
        )
        for row in db_history
    ]

    return ChatResponse(
        answer=answer,
        conversationId=conv_id,
        timestamp=datetime.now().isoformat(),
        messages=messages,
    )


@router.get("/history")
def get_chat_history(
    clientId: str = Query("CLT-001"),
    conversationId: str = Query(None),
):
    """
    Get chat history.
    - If conversationId is given, returns that conversation's messages.
    - If not, returns a list of all conversations with their latest message.
    """
    if conversationId:
        rows = _get_history(clientId, conversationId)
        return {
            "conversationId": conversationId,
            "messages": [
                {
                    "id": r["id"],
                    "role": r["role"],
                    "content": r["content"],
                    "timestamp": r["created_at"].isoformat() if r.get("created_at") else None,
                }
                for r in rows
            ],
        }

    # Return list of conversations
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT conversation_id,
                       MIN(created_at) AS started_at,
                       MAX(created_at) AS last_message_at,
                       COUNT(*) AS message_count
                FROM chat_messages
                WHERE client_id = :client_id
                GROUP BY conversation_id
                ORDER BY MAX(created_at) DESC
                LIMIT 50
            """), {"client_id": clientId}).mappings().all()

        return {
            "conversations": [
                {
                    "conversationId": r["conversation_id"],
                    "startedAt": r["started_at"].isoformat() if r.get("started_at") else None,
                    "lastMessageAt": r["last_message_at"].isoformat() if r.get("last_message_at") else None,
                    "messageCount": r["message_count"],
                }
                for r in rows
            ]
        }
    except Exception as e:
        log.error("Failed to list conversations: %s", e)
        return {"conversations": []}


@router.post("/clear")
def clear_history(clientId: str = Query("CLT-001"), conversationId: str = Query(None)):
    """Clear chat history for a conversation or all conversations."""
    try:
        with engine.begin() as conn:
            if conversationId:
                conn.execute(text("""
                    DELETE FROM chat_messages
                    WHERE client_id = :client_id AND conversation_id = :conversation_id
                """), {"client_id": clientId, "conversation_id": conversationId})
            else:
                conn.execute(text("""
                    DELETE FROM chat_messages WHERE client_id = :client_id
                """), {"client_id": clientId})
        return {"status": "cleared"}
    except Exception as e:
        log.error("Failed to clear history: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions")
def get_suggestions():
    """Return example questions the user can click to try."""
    return {
        "suggestions": [
            {"text": "How many customers are at high churn risk?", "icon": "📊"},
            {"text": "Show me the top 5 at-risk Platinum customers", "icon": "🔍"},
            {"text": "What features drive churn the most?", "icon": "📈"},
            {"text": "Get the profile of WMT-CUST-00042", "icon": "👤"},
            {"text": "What is the average churn probability by tier?", "icon": "📉"},
            {"text": "Find customers with open support tickets and high churn risk", "icon": "🎫"},
        ]
    }
