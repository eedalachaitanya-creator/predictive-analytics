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
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.database import engine
from app.auth_router import get_current_user
from app.audit_logger import log_audit_event

log = logging.getLogger("crp_api.chat")

router = APIRouter(prefix="/api/v1/chat", tags=["chat"])

# ═══════════════════════════════════════════════════════════════════════════
# DATABASE TABLE
# ═══════════════════════════════════════════════════════════════════════════

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chat_messages (
    id              SERIAL PRIMARY KEY,
    -- Audit fix 2026-04-29: NO `DEFAULT 'CLT-001'` — every INSERT must
    -- supply client_id explicitly so the application's tenant-scoping
    -- can't be silently bypassed. The matching ALTER on the live DB
    -- happened in 2026_04_29_schema_audit_fixes.sql.
    client_id       VARCHAR(20) NOT NULL,
    conversation_id VARCHAR(50) NOT NULL,
    role            VARCHAR(10) NOT NULL CHECK (role IN ('user', 'assistant')),
    content         TEXT NOT NULL,
    tokens_used     INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Audit fix 2026-04-29: enforce tenant referential integrity. Without
-- this FK, deleting a client_config row leaves orphan chat_messages
-- and accepting a typo client_id silently writes "free-text" rows.
-- The constraint is added by 2026_04_29_schema_audit_fixes.sql on
-- existing deployments; this DO block makes fresh installs self-heal.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_chat_messages_client'
    ) THEN
        ALTER TABLE chat_messages
            ADD CONSTRAINT fk_chat_messages_client
            FOREIGN KEY (client_id) REFERENCES client_config(client_id);
    END IF;
END$$;

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
    """Save a single message to the database.

    Audit fix 2026-04-29: failures now log at ERROR (was WARNING) and
    include client_id / conversation_id / role for traceability. Still
    swallowed so a transient DB hiccup doesn't kill the request — the
    user still gets the agent's answer back — but the failure is now
    loud enough to spot in production logs.
    """
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
        log.error(
            "Failed to save chat message (client=%s conv=%s role=%s): %s",
            client_id, conversation_id, role, e,
        )


# Cap the number of messages we ever pull for a single conversation.
# Anything beyond this is older than the LLM ever sees (we slice further
# down to MAX_HISTORY_MESSAGES) and longer than any UI scrollback the
# product cares about. Bounds memory + payload size for runaway sessions.
DB_HISTORY_FETCH_CAP = 200


def _get_history(client_id: str, conversation_id: str) -> list[dict]:
    """Get conversation history from the database.

    Audit fix 2026-04-29:
      * SELECT now LIMIT'ed to DB_HISTORY_FETCH_CAP rows so a misbehaving
        conversation can't pull thousands of messages into memory. The
        ORDER BY DESC + reverse keeps the MOST RECENT N rows (older
        context is dropped, mirroring what the LLM actually sees).
      * DB errors log at ERROR with context instead of being silently
        swallowed. Returns [] on failure (consumers already handle that
        as "no history") but the operator now knows.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, role, content, created_at
                FROM chat_messages
                WHERE client_id = :client_id AND conversation_id = :conversation_id
                ORDER BY created_at DESC
                LIMIT :cap
            """), {
                "client_id": client_id,
                "conversation_id": conversation_id,
                "cap": DB_HISTORY_FETCH_CAP,
            }).mappings().all()
        # Reverse so the caller still sees rows in chronological order.
        return [dict(r) for r in reversed(rows)]
    except Exception as e:
        log.error(
            "Failed to load chat history (client=%s conv=%s): %s",
            client_id, conversation_id, e,
        )
        return []


# Maximum number of prior messages we replay to the LLM every turn.
# Groq's free-tier TPM ceiling on llama-3.1-8b-instant is ~6,000 tokens per
# minute. System prompt + tool definitions already eat ~1,500 tokens, so we
# cap the replay history at 6 messages (≈ 3 user + 3 assistant turns).
# Anything older is still persisted in Postgres — the user can see it in the
# UI — it just isn't re-sent to the LLM. Raising this cap risks HTTP 413.
MAX_HISTORY_MESSAGES = 6

# Hard per-message character cap, as a second line of defense against one
# very long prior answer (e.g. a big table) blowing the TPM budget even
# when MAX_HISTORY_MESSAGES is respected.
MAX_MESSAGE_CHARS = 2000


def _get_langchain_history(client_id: str, conversation_id: str):
    """Convert DB history to LangChain message objects for the agent.

    Returns only the most recent MAX_HISTORY_MESSAGES messages, each
    truncated to MAX_MESSAGE_CHARS characters. This keeps the total
    request size well below Groq's per-minute token ceiling.
    """
    from langchain_core.messages import HumanMessage, AIMessage

    rows = _get_history(client_id, conversation_id)

    # Keep only the most recent N rows (older context is dropped from the
    # LLM prompt but still exists in the DB / UI scrollback).
    rows = rows[-MAX_HISTORY_MESSAGES:]

    messages = []
    for row in rows:
        content = row["content"] or ""
        if len(content) > MAX_MESSAGE_CHARS:
            content = content[:MAX_MESSAGE_CHARS] + "\n... [truncated]"
        if row["role"] == "user":
            messages.append(HumanMessage(content=content))
        else:
            messages.append(AIMessage(content=content))
    return messages


# ═══════════════════════════════════════════════════════════════════════════
# TENANT ACCESS CHECK
# ═══════════════════════════════════════════════════════════════════════════
# super_admin carries clientAccess == ["*"] and can query any tenant; everyone
# else must have the requested client_id in their clientAccess list. This is
# the single choke-point that prevents a CLT-001 user from asking the agent
# about CLT-002 by swapping the body field.

def _require_client_access(user: dict, client_id: str) -> None:
    if user.get("role") == "super_admin" or "*" in (user.get("clientAccess") or []):
        return
    if client_id not in (user.get("clientAccess") or []):
        raise HTTPException(
            status_code=403,
            detail=f"You do not have access to client {client_id}",
        )


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/ask", response_model=ChatResponse)
def ask_agent_endpoint(req: ChatRequest, user: dict = Depends(get_current_user)):
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
    _require_client_access(user, req.clientId)
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
    user: dict = Depends(get_current_user),
):
    """
    Get chat history.
    - If conversationId is given, returns that conversation's messages.
    - If not, returns a list of all conversations with their latest message.
    """
    _require_client_access(user, clientId)
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
def clear_history(
    request: Request,
    clientId: str = Query("CLT-001"),
    conversationId: str = Query(None),
    user: dict = Depends(get_current_user),
):
    """Clear chat history for a conversation or all conversations.

    Audit fix 2026-04-29:
      * Docstring moved before the access check so it survives in
        function.__doc__ and FastAPI /docs.
      * Destructive DELETEs are now audit-logged with row count so a
        bulk "wipe-all-conversations" call leaves a trail.
      * RETURNING returns the deleted-row count so the UI / caller can
        confirm scope ("X messages cleared") and detect no-op calls.
    """
    _require_client_access(user, clientId)
    try:
        with engine.begin() as conn:
            if conversationId:
                deleted = conn.execute(text("""
                    DELETE FROM chat_messages
                    WHERE client_id = :client_id AND conversation_id = :conversation_id
                    RETURNING id
                """), {"client_id": clientId, "conversation_id": conversationId})
                row_count = len(deleted.fetchall())
                scope = f"conversation_id={conversationId}"
            else:
                deleted = conn.execute(text("""
                    DELETE FROM chat_messages WHERE client_id = :client_id
                    RETURNING id
                """), {"client_id": clientId})
                row_count = len(deleted.fetchall())
                scope = "ALL conversations for tenant"
    except Exception as e:
        log.error("Failed to clear history: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # Persist a tamper-resistant audit record for this destructive op.
    # Always run, even when row_count == 0, so we capture the *intent*
    # (someone asked to wipe history) not just successful deletions.
    log_audit_event(
        request,
        action_type="chat_history_clear",
        details=f"{scope} · {row_count} messages deleted",
        client_id=clientId,
        user_id=user.get("id"),
        user_email=user.get("email"),
        outcome="success",
    )
    return {"status": "cleared", "rows_deleted": row_count}


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
