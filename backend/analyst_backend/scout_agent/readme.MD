"""
main.py integration — add these two lines to wire the agent in.

1. After the other imports (around line 51), add:
───────────────────────────────────────────────────────────
    from agent.routes import router as agent_router
───────────────────────────────────────────────────────────

2. After app.add_middleware(...) (around line 74), add:
───────────────────────────────────────────────────────────
    app.include_router(agent_router, prefix="/agent", tags=["agent"])
───────────────────────────────────────────────────────────

That's it. The agent is now live at:
    POST /agent/chat
    GET  /agent/history/{session_id}
    DELETE /agent/session/{session_id}

Example curl:
    curl -X POST http://localhost:8000/agent/chat \\
         -H "Content-Type: application/json" \\
         -d '{"message": "Where is boAt Airdopes 141 cheapest?", "session_id": "user1"}'
"""