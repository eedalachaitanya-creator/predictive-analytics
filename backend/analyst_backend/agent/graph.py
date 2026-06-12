"""
graph.py — Analyst Agent | LangGraph State Machine
====================================================
Defines the LangGraph graph that powers the Analyst Agent.
The agent uses OpenAI gpt-4o-mini as the reasoning engine (Groq fallback via
AGENT_MODEL / LLM_PROVIDER) and binds the tools from tools.py for data access,
ML inference, and RAG retrieval over customer feedback.

Architecture:
    ┌──────────┐
    │  START   │
    └────┬─────┘
         │
    ┌────▼─────┐     tool_calls     ┌───────────┐
    │  agent   │──────────────────▶│   tools   │
    │  (LLM)   │◀──────────────────│ (execute) │
    └────┬─────┘     results        └───────────┘
         │
     no tool_calls
         │
    ┌────▼─────┐
    │   END    │
    └──────────┘

Requirements:
    pip install langgraph langchain-groq langchain-core python-dotenv
"""

import os
import logging
from typing import Annotated, TypedDict

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from dotenv import load_dotenv

from agent.tools import ALL_TOOLS, current_client_id
from agent.llm import build_chat_model

load_dotenv()

log = logging.getLogger("analyst_agent.graph")


# ═══════════════════════════════════════════════════════════════════════════
# 1. STATE DEFINITION
# ═══════════════════════════════════════════════════════════════════════════

class AgentState(TypedDict):
    """
    State that flows through the graph.
    `messages` accumulates the full conversation (system + human + AI + tool).
    The `add_messages` annotation tells LangGraph to *append* new messages
    rather than overwriting the list.
    """
    messages: Annotated[list[BaseMessage], add_messages]


# ═══════════════════════════════════════════════════════════════════════════
# 2. SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT_TEMPLATE = """You are the CRP Analyst Agent, a data analyst for a retail churn platform.
You are answering for tenant {client_id} only. All tools are already scoped to this tenant.

Prefer specialized tools over raw SQL:
- "How many at high/medium/low risk?" -> get_risk_summary
- "Show at-risk customers" -> search_at_risk_customers
- "Profile customer X" -> get_customer_profile
- "Predict churn for X" -> predict_churn
- "What drives churn?" -> get_feature_importance
- "What are customers saying / complaining about / why unhappy?" -> search_customer_feedback
- Only use query_database when the above tools cannot answer.

Choosing structured vs. semantic:
- Counts, rankings, averages, filters, customer IDs -> structured tools / query_database.
- Opinions, themes, reasons, complaints, "what are people saying" -> search_customer_feedback,
  then summarize the reviews it returns. Quote specifics from the tool output; never
  invent feedback that the tool did not return.

When using query_database:
- Always include `WHERE client_id = :client_id` (use the bind param :client_id, no quotes around it).
- Add LIMIT 50 unless the user asks for more.
- SELECT only.

Thresholds: HIGH >= 0.65, MEDIUM >= 0.35, LOW < 0.35.
Be concise. Back answers with numbers from tool output.
"""


def _build_system_prompt(client_id: str) -> str:
    """Render the system prompt with the current tenant id baked in, plus the
    curated table catalog so the agent can answer structured questions across
    all business tables (not just the specialized tools)."""
    prompt = SYSTEM_PROMPT_TEMPLATE.format(client_id=client_id)

    # Scope policy (second layer behind agent/scope_guard's pre-flight gate):
    # the Analyst must refuse anything outside its churn/retention domain instead
    # of being a general-purpose assistant.
    from agent.scope_guard import SCOPE_DESCRIPTION, SCOPE_REFUSAL
    prompt += (
        f"\n\nSCOPE — you ONLY answer questions about: {SCOPE_DESCRIPTION}\n"
        "If the user asks anything outside this scope (general programming or "
        "coding, math, trivia, world knowledge, writing unrelated content, etc.), "
        "do NOT answer it and do NOT provide the requested content. Reply with "
        f"exactly: \"{SCOPE_REFUSAL}\""
    )
    try:
        from app.database import engine
        from agent.schema_catalog import compact_catalog
        catalog = compact_catalog(engine)
        prompt += (
            "\n\nQUERYABLE TABLES (all tenant-scoped by client_id). For a structured "
            "question the specialized tools don't cover:\n"
            '1) pick relevant tables below, 2) call describe_schema("t1, t2") to get '
            "their exact columns, 3) write query_database SELECT ... WHERE client_id = "
            ":client_id, 4) if query_database returns an error, call describe_schema for "
            "the correct names and retry (up to twice) before giving up. Auth/system "
            "tables are NOT listed and are off-limits.\n"
            f"{catalog}"
        )
    except Exception:
        pass  # never break the agent if catalog introspection is unavailable
    return prompt


# ═══════════════════════════════════════════════════════════════════════════
# 3. LLM SETUP
# ═══════════════════════════════════════════════════════════════════════════

def _build_llm():
    """Create the chat model (provider chosen by agent.llm) with tools bound."""
    # Provider/model selection lives in agent.llm.build_chat_model: OpenAI
    # gpt-4o-mini by default, Groq only when AGENT_MODEL/LLM_PROVIDER selects it.
    # Moving off Groq's free-tier ~6k token/min ceiling is what lets us feed
    # retrieved RAG context into the prompt.
    llm = build_chat_model(temperature=0.1, max_tokens=4096)

    # NOTE: LangFuse cost tracking is done AFTER the LLM call in agent_node()
    # via track_cost(), not via a LangChain CallbackHandler. The handler path
    # pulled in langchain_core.pydantic_v1 which was removed in langchain-core
    # 1.x and crashed the agent mid-invocation. Direct SDK calls are stable.

    # Bind the tools so the model can call them
    return llm.bind_tools(ALL_TOOLS)


# ═══════════════════════════════════════════════════════════════════════════
# 4. GRAPH NODES
# ═══════════════════════════════════════════════════════════════════════════

def agent_node(state: AgentState) -> dict:
    """
    The 'agent' node — sends the conversation to the LLM and gets a response.
    If the response contains tool_calls, the graph routes to the tools node.
    Otherwise, it routes to END.
    """
    llm = _build_llm()
    messages = state["messages"]

    # Pull the tenant id set by ask_agent() and bake it into the system prompt
    # so the LLM knows which client it's serving. Fallback to a clearly-marked
    # "unknown" if the ContextVar wasn't set — that case is caught loudly by
    # tools._get_client_id() anyway.
    tenant = current_client_id.get() or "UNKNOWN"
    system_prompt = _build_system_prompt(tenant)

    # Inject (or replace) the system prompt so it reflects the current tenant.
    if messages and isinstance(messages[0], SystemMessage):
        messages = [SystemMessage(content=system_prompt)] + list(messages[1:])
    else:
        messages = [SystemMessage(content=system_prompt)] + list(messages)

    response = llm.invoke(messages)

    # ── LangFuse cost tracking (direct SDK, no LangChain callback) ──
    # Groq returns token usage in response.response_metadata["token_usage"].
    # We swallow every error here — cost tracking must never break the agent.
    try:
        usage = getattr(response, "response_metadata", {}).get("token_usage", {}) \
            or getattr(response, "usage_metadata", {}) or {}
        input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
        output_tokens = usage.get("completion_tokens") or usage.get("output_tokens") or 0
        if input_tokens or output_tokens:
            from app.langfuse_tracker import track_cost
            track_cost(
                input_tokens=int(input_tokens),
                output_tokens=int(output_tokens),
                model=os.getenv("AGENT_MODEL") or "gpt-4o-mini",
                call_type="analyst_agent_query",
                client_id=tenant,
            )
    except Exception:
        pass  # never break the agent over telemetry

    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    """
    Edge function — decides whether the agent should continue to tool
    execution or end the turn.

    Returns:
        "tools"  — if the last message has tool_calls (agent wants data)
        "end"    — if the last message has no tool_calls (agent is done)
    """
    last_message = state["messages"][-1]

    # If the LLM issued tool calls, route to the tools node
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"

    return "end"


# ═══════════════════════════════════════════════════════════════════════════
# 5. BUILD THE GRAPH
# ═══════════════════════════════════════════════════════════════════════════

def build_graph():
    """
    Construct and compile the LangGraph state machine.

    Graph topology:
        START ──▶ agent ──▶ (should_continue?)
                               │ "tools" ──▶ tools ──▶ agent  (loop)
                               │ "end"   ──▶ END
    """
    # Create ToolNode from our tool list
    tool_node = ToolNode(ALL_TOOLS)

    # ④ tool-output guard: every tool result is sanitized before it re-enters the
    # model context. Tool outputs are attacker-influenced — DB rows, customer
    # reviews, and scraped text can carry an injection planted in a customer field.
    def guarded_tools(state):
        from app.llm_gateway import guard_tool
        result = tool_node.invoke(state)
        msgs = result["messages"] if isinstance(result, dict) and "messages" in result else (result or [])
        guarded = []
        for m in msgs:
            content = getattr(m, "content", None)
            if isinstance(content, str):
                clean = guard_tool(content)
                if clean != content:
                    m = m.model_copy(update={"content": clean})
            guarded.append(m)
        return {"messages": guarded}

    # Initialize graph
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("agent", agent_node)
    graph.add_node("tools", guarded_tools)

    # Set entry point
    graph.set_entry_point("agent")

    # Add conditional edge from agent
    graph.add_conditional_edges(
        "agent",
        should_continue,
        {
            "tools": "tools",
            "end": END,
        },
    )

    # After tools execute, always go back to agent for reasoning
    graph.add_edge("tools", "agent")

    # Compile
    compiled = graph.compile()
    log.info("LangGraph agent compiled successfully (%d tools bound)", len(ALL_TOOLS))
    return compiled


# ═══════════════════════════════════════════════════════════════════════════
# 6. CONVENIENCE — single-turn invocation
# ═══════════════════════════════════════════════════════════════════════════

_graph_instance = None


def get_agent():
    """Get or create the singleton compiled graph."""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = build_graph()
    return _graph_instance


def ask_agent(
    question: str,
    history: list[BaseMessage] | None = None,
    client_id: str | None = None,
) -> str:
    """
    Send a question to the agent and return the final text response.

    Args:
        question:  The user's question in natural language
        history:   Optional prior conversation messages
        client_id: REQUIRED for multi-tenant scoping — the authenticated
                   tenant making this request. Stored in a ContextVar so
                   every tool call reads it and adds `WHERE client_id = :cid`
                   to its SQL. Passing None raises so we never accidentally
                   run the agent with global access.

    Returns:
        The agent's final text answer
    """
    if not client_id:
        raise ValueError(
            "ask_agent requires a client_id — refusing to run the agent "
            "without tenant scoping."
        )

    # Domain scope gate: the Analyst answers ONLY churn / retention / customer-data
    # questions. Off-topic requests (general coding, math, trivia, …) are refused
    # HERE — before the agent is built or any tool runs. Fail-open: a classifier
    # error lets the question through rather than wrongly refusing a real one.
    from agent.scope_guard import is_in_scope, SCOPE_REFUSAL
    if not is_in_scope(question):
        return SCOPE_REFUSAL

    agent = get_agent()

    messages = []
    if history:
        messages.extend(history)
    messages.append(HumanMessage(content=question))

    # Set the tenant in a ContextVar for the duration of this request. Every
    # tool reads from this, and the ContextVar is isolated per request (no
    # cross-talk between concurrent users).
    token = current_client_id.set(client_id)
    try:
        result = agent.invoke({"messages": messages})
    finally:
        # Always reset — leaving it set could leak into the next request
        # on the same worker if something else touches it.
        current_client_id.reset(token)

    # Extract the last AI message and guard it on the way out (⑤ egress: redact
    # any injection the model echoed back, before it reaches the user).
    from app.llm_gateway import guard_response
    for msg in reversed(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            return guard_response(msg.content)

    return "Agent did not produce a response."
