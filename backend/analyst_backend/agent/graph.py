"""
graph.py — Analyst Agent | LangGraph State Machine
====================================================
Defines the LangGraph graph that powers the Analyst Agent.
The agent uses Google Gemini as the reasoning engine and binds
the 6 tools from tools.py for data access and ML inference.

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
from langchain_groq import ChatGroq
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from dotenv import load_dotenv

from agent.tools import ALL_TOOLS, current_client_id

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
- Only use query_database when the above tools cannot answer.

When using query_database:
- Always include `WHERE client_id = :client_id` (use the bind param :client_id, no quotes around it).
- Add LIMIT 50 unless the user asks for more.
- SELECT only.

Thresholds: HIGH >= 0.65, MEDIUM >= 0.35, LOW < 0.35.
Be concise. Back answers with numbers from tool output.
"""


def _build_system_prompt(client_id: str) -> str:
    """Render the system prompt with the current tenant id baked in."""
    return SYSTEM_PROMPT_TEMPLATE.format(client_id=client_id)


# ═══════════════════════════════════════════════════════════════════════════
# 3. LLM SETUP
# ═══════════════════════════════════════════════════════════════════════════

def _build_llm():
    """Create the ChatGroq instance with tools bound."""
    api_key = os.getenv("GROQ_API_KEY", "")
    model_name = os.getenv("AGENT_MODEL", "llama-3.3-70b-versatile")

    if not api_key:
        raise ValueError(
            "GROQ_API_KEY not set. Add it to your .env file."
        )

    llm = ChatGroq(
        model=model_name,
        temperature=0.1,          # low temp for analytical accuracy
        groq_api_key=api_key,
        max_tokens=4096,
    )

    # Attach LangFuse callback for cost tracking if available
    try:
        from app.langfuse_tracker import get_langfuse_handler
        handler = get_langfuse_handler(
            trace_name="analyst_agent_query",
            metadata={"model": model_name, "component": "agent"},
        )
        if handler:
            llm = llm.with_config({"callbacks": [handler]})
    except ImportError:
        pass  # LangFuse not available, continue without tracking

    # Bind the tools so the model can call them
    llm_with_tools = llm.bind_tools(ALL_TOOLS)
    return llm_with_tools


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

    # Initialize graph
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)

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

    # Extract the last AI message
    for msg in reversed(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            return msg.content

    return "Agent did not produce a response."
