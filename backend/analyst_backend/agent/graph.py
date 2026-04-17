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

from agent.tools import ALL_TOOLS

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

SYSTEM_PROMPT = """You are the **CRP Analyst Agent** — an expert data analyst for a Walmart-scale
retail platform. Your job is to help the business team understand customer churn
risk, identify at-risk customers, and recommend data-driven retention strategies.

**What you can do:**
1. Run read-only SQL queries against the customer database (PostgreSQL)
2. Predict churn probability for specific customers using a trained ML model
3. Pull full 360-degree customer profiles (orders, reviews, tickets, RFM scores)
4. Generate aggregate churn risk summaries
5. Show which features drive churn predictions (feature importance)
6. Search and filter at-risk customers by risk level, tier, or spend

**How you should respond:**
- Always back up your answers with data — use the tools to query the database or
  run predictions before stating conclusions.
- When discussing churn risk, include the actual probability and risk tier.
- If the user asks about a specific customer, pull their full profile first.
- For broad questions like "how many customers are at risk?", use the risk summary
  tool or run an aggregate SQL query.
- Be concise but thorough. Use tables/formatted output when showing data.
- If a query fails, explain the issue clearly and suggest an alternative approach.
- You are read-only — you cannot modify any data.
- Suggest retention actions when appropriate (e.g., targeted discounts for high-risk
  Platinum customers, outreach for customers with open support tickets).

**Key thresholds:**
- HIGH risk: churn probability >= 0.65
- MEDIUM risk: churn probability >= 0.35
- LOW risk: churn probability < 0.35
"""


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
    The 'agent' node — sends the conversation to GPT-4 and gets a response.
    If the response contains tool_calls, the graph routes to the tools node.
    Otherwise, it routes to END.
    """
    llm = _build_llm()
    messages = state["messages"]

    # Inject system prompt if not already present
    if not messages or not isinstance(messages[0], SystemMessage):
        messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(messages)

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


def ask_agent(question: str, history: list[BaseMessage] | None = None) -> str:
    """
    Send a question to the agent and return the final text response.

    Args:
        question: The user's question in natural language
        history:  Optional prior conversation messages

    Returns:
        The agent's final text answer
    """
    agent = get_agent()

    messages = []
    if history:
        messages.extend(history)
    messages.append(HumanMessage(content=question))

    result = agent.invoke({"messages": messages})

    # Extract the last AI message
    for msg in reversed(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            return msg.content

    return "Agent did not produce a response."
