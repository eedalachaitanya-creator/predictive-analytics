"""
scout_agent/scout_agent.py — Scout LangChain Agent

Architecture
────────────
  User query
      │
      ▼
  ScoutAgent.chat()
      │
      ▼
  LangChain OPENAI_FUNCTIONS agent
      │ decides which tools to call
      ├─► search_products       — live scrape across platforms
      ├─► compare_prices        — entity-resolved price comparison
      ├─► get_features          — spec / feature matrix
      ├─► get_price_history     — time-series trend
      ├─► get_alerts            — unread price-change alerts
      ├─► run_price_monitor     — trigger full refresh
      └─► list_platforms        — active platform list
      │
      ▼
  Natural-language answer with structured data

Usage
─────
    from agent.scout_agent import ScoutAgent

    agent = ScoutAgent()

    # one-shot
    print(agent.chat("Where is the Dyson V15 cheapest?"))

    # multi-turn (memory preserved)
    agent.chat("Search for boAt Airdopes 141")
    agent.chat("Which platform has the best price?")   # agent remembers context
    agent.chat("Has the price changed recently?")

    # FastAPI integration — already wired in main.py
    # POST /agent/chat   {"message": "...", "session_id": "..."}
"""

"""
scout_agent/scout_agent.py — Scout LangChain Agent (LangGraph version)

Rewritten for langchain >= 1.0 / langgraph >= 1.0.
Uses LangGraph's create_react_agent instead of the removed
initialize_agent + AgentType.

Architecture:
    User query → LangGraph ReAct agent → tools → response
"""

import logging
import os
from typing import Optional

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.prebuilt import create_react_agent
from scout_agent.tools import SCOUT_TOOLS

# Langfuse integration for cost tracking
from scout.langfuse_config import get_langchain_handler, flush as langfuse_flush

logger = logging.getLogger(__name__)

# ── System prompt ──────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are Scout, an AI shopping assistant that helps users find the best prices across Indian e-commerce platforms (Amazon, Flipkart, Myntra, Nykaa, Beato, Walmart and more).

You have access to live product data. Here is how to use your tools:

WORKFLOW
1. When asked about a product → call search_products first to fetch live data.
2. When asked "where is it cheapest?" → call compare_prices (search first if needed).
3. When asked about specs or features → call get_features.
4. When asked about price trends or "should I buy now?" → call get_price_history.
5. When asked about alerts or price changes → call get_alerts.
6. When asked to refresh/update prices → call run_price_monitor (confirm="yes").
7. When asked which platforms are supported → call list_platforms.

RESPONSE STYLE
- Lead with the direct answer: cheapest price, best platform, key finding.
- Use ₹ for Indian Rupee prices.
- Mention savings in both absolute (₹) and percentage terms.
- If a product is cross-platform (found on 2+ sites), highlight the price spread.
- Keep responses concise but complete. Use bullet points for multi-item lists.
- If search returns no results, suggest the user rephrase or check the platform list.

GUARDRAILS
- Never invent prices or availability — only report what the tools return.
- If tools return an error key, explain it to the user and suggest next steps.
- For run_price_monitor, always confirm the user's intent before calling it.
"""


# ── Build agent ────────────────────────────────────────────────────────

def _build_agent():
    """Create a LangGraph ReAct agent with Scout tools."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is not set in environment.")

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0,
        openai_api_key=api_key,
    )

    agent = create_react_agent(
        model=llm,
        tools=SCOUT_TOOLS,
        prompt=SYSTEM_PROMPT,
    )

    logger.info("[ScoutAgent] LangGraph ReAct agent built (gpt-4o-mini)")
    return agent


# ── Agent class ────────────────────────────────────────────────────────

class ScoutAgent:
    """
    Stateful Scout agent with per-session conversation memory.
    Uses LangGraph's create_react_agent (langchain >= 1.0).
    """

    def __init__(self, session_id: str = "default"):
        self.session_id = session_id
        self.agent = _build_agent()
        self.history: list = []
        logger.info(f"[ScoutAgent] Session '{session_id}' initialized")

    def chat(self, message: str) -> str:
        """Send a message and get a response."""
        try:
            callbacks = []
            handler = get_langchain_handler(session_id=self.session_id)
            if handler:
                callbacks.append(handler)

            self.history.append(HumanMessage(content=message))

            result = self.agent.invoke(
                {"messages": self.history},
                config={"callbacks": callbacks} if callbacks else {},
            )

            # Extract new messages from result
            new_messages = result["messages"][len(self.history):]
            self.history.extend(new_messages)

            # Find last AI response
            for msg in reversed(result["messages"]):
                if isinstance(msg, AIMessage) and msg.content:
                    langfuse_flush()
                    return msg.content

            langfuse_flush()
            return "I couldn't generate a response. Please try rephrasing."

        except Exception as e:
            logger.error(f"[ScoutAgent] Error: {e}")
            return f"Sorry, I ran into an issue: {str(e)}. Please try rephrasing your query."

    def reset_memory(self):
        """Clear conversation history."""
        self.history = []
        logger.info("[ScoutAgent] Memory cleared.")

    def get_history(self) -> list[dict]:
        """Return conversation history as a list of {role, content} dicts."""
        return [
            {
                "role": "user" if isinstance(m, HumanMessage) else "assistant",
                "content": m.content,
            }
            for m in self.history
            if isinstance(m, (HumanMessage, AIMessage)) and m.content
        ]


# ── Session manager ───────────────────────────────────────────────────

class SessionManager:
    def __init__(self):
        self._sessions: dict[str, ScoutAgent] = {}

    def get_or_create(self, session_id: str) -> ScoutAgent:
        if session_id not in self._sessions:
            self._sessions[session_id] = ScoutAgent(session_id=session_id)
            logger.info(f"[SessionManager] New session: {session_id}")
        return self._sessions[session_id]

    def delete(self, session_id: str):
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"[SessionManager] Session deleted: {session_id}")

    def active_sessions(self) -> list[str]:
        return list(self._sessions.keys())


session_manager = SessionManager()


# ── CLI entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    agent = ScoutAgent(session_id="cli")

    print("Scout Agent — type 'quit' to exit, 'reset' to clear memory\n")
    while True:
        try:
            user_input = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nBye!")
            sys.exit(0)

        if not user_input:
            continue
        if user_input.lower() == "quit":
            print("Bye!")
            sys.exit(0)
        if user_input.lower() == "reset":
            agent.reset_memory()
            print("Scout: Memory cleared. Fresh start!\n")
            continue

        response = agent.chat(user_input)
        print(f"\nScout: {response}\n")