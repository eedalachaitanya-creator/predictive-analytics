"""
main.py — Analyst Agent | Interactive CLI & API Entry Point
============================================================
Provides two ways to interact with the Analyst Agent:

1. Interactive CLI:  python -m agent.main
2. Single query:     python -m agent.main --query "How many customers are high risk?"
3. FastAPI endpoint:  (imported by app/main.py)

Requirements:
    pip install langgraph langchain-openai langchain-core python-dotenv rich
"""

import argparse
import logging
import sys
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("analyst_agent.main")

# Silence noisy libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════════════════════
# BANNER
# ═══════════════════════════════════════════════════════════════════════════

BANNER = r"""
 ╔═══════════════════════════════════════════════════════════════╗
 ║          CRP ANALYST AGENT  (LangGraph + Groq)               ║
 ║   Churn Prediction & Retention Analytics for Walmart CRP     ║
 ╠═══════════════════════════════════════════════════════════════╣
 ║  Tools:                                                      ║
 ║    - query_database        (SQL against PostgreSQL)           ║
 ║    - predict_churn         (ML model scoring)                 ║
 ║    - get_customer_profile  (360-degree customer view)         ║
 ║    - get_risk_summary      (aggregate risk distribution)      ║
 ║    - get_feature_importance(churn drivers)                    ║
 ║    - search_at_risk_customers (filter at-risk)                ║
 ╠═══════════════════════════════════════════════════════════════╣
 ║  Commands:  /help  /clear  /tools  /quit                     ║
 ╚═══════════════════════════════════════════════════════════════╝
"""

HELP_TEXT = """
Available commands:
  /help    — Show this help message
  /clear   — Clear conversation history and start fresh
  /tools   — List available tools
  /quit    — Exit the agent

Example questions:
  "How many customers are at high churn risk?"
  "Show me the profile of WMT-CUST-00042"
  "What are the top 5 features driving churn?"
  "Find Platinum customers with high churn risk and over $5000 in spend"
  "What is the average churn probability by tier?"
  "Predict churn for WMT-CUST-00001, WMT-CUST-00050"
"""


# ═══════════════════════════════════════════════════════════════════════════
# INTERACTIVE CLI
# ═══════════════════════════════════════════════════════════════════════════

def run_interactive():
    """Run the agent in interactive CLI mode with conversation history."""
    from agent.graph import get_agent, SYSTEM_PROMPT
    from langchain_core.messages import SystemMessage

    print(BANNER)

    agent = get_agent()
    history: list = [SystemMessage(content=SYSTEM_PROMPT)]

    # Try to use rich for pretty output
    try:
        from rich.console import Console
        from rich.markdown import Markdown
        console = Console()
        use_rich = True
    except ImportError:
        console = None
        use_rich = False

    print("Type your question below (or /help for commands).\n")

    while True:
        try:
            user_input = input("\n You > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\nGoodbye!")
            break

        if not user_input:
            continue

        # Handle commands
        if user_input.lower() in ('/quit', '/exit', '/q'):
            print("\nGoodbye!")
            break

        if user_input.lower() == '/help':
            print(HELP_TEXT)
            continue

        if user_input.lower() == '/clear':
            history = [SystemMessage(content=SYSTEM_PROMPT)]
            print("\n  Conversation cleared. Starting fresh.\n")
            continue

        if user_input.lower() == '/tools':
            from agent.tools import ALL_TOOLS
            print("\n  Available tools:")
            for t in ALL_TOOLS:
                print(f"    - {t.name}: {t.description[:80]}...")
            continue

        # Add user message to history
        history.append(HumanMessage(content=user_input))

        # Invoke the agent
        print("\n Agent > ", end="", flush=True)
        try:
            result = agent.invoke({"messages": history})

            # Update history with new messages from this turn
            new_messages = result["messages"][len(history):]
            history.extend(new_messages)

            # Extract and display the final AI response
            answer = None
            for msg in reversed(result["messages"]):
                if isinstance(msg, AIMessage) and msg.content:
                    answer = msg.content
                    break

            if answer:
                if use_rich:
                    console.print()
                    console.print(Markdown(answer))
                else:
                    print(answer)
            else:
                print("(No response generated)")

        except Exception as e:
            log.error("Agent error: %s", e)
            print(f"\n  Error: {e}")
            print("  Try rephrasing your question, or type /help for guidance.")


# ═══════════════════════════════════════════════════════════════════════════
# SINGLE-QUERY MODE
# ═══════════════════════════════════════════════════════════════════════════

def run_single_query(question: str):
    """Run a single question through the agent and print the answer."""
    from agent.graph import ask_agent

    log.info("Single query: %s", question)
    answer = ask_agent(question)
    print(answer)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CRP Analyst Agent — Churn Prediction & Retention Analytics"
    )
    parser.add_argument(
        "--query", "-q",
        type=str,
        default=None,
        help="Run a single query and exit (non-interactive mode)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.query:
        run_single_query(args.query)
    else:
        run_interactive()


if __name__ == "__main__":
    main()
