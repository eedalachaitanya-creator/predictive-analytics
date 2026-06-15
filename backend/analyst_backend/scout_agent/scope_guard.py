"""Domain scope guardrail for the Scout Agent.

Scout answers ONLY questions about shopping and price comparison across the
e-commerce platforms it tracks. General requests — "write a python program",
trivia, math puzzles, world knowledge, essays — are benign (so the
prompt-injection firewall passes them) but OFF-TOPIC, and must be refused
instead of answered.

`is_in_scope()` classifies a message with a cheap, fast LLM call (gpt-4o-mini,
temperature 0, a few tokens) and `ScoutAgent.chat` refuses out-of-scope
messages BEFORE invoking the ReAct agent or calling any tool. The check is
FAIL-OPEN: if the classifier errors or returns something unparseable, the
message is allowed through — a transient model hiccup must never wrongly
refuse a real shopping question. The system prompt (scout_agent.py) is the
second layer of defense.

Mirrors agent/scope_guard.py (the Analyst agent's equivalent guard).
"""
import logging

log = logging.getLogger("crp_api.scout.scope")

# What Scout is allowed to talk about — kept in one place and reused by the
# classifier prompt below and (via SCOPE_REFUSAL) the user-facing refusal.
SCOPE_DESCRIPTION = (
    "market intelligence and competitor price tracking — searching for "
    "products, comparing prices across e-commerce platforms (Amazon, "
    "Flipkart, Myntra, Nykaa, Beato, Walmart, etc.), product specs and "
    "features, price history and trends, price monitoring and price-drop "
    "alerts, and which platforms are supported — plus greetings and "
    "questions about what this assistant can do."
)

SCOPE_REFUSAL = (
    "That's outside what I can help with here. As Scout, I'm focused on "
    "market intelligence and competitor price tracking — product search, "
    "price comparison across platforms, price monitoring, history, and "
    "alerts. Let me know if you'd like help with any of those."
)

_CLASSIFIER_SYSTEM = (
    "You are a strict topic gate for Scout, a market-intelligence and "
    "competitor price-tracking assistant.\n"
    f"IN SCOPE: {SCOPE_DESCRIPTION}\n"
    "OUT OF SCOPE: general programming or coding help, math problems or "
    "puzzles, general knowledge, trivia, current events, writing "
    "essays/poems/jokes, translation, or anything not about products, "
    "prices, competitors, or this platform.\n"
    "Classify the user's message. Reply with EXACTLY one word: RELEVANT or "
    "OFFTOPIC."
)


def _default_classify(message: str) -> str:
    """Ask gpt-4o-mini for a one-word RELEVANT/OFFTOPIC verdict."""
    from langchain_core.messages import SystemMessage, HumanMessage
    from agent.llm import build_chat_model

    llm = build_chat_model(temperature=0, max_tokens=8)
    resp = llm.invoke([
        SystemMessage(content=_CLASSIFIER_SYSTEM),
        HumanMessage(content=message),
    ])
    return (getattr(resp, "content", "") or "").strip()


def is_in_scope(message, *, classify=None) -> bool:
    """True if `message` is within Scout's shopping/price domain, False to refuse it.

    `classify` is an injectable callable(message)->verdict_string (for tests);
    defaults to the gpt-4o-mini classifier. FAIL-OPEN on any error or
    unparseable verdict — only an explicit OFFTOPIC verdict refuses the
    message.
    """
    if not isinstance(message, str) or not message.strip():
        return True  # empty/blank — let the endpoint's own empty-check handle it
    try:
        verdict = (classify or _default_classify)(message)
    except Exception as exc:  # classifier down — don't refuse a real question
        log.error("scout scope classifier failed (%s); allowing message (fail-open).", exc)
        return True
    out_of_scope = "OFFTOPIC" in (verdict or "").upper()
    if out_of_scope:
        log.info("scout scope gate: OFF-TOPIC message refused.")
    return not out_of_scope