"""Domain scope guardrail for the Analyst Agent.

The Analyst answers ONLY questions about this retail churn & retention platform.
General requests — "write a python program", trivia, world knowledge, essays —
are benign (so the prompt-injection firewall passes them) but OFF-TOPIC, and must
be refused instead of answered.

`is_in_scope()` classifies a question with a cheap, fast LLM call (gpt-4o-mini,
temperature 0, a few tokens) and `ask_agent` refuses out-of-scope questions
BEFORE building the agent or calling any tool. The check is FAIL-OPEN: if the
classifier errors or returns something unparseable, the question is allowed
through — a transient model hiccup must never wrongly refuse a real churn
question. The system prompt (graph.py) is the second layer of defense.
"""
import logging

log = logging.getLogger("crp_api.scope")

# What the Analyst is allowed to talk about — kept in one place and reused by the
# classifier prompt below and (via SCOPE_REFUSAL) the user-facing refusal.
SCOPE_DESCRIPTION = (
    "customer churn and churn-risk prediction, customer retention strategies, "
    "customer profiles / segments / RFM, customer reviews, feedback and sentiment, "
    "and the customer, order, product and churn data in this retail analytics "
    "platform — plus greetings and questions about what this assistant can do."
)

SCOPE_REFUSAL = (
    "I can only help with questions about customer churn, retention, customer data, "
    "and this analytics platform — so I can't help with that one. Try asking about "
    "churn risk, customer profiles, retention strategies, or what your customers are "
    "saying."
)

_CLASSIFIER_SYSTEM = (
    "You are a strict topic gate for a customer-churn & retention analytics "
    "assistant.\n"
    f"IN SCOPE: {SCOPE_DESCRIPTION}\n"
    "OUT OF SCOPE: general programming or coding help, math problems or puzzles, "
    "general knowledge, trivia, current events, writing essays/poems/jokes, "
    "translation, or anything not about this platform's customer / churn / "
    "retention domain.\n"
    "Classify the user's message. Reply with EXACTLY one word: RELEVANT or OFFTOPIC."
)


def _default_classify(question: str) -> str:
    """Ask gpt-4o-mini for a one-word RELEVANT/OFFTOPIC verdict."""
    from langchain_core.messages import SystemMessage, HumanMessage
    from agent.llm import build_chat_model

    llm = build_chat_model(temperature=0, max_tokens=8)
    resp = llm.invoke([
        SystemMessage(content=_CLASSIFIER_SYSTEM),
        HumanMessage(content=question),
    ])
    return (getattr(resp, "content", "") or "").strip()


def is_in_scope(question, *, classify=None) -> bool:
    """True if `question` is within the Analyst's domain, False to refuse it.

    `classify` is an injectable callable(question)->verdict_string (for tests);
    defaults to the gpt-4o-mini classifier. FAIL-OPEN on any error or unparseable
    verdict — only an explicit OFFTOPIC verdict refuses the question.
    """
    if not isinstance(question, str) or not question.strip():
        return True  # empty/blank — let the endpoint's own empty-check handle it
    try:
        verdict = (classify or _default_classify)(question)
    except Exception as exc:  # classifier down — don't refuse a real question
        log.error("scope classifier failed (%s); allowing question (fail-open).", exc)
        return True
    out_of_scope = "OFFTOPIC" in (verdict or "").upper()
    if out_of_scope:
        log.info("scope gate: OFF-TOPIC question refused.")
    return not out_of_scope
