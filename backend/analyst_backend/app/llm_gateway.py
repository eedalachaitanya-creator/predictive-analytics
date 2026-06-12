"""Single configured ai_firewall SecureGateway for the Analyst agent — the
reusable LLM I/O gateway, wired at the model's boundaries.

Surfaces guarded across CRP:
  ① input     — app/prompt_firewall.guard_question  (enforce + audit_log persistence)
  ③ retrieval — agent/tools.search_customer_feedback → filter_chunks()  (this module)
  ⑤ output    — agent/graph.ask_agent               → guard_response()  (this module)

Egress (③/⑤) is sanitize-only and NEVER raises — an output/retrieval guard must
not crash a response. Defensive on import: if the installed ai_firewall predates
the gateway API (e.g. the vendored wheel < the gateway commit), this degrades to
a no-op pass-through so chat never breaks.
"""
import logging

log = logging.getLogger("crp_api.firewall")

try:
    from ai_firewall import SecureGateway, Firewall, Policy

    # Enforce mode here only affects guard_input (which CRP routes through
    # prompt_firewall, not this gateway); ③/⑤ are sanitize-only regardless.
    _GATEWAY = SecureGateway(firewall=Firewall(Policy(mode="enforce"), logger=log))
    _ENABLED = True
except Exception as exc:  # ai_firewall missing / older than the gateway API
    _GATEWAY = None
    _ENABLED = False
    log.warning("ai_firewall gateway unavailable (%s); LLM I/O guards disabled (no-op).", exc)


def guard_response(answer: str) -> str:
    """⑤ EGRESS — sanitize the agent's final answer before it reaches the user
    (redacts any injection the model echoed back). Never raises."""
    if not _ENABLED or not isinstance(answer, str):
        return answer
    try:
        return _GATEWAY.guard_response(answer)
    except Exception as exc:
        log.error("gateway guard_response failed (%s); returning answer unchanged.", exc)
        return answer


def filter_chunks(hits: list) -> list:
    """③ RETRIEVAL — sanitize retrieved RAG chunks (dicts carrying 'content')
    before they enter the LLM context, neutralizing indirect prompt injection
    planted in customer-provided text (e.g. a review). Never raises."""
    if not _ENABLED or not hits:
        return hits
    try:
        return _GATEWAY.filter_context(
            hits,
            text_of=lambda h: (h.get("content") or "") if isinstance(h, dict) else str(h),
            set_text=lambda h, s: {**h, "content": s},
        )
    except Exception as exc:
        log.error("gateway filter_chunks failed (%s); returning chunks unchanged.", exc)
        return hits


def sanitize_ingest(text: str) -> str:
    """② INGEST — sanitize a document before it is embedded into the vector store,
    so a poisoned customer review can't plant an injection that later gets
    retrieved into the LLM context. Never raises (a sanitize failure must not
    abort a reindex)."""
    if not _ENABLED or not isinstance(text, str):
        return text
    try:
        return _GATEWAY.ingest_document(text)
    except Exception as exc:
        log.error("gateway sanitize_ingest failed (%s); embedding text unchanged.", exc)
        return text


def guard_tool(text: str) -> str:
    """④ TOOL OUTPUT — sanitize a tool result (DB rows, scraped/retrieved text)
    before it re-enters the model context. Tool outputs are attacker-influenced
    (a customer-provided name/review/address can carry injection). Never raises."""
    if not _ENABLED or not isinstance(text, str):
        return text
    try:
        return _GATEWAY.guard_tool(text)
    except Exception as exc:
        log.error("gateway guard_tool failed (%s); returning tool output unchanged.", exc)
        return text
