"""
llm.py — Provider factory for the Analyst Agent.

Single choke-point for choosing the chat model + embeddings so the rest of the
agent never hard-codes a provider. Defaults to OpenAI `gpt-4o-mini` (needs
OPENAI_API_KEY in .env); falls back to Groq ONLY when explicitly selected via
`AGENT_MODEL` (a Groq model name) or `LLM_PROVIDER=groq`.

This exists because RAG needs to feed retrieved context into the prompt, which
Groq's free-tier ~6k tokens/min ceiling cannot accommodate — so the reasoning
model moves to OpenAI while keeping Groq reachable behind one env var.
"""
import os

# Substrings that identify a Groq-hosted open model. Used to infer the provider
# from AGENT_MODEL when LLM_PROVIDER isn't set explicitly.
_GROQ_HINTS = ("llama", "mixtral", "gemma", "groq", "qwen", "deepseek")

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
EMBED_MODEL = "text-embedding-3-small"


def _resolve_provider(model: str) -> str:
    """Decide 'openai' or 'groq'. Explicit LLM_PROVIDER wins; else infer from
    the model name; else default to OpenAI."""
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    if provider in ("openai", "groq"):
        return provider
    if model and any(h in model.lower() for h in _GROQ_HINTS):
        return "groq"
    return "openai"


def build_chat_model(temperature: float = 0.1, max_tokens: int = 4096):
    """Return a LangChain chat model for the agent; provider chosen from env."""
    model = os.getenv("AGENT_MODEL", "").strip()
    provider = _resolve_provider(model)

    if provider == "groq":
        from langchain_groq import ChatGroq
        return ChatGroq(
            model=model or DEFAULT_GROQ_MODEL,
            temperature=temperature,
            groq_api_key=os.getenv("GROQ_API_KEY", ""),
            max_tokens=max_tokens,
        )

    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=model or DEFAULT_OPENAI_MODEL,
        temperature=temperature,
        api_key=os.getenv("OPENAI_API_KEY", ""),
        max_tokens=max_tokens,
    )


def build_embeddings():
    """Return the OpenAI embeddings client used for the RAG vector store."""
    from langchain_openai import OpenAIEmbeddings
    return OpenAIEmbeddings(
        model=EMBED_MODEL,
        api_key=os.getenv("OPENAI_API_KEY", ""),
    )
