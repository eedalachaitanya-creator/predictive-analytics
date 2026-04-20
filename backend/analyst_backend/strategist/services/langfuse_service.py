"""
services/langfuse_service.py — Customer Retention Platform
===========================================================

LangFuse observability service with full cost tracking.

What this module provides
--------------------------
1. get_langfuse_safe()       — singleton LangFuse client (None if not configured)
2. StrategistTracer          — per-run tracer that wraps every graph node as a span
                               and records latency + token cost for each step
3. get_cost_summary()        — aggregate cost/latency stats pulled from LangFuse API
                               powers GET /api/strategist/costs

LangFuse trace structure per run
----------------------------------
  Trace: strategist_pipeline  (run_id)
    ├── Span: validate_input         (latency ms)
    ├── Span: load_market_context    (latency ms)
    ├── Span: build_churn_lookup     (latency ms)
    ├── Span: run_pricing_engine     (latency ms)
    ├── Span: apply_charm_pricing    (latency ms)
    ├── Span: [retention|soft_flag]  (latency ms)
    └── Span: persist_results        (latency ms)
    Score: margin_health             (float 0-1)
    Score: pipeline_latency_ms       (total ms)
    Score: retention_rate            (fraction)

Cost tracking
--------------
This platform uses rule-based pricing (no LLM tokens), so cost_usd = 0.0 per
run. The infrastructure is wired and ready — when you add an LLM call (e.g.
personalised retention offer generation), call tracer.record_llm_call() with
the token counts and costs will flow through automatically.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

# ── Module-level singleton ────────────────────────────────────────────────────
_langfuse_client = None
_init_attempted  = False


def get_langfuse_safe() -> Optional[Any]:
    """
    Return the LangFuse client singleton, or None if not configured.
    Thread-safe via Python GIL. Never raises.
    """
    global _langfuse_client, _init_attempted

    if _init_attempted:
        return _langfuse_client

    _init_attempted = True

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", "").strip()
    secret_key = os.getenv("LANGFUSE_SECRET_KEY", "").strip()
    host       = os.getenv("LANGFUSE_HOST",
                           os.getenv("LANGFUSE_BASE_URL", "https://cloud.langfuse.com")).strip()

    if not public_key or not secret_key:
        logger.info(
            "LangFuse not configured (LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY missing). "
            "Tracing disabled."
        )
        return None

    if not public_key.startswith("pk-lf-") or not secret_key.startswith("sk-lf-"):
        logger.warning("LangFuse keys malformed (expected pk-lf-... / sk-lf-...). Disabled.")
        return None

    try:
        from langfuse import Langfuse
        _langfuse_client = Langfuse(public_key=public_key, secret_key=secret_key, host=host)
        logger.info("LangFuse client initialised (host=%s).", host)
    except ImportError:
        logger.warning("langfuse package not installed. Run: pip install langfuse")
    except Exception as exc:
        logger.warning("LangFuse init failed: %s — tracing disabled.", exc)

    return _langfuse_client


# =============================================================================
# NodeSpan — timing for one graph node
# =============================================================================

@dataclass
class NodeSpan:
    node_name:   str
    started_at:  float            = field(default_factory=time.perf_counter)
    ended_at:    Optional[float]  = None
    input_meta:  dict             = field(default_factory=dict)
    output_meta: dict             = field(default_factory=dict)
    error:       Optional[str]    = None

    @property
    def latency_ms(self) -> float:
        if self.ended_at is None:
            return 0.0
        return round((self.ended_at - self.started_at) * 1000, 2)

    def end(self, output_meta: dict | None = None, error: str | None = None) -> None:
        self.ended_at    = time.perf_counter()
        self.output_meta = output_meta or {}
        self.error       = error


@dataclass
class LLMCall:
    model:         str
    input_tokens:  int   = 0
    output_tokens: int   = 0
    cost_usd:      float = 0.0


# =============================================================================
# StrategistTracer — one instance per pipeline run
# =============================================================================

class StrategistTracer:
    """
    Per-run tracer. Wraps every graph node as a LangFuse span and collects
    latency + token costs for the run summary.

    Usage in strategist_graph.py:
        tracer = StrategistTracer(run_id, client_id)
        tracer.start_node("validate_input", {"products": 3})
        # ... node runs ...
        tracer.end_node("validate_input", {"ok": True})
        tracer.flush(recommendations=recs)

    All methods are best-effort — never raise even if LangFuse is down.
    """

    def __init__(self, run_id: str, client_id: str = "CLT-001", metadata: dict | None = None):
        self.run_id     = run_id
        self.client_id  = client_id
        self.metadata   = metadata or {}
        self.started_at = time.perf_counter()

        self._spans:     dict[str, NodeSpan] = {}
        self._llm_calls: list[LLMCall]       = []
        self._lf_trace   = None
        self._lf_spans:  dict[str, Any]      = {}

        self._init_trace()

    # ── trace init ────────────────────────────────────────────────────────────

    def _init_trace(self) -> None:
        try:
            lf = get_langfuse_safe()
            if not lf:
                return
            self._lf_trace = lf.trace(
                id       = self.run_id,
                name     = "strategist_pipeline",
                metadata = {"client_id": self.client_id, **self.metadata},
            )
        except Exception as exc:
            logger.debug("LangFuse trace init failed: %s", exc)

    # ── node spans ────────────────────────────────────────────────────────────

    def start_node(self, node_name: str, input_meta: dict | None = None) -> None:
        """Mark start of a graph node. Call at top of each node function."""
        self._spans[node_name] = NodeSpan(node_name=node_name, input_meta=input_meta or {})
        try:
            if self._lf_trace:
                self._lf_spans[node_name] = self._lf_trace.span(
                    name  = node_name,
                    input = input_meta or {},
                )
        except Exception as exc:
            logger.debug("LangFuse span start failed (%s): %s", node_name, exc)

    def end_node(
        self,
        node_name:   str,
        output_meta: dict | None = None,
        error:       str | None  = None,
    ) -> float:
        """Mark end of a graph node. Returns latency_ms for this node."""
        span = self._spans.get(node_name)
        if span:
            span.end(output_meta=output_meta, error=error)
        try:
            lf_span = self._lf_spans.get(node_name)
            if lf_span:
                lf_span.end(
                    output         = output_meta or {},
                    level          = "ERROR" if error else "DEFAULT",
                    status_message = error,
                )
        except Exception as exc:
            logger.debug("LangFuse span end failed (%s): %s", node_name, exc)
        return span.latency_ms if span else 0.0

    @contextmanager
    def node_span(self, node_name: str, input_meta: dict | None = None) -> Generator:
        """
        Context-manager shorthand:
            async with tracer.node_span("load_market_context", {...}):
                result = await do_work()
        """
        self.start_node(node_name, input_meta)
        try:
            yield
        except Exception as exc:
            self.end_node(node_name, error=str(exc))
            raise
        else:
            self.end_node(node_name)

    # ── LLM cost tracking ─────────────────────────────────────────────────────

    def record_llm_call(
        self,
        model:         str,
        input_tokens:  int,
        output_tokens: int,
        cost_usd:      float = 0.0,
    ) -> None:
        """
        Record token usage for an LLM call in this run.
        Cost_usd = 0.0 for rule-based pricing runs (no LLM used).
        Pass real token counts here when you add LLM-generated offer copy.
        """
        self._llm_calls.append(
            LLMCall(model=model, input_tokens=input_tokens,
                    output_tokens=output_tokens, cost_usd=cost_usd)
        )

    # ── run summary ───────────────────────────────────────────────────────────

    def get_run_summary(self) -> dict:
        """Structured summary of timing + cost for this run."""
        total_ms   = round((time.perf_counter() - self.started_at) * 1000, 2)
        total_cost = sum(c.cost_usd for c in self._llm_calls)

        by_model: dict[str, dict] = {}
        for call in self._llm_calls:
            if call.model not in by_model:
                by_model[call.model] = {
                    "model": call.model, "input_tokens": 0,
                    "output_tokens": 0, "cost_usd": 0.0,
                }
            by_model[call.model]["input_tokens"]  += call.input_tokens
            by_model[call.model]["output_tokens"] += call.output_tokens
            by_model[call.model]["cost_usd"]      += call.cost_usd

        return {
            "run_id":           self.run_id,
            "client_id":        self.client_id,
            "total_latency_ms": total_ms,
            "total_cost_usd":   total_cost,
            "node_latencies":   {n: s.latency_ms for n, s in self._spans.items()},
            "llm_calls":        list(by_model.values()),
        }

    # ── flush to LangFuse ─────────────────────────────────────────────────────

    def flush(self, recommendations: list | None = None, error_message: str | None = None) -> None:
        """
        Finalise the LangFuse trace. Call once at end of pipeline (in persist_results).
        Writes scores (margin_health, latency, retention_rate) and full output.
        """
        try:
            if not self._lf_trace:
                return

            summary = self.get_run_summary()
            recs    = recommendations or []

            # Score: margin_health — fraction of products above min margin
            if recs:
                healthy       = sum(1 for r in recs if getattr(r, "margin_percent", 0) >= 8.0)
                margin_health = round(healthy / len(recs), 3)
                self._lf_trace.score(
                    name    = "margin_health",
                    value   = margin_health,
                    comment = f"{healthy}/{len(recs)} products above 8% min margin",
                )

                # Score: retention_rate
                ret_count = sum(1 for r in recs if getattr(r, "strategy", "") == "retention")
                self._lf_trace.score(
                    name    = "retention_rate",
                    value   = round(ret_count / len(recs), 3),
                    comment = f"{ret_count}/{len(recs)} products with churn discount",
                )

            # Score: pipeline latency
            self._lf_trace.score(
                name    = "pipeline_latency_ms",
                value   = summary["total_latency_ms"],
                comment = "Total wall-clock time for graph execution",
            )

            # Update trace output
            self._lf_trace.update(
                output = {
                    "total_products":  len(recs),
                    "flagged_count":   sum(1 for r in recs if getattr(r, "flag", None)),
                    "retention_count": sum(1 for r in recs if getattr(r, "strategy", "") == "retention"),
                    "strategies_used": sorted({getattr(r, "strategy", "") for r in recs}),
                    "avg_margin_pct":  (
                        round(sum(getattr(r, "margin_percent", 0) for r in recs) / len(recs), 1)
                        if recs else 0.0
                    ),
                    "total_cost_usd":   summary["total_cost_usd"],
                    "total_latency_ms": summary["total_latency_ms"],
                    "node_latencies":   summary["node_latencies"],
                    "llm_calls":        summary["llm_calls"],
                    "error":            error_message,
                },
                level = "ERROR" if error_message else "DEFAULT",
            )

            logger.info(
                "LangFuse trace flushed: run_id=%s latency=%.0fms cost=$%.4f",
                self.run_id, summary["total_latency_ms"], summary["total_cost_usd"],
            )

        except Exception as exc:
            logger.debug("LangFuse flush failed: %s", exc)


# =============================================================================
# Cost summary — powers GET /api/strategist/costs
# =============================================================================

async def get_cost_summary(limit: int = 100) -> dict:
    """
    Pull aggregated cost + latency metrics from LangFuse for recent runs.
    Returns a dict ready for the /costs endpoint response.
    """
    lf = get_langfuse_safe()
    if not lf:
        return {
            "configured":       False,
            "message":          (
                "LangFuse not configured. Add LANGFUSE_PUBLIC_KEY and "
                "LANGFUSE_SECRET_KEY to .env to enable cost tracking."
            ),
            "total_runs":       0,
            "total_cost_usd":   0.0,
            "avg_cost_per_run": 0.0,
            "avg_latency_ms":   0.0,
            "by_model":         [],
            "recent_runs":      [],
        }

    try:
        traces     = lf.get_traces(name="strategist_pipeline", limit=limit)
        trace_list = getattr(traces, "data", [])
        total_runs = len(trace_list)

        if not total_runs:
            return {
                "configured":       True,
                "dashboard":        _dashboard_url(),
                "total_runs":       0,
                "total_cost_usd":   0.0,
                "avg_cost_per_run": 0.0,
                "avg_latency_ms":   0.0,
                "by_model":         [],
                "recent_runs":      [],
            }

        total_cost    = 0.0
        total_latency = 0.0
        model_totals: dict[str, dict] = {}
        recent_runs: list[dict]       = []

        for trace in trace_list:
            output    = getattr(trace, "output", {}) or {}
            cost      = float(output.get("total_cost_usd", 0.0))
            latency   = float(output.get("total_latency_ms", 0.0))
            total_cost    += cost
            total_latency += latency

            for llm in output.get("llm_calls", []):
                m = llm.get("model", "unknown")
                if m not in model_totals:
                    model_totals[m] = {
                        "model": m, "input_tokens": 0,
                        "output_tokens": 0, "cost_usd": 0.0,
                    }
                model_totals[m]["input_tokens"]  += llm.get("input_tokens", 0)
                model_totals[m]["output_tokens"] += llm.get("output_tokens", 0)
                model_totals[m]["cost_usd"]      += llm.get("cost_usd", 0.0)

            if len(recent_runs) < 10:
                recent_runs.append({
                    "run_id":          getattr(trace, "id", ""),
                    "timestamp":       str(getattr(trace, "timestamp", "")),
                    "total_products":  output.get("total_products", 0),
                    "retention_count": output.get("retention_count", 0),
                    "avg_margin_pct":  output.get("avg_margin_pct", 0.0),
                    "latency_ms":      latency,
                    "cost_usd":        cost,
                    "node_latencies":  output.get("node_latencies", {}),
                    "error":           output.get("error"),
                })

        return {
            "configured":       True,
            "dashboard":        _dashboard_url(),
            "total_runs":       total_runs,
            "total_cost_usd":   round(total_cost, 6),
            "avg_cost_per_run": round(total_cost / total_runs, 6),
            "avg_latency_ms":   round(total_latency / total_runs, 1),
            "by_model":         list(model_totals.values()),
            "recent_runs":      recent_runs,
        }

    except Exception as exc:
        logger.warning("get_cost_summary failed: %s", exc)
        return {
            "configured":       True,
            "error":            str(exc),
            "dashboard":        _dashboard_url(),
            "total_runs":       0,
            "total_cost_usd":   0.0,
            "avg_cost_per_run": 0.0,
            "avg_latency_ms":   0.0,
            "by_model":         [],
            "recent_runs":      [],
        }


def _dashboard_url() -> str:
    host = os.getenv("LANGFUSE_HOST",
                     os.getenv("LANGFUSE_BASE_URL", "https://cloud.langfuse.com"))
    return f"{host.rstrip('/')}/dashboard"