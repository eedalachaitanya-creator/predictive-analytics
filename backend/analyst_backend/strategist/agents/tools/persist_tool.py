"""
agents/tools/persist_tool.py — Customer Retention Platform
===========================================================

PersistRecommendationTool
--------------------------
A LangChain BaseTool that wraps the persist_run() DB write as an observable,
retryable, non-blocking tool step in the LangGraph pipeline.

WHY THIS IS A TOOL:
  persist_run() is the final write step — it inserts:
    1. pricing_recommendations  (one row per product)
    2. customer_price_context   (one row per HIGH-risk customer — the handoff
                                 to the Retention Agent for double-discount prevention)

  Previously this was a try/except block in the router. As a tool:
    1. The DB write duration is a named LangSmith span — slow writes are visible
    2. Retry logic for transient DB errors (connection pool exhausted, deadlock)
       lives here, not scattered across the graph
    3. Tests can inject a NoOpPersistTool that records calls without hitting DB
    4. The tool returns a structured PersistResult so downstream nodes can
       inspect what was written (useful for the summary response)

CRITICAL INVARIANT — this tool NEVER raises:
  A DB write failure must not lose the pricing recommendations.
  The client must receive their recommendations even if persistence fails.
  On all failures, the tool returns PersistResult(success=False, error=...).

INPUTS:
    request:         StrategistRequest
    recommendations: list[PricingRecommendation]
    run_id:          str

OUTPUTS:
    PersistResult — {success, rows_written, contexts_written, error}
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from strategist.models.schemas import PricingRecommendation, StrategistRequest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output model — structured result from the persist operation
# ---------------------------------------------------------------------------

class PersistResult(BaseModel):
    """Result of a persist_run() call. Always returned, never raises."""
    success:           bool
    recommendations_written: int = 0
    contexts_written:  int = 0
    run_id:            str = ""
    error:             Optional[str] = None


# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------

class PersistInput(BaseModel):
    """
    We pass complex objects (StrategistRequest, list of PricingRecommendation)
    through the graph state, not as tool string args. So this schema is minimal —
    the node calls the tool directly via ainvoke({...}) with the real objects.
    """
    run_id: str = Field(description="UUID for this pricing run.")


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

class PersistRecommendationTool(BaseTool):
    """
    Persists Strategist Agent results to the Scout DB after a successful run.

    Writes:
      - pricing_recommendations: one row per product
      - customer_price_context:  one row per HIGH-risk customer
        (strategy='retention') for Retention Agent double-discount prevention

    This tool NEVER raises — failures are returned as PersistResult(success=False).
    The client always receives their pricing recommendations regardless of DB state.
    """

    name: str = "persist_recommendations"
    description: str = (
        "Persist pricing recommendations and customer price contexts to the Scout DB. "
        "Returns a structured result — never raises even on DB failure."
    )
    args_schema: Type[BaseModel] = PersistInput
    max_retries: int = 2

    def _run(self, run_id: str, **kwargs: Any) -> PersistResult:
        # Sync path not used in production (FastAPI is async)
        # Included for test compatibility
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            self._arun_with_data(
                run_id=run_id,
                request=kwargs.get("request"),
                recommendations=kwargs.get("recommendations", []),
            )
        )

    async def _arun(self, run_id: str, **kwargs: Any) -> PersistResult:
        """
        LangChain calls _arun when ainvoke() is used.
        The node passes request and recommendations as extra kwargs.
        """
        return await self._arun_with_data(
            run_id=run_id,
            request=kwargs.get("request"),
            recommendations=kwargs.get("recommendations", []),
        )

    async def _arun_with_data(
        self,
        run_id: str,
        request: Optional[StrategistRequest],
        recommendations: list[PricingRecommendation],
    ) -> PersistResult:
        """
        Core persist logic with retry on transient failures.

        Retry policy:
          - Retries on asyncpg.TooManyConnectionsError (pool exhausted)
          - Retries on asyncpg.DeadlockDetectedError
          - Does NOT retry on data errors (bad schema, constraint violations)
        """
        if not request or not recommendations:
            logger.warning(
                "PersistRecommendationTool: called with empty request or recommendations"
            )
            return PersistResult(success=True, run_id=run_id)

        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                from strategist.db.persistence import persist_run
                summary = await persist_run(request, recommendations, run_id)

                recs_written     = summary.get("recommendations_written", 0)
                contexts_written = summary.get("price_contexts_written", 0)

                logger.info(
                    "PersistRecommendationTool: %d recommendations + %d price contexts "
                    "written (run_id=%s, attempt=%d)",
                    recs_written, contexts_written, run_id, attempt + 1,
                )

                return PersistResult(
                    success=True,
                    recommendations_written=recs_written,
                    contexts_written=contexts_written,
                    run_id=run_id,
                )

            except Exception as exc:
                last_error = exc
                exc_name = type(exc).__name__

                # Only retry on transient connection-level errors
                retryable = any(
                    name in exc_name
                    for name in ("TooManyConnections", "DeadlockDetected", "ConnectionDoesNotExist")
                )

                if retryable and attempt < self.max_retries:
                    logger.warning(
                        "PersistRecommendationTool: transient error on attempt %d (%s) — retrying.",
                        attempt + 1, exc,
                    )
                    import asyncio
                    await asyncio.sleep(0.5 * (attempt + 1))   # simple back-off
                else:
                    # Non-retryable or final attempt — stop
                    break

        logger.error(
            "PersistRecommendationTool: all %d attempts failed (run_id=%s): %s",
            self.max_retries + 1, run_id, last_error,
        )
        return PersistResult(
            success=False,
            run_id=run_id,
            error=str(last_error),
        )