"""Pipeline execution strategy: in-process (default) or queue (worker).

PIPELINE_EXECUTOR=inprocess (default) -> InProcessExecutor: today's behavior —
    a daemon thread runs the pipeline on the API host.
PIPELINE_EXECUTOR=worker             -> QueueExecutor: enqueue to RQ; a separate
    ML worker process runs it (independently scalable).

If `worker` is selected but rq/Redis is unavailable, get_executor() logs and
returns InProcessExecutor — the run still completes, just not distributed. The
API never 500s on this path.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Callable, Optional

log = logging.getLogger("app.pipeline_executor")

_ML_QUEUE = "ml"
# Enqueue by DOTTED-PATH STRING so the executor never imports the worker (which
# imports the router, which imports the executor) — breaks the cycle.
_JOB_FN = "app.worker.run_pipeline_job"
_JOB_TIMEOUT_SECS = 3600


class InProcessExecutor:
    """Runs the pipeline in a background daemon thread (current behavior)."""
    def __init__(self, runner: Callable[[str, str, str], None]):
        self._runner = runner

    def submit(self, job_id: str, client_id: str, mode: str) -> None:
        threading.Thread(
            target=self._runner, args=(job_id, client_id, mode), daemon=True
        ).start()


class QueueExecutor:
    """Enqueues the pipeline to RQ for a separate worker to execute."""
    def __init__(self, queue):
        self._q = queue

    def submit(self, job_id: str, client_id: str, mode: str) -> None:
        self._q.enqueue(_JOB_FN, job_id, client_id, mode, job_timeout=_JOB_TIMEOUT_SECS)


def _make_queue():
    """Build an RQ Queue on the configured Redis. Raises on any failure."""
    import redis
    from rq import Queue

    url = os.environ.get("REDIS_URL") or "redis://localhost:6379/0"
    conn = redis.Redis.from_url(url, socket_connect_timeout=2)
    conn.ping()
    return Queue(_ML_QUEUE, connection=conn)


def get_executor(runner: Optional[Callable[[str, str, str], None]] = None):
    """Choose the executor from PIPELINE_EXECUTOR; fall back to in-process if the
    worker queue can't be built. `runner` is the in-process pipeline callable
    (lazily imported from the router if not supplied, to avoid an import cycle)."""
    if runner is None:
        from app.pipeline_router import _execute_pipeline as runner  # lazy
    mode = os.environ.get("PIPELINE_EXECUTOR", "inprocess").strip().lower()
    if mode == "worker":
        try:
            return QueueExecutor(_make_queue())
        except Exception as exc:  # noqa: BLE001 — graceful fallback, never crash
            log.warning("PIPELINE_EXECUTOR=worker but queue unavailable (%s); "
                        "falling back to in-process execution", exc)
    return InProcessExecutor(runner)
