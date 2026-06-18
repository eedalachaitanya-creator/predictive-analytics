"""ML worker entrypoint — runs pipeline jobs off the RQ 'ml' queue.

The API (PIPELINE_EXECUTOR=worker) enqueues `app.worker.run_pipeline_job`; this
process dequeues and runs the existing `_execute_pipeline`. Run as its own
container/Deployment so the compute-heavy ML scales independently of the API:

    python -m app.worker
"""
from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("app.worker")


def _load_execute():
    """Lazy import: keeps enqueue-by-string decoupled from the router and defers
    the heavy router/ML imports to inside the worker process (and to test time)."""
    from app.pipeline_router import _execute_pipeline
    return _execute_pipeline


def run_pipeline_job(job_id: str, client_id: str, mode: str) -> None:
    """RQ job: execute one pipeline run. Run state is shared via app.job_store,
    so the API (a different process) can poll /pipeline/status while this runs."""
    log.info("worker: pipeline job=%s client=%s mode=%s", job_id, client_id, mode)
    _load_execute()(job_id, client_id, mode)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")
    import redis
    from rq import Queue, SimpleWorker, Worker

    url = os.environ.get("REDIS_URL") or "redis://localhost:6379/0"
    conn = redis.Redis.from_url(url)
    queue = Queue("ml", connection=conn)

    # The default Worker forks a work-horse per job (per-job crash isolation —
    # the right choice on Linux/k8s). On macOS that fork crashes once numpy/objc
    # have initialized, so use SimpleWorker (runs the job in-process; pod
    # restarts provide isolation in k8s). Override explicitly with
    # RQ_WORKER_CLASS=simple|fork.
    cls = os.environ.get("RQ_WORKER_CLASS", "").lower()
    if cls == "simple" or (cls == "" and sys.platform == "darwin"):
        worker_cls = SimpleWorker
    else:
        worker_cls = Worker
    log.info("ml-worker: %s on queue 'ml' via %s", worker_cls.__name__, url)
    worker_cls([queue], connection=conn).work(with_scheduler=False)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
