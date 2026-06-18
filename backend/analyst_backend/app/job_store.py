"""Externalized pipeline job state — Redis-backed, in-memory fallback.

The pipeline run/status endpoints store one job dict per run:
    {jobId, status, progress, stages: [...], startedAt, completedAt, ...}

In-process mode can use either backend; worker mode NEEDS Redis so the API and
the worker (separate processes) share state. Falls back to an in-memory dict if
Redis is unreachable, so single-process/dev keeps working with no crash.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import List, Optional

log = logging.getLogger("app.job_store")

_JOB_PREFIX = "crp:job:"
_JOB_TTL_SECS = 86400  # evict run state after a day


class InMemoryBackend:
    """Process-local dict. Fine for single-process / in-process execution."""
    def __init__(self):
        self._d: dict = {}

    def get(self, job_id: str) -> Optional[dict]:
        return self._d.get(job_id)

    def set(self, job_id: str, job: dict) -> None:
        self._d[job_id] = job

    def delete(self, job_id: str) -> None:
        self._d.pop(job_id, None)

    def keys(self) -> List[str]:
        return list(self._d.keys())


class RedisBackend:
    """Shared state for multi-process (API + worker). One JSON blob per job."""
    def __init__(self, client):
        self._r = client

    def get(self, job_id: str) -> Optional[dict]:
        raw = self._r.get(_JOB_PREFIX + job_id)
        return json.loads(raw) if raw else None

    def set(self, job_id: str, job: dict) -> None:
        self._r.setex(_JOB_PREFIX + job_id, _JOB_TTL_SECS, json.dumps(job))

    def delete(self, job_id: str) -> None:
        self._r.delete(_JOB_PREFIX + job_id)

    def keys(self) -> List[str]:
        # SCAN (cursor) rather than KEYS so we never block a Redis shared with
        # sessions/cache while listing jobs.
        out, cursor = [], 0
        while True:
            cursor, batch = self._r.scan(cursor, match=_JOB_PREFIX + "*", count=100)
            for k in batch:
                k = k.decode() if isinstance(k, bytes) else k
                out.append(k[len(_JOB_PREFIX):])
            if cursor == 0:
                break
        return out


class JobStore:
    def __init__(self, backend):
        self.backend = backend

    def create_job(self, job_id: str, stages: list) -> dict:
        job = {
            "jobId": job_id,
            "status": "queued",
            "progress": 0,
            "stages": stages,
            "startedAt": datetime.now().isoformat(),  # response model needs a str
            "completedAt": None,
            "durationSeconds": None,
            "summary": None,
        }
        self.backend.set(job_id, job)
        return job

    def get_job(self, job_id: str) -> Optional[dict]:
        return self.backend.get(job_id)

    def update_job(self, job_id: str, job: dict) -> None:
        self.backend.set(job_id, job)

    def delete_job(self, job_id: str) -> None:
        self.backend.delete(job_id)

    def list_job_ids(self) -> List[str]:
        return self.backend.keys()


def _make_backend():
    """Pick Redis if reachable, else in-memory. Decided once at import."""
    url = os.environ.get("REDIS_URL") or "redis://localhost:6379/0"
    try:
        import redis
        client = redis.Redis.from_url(url, socket_connect_timeout=2)
        client.ping()
        log.info("job_store: Redis backend (%s)", url)
        return RedisBackend(client)
    except Exception as exc:  # noqa: BLE001 — degrade, never crash on import
        log.warning("job_store: Redis unavailable (%s); in-memory backend", exc)
        return InMemoryBackend()


# Module-level singleton used by the app; tests construct their own JobStore.
store = JobStore(_make_backend())


def create_job(job_id: str, stages: list) -> dict:
    return store.create_job(job_id, stages)


def get_job(job_id: str) -> Optional[dict]:
    return store.get_job(job_id)


def update_job(job_id: str, job: dict) -> None:
    store.update_job(job_id, job)


def delete_job(job_id: str) -> None:
    store.delete_job(job_id)


def list_job_ids() -> List[str]:
    return store.list_job_ids()
