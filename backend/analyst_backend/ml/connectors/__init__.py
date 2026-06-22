"""External-signal connectors (Jira, Google reviews, …)."""
import logging
import os

from sqlalchemy import text

from ml.connectors.synthetic import (
    SyntheticJiraConnector,
    SyntheticGoogleReviewsConnector,
)

# Back-compat synthetic default list (demos / explicit callers / tests).
CONNECTORS = [SyntheticJiraConnector(), SyntheticGoogleReviewsConnector()]


def default_connectors(client_id=None, engine=None):
    """Connector set used by ``run_ingest`` at pipeline time.

    Priority:
      * per-tenant DB config (``tenant_integrations``, enabled) → ALL of that
        tenant's enabled providers, each built via the registry (multi-tenant,
        multi-provider; needs ``client_id`` + ``engine``)
      * else real ``JIRA_*`` env configured  → env ``JiraConnector`` (single-tenant)
      * else ``EXTERNAL_SIGNALS_SYNTHETIC=1`` → synthetic connectors (demo/test)
      * else                                  → ``[]`` (ingest is a no-op)

    A configured DB integration takes precedence over the synthetic flag: a tenant
    wired to real providers must never get synthetic tickets mixed in. Never raises
    — a bad config degrades to no connectors, not a pipeline crash.
    """
    from ml.connectors.jira import JiraConnector
    log = logging.getLogger("ml.connectors")

    # 1) Per-tenant integrations from the DB — build EVERY enabled provider.
    if client_id and engine is not None:
        try:
            from ml.connectors.registry import CONNECTOR_REGISTRY
            _sql = text("SELECT provider FROM tenant_integrations "
                        "WHERE client_id = :c AND enabled = true")
            if hasattr(engine, "connect") and not hasattr(engine, "execute"):  # Engine
                with engine.connect() as cx:
                    rows = cx.execute(_sql, {"c": client_id}).fetchall()
            else:                                                              # Connection
                rows = engine.execute(_sql, {"c": client_id}).fetchall()
            built = []
            for (provider,) in rows:
                cls = CONNECTOR_REGISTRY.get(provider)
                if cls is None:
                    log.warning("tenant %s has unknown provider %r; skipping",
                                client_id, provider)
                    continue
                try:
                    conn = cls.from_client(engine, client_id)
                    if conn is not None:
                        built.append(conn)
                except Exception as exc:  # noqa: BLE001 — one bad provider must not block others
                    log.warning("tenant %s %s from_client failed (%s); skipping",
                                client_id, provider, exc)
            if built:
                return built
        except Exception as exc:  # noqa: BLE001 — never break ingest on config error
            log.warning("tenant %s integrations read failed (%s); falling back",
                        client_id, exc)

    # 2) Global env config (single-tenant / back-compat).
    if JiraConnector.is_configured():
        try:
            return [JiraConnector.from_env()]
        except Exception as exc:  # noqa: BLE001
            log.warning("Jira env present but from_env() failed (%s); skipping", exc)
            return []

    # 3) Synthetic (demo/test).
    if os.getenv("EXTERNAL_SIGNALS_SYNTHETIC") == "1":
        return [SyntheticJiraConnector(), SyntheticGoogleReviewsConnector()]
    return []
