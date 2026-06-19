"""External-signal connectors (Jira, Google reviews, …)."""
import logging
import os

from ml.connectors.synthetic import (
    SyntheticJiraConnector,
    SyntheticGoogleReviewsConnector,
)

# Back-compat synthetic default list (demos / explicit callers / tests).
CONNECTORS = [SyntheticJiraConnector(), SyntheticGoogleReviewsConnector()]


def default_connectors(client_id=None, engine=None):
    """Connector set used by ``run_ingest`` at pipeline time.

    Priority:
      * per-tenant DB config (``tenant_integrations``, enabled) → that tenant's
        ``JiraConnector`` — the multi-tenant path (needs ``client_id`` + ``engine``)
      * else real ``JIRA_*`` env configured  → env ``JiraConnector`` (single-tenant)
      * else ``EXTERNAL_SIGNALS_SYNTHETIC=1`` → synthetic connectors (demo/test)
      * else                                  → ``[]`` (ingest is a no-op)

    A configured Jira (DB or env) takes precedence over the synthetic flag: a
    tenant wired to a real Jira must never get synthetic tickets mixed in. Never
    raises — a bad config degrades to no connectors, not a pipeline crash.
    """
    from ml.connectors.jira import JiraConnector
    log = logging.getLogger("ml.connectors")

    # 1) Per-tenant integration from the DB (multi-tenant).
    if client_id and engine is not None:
        try:
            conn = JiraConnector.from_client(engine, client_id)
            if conn is not None:
                return [conn]
        except Exception as exc:  # noqa: BLE001 — never break ingest on config error
            log.warning("tenant %s Jira from_client failed (%s); falling back",
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
