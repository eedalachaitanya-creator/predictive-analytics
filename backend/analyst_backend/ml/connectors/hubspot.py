"""Real HubSpot connector — pulls CRM tickets + feedback via the official SDK and
normalizes to RawTicket / RawReview.

Same ``ExternalSignalConnector`` port the Jira connector implements, so the churn
pipeline is unchanged: records land in support_tickets / customer_reviews →
emotion/sentiment → point-in-time features → temporal churn model.

Auth ........ HubSpot Private App access token (Bearer). The base URL is fixed
              (api.hubapi.com), so unlike Jira there is NO tenant-supplied URL and
              NO SSRF guard is needed.
Customer link EMAIL-first: the record's associated contact email → our
              customers.customer_email (case-insensitive). Falls back to a named
              HubSpot property holding our customer_id. strategy ∈ {email, field}.
Resilience .. rate limits handled by the SDK's urllib3 retry (429); one bad
              connector never kills ingest (run_ingest wraps fetch in try/except).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Iterable, List, Optional, Set, Union

from ml.connectors.base import ExternalSignalConnector, RawTicket, RawReview

logger = logging.getLogger("ml.connectors.hubspot")


class HubSpotConnector(ExternalSignalConnector):
    source = "hubspot"
    signal_kind = "ticket"   # informational: fetch() yields BOTH tickets and reviews

    def __init__(self, token: str, *, customer_strategy: str = "email",
                 customer_field_name: str = "Customer ID", client=None,
                 page_size: int = 100, max_records: Optional[int] = None):
        # client is injected in tests (a fake) so resolution/fetch stay hermetic.
        # For real use we build the official SDK client with built-in 429 retry.
        if client is None:
            from hubspot import HubSpot                 # lazy: only for real network use
            from urllib3.util.retry import Retry
            client = HubSpot(access_token=token,
                             retry=Retry(total=5, status_forcelist=(429,)))
        self._client = client
        self._token = token
        self.customer_strategy = customer_strategy or "email"
        self.customer_field_name = customer_field_name or "Customer ID"
        self.page_size = page_size
        self.max_records = max_records

    # ── customer resolution (pure, unit-tested directly) ─────────────────────
    def _resolve_customer(self, props: dict, contact_email: Optional[str],
                          known: Optional[Set[str]],
                          email_to_id: dict) -> Optional[str]:
        """Map a HubSpot record to our canonical customer_id.

        strategy 'email' (default): match the associated contact email to a
        customer (case-insensitive), then fall back to the configured property.
        strategy 'field': use the configured property only.
        Returns the CANONICAL stored id (so it survives the known-customer guard),
        or None when nothing matches (caller skips the record)."""
        # Email path (default strategy, email-first)
        if self.customer_strategy == "email" and contact_email:
            hit = (email_to_id or {}).get(str(contact_email).strip().lower())
            if hit:
                return hit
        # Field path: strategy 'field', OR 'email' fallback when no email match.
        if self.customer_strategy in ("field", "email"):
            raw = (props or {}).get(self.customer_field_name)
            if raw:
                canon = {str(c).lower(): c for c in (known or set())}
                return canon.get(str(raw).strip().lower())
        return None

    # ── fetch (implemented in the next task) ─────────────────────────────────
    def fetch(self, client_id: str, since: Optional[date] = None,
              customer_ids: Optional[List[str]] = None
              ) -> Iterable[Union[RawTicket, RawReview]]:
        return iter(())   # placeholder — implemented in Task 4
