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

    TICKET_PROPS = ["subject", "content", "hs_pipeline_stage", "hs_ticket_priority",
                    "hs_ticket_category", "createdate", "closed_date"]
    FEEDBACK_PROPS = ["hs_survey_type", "hs_value", "hs_content", "hs_submission_timestamp"]

    def __init__(self, token: str, *, customer_strategy: str = "email",
                 customer_field_name: str = "Customer ID", client=None,
                 engine=None, page_size: int = 100, max_records: Optional[int] = None):
        # client is injected in tests (a fake) so resolution/fetch stay hermetic.
        # For real use we build the official SDK client with built-in 429 retry.
        if client is None:
            from hubspot import HubSpot                 # lazy: only for real network use
            from urllib3.util.retry import Retry
            client = HubSpot(access_token=token,
                             retry=Retry(total=5, status_forcelist=(429,)))
        self._client = client
        self._token = token
        self._engine = engine          # set by from_client; used by _load_email_map
        self.customer_strategy = customer_strategy or "email"
        self.customer_field_name = customer_field_name or "Customer ID"
        self.page_size = page_size
        self.max_records = max_records

    # ── config ───────────────────────────────────────────────────────────────
    @classmethod
    def from_client(cls, engine_or_conn, client_id: str, *,
                    require_enabled: bool = True):
        """Build a connector from the tenant's ``tenant_integrations`` row,
        decrypting the stored token. Returns None when there is no enabled
        HubSpot row or the token is empty (so ingest simply skips this tenant)."""
        from sqlalchemy import text
        from app.crypto import decrypt_secret

        where = "client_id = :c AND provider = 'hubspot'"
        if require_enabled:
            where += " AND enabled = true"
        sql = text("SELECT api_token_enc, customer_strategy, customer_field_name "
                   f"FROM tenant_integrations WHERE {where}")
        if hasattr(engine_or_conn, "begin") and not hasattr(engine_or_conn, "execute"):
            with engine_or_conn.connect() as cx:        # an Engine
                row = cx.execute(sql, {"c": client_id}).mappings().first()
        else:                                           # an open Connection
            row = engine_or_conn.execute(sql, {"c": client_id}).mappings().first()
        if not row:
            return None
        token = decrypt_secret(row["api_token_enc"])
        if not token:
            return None
        return cls(token=token, engine=engine_or_conn,
                   customer_strategy=row["customer_strategy"] or "email",
                   customer_field_name=row["customer_field_name"] or "Customer ID")

    def verify(self) -> dict:
        """Cheap auth/connectivity check for the Test button — list one owner."""
        page = self._client.crm.owners.owners_api.get_page(limit=1)
        return {"ok": True, "owners_seen": len(getattr(page, "results", []) or [])}

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

    # ── DB-backed email map (our customers) ──────────────────────────────────
    def _load_email_map(self, client_id: str) -> dict:
        """{lower(customer_email): customer_id} for this tenant — the email join
        key. Empty when no engine is available (e.g. injected-client tests)."""
        eng = self._engine
        if eng is None:
            return {}
        from sqlalchemy import text
        sql = text("SELECT customer_email, customer_id FROM customers "
                   "WHERE client_id = :c AND customer_email IS NOT NULL")
        if hasattr(eng, "connect") and not hasattr(eng, "execute"):   # Engine
            with eng.connect() as cx:
                rows = cx.execute(sql, {"c": client_id}).fetchall()
        else:                                                         # Connection
            rows = eng.execute(sql, {"c": client_id}).fetchall()
        return {str(e).strip().lower(): cid for (e, cid) in rows if e}

    # ── HubSpot response helpers ─────────────────────────────────────────────
    @staticmethod
    def _first_contact_id(record) -> Optional[str]:
        assoc = getattr(record, "associations", None) or {}
        contacts = assoc.get("contacts") if isinstance(assoc, dict) else getattr(assoc, "contacts", None)
        if not contacts:
            return None
        results = (contacts.get("results") if isinstance(contacts, dict)
                   else getattr(contacts, "results", None)) or []
        if not results:
            return None
        first = results[0]
        cid = first.get("id") if isinstance(first, dict) else getattr(first, "id", None)
        return str(cid) if cid is not None else None

    def _emails_for_contacts(self, contact_ids: List[str]) -> dict:
        ids = [c for c in dict.fromkeys(contact_ids) if c]   # dedupe, drop falsy
        if not ids:
            return {}
        from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId
        inp = BatchReadInputSimplePublicObjectId(
            inputs=[{"id": i} for i in ids], properties=["email"])
        resp = self._client.crm.contacts.batch_api.read(
            batch_read_input_simple_public_object_id=inp)
        out = {}
        for r in (getattr(resp, "results", []) or []):
            out[str(r.id)] = (r.properties or {}).get("email")
        return out

    @staticmethod
    def _parse_dt(value):
        from ml.connectors.jira import parse_jira_dt   # reuse ISO-8601 parser
        try:
            return parse_jira_dt(value)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _normalize_rating(survey_type: Optional[str], value) -> int:
        """NPS 0–10 → 1–5; CSAT/CES/unknown clamped to 1–5. 0 if unparseable."""
        try:
            v = float(value)
        except (TypeError, ValueError):
            return 0
        if (survey_type or "").upper() == "NPS":
            return max(1, min(5, round(v / 2)))
        return max(1, min(5, round(v)))

    def _iter_pages(self, get_page, **base_kwargs):
        """Yield each PAGE's result list across cursor-paginated get_page() calls,
        so callers can batch per page (one contact-email batch-read per page,
        not one per record)."""
        after = None
        while True:
            page = get_page(after=after, **base_kwargs)
            yield (getattr(page, "results", []) or [])
            paging = getattr(page, "paging", None)
            after = paging.next.after if (paging and getattr(paging, "next", None)) else None
            if not after:
                break

    # ── fetch ────────────────────────────────────────────────────────────────
    def fetch(self, client_id: str, since: Optional[date] = None,
              customer_ids: Optional[List[str]] = None
              ) -> Iterable[Union[RawTicket, RawReview]]:
        known: Optional[Set[str]] = set(customer_ids) if customer_ids else None
        email_to_id = self._load_email_map(client_id)
        yielded = 0

        def _capped():
            return self.max_records and yielded >= self.max_records

        # Tickets → support_tickets (one contact-email batch-read per page)
        for results in self._iter_pages(self._client.crm.tickets.basic_api.get_page,
                                        limit=self.page_size, properties=self.TICKET_PROPS,
                                        associations=["contacts"]):
            id_to_email = self._emails_for_contacts(
                [self._first_contact_id(r) for r in results])
            for r in results:
                props = r.properties or {}
                email = id_to_email.get(self._first_contact_id(r))
                customer = self._resolve_customer(props, email, known, email_to_id)
                if not customer or (known is not None and customer not in known):
                    continue
                yield RawTicket(
                    client_id=client_id, ticket_id=str(r.id), customer_id=customer,
                    source=self.source, subject=props.get("subject") or "",
                    text=(props.get("content") or props.get("subject") or "").strip()
                         or (props.get("subject") or ""),
                    ticket_type=props.get("hs_ticket_category"),
                    priority=props.get("hs_ticket_priority"),
                    status=props.get("hs_pipeline_stage"),
                    opened_date=self._parse_dt(props.get("createdate")),
                    resolved_date=self._parse_dt(props.get("closed_date")),
                )
                yielded += 1
                if _capped():
                    return

        # Feedback / NPS → customer_reviews
        for results in self._iter_pages(self._client.crm.objects.basic_api.get_page,
                                        object_type="feedback_submissions",
                                        limit=self.page_size, properties=self.FEEDBACK_PROPS,
                                        associations=["contacts"]):
            id_to_email = self._emails_for_contacts(
                [self._first_contact_id(r) for r in results])
            for r in results:
                props = r.properties or {}
                email = id_to_email.get(self._first_contact_id(r))
                customer = self._resolve_customer(props, email, known, email_to_id)
                if not customer or (known is not None and customer not in known):
                    continue
                dt = self._parse_dt(props.get("hs_submission_timestamp"))
                yield RawReview(
                    client_id=client_id, review_id=str(r.id), customer_id=customer,
                    source=self.source,
                    rating=self._normalize_rating(props.get("hs_survey_type"),
                                                  props.get("hs_value")),
                    text=props.get("hs_content") or "",
                    review_date=dt.date() if dt else None,
                )
                yielded += 1
                if _capped():
                    return
