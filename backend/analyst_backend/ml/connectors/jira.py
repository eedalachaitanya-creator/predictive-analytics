"""Real Jira Cloud connector — pulls issues via REST and normalizes to RawTicket.

This is the production adapter behind the same ``ExternalSignalConnector`` port the
``SyntheticJiraConnector`` implements, so the rest of the churn pipeline is
unchanged: tickets land in ``support_tickets`` → emotion classifier → point-in-time
emotion features → temporal churn model.

Auth ........ HTTP Basic (Atlassian email + API token).
Customer link a Jira **custom field** holding the CRP ``customer_id``. We resolve
              it BY NAME (default "Customer ID") so no ``customfield_NNNNN`` id is
              ever hardcoded — it differs per Jira site.
Config ...... env only (never commit secrets):
                JIRA_BASE_URL        https://your-site.atlassian.net
                JIRA_EMAIL           your Atlassian account email
                JIRA_API_TOKEN       id.atlassian.com → Security → API tokens
                JIRA_CUSTOMER_FIELD  custom-field name (default "Customer ID")
                JIRA_PROJECT_MAP     optional JSON {client_id: project_key}
                JIRA_PROJECT_KEY     optional single project key (fallback)

Resilience .. one bad connector must never kill ingest, so callers wrap fetch()
              in try/except (see ml.connectors.ingest.run_ingest).
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import socket
from datetime import date, datetime
from typing import Iterable, List, Optional, Set
from urllib.parse import urlparse

from ml.connectors.base import ExternalSignalConnector, RawTicket

logger = logging.getLogger("ml.connectors.jira")

_SEARCH_PATH = "/rest/api/3/search/jql"   # enhanced JQL search (token pagination)
_FIELD_PATH = "/rest/api/3/field"
# ADF container node types whose children are separate text blocks (newline-join);
# everything else (paragraph, heading, inline marks) concatenates with no sep.
_BLOCK_TYPES = {"doc", "bulletList", "orderedList", "listItem", "blockquote",
                "panel", "table", "tableRow"}
# A Jira label that names a CRP customer (fallback when no custom field is set).
_CUST_LABEL_RE = re.compile(r"^CUST-\w+$", re.IGNORECASE)


# ── pure helpers (unit-tested directly) ──────────────────────────────────────
def adf_to_text(node) -> str:
    """Flatten a Jira v3 Atlassian Document Format description to plain text.
    Tolerates None (→ "") and a legacy plain string (→ itself)."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    return _render(node).strip()


def _render(node) -> str:
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""
    txt = node.get("text")
    if isinstance(txt, str):
        return txt
    children = node.get("content") or []
    sep = "\n" if node.get("type") in _BLOCK_TYPES else ""
    return sep.join(_render(c) for c in children)


def assert_public_https_url(url: str) -> None:
    """SSRF guard for the tenant-supplied Jira base URL. Raise ``ValueError``
    unless the URL is ``https`` and its host resolves *only* to public IPs.

    Rejects loopback / private / link-local (incl. 169.254.169.254 cloud
    metadata) / multicast / reserved / unspecified targets. Called both at save
    time (input validation) and before every outbound request (defeats DNS
    rebinding + redirects-to-internal)."""
    parsed = urlparse(url or "")
    if parsed.scheme != "https":
        raise ValueError("Jira base URL must use https")
    host = parsed.hostname
    if not host:
        raise ValueError("Jira base URL has no host")
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise ValueError(f"Jira base URL host does not resolve: {host}")
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
                or ip.is_reserved or ip.is_unspecified):
            raise ValueError("Jira base URL must resolve to a public address")


def parse_jira_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse Jira ISO-8601 timestamps ('...+0000' / '...Z') → aware datetime."""
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Insert the missing colon in a basic-format offset (+0000 → +00:00)
        if len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
            return datetime.fromisoformat(s[:-2] + ":" + s[-2:])
        raise


# ── connector ────────────────────────────────────────────────────────────────
class JiraConnector(ExternalSignalConnector):
    source = "jira"
    signal_kind = "ticket"

    def __init__(self, base_url: str, email: str, api_token: str, *,
                 customer_field_name: str = "Customer ID",
                 customer_strategy: str = "auto",
                 project_for_client: Optional[dict] = None,
                 project_key: Optional[str] = None,
                 session=None, page_size: int = 100,
                 max_issues: Optional[int] = None, timeout: int = 30):
        # We guard egress only for REAL network use (we created the session). When
        # a caller injects a session (tests), it's a fake — skip DNS/SSRF checks so
        # the unit tests stay hermetic.
        self._egress_check = session is None
        if session is None:
            import requests  # lazy: only needed for real network use
            session = requests.Session()
        self.base_url = base_url.rstrip("/")
        self._auth = (email, api_token)
        self.customer_field_name = customer_field_name
        # 'auto'  → custom field if present, else a CUST-* label
        # 'field' → custom field only (missing field is a hard error)
        # 'label' → label only (no custom-field lookup at all)
        self.customer_strategy = customer_strategy
        self.project_for_client = project_for_client or {}
        self.project_key = project_key
        self._session = session
        self.page_size = page_size
        self.max_issues = max_issues
        self.timeout = timeout
        self._field_id: Optional[str] = None

    # config ----------------------------------------------------------------
    @staticmethod
    def is_configured() -> bool:
        return all(os.getenv(k) for k in
                   ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN"))

    @classmethod
    def from_env(cls) -> "JiraConnector":
        missing = [k for k in ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN")
                   if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"JiraConnector.from_env: missing {missing}")
        project_map = {}
        raw_map = os.getenv("JIRA_PROJECT_MAP")
        if raw_map:
            try:
                project_map = json.loads(raw_map)
            except json.JSONDecodeError as exc:
                logger.warning("JIRA_PROJECT_MAP is not valid JSON (%s); ignoring", exc)
        return cls(
            base_url=os.environ["JIRA_BASE_URL"],
            email=os.environ["JIRA_EMAIL"],
            api_token=os.environ["JIRA_API_TOKEN"],
            customer_field_name=os.getenv("JIRA_CUSTOMER_FIELD", "Customer ID"),
            customer_strategy=os.getenv("JIRA_CUSTOMER_STRATEGY", "auto"),
            project_for_client=project_map,
            project_key=os.getenv("JIRA_PROJECT_KEY"),
        )

    @classmethod
    def from_client(cls, engine_or_conn, client_id: str, *, session=None,
                    require_enabled: bool = True):
        """Build a connector from a tenant's ``tenant_integrations`` row, decrypting
        the stored API token. Returns ``None`` when the tenant has no Jira row (or
        no *enabled* one, by default) or the row is incomplete — so the pipeline
        simply skips ingest for that tenant rather than erroring. Pass
        ``require_enabled=False`` to build from a saved-but-not-yet-enabled row
        (used by the 'Test connection' endpoint)."""
        from sqlalchemy import text
        from app.crypto import decrypt_secret  # deferred: keeps ml import-light

        where = "client_id = :c AND provider = 'jira'"
        if require_enabled:
            where += " AND enabled = true"
        sql = text(
            "SELECT base_url, email, api_token_enc, project_key, customer_strategy, "
            f"customer_field_name FROM tenant_integrations WHERE {where}")
        if hasattr(engine_or_conn, "begin") and not hasattr(engine_or_conn, "execute"):
            with engine_or_conn.connect() as cx:
                row = cx.execute(sql, {"c": client_id}).mappings().first()
        else:
            row = engine_or_conn.execute(sql, {"c": client_id}).mappings().first()
        if not row:
            return None
        token = decrypt_secret(row["api_token_enc"])
        if not (row["base_url"] and row["email"] and token):
            return None
        return cls(
            base_url=row["base_url"], email=row["email"], api_token=token,
            customer_field_name=row["customer_field_name"] or "Customer ID",
            customer_strategy=row["customer_strategy"] or "auto",
            project_key=row["project_key"], session=session)

    def verify(self) -> dict:
        """Cheap auth/connectivity check — GET /myself. Returns the authenticated
        Atlassian account, or raises (the endpoint turns failures into a friendly
        message). Used by the 'Test connection' button."""
        data = self._get("/rest/api/3/myself", {})
        return {"account_id": data.get("accountId"),
                "display_name": data.get("displayName"),
                "email": data.get("emailAddress")}

    # http ------------------------------------------------------------------
    def _get(self, path: str, params: dict) -> dict:
        url = self.base_url + path
        if self._egress_check:
            # Re-validate the host on EVERY request (defeats DNS rebinding) and
            # forbid redirects (a 3xx could bounce us to an internal target).
            assert_public_https_url(url)
        resp = self._session.get(
            url, params=params, auth=self._auth,
            headers={"Accept": "application/json"}, timeout=self.timeout,
            allow_redirects=False)
        resp.raise_for_status()
        return resp.json()

    def customer_field_id(self) -> str:
        """Resolve the ``customfield_NNNNN`` id for the configured field NAME."""
        if self._field_id is not None:
            return self._field_id
        fields = self._get(_FIELD_PATH, {})
        want = self.customer_field_name.strip().lower()
        for f in fields:
            if str(f.get("name", "")).strip().lower() == want:
                self._field_id = f["id"]
                logger.info("Jira customer field '%s' → %s",
                            self.customer_field_name, self._field_id)
                return self._field_id
        raise ValueError(
            f"Jira custom field named '{self.customer_field_name}' not found on "
            f"{self.base_url}. Create it (a text field on the issue) or set "
            f"JIRA_CUSTOMER_FIELD to the exact field name.")

    def _safe_field_id(self) -> Optional[str]:
        """Resolve the custom-field id, tolerating absence when the strategy
        allows labels. 'field' → missing field is a hard error; 'label' → never
        look up a field; 'auto' → try the field, fall back to labels on miss."""
        if self.customer_strategy == "label":
            return None
        try:
            return self.customer_field_id()
        except ValueError:
            if self.customer_strategy == "auto":
                logger.info("Jira: custom field '%s' not found — falling back to "
                            "label strategy (tag issues with a CUST-* label).",
                            self.customer_field_name)
                return None
            raise

    def _customer_from_labels(self, f: dict,
                              known: Optional[Set[str]]) -> Optional[str]:
        """Fallback customer link: a Jira label naming a CRP customer. Prefers an
        exact match against the known customer set; else any CUST-* label."""
        for lbl in (f.get("labels") or []):
            if known is not None and lbl in known:
                return lbl
            if known is None and _CUST_LABEL_RE.match(str(lbl)):
                return lbl
        return None

    # query -----------------------------------------------------------------
    def _project(self, client_id: str) -> Optional[str]:
        return self.project_for_client.get(client_id) or self.project_key

    def _jql(self, client_id: str, since: Optional[date]) -> str:
        parts: List[str] = []
        proj = self._project(client_id)
        if proj:
            parts.append(f'project = "{proj}"')
        if since:
            parts.append(f'updated >= "{since.isoformat()}"')
        jql = " AND ".join(parts)
        return f"{jql} ORDER BY created ASC" if jql else "ORDER BY created ASC"

    # fetch -----------------------------------------------------------------
    def fetch(self, client_id: str, since: Optional[date] = None,
              customer_ids: Optional[List[str]] = None) -> Iterable[RawTicket]:
        fid = self._safe_field_id()
        known: Optional[Set[str]] = set(customer_ids) if customer_ids else None
        flds = ["summary", "description", "issuetype", "priority", "status",
                "created", "resolutiondate", "labels"]
        if fid:
            flds.append(fid)
        fields_param = ",".join(flds)
        jql = self._jql(client_id, since)

        token: Optional[str] = None
        yielded = 0
        while True:
            params = {"jql": jql, "fields": fields_param, "maxResults": self.page_size}
            if token:
                params["nextPageToken"] = token
            data = self._get(_SEARCH_PATH, params)
            for issue in data.get("issues", []):
                ticket = self._to_ticket(client_id, issue, fid, known)
                if ticket is None:
                    continue
                yield ticket
                yielded += 1
                if self.max_issues and yielded >= self.max_issues:
                    return
            token = data.get("nextPageToken")
            if data.get("isLast") or not token:
                break

    def _to_ticket(self, client_id: str, issue: dict, fid: Optional[str],
                   known: Optional[Set[str]]) -> Optional[RawTicket]:
        f = issue.get("fields", {}) or {}
        customer_id: Optional[str] = None
        if fid is not None:
            raw_cust = f.get(fid)
            if raw_cust is not None:
                customer_id = str(raw_cust).strip() or None
        if not customer_id:                       # fall back to a CUST-* label
            customer_id = self._customer_from_labels(f, known)
        if not customer_id:
            return None
        if known is not None and customer_id not in known:
            return None  # ticket for a customer we don't track — skip

        summary = f.get("summary") or ""
        body = adf_to_text(f.get("description"))
        text = body.strip() or summary  # emotion runs on this; prefer the body

        return RawTicket(
            client_id=client_id,
            ticket_id=issue["key"],
            customer_id=customer_id,
            source=self.source,
            subject=summary,
            text=text,
            ticket_type=(f.get("issuetype") or {}).get("name"),
            priority=(f.get("priority") or {}).get("name"),
            status=(f.get("status") or {}).get("name"),
            opened_date=parse_jira_dt(f.get("created")),
            resolved_date=parse_jira_dt(f.get("resolutiondate")),
        )
