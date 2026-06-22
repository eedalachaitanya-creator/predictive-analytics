# Third-Party Integration Framework + HubSpot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline) or superpowers:subagent-driven-development to implement task-by-task. Steps use `- [ ]` checkboxes.

**Goal:** Generalize the Jira-only external-signal plumbing into a provider-agnostic framework (registry + generic `/integrations/{provider}` API + generic UI) and ship HubSpot end-to-end (tickets → `support_tickets`, feedback/NPS → `customer_reviews`).

**Architecture:** A provider registry sits in front of the existing port/adapter connector layer. `default_connectors()` builds every enabled provider per tenant. The router dispatches `/integrations/{provider}/{config,test,sync}` through the registry. Downstream ingest → emotion/sentiment → churn model is untouched.

**Tech Stack:** FastAPI + SQLAlchemy core; Angular 21 (zoneless signals); `hubspot-api-client` (official SDK); pytest in `tests_temporal/` (git-excluded); existing Fernet crypto.

## Global Constraints
- Reuse `support_tickets` / `customer_reviews` — NO model/schema change; **zero DDL** (HubSpot reuses existing `tenant_integrations` columns; `customer_strategy='email'`).
- Token encrypted at rest (`api_token_enc`), NEVER returned by GET.
- Customer match: email-first → `customers.customer_email`, case-insensitive `{lower:canonical}`; fallback configured field; no match → skip+log.
- No SSRF guard for HubSpot (fixed base URL `api.hubapi.com`); keep Jira's SSRF guard.
- All `/integrations` endpoints enforce `_require_client_access`; `provider` validated against `CONNECTOR_REGISTRY`.
- TDD: hermetic tests with a fake HubSpot client (mirror Jira's `_FakeSession`). The existing 24 Jira tests MUST stay green.
- Tests live in `backend/analyst_backend/tests_temporal/` (git-excluded). Run with `./venv/bin/python -m pytest`.

---

## File Structure
- Create `backend/analyst_backend/ml/connectors/registry.py` — `CONNECTOR_REGISTRY`, `PROVIDER_META`.
- Create `backend/analyst_backend/ml/connectors/hubspot.py` — `HubSpotConnector`.
- Modify `backend/analyst_backend/ml/connectors/base.py` — ABC `fetch(..., customer_ids=None)`.
- Modify `backend/analyst_backend/ml/connectors/__init__.py` — `default_connectors()` multi-provider via registry.
- Modify `backend/analyst_backend/app/integrations_router.py` — generic `/integrations/{provider}/*` + `/jira` aliases.
- Modify `backend/analyst_backend/requirements.txt` — add `hubspot-api-client`.
- Create `UI/UI/src/app/pages/integration-card.ts` + `.html` — generalized card (from `jira-integration`).
- Modify `UI/UI/src/app/pages/settings.ts` + `.html` — render one card per provider.
- Tests: `tests_temporal/test_connector_registry.py`, `test_hubspot_connector.py`, `test_integrations_router_generic.py`.

---

### Task 1: Dependency + ABC contract

**Files:** Modify `requirements.txt`, `ml/connectors/base.py`. Test: `tests_temporal/test_connector_registry.py` (import smoke).

- [ ] **Step 1 — install SDK + pin it**
```bash
cd backend/analyst_backend && ./venv/bin/python -m pip install "hubspot-api-client>=11,<13"
echo 'hubspot-api-client==11.1.0   # HubSpot connector (CRM tickets + feedback)' >> requirements.txt
```
(Use the actual installed version from `pip show hubspot-api-client`.)

- [ ] **Step 2 — align the ABC** in `base.py`: change the abstract signature to
```python
@abstractmethod
def fetch(self, client_id: str, since: Optional[date] = None,
          customer_ids: Optional[list] = None) -> Iterable[Union[RawTicket, RawReview]]:
    ...
```
- [ ] **Step 3 — verify** Jira still imports/passes: `./venv/bin/python -m pytest tests_temporal/test_jira_connector.py -q` → 24 passed.
- [ ] **Step 4 — commit** `feat(connectors): add hubspot-api-client + align ABC fetch signature`.

---

### Task 2: Provider registry + multi-provider `default_connectors()`

**Files:** Create `ml/connectors/registry.py`; modify `ml/connectors/__init__.py`. Test: `tests_temporal/test_connector_registry.py`.

- [ ] **Step 1 — failing tests**
```python
# test_connector_registry.py
from sqlalchemy import text as _sql
from ml.connectors.registry import CONNECTOR_REGISTRY, PROVIDER_META
from ml.connectors import default_connectors

def test_registry_has_jira_and_hubspot():
    assert set(CONNECTOR_REGISTRY) >= {"jira", "hubspot"}
    assert PROVIDER_META["hubspot"]["fields"] == ["api_token"]
    assert "email" in PROVIDER_META["hubspot"]["strategies"]

def _seed(tx, client, provider, token="t"):
    from app.crypto import encrypt_secret
    tx.execute(_sql("INSERT INTO tenant_integrations (client_id, provider, api_token_enc, enabled) "
                    "VALUES (:c,:p,:t,true)"), {"c": client, "p": provider, "t": encrypt_secret(token)})

def test_default_connectors_returns_all_enabled_providers(tx, test_client, monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv("INTEGRATION_ENC_KEY", Fernet.generate_key().decode())
    _seed(tx, test_client, "jira"); _seed(tx, test_client, "hubspot")
    sources = {c.source for c in default_connectors(client_id=test_client, engine=tx)}
    assert sources == {"jira", "hubspot"}
```
- [ ] **Step 2 — run, expect fail** (`registry` missing; `default_connectors` returns ≤1).
- [ ] **Step 3 — implement `registry.py`**
```python
from ml.connectors.jira import JiraConnector
from ml.connectors.hubspot import HubSpotConnector

CONNECTOR_REGISTRY = {"jira": JiraConnector, "hubspot": HubSpotConnector}
PROVIDER_META = {
    "jira":    {"label": "Jira", "fields": ["base_url", "email", "api_token", "project_key"],
                "strategies": ["auto", "field", "label"]},
    "hubspot": {"label": "HubSpot", "fields": ["api_token"],
                "strategies": ["email", "field"]},
}
```
- [ ] **Step 4 — rewrite `default_connectors()`** so the DB branch queries every enabled provider and builds via the registry:
```python
def default_connectors(client_id=None, engine=None):
    from ml.connectors.registry import CONNECTOR_REGISTRY
    log = logging.getLogger("ml.connectors")
    if client_id and engine is not None:
        try:
            with engine.connect() as cx:
                rows = cx.execute(text(
                    "SELECT provider FROM tenant_integrations "
                    "WHERE client_id=:c AND enabled=true"), {"c": client_id}).fetchall()
            built = []
            for (provider,) in rows:
                cls = CONNECTOR_REGISTRY.get(provider)
                if not cls:
                    continue
                try:
                    conn = cls.from_client(engine, client_id)
                    if conn is not None:
                        built.append(conn)
                except Exception as exc:  # noqa: BLE001
                    log.warning("tenant %s %s from_client failed (%s); skipping",
                                client_id, provider, exc)
            if built:
                return built
        except Exception as exc:  # noqa: BLE001
            log.warning("default_connectors db read failed (%s); falling back", exc)
    # env Jira / synthetic fallback — UNCHANGED from current code
    ...
```
  (Note: `tx` fixture is a Connection; `from_client` already accepts Engine OR Connection.)
- [ ] **Step 5 — run** registry tests + Jira regression → all green.
- [ ] **Step 6 — commit** `feat(connectors): provider registry + multi-provider default_connectors`.

---

### Task 3: HubSpot connector — customer resolution

**Files:** Create `ml/connectors/hubspot.py`. Test: `tests_temporal/test_hubspot_connector.py`.

**Interfaces — Produces:** `HubSpotConnector(token, *, customer_strategy="email", customer_field_name="Customer ID", client=None)`; `source="hubspot"`; `_resolve_customer(props, contact_email, known) -> Optional[str]`; classmethods `from_client`, instance `verify`, `fetch`.

- [ ] **Step 1 — failing tests for resolution** (pure, no network)
```python
import ml.connectors.hubspot as hs
def _c(strategy="email", field="Customer ID"):
    return hs.HubSpotConnector(token="t", customer_strategy=strategy,
                               customer_field_name=field, client=object())

def test_email_match_case_insensitive_returns_canonical():
    c = _c(); known = {"CUST-002"}
    assert c._resolve_customer({}, "a@b.com", known, email_to_id={"a@b.com": "CUST-002"}) == "CUST-002"

def test_no_match_returns_none():
    c = _c()
    assert c._resolve_customer({}, "x@y.com", {"CUST-002"}, email_to_id={}) is None

def test_field_override_uses_property():
    c = _c(strategy="field", field="crp_customer_id")
    assert c._resolve_customer({"crp_customer_id": "cust-002"}, None, {"CUST-002"},
                               email_to_id={}) == "CUST-002"
```
  (Resolution builds `{id.lower(): id}` from `known`; email path lowercases the email→id map; field path lowercases the property value — reusing the Jira case-insensitive fix.)
- [ ] **Step 2 — run, expect fail** (module/class missing).
- [ ] **Step 3 — implement the class skeleton + `_resolve_customer`** with the `{lower:canonical}` map (email-first, field fallback, case-insensitive). Construct `HubSpot(access_token=token, retry=Retry(total=5, status_forcelist=(429,)))` when `client is None`.
- [ ] **Step 4 — run** → green. **Commit** `feat(hubspot): connector skeleton + email-first customer resolution`.

---

### Task 4: HubSpot connector — fetch tickets + feedback

**Files:** `ml/connectors/hubspot.py`. Test: `test_hubspot_connector.py`.

- [ ] **Step 1 — failing tests** using a fake client returning canned pages (mirror Jira `_FakeSession`):
```python
class _FakeBasicApi:
    def __init__(self, pages): self._pages = list(pages)
    def get_page(self, **kw): return self._pages.pop(0)
# build a fake `client.crm.tickets.basic_api`, `client.crm.objects.basic_api`,
# `client.crm.contacts.batch_api.read` returning email for an id.

def test_fetch_tickets_maps_to_rawticket(...):
    out = list(connector.fetch("CLT-003", customer_ids=["CUST-002"]))
    tickets = [r for r in out if isinstance(r, RawTicket)]
    assert tickets[0].customer_id == "CUST-002"
    assert tickets[0].source == "hubspot"
    assert tickets[0].subject and tickets[0].text

def test_fetch_feedback_maps_to_rawreview(...):
    reviews = [r for r in out if isinstance(r, RawReview)]
    assert 0 <= reviews[0].rating <= 5            # NPS/CSAT normalized

def test_unlinkable_record_skipped(...):
    # contact email not in known set -> not yielded
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement `fetch`**: page tickets (`crm.tickets.basic_api.get_page(limit=100, after=…, properties=[...], associations=["contacts"])`, loop on `paging.next.after`); collect associated contact ids; `crm.contacts.batch_api.read(properties=["email"])` → `{id: email}`; map → `RawTicket`. Page `crm.objects.basic_api.get_page(object_type="feedback_submissions", …)` → `RawReview` with `_normalize_rating(hs_survey_type, hs_value)` (NPS 0–10 → 1–5; CSAT passthrough/clamp). Skip unresolved.
- [ ] **Step 4 — run** → green. **Commit** `feat(hubspot): fetch tickets + feedback → RawTicket/RawReview`.

- [ ] **Step 5 — `from_client` + `verify`** (TDD): `from_client(engine_or_conn, client_id)` reads the `provider='hubspot'` row, decrypts `api_token_enc`, returns `None` if missing/disabled; `verify()` does a cheap `crm.owners` call → `{ "account": ... }`. Tests mirror Jira's `from_client` tests. **Commit** `feat(hubspot): from_client + verify`.

---

### Task 5: Generic integrations router

**Files:** Modify `app/integrations_router.py`. Test: `tests_temporal/test_integrations_router_generic.py`.

**Per-provider config columns** come from `PROVIDER_META[provider]["fields"]` mapped to table columns (`api_token`→`api_token_enc`); HubSpot writes only `api_token`, `customer_strategy`, `customer_field_name`, `enabled`.

- [ ] **Step 1 — failing tests** (TestClient, dependency-override `get_current_user`, monkeypatch `_find_user_by_token`):
```python
def test_list_integrations_returns_all_providers(client, cid):
    body = client.get(f"/api/v1/integrations?clientId={cid}").json()
    assert {"jira", "hubspot"} <= set(body)           # dict keyed by provider

def test_unknown_provider_rejected(client, cid):
    assert client.get(f"/api/v1/integrations/bogus?clientId={cid}").status_code in (400, 404)

def test_foreign_tenant_forbidden(...):
    assert resp.status_code == 403

def test_put_hubspot_encrypts_token_and_get_hides_it(client, cid):
    client.put(f"/api/v1/integrations/hubspot?clientId={cid}", json={"api_token":"sekret","enabled":True})
    got = client.get(f"/api/v1/integrations/hubspot?clientId={cid}").json()
    assert got["token_set"] is True and "api_token" not in got and "sekret" not in str(got)
```
- [ ] **Step 2 — run, expect fail.**
- [ ] **Step 3 — implement** `GET /integrations` (loop `PROVIDER_META`, return `{provider: _status_row(conn, cid, provider)}`); generalize `_status_row(conn, cid, provider)` to take a provider; convert `/jira` GET/PUT/test/sync to `/{provider}` with `provider` validated against `CONNECTOR_REGISTRY`; `_build_connector(provider, client_id)` uses `CONNECTOR_REGISTRY[provider].from_client`. Keep SSRF guard ONLY when `provider=="jira"` and `base_url` supplied. Keep thin `/jira/*` route aliases delegating to the generic handlers (back-compat until UI migrates).
- [ ] **Step 4 — run** generic router tests + the Jira router behavior (via aliases) → green.
- [ ] **Step 5 — commit** `feat(integrations): generic /integrations/{provider} router via registry`.

---

### Task 6: Frontend — generic integration cards

**Files:** Create `UI/UI/src/app/pages/integration-card.ts` + `.html`; modify `settings.ts`, `settings.html`. (`jira-integration.*` becomes the generic card or is replaced.)

- [ ] **Step 1 — pure-function vitest** for `providerFields(meta)` / label mapping if any helper is extracted (e.g. which inputs to show). Run `npm run test -- integration-card` (or the repo's vitest invocation).
- [ ] **Step 2 — implement `IntegrationCardComponent`** with an `@Input() provider` + `@Input() meta`, rendering inputs from `meta.fields` and strategy `<option>`s from `meta.strategies`, calling `/integrations/{provider}` `{get,put,test,sync}`. Reuse the existing jira card markup/logic, parameterized. Plain-English copy per the earlier Jira-card fix (no "custom field"/"CUST-*" jargon for the HubSpot card).
- [ ] **Step 3 — settings**: fetch `GET /integrations`, `@for` a `<app-integration-card>` per provider.
- [ ] **Step 4 — `ng build --configuration production`** → clean.
- [ ] **Step 5 — commit** `feat(ui/integrations): provider-driven integration cards (Jira + HubSpot)`.

---

### Task 7: End-to-end + regression

- [ ] **Step 1 — full backend suite**: `./venv/bin/python -m pytest tests_temporal/ -q` → only the known pre-existing `validate_password` failure; everything else green (esp. all 24 Jira tests + new HubSpot/registry/router tests).
- [ ] **Step 2 — E2E sync** (mocked client, or a HubSpot sandbox token in a test tenant): call `run_ingest(engine, CLT-XXX, connectors=[HubSpotConnector...])` → assert rows appear in `support_tickets` (source='hubspot') and `customer_reviews`; re-run → idempotent (counts stable).
- [ ] **Step 3 — Playwright UI**: log into an idle test tenant, open Settings → confirm a HubSpot card renders with plain-English copy and Save/Test/Sync; confirm Jira card still works.
- [ ] **Step 4 — finish branch** via superpowers:finishing-a-development-branch (tests pass → present merge/PR options).

---

## Self-Review
- **Spec coverage:** registry+meta (T2), multi-provider default_connectors (T2), HubSpot connector tickets+feedback+resolution+from_client/verify (T3,T4), generic router (T5), generic UI (T6), email-first case-insensitive mapping (T3), zero DDL (reuse columns — no migration task), token encrypted/never-returned (T5), Jira-tests-green regression (T1,T5,T7), E2E (T7). All spec sections mapped.
- **Placeholders:** none — test code shown per task; fallback code shown verbatim for default_connectors.
- **Type consistency:** `fetch(client_id, since, customer_ids)`, `from_client(engine_or_conn, client_id)`, `verify()`, `_resolve_customer(...)`, `_status_row(conn, cid, provider)`, `_build_connector(provider, client_id)` used consistently across tasks.
