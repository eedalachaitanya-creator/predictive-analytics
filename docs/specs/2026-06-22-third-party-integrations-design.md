# Third-Party Integration Framework + HubSpot Connector — Design

**Date:** 2026-06-22
**Status:** Approved (brainstorm) → ready for implementation plan
**Branch:** `feat/third-party-integrations`

**Goal:** Generalize the Jira-specific external-signal plumbing into a provider-agnostic
**integration framework**, and ship **HubSpot** as the first connector end-to-end —
pulling support tickets and customer feedback into the existing churn pipeline.

**Architecture (one line):** A provider registry + generic `/integrations/{provider}`
API + a generic Settings UI sit in front of the existing port/adapter connector layer;
each new app is one adapter class + a registry entry + a metadata blob. Downstream
(ingest → emotion/sentiment → point-in-time features → churn model) is unchanged.

---

## Decisions (from brainstorm)

| Topic | Decision |
|---|---|
| **Signal type** | Cases/tickets → `support_tickets` (emotion); feedback/NPS/surveys → `customer_reviews` (sentiment). Reuse existing tables — no model/schema change. |
| **Customer mapping** | Email-first: CRM contact email → `customers.customer_email` (case-insensitive). Fallback: a configured CRM field holding our `customer_id`. No match → skip + log. |
| **Scope** | Generalize the framework + ship HubSpot E2E. SugarCRM / Google Reviews / etc. are follow-on specs (one adapter each). |
| **Auth (HubSpot)** | Private App access token, stored encrypted, sent as Bearer. (OAuth deferred — only needed for a public Marketplace app.) |
| **Scaling/containerization** | Out of scope. Ingest stays on the existing RQ worker (already scales horizontally). Per-connector services + compose/k8s are a separate future spec. |

---

## Architecture & Components

### 1. Connector port (explicit contract)
`ml/connectors/base.py` — `ExternalSignalConnector` standardized so every connector provides:
- `source: str`, `signal_kind` (informational; a connector MAY yield both `RawTicket` and `RawReview` — ingest dispatches by `isinstance`).
- `fetch(client_id, since=None, customer_ids=None) -> Iterable[RawTicket | RawReview]` (align the ABC with what Jira already does — add `customer_ids`).
- classmethod `from_client(engine_or_conn, client_id, *, session=None, require_enabled=True) -> Optional[connector]`.
- `verify() -> dict` (cheap auth/connectivity check for the Test button).

### 2. Provider registry — new `ml/connectors/registry.py`
```python
CONNECTOR_REGISTRY = {"jira": JiraConnector, "hubspot": HubSpotConnector}

PROVIDER_META = {   # drives the generic UI + which fields each provider needs
  "jira":    {"label": "Jira",    "fields": ["base_url","email","api_token","project_key"],
              "strategies": ["auto","field","label"]},
  "hubspot": {"label": "HubSpot", "fields": ["api_token"],
              "strategies": ["email","field"]},
}
```

### 3. `default_connectors()` generalized — `ml/connectors/__init__.py`
Today it builds **only** Jira and returns a single connector. Generalize to query
`tenant_integrations` for **all enabled providers** for the tenant and build each via its
registry class's `from_client`, returning the list. Env/synthetic fallback unchanged.
(This single-connector return is the reason multi-provider is impossible today.)

### 4. Generic router — `app/integrations_router.py`
Replace the five `/jira/*` routes with provider-parameterized routes that validate
`provider` against the registry and dispatch through it:
- `GET  /integrations` — every provider + this tenant's config status (renders the UI).
- `GET  /integrations/{provider}` — one provider's saved config (non-secret fields only).
- `PUT  /integrations/{provider}` — upsert config; token encrypted via existing crypto.
- `POST /integrations/{provider}/test` — `connector.verify()`.
- `POST /integrations/{provider}/sync` — `run_ingest` with that provider's connector.

Keep thin `/jira/*` aliases temporarily so nothing breaks during migration; remove once the UI is migrated.

### 5. Generic Settings UI
The current Jira card becomes a reusable `IntegrationCardComponent`, rendered once per
provider from `GET /integrations`, with fields/strategies driven by `PROVIDER_META`.

---

## Data flow (downstream unchanged — the point of the design)

```
HubSpotConnector.fetch()
  ├─ tickets   → RawTicket  → support_tickets  → emotion   ┐
  └─ feedback  → RawReview  → customer_reviews → sentiment  ├→ PIT features → churn model
JiraConnector.fetch() → RawTicket → support_tickets ────────┘
```

### Customer resolution (email-first, case-insensitive)
Reuse the `{lower(id): canonical_id}` map technique from the Jira fix:
1. **Email match** (default): record's associated contact email → `customers.customer_email` → `customer_id`.
2. **Field override** (`customer_strategy='field'`): a named CRM property holding our `customer_id`.
3. **No match → skip the record** (logged), same safe behavior as Jira.

---

## HubSpot connector — `ml/connectors/hubspot.py`

- **Official SDK** `hubspot-api-client` (add to `requirements.txt`). HubSpot's base URL is
  fixed (`api.hubapi.com`) → **no SSRF guard** needed (Jira needed one only because its site
  URL is tenant-supplied). Built-in rate-limit retry:
  `HubSpot(access_token=token, retry=Retry(total=5, status_forcelist=(429,)))`.
- **`from_client`** / **`verify()`** — same contract as Jira (decrypt token; `verify()` = a cheap call).
- **`fetch(client_id, since=None, customer_ids=None)`** yields BOTH record types:
  - **Tickets** → `crm.tickets` paged (cursor `paging.next.after`; `properties=[subject, content,
    hs_pipeline_stage, hs_ticket_priority, createdate, closed_date]`, `associations=['contacts']`)
    → `RawTicket` (text = `content` or `subject`; type/priority/status from properties; dates parsed).
  - **Feedback/NPS** → generic `crm.objects` `object_type="feedback_submissions"`
    (`hs_survey_type, hs_value, hs_content, hs_submission_timestamp`) → `RawReview`
    (text = `hs_content`; **rating normalized** — NPS 0–10 / CSAT → our rating scale).
  - **Contact email** resolved via batch-read of associated contact ids (`properties=['email']`).
  - **IDs:** HubSpot native object id as `ticket_id`/`review_id` (numeric — no collision with Jira
    keys; `source='hubspot'` disambiguates). Re-sync **upserts** (`ON CONFLICT (client_id, ticket_id)`),
    so syncs are idempotent.
- **Incrementality:** v1 = full pull + upsert. `since`-based incremental (search filter on
  `hs_lastmodifieddate`) is a noted enhancement, not v1.

---

## Error handling & resilience
- `run_ingest` already wraps each connector in try/except → one provider's failure never kills
  others' ingest or the pipeline.
- 429s handled by the SDK retry. Records with no resolvable customer → skipped + logged.
- `test`/`sync` endpoints return friendly messages, never raw API bodies (mirrors Jira).

## Security
- Token **encrypted at rest** (`api_token_enc`, existing crypto) and **never returned** to the UI
  (PUT accepts it; GET omits it) — same as Jira.
- Endpoints enforce `_require_client_access`; connector always built from the tenant's own row;
  `provider` validated against the registry (no arbitrary dispatch).
- Pulled text is **data** for the local emotion/sentiment models (not an LLM prompt) → injection
  surface unchanged from Jira.

## Schema impact — zero DDL
HubSpot reuses existing `tenant_integrations` columns (encrypted token + `customer_strategy='email'`
+ optional `customer_field_name`); `base_url`/`email`/`project_key` stay null. A future provider with
richer config (e.g. SugarCRM OAuth2 username/password) adds a JSONB `config` column **then**.

---

## Testing plan (TDD — hermetic, fake HubSpot client like Jira's `_FakeSession`)
**Connector units:** email-match (case-insensitive → canonical), field-override, no-match→skip,
ticket mapping, feedback mapping + rating normalization, cursor pagination, `from_client`
(decrypted token; `None` when disabled/missing).
**Framework units:** `default_connectors()` returns **all** enabled providers (the multi-provider
fix); `GET /integrations` lists providers; `{config,test,sync}` dispatch by provider; unknown
provider→400/404; foreign tenant→403.
**Regression safety net:** the existing **24 Jira tests must stay green** after Jira is refactored
into the registry (proves the generalization is behavior-preserving).
**E2E:** sync against a HubSpot sandbox (or mocked) → rows land in `support_tickets` +
`customer_reviews` for a test tenant; re-sync is idempotent.

---

## Out of scope (explicit)
- SugarCRM, Google Reviews, and other connectors (each a follow-on spec — one adapter + registry entry + metadata).
- Independent per-connector scaling, dedicated ingest worker, container topology (compose/k8s), frontend image.
- Richer CRM signals (engagement/deals/lifecycle) requiring new feature columns + model retraining.
- OAuth 2.0 / HubSpot Marketplace distribution.
