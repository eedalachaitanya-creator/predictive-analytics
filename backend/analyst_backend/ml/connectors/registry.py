"""Provider registry — the single place that maps a provider key to its connector
class and the UI metadata describing how to configure it.

Adding a new third-party app = one entry here + the connector class + (if its
config needs columns beyond the existing tenant_integrations ones) a migration.
"""
from ml.connectors.jira import JiraConnector
from ml.connectors.hubspot import HubSpotConnector

# provider key -> connector class (must implement from_client / fetch / verify)
CONNECTOR_REGISTRY = {
    "jira": JiraConnector,
    "hubspot": HubSpotConnector,
}

# provider key -> UI metadata: which config fields to show + customer-link options.
PROVIDER_META = {
    "jira": {
        "label": "Jira",
        "fields": ["base_url", "email", "api_token", "project_key"],
        "strategies": ["auto", "field", "label"],
    },
    "hubspot": {
        "label": "HubSpot",
        "fields": ["api_token"],
        "strategies": ["email", "field"],
    },
}

# Source catalog for the upload dropdown — derived from the provider registry so
# adding a provider above automatically adds a source here. "other" (free-text,
# normalized) is always appended last for sources that aren't a known provider.
SOURCES = [
    {
        "key": key,
        "label": PROVIDER_META.get(key, {}).get("label", key.title()),
        # how an uploaded row of this source is matched to a customer; manual
        # uploads use id-then-email regardless, but this documents intent and
        # lets the future scheduler vary it per provider.
        "customer_match": ["id", "email"],
        "custom": False,
    }
    for key in CONNECTOR_REGISTRY
] + [
    {"key": "other", "label": "Other", "customer_match": ["id", "email"], "custom": True},
]
