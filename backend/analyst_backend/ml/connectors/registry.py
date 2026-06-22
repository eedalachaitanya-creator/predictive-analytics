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
