"""External-signal connectors (Jira, Google reviews, …)."""
from ml.connectors.synthetic import (
    SyntheticJiraConnector,
    SyntheticGoogleReviewsConnector,
)

# Real OAuth connectors append here later; the rest of the pipeline is unchanged.
CONNECTORS = [SyntheticJiraConnector(), SyntheticGoogleReviewsConnector()]
