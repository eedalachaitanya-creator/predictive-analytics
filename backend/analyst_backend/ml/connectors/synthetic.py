from __future__ import annotations
import hashlib
import random
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional

from ml.connectors.base import RawTicket, RawReview, ExternalSignalConnector

# Text templates keyed by emotion bucket. The latent picks the bucket.
_NEG = [
    "This is the third time my order is wrong. Absolutely disappointed.",
    "Terrible experience, the product broke in a week. Useless support.",
    "I am frustrated — nobody answers my emails. Worst service ever.",
]
_NEU = [
    "Order arrived. It is fine, nothing special.",
    "Had a question about delivery timing, got an answer eventually.",
    "Average experience overall, no major issues.",
]
_POS = [
    "Fantastic service, resolved my issue in minutes. Delighted!",
    "Love the product, will definitely buy again. Very satisfied.",
    "Quick and helpful support, thank you so much.",
]


def _latent(customer_id: str, seed: int) -> float:
    """Deterministic per-customer dissatisfaction in [0,1]. Pure function of
    (customer_id, seed) — NEVER of churn outcome, so features stay honest."""
    h = hashlib.sha256(f"{customer_id}:{seed}".encode()).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


class _Base(ExternalSignalConnector):
    def __init__(self, seed: int = 42):
        self.seed = seed

    def _dissatisfaction(self, customer_id: str) -> float:
        return _latent(customer_id, self.seed)

    def _bucket_text(self, rng: random.Random, dissat: float) -> str:
        # higher dissatisfaction → more weight on negative templates
        roll = rng.random()
        if roll < dissat * 0.8:
            return rng.choice(_NEG)
        if roll < dissat * 0.8 + 0.3:
            return rng.choice(_NEU)
        return rng.choice(_POS)


class SyntheticJiraConnector(_Base):
    source = "jira_synthetic"
    signal_kind = "ticket"

    def fetch(self, client_id: str, since: Optional[date] = None,
              customer_ids: Optional[List[str]] = None,
              n_max: int = 4) -> Iterable[RawTicket]:
        custs = customer_ids or []
        for cust in custs:
            rng = random.Random(f"{self.seed}:{client_id}:{cust}:jira")
            dissat = self._dissatisfaction(cust)
            n = rng.randint(0, n_max)
            for i in range(n):
                age = rng.randint(5, 360)
                opened = datetime.combine(date.today() - timedelta(days=age),
                                          datetime.min.time())
                resolved = (opened + timedelta(hours=rng.randint(2, 240))
                            if rng.random() > 0.3 else None)
                pr = "critical" if dissat > 0.8 and rng.random() > 0.5 else \
                     rng.choice(["low", "medium", "high"])
                yield RawTicket(
                    client_id=client_id,
                    ticket_id=f"SYN-{cust}-{i}",
                    customer_id=cust,
                    source=self.source,
                    subject=f"Issue #{i}",
                    text=self._bucket_text(rng, dissat),
                    ticket_type=rng.choice(["bug", "billing", "delivery", "account"]),
                    priority=pr,
                    status="resolved" if resolved else "open",
                    opened_date=opened,
                    resolved_date=resolved,
                )


class SyntheticGoogleReviewsConnector(_Base):
    source = "google_synthetic"
    signal_kind = "review"

    def fetch(self, client_id: str, since: Optional[date] = None,
              customer_ids: Optional[List[str]] = None,
              n_max: int = 3) -> Iterable[RawReview]:
        custs = customer_ids or []
        for cust in custs:
            rng = random.Random(f"{self.seed}:{client_id}:{cust}:google")
            dissat = self._dissatisfaction(cust)
            n = rng.randint(0, n_max)
            for i in range(n):
                age = rng.randint(5, 360)
                txt = self._bucket_text(rng, dissat)
                rating = 1 + int(round((1.0 - dissat) * 4))  # high dissat → low stars
                yield RawReview(
                    client_id=client_id,
                    review_id=f"SYN-{cust}-{i}",
                    customer_id=cust,
                    source=self.source,
                    rating=max(1, min(5, rating)),
                    text=txt,
                    review_date=date.today() - timedelta(days=age),
                )
