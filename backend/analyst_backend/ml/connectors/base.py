from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Literal, Optional, Union


@dataclass(frozen=True)
class RawTicket:
    client_id: str
    ticket_id: str
    customer_id: str
    source: str
    subject: str
    text: str
    ticket_type: Optional[str]
    priority: Optional[str]
    status: Optional[str]
    opened_date: datetime
    resolved_date: Optional[datetime]


@dataclass(frozen=True)
class RawReview:
    client_id: str
    review_id: str
    customer_id: str
    source: str
    rating: int
    text: str
    review_date: date


class ExternalSignalConnector(ABC):
    """One external app as a source of normalized churn signals.
    
    Concrete subclasses must set the required class attributes source and signal_kind.
    """
    source: str
    signal_kind: Literal["ticket", "review"]

    @abstractmethod
    def fetch(self, client_id: str,
              since: Optional[date] = None) -> Iterable[Union[RawTicket, RawReview]]:
        ...
