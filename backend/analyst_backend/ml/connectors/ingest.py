from __future__ import annotations
import argparse
import os
from typing import Iterable, List, Optional

from sqlalchemy import create_engine, text

from ml.connectors.base import RawTicket, RawReview

_TICKET_UPSERT = text("""
    INSERT INTO support_tickets
      (client_id, ticket_id, customer_id, source, subject, ticket_text,
       ticket_type, priority, status, opened_date, resolved_date)
    VALUES
      (:client_id, :ticket_id, :customer_id, :source, :subject, :text,
       :ticket_type, :priority, :status, :opened_date, :resolved_date)
    ON CONFLICT (client_id, ticket_id) DO UPDATE SET
       ticket_text = EXCLUDED.ticket_text,
       source      = EXCLUDED.source,
       subject     = EXCLUDED.subject,
       priority    = EXCLUDED.priority,
       status      = EXCLUDED.status,
       resolved_date = EXCLUDED.resolved_date
""")

_REVIEW_UPSERT = text("""
    INSERT INTO customer_reviews
      (client_id, review_id, customer_id, source, rating, review_text, review_date)
    VALUES
      (:client_id, :review_id, :customer_id, :source, :rating, :text, :review_date)
    ON CONFLICT (client_id, review_id) DO UPDATE SET
       review_text = EXCLUDED.review_text,
       source      = EXCLUDED.source,
       rating      = EXCLUDED.rating
""")


def ingest_records(conn, records: Iterable) -> dict:
    """Upsert a batch of RawTicket/RawReview onto an open connection.
    Caller owns the transaction (so tests can roll back)."""
    counts = {"tickets": 0, "reviews": 0}
    for r in records:
        if isinstance(r, RawTicket):
            conn.execute(_TICKET_UPSERT, r.__dict__)
            counts["tickets"] += 1
        elif isinstance(r, RawReview):
            conn.execute(_REVIEW_UPSERT, r.__dict__)
            counts["reviews"] += 1
    return counts


def _customer_ids(conn, client_id: str, limit: Optional[int]) -> List[str]:
    sql = "SELECT customer_id FROM customers WHERE client_id=:c ORDER BY customer_id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [row[0] for row in conn.execute(text(sql), {"c": client_id}).fetchall()]


def run_ingest(engine, client_id: str, connectors=None,
               limit_customers: Optional[int] = None) -> dict:
    from ml.connectors import CONNECTORS
    connectors = connectors if connectors is not None else CONNECTORS
    totals = {"tickets": 0, "reviews": 0}
    with engine.begin() as conn:
        custs = _customer_ids(conn, client_id, limit_customers)
        for conn_impl in connectors:
            try:
                recs = list(conn_impl.fetch(client_id, customer_ids=custs))
            except Exception as exc:  # one bad connector must not kill ingest
                print(f"[ingest] connector {conn_impl.source} failed: {exc}")
                continue
            c = ingest_records(conn, recs)
            totals["tickets"] += c["tickets"]
            totals["reviews"] += c["reviews"]
    return totals


def main():
    ap = argparse.ArgumentParser(description="Ingest external signals")
    ap.add_argument("--db-url", default=os.getenv("DB_URL"))
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--limit-customers", type=int, default=None)
    a = ap.parse_args()
    eng = create_engine(a.db_url, future=True)
    print(run_ingest(eng, a.client_id, limit_customers=a.limit_customers))


if __name__ == "__main__":
    main()
