"""Shared ingest-source helpers — used by manual upload, live connectors, and
(later) the scheduler so 'source' and customer matching mean the same thing
everywhere. Pure functions (no DB) for easy testing."""
from __future__ import annotations
import re
from typing import Optional


def slugify_source(name: str) -> str:
    """Lowercase, non-alnum → underscore, collapse/trim underscores."""
    s = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower())
    return s.strip("_")


def normalize_source(selected_key: Optional[str], custom_name: Optional[str] = None) -> str:
    """Canonical `source` value to stamp on every staged row.

    Known provider key → the key lowercased ('jira', 'hubspot').
    'other' → slugified custom name, or 'other' if none given.
    Missing/blank → 'other'.
    """
    key = (selected_key or "").strip().lower()
    if not key:
        return "other"
    if key == "other":
        slug = slugify_source(custom_name or "")
        return slug or "other"
    return key


def apply_source(existing, fill: str) -> str:
    """Hybrid per-row source value: keep a row's existing source but canonicalize
    it (e.g. 'JIRA' -> 'jira') for uniformity; fall back to ``fill`` (the upload
    dropdown's normalized value) when the row's source is blank/missing or
    slugifies to nothing. Lets a pre-labeled file keep its sources while a file
    without the column is stamped from the dropdown."""
    if existing is not None and str(existing).strip():
        slug = slugify_source(str(existing))
        if slug:
            return slug
    return fill


def resolve_customer_id(row: dict, by_id: set, by_email: dict) -> "str | None":
    """Match an uploaded row to a known customer: customer_id first, then email
    (case-insensitive). Returns the resolved customer_id or None."""
    cid = row.get("customer_id")
    cid = str(cid).strip() if cid is not None else ""
    if cid and cid in by_id:
        return cid
    for k in ("customer_email", "email"):
        v = row.get(k)
        if v is not None and str(v).strip():
            email = str(v).strip().lower()
            if email in by_email:
                return by_email[email]
            break
    return None
