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
