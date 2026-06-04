"""Password hashing + verification.

Passwords were historically stored as PLAINTEXT in ``users.password_hash``.
This module introduces bcrypt hashing while staying backward-compatible:
``verify_password`` accepts both a bcrypt hash AND a legacy plaintext value, so
existing accounts keep working and can be migrated lazily (on next login) or via
a one-time migration — with zero lockout risk.

Implementation note: we use the ``bcrypt`` library DIRECTLY rather than passlib's
CryptContext, because passlib 1.7.4's bcrypt backend is broken against bcrypt
>= 4.1/5.x (it raises "password cannot be longer than 72 bytes" on hash()).
bcrypt's API is small and stable, so a thin wrapper is simpler and robust.

Usage:
    from app.security import hash_password, verify_password, is_hashed
    stored = hash_password(plain)            # on create / change / reset
    ok     = verify_password(plain, stored)  # on login  (handles hash OR plaintext)
"""
from __future__ import annotations

import hmac

import bcrypt

# Identifiers bcrypt hashes start with. A stored value NOT starting with one of
# these is treated as legacy plaintext.
_BCRYPT_PREFIXES = ("$2a$", "$2b$", "$2y$")


def _to_72_bytes(plain: str) -> bytes:
    """Encode + cap at bcrypt's 72-byte limit (bcrypt 5.x raises past it)."""
    return str(plain).encode("utf-8")[:72]


def is_hashed(stored: str | None) -> bool:
    """True if ``stored`` looks like a bcrypt hash (vs. legacy plaintext)."""
    return bool(stored) and str(stored).startswith(_BCRYPT_PREFIXES)


def hash_password(plain: str) -> str:
    """Return a salted bcrypt hash of ``plain`` (unique per call)."""
    return bcrypt.hashpw(_to_72_bytes(plain), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, stored: str | None) -> bool:
    """Verify ``plain`` against ``stored``.

    Backward-compatible:
      * if ``stored`` is a bcrypt hash → verify cryptographically;
      * if ``stored`` is legacy plaintext → constant-time string compare
        (so accounts authenticate until migrated).
    Returns False for an empty/None stored value.
    """
    if not stored:
        return False
    if is_hashed(stored):
        try:
            return bcrypt.checkpw(_to_72_bytes(plain), str(stored).encode("utf-8"))
        except Exception:
            return False
    # Legacy plaintext path — constant-time compare to avoid timing leaks.
    return hmac.compare_digest(str(plain), str(stored))
