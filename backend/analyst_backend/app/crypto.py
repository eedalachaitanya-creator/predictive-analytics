"""Symmetric encryption for integration secrets (Jira API tokens, etc.).

Tokens are stored as Fernet ciphertext in ``tenant_integrations.api_token_enc`` and
decrypted only in-process when building a connector. The key comes from env
``INTEGRATION_ENC_KEY`` (a urlsafe-base64 32-byte Fernet key). Generate one with::

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

NEVER commit the key — put it in the server's env / .env (git-ignored). We **fail
closed**: if the key is unset, ``encrypt_secret`` raises rather than silently
storing plaintext, so a misconfiguration can never persist a token in the clear.
"""
from __future__ import annotations

import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

_ENV_KEY = "INTEGRATION_ENC_KEY"


class EncryptionUnavailable(RuntimeError):
    """Raised when INTEGRATION_ENC_KEY is missing — we refuse to touch secrets."""


def _get_key() -> Optional[str]:
    """The Fernet key. The process env wins (test overrides / explicit export),
    else the ``.env``-backed Settings — so the key lives in ONE place that both
    the API process and the pipeline subprocess read."""
    key = os.getenv(_ENV_KEY)
    if key:
        return key
    try:
        from app.config import settings
        return settings.integration_enc_key or None
    except Exception:  # pragma: no cover — app.config unavailable (standalone use)
        return None


def encryption_available() -> bool:
    """True if a key is configured (lets callers surface a clear setup error)."""
    return bool(_get_key())


def _fernet() -> Fernet:
    key = _get_key()
    if not key:
        raise EncryptionUnavailable(
            f"{_ENV_KEY} not set — cannot encrypt/decrypt integration secrets. "
            "Generate one: python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\" and put it in the server env."
        )
    # Read per-call (not cached) so tests/rotation see a changed key immediately.
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt_secret(plaintext: str) -> str:
    """Encrypt a secret → urlsafe ciphertext string. Fail closed if no key."""
    if plaintext is None:
        raise ValueError("encrypt_secret: plaintext is required")
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: Optional[str]) -> Optional[str]:
    """Decrypt ciphertext → plaintext. Returns None for empty input. Raises
    ``InvalidToken`` if the ciphertext doesn't match the current key."""
    if not ciphertext:
        return None
    return _fernet().decrypt(ciphertext.encode()).decode()
