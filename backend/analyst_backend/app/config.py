"""
config.py — Application settings loaded from environment / .env file.
"""
import re
from urllib.parse import unquote_plus, quote_plus
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://dev-users:yT9Lp5tCtfSc/8kTiDoZxg==@10.0.0.15:5432/predictive_analytics"

    @field_validator("database_url", mode="after")
    @classmethod
    def sanitize_database_url(cls, v: str) -> str:
        # urlparse breaks when the password contains a literal '/' — it
        # misidentifies the slash as a path separator and returns username=None.
        # We use regex instead to extract user, raw_pass, host, db reliably.
        #
        # The decode→re-encode round-trip is idempotent:
        #   already encoded:  %2F → / → %2F  (no change)
        #   raw special char: /   → / → %2F  (fixed)
        m = re.match(
            r'^(postgresql(?:\+asyncpg)?://)([^:]+):(.+)@([^/]+)(/.*)$', v
        )
        if not m:
            return v
        scheme, user, raw_pass, host, db = m.groups()
        safe_pass = quote_plus(unquote_plus(raw_pass))
        return f"{scheme}{user}:{safe_pass}@{host}{db}"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Auth
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    # App
    environment: str = "development"
    log_level: str = "info"
    app_name: str = "CRP Analyst Agent"
    app_version: str = "0.1.0"


settings = Settings()