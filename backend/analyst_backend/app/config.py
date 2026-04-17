"""
config.py — Application settings loaded from environment / .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql://chaitanya@localhost:5432/walmart_crp"

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
