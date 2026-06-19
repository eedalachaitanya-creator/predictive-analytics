"""
database.py — SQLAlchemy engine + session factory.
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,       # reconnect on stale connections
    pool_size=3,              # reduced from 10 — shared QA server has limited slots
    max_overflow=2,           # reduced from 20 — max 5 total connections per process
    pool_timeout=10,          # fail fast instead of queuing — avoids connection pile-up
    pool_recycle=300,         # recycle connections every 5 min to release idle slots
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI dependency — yields a DB session, closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    """Returns True if the database is reachable."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False