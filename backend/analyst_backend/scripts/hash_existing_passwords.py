"""One-time migration: bcrypt-hash any PLAINTEXT passwords in users.password_hash.

The application is backward-compatible (login verifies a bcrypt hash OR legacy
plaintext, and lazily re-hashes on next login), so running this is OPTIONAL — but
recommended, to remove cleartext passwords at rest immediately rather than waiting
for every user to log in.

Idempotent: rows already bcrypt-hashed are skipped. Safe to run repeatedly and
against any DB (local or the shared remote).

Run from backend/analyst_backend/:
    DATABASE_URL=postgresql://... ./venv/bin/python -m scripts.hash_existing_passwords
    # or:
    ./venv/bin/python -m scripts.hash_existing_passwords --db-url postgresql://...
"""
import argparse
import os
import sys

from sqlalchemy import create_engine, text

# Make `app` importable when run as `python -m scripts.hash_existing_passwords`
# from the backend root (the package's parent is already on sys.path), but also
# when run as a plain file.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.security import hash_password, is_hashed  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-url", default=None,
                        help="Postgres URL (falls back to DATABASE_URL / DB_URL env).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would change without writing.")
    args = parser.parse_args(argv)

    db_url = args.db_url or os.environ.get("DATABASE_URL") or os.environ.get("DB_URL")
    if not db_url:
        raise SystemExit("No DB URL: pass --db-url or set DATABASE_URL/DB_URL")

    engine = create_engine(db_url)
    migrated, skipped = 0, 0
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT user_id, email, password_hash FROM users")
        ).fetchall()
        for user_id, email, stored in rows:
            if is_hashed(stored):
                skipped += 1
                continue
            if args.dry_run:
                print(f"  WOULD hash: {email}")
            else:
                conn.execute(
                    text("UPDATE users SET password_hash = :h WHERE user_id = :u"),
                    {"h": hash_password(stored), "u": user_id},
                )
            migrated += 1
        if args.dry_run:
            conn.rollback()

    verb = "would migrate" if args.dry_run else "migrated"
    print(f"{verb}={migrated}  already-hashed(skipped)={skipped}  total={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
