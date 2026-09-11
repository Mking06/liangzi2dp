"""Account storage backend factory."""

from __future__ import annotations

import os

from app.control.account.backends.base import AccountRepository
from app.control.account.backends.local import LocalAccountRepository
from app.control.account.backends.redis import RedisAccountRepository


def describe_repository_target() -> tuple[str, str]:
    """Return (backend_name, target_description)."""
    backend = os.getenv("ACCOUNT_STORAGE", "local").lower()

    if backend == "redis":
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        return ("redis", url)
    elif backend == "mysql":
        url = os.getenv("DATABASE_URL", "")
        return ("mysql", url or "not configured")
    elif backend == "postgresql":
        url = os.getenv("DATABASE_URL", "")
        return ("postgresql", url or "not configured")
    else:
        return ("local", "accounts.db")


def create_repository() -> AccountRepository:
    """Create the configured account repository."""
    backend = os.getenv("ACCOUNT_STORAGE", "local").lower()

    if backend == "redis":
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        return RedisAccountRepository(url)
    elif backend == "mysql":
        from app.control.account.backends.mysql import MySQLAccountRepository
        url = os.getenv("DATABASE_URL", "")
        return MySQLAccountRepository(url)
    elif backend == "postgresql":
        from app.control.account.backends.postgresql import PostgreSQLAccountRepository
        url = os.getenv("DATABASE_URL", "")
        return PostgreSQLAccountRepository(url)
    else:
        db_path = os.getenv("ACCOUNT_DB_PATH")
        return LocalAccountRepository(db_path)
