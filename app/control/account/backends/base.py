"""Base account repository interface."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import Any


class AccountRepository(abc.ABC):
    """Abstract account storage backend."""

    @abc.abstractmethod
    async def initialize(self) -> None:
        """Create tables if needed."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close connections."""
        ...

    @abc.abstractmethod
    async def list_all(self) -> list[dict[str, Any]]:
        """List all accounts."""
        ...

    @abc.abstractmethod
    async def get(self, account_id: str) -> dict[str, Any] | None:
        """Get a single account by ID."""
        ...

    @abc.abstractmethod
    async def upsert(self, account: dict[str, Any]) -> None:
        """Insert or update an account."""
        ...

    @abc.abstractmethod
    async def delete(self, account_id: str) -> None:
        """Delete an account."""
        ...

    @abc.abstractmethod
    async def scan_changes(self, since: datetime | None) -> list[dict[str, Any]]:
        """Return changes since the given timestamp."""
        ...
