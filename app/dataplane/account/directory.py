"""In-memory account directory — synced from repository."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.dataplane.account.model import LiangzidpAccount

if TYPE_CHECKING:
    from app.control.account.backends.base import AccountRepository


class AccountDirectory:
    """Thread-safe in-memory account table.

    All workers maintain a local copy. The sync loop keeps it
    eventually consistent with the repository.
    """

    def __init__(self):
        self._accounts: dict[str, LiangzidpAccount] = {}
        self._lock = asyncio.Lock()
        self._last_sync: datetime | None = None
        self._dirty = False

    async def load_from_repo(self, repo: "AccountRepository") -> None:
        """Replace local table with full repo state."""
        sync_started = datetime.now(UTC)
        async with self._lock:
            accounts = await repo.list_all()
            parsed = [
                a if isinstance(a, LiangzidpAccount) else LiangzidpAccount.from_dict(a)
                for a in accounts
            ]
            self._accounts = {a.id: a for a in parsed}
            self._last_sync = sync_started
            self._dirty = False

    async def sync_if_changed(self, repo: "AccountRepository") -> bool:
        """Incremental sync: only reload if repo changed.

        Returns True if changes were detected and applied.
        """
        sync_started = datetime.now(UTC)
        changes = await repo.scan_changes(since=self._last_sync)
        if not changes:
            return False

        async with self._lock:
            for change in changes:
                if change["op"] == "upsert":
                    account = LiangzidpAccount.from_dict(change["data"])
                    self._accounts[account.id] = account
                elif change["op"] == "delete":
                    self._accounts.pop(change["id"], None)
            self._last_sync = sync_started
            self._dirty = True
        return True

    def list_available(self) -> list[LiangzidpAccount]:
        """Get all accounts that can be used for requests."""
        return [a for a in self._accounts.values() if a.is_available()]

    def get(self, account_id: str) -> LiangzidpAccount | None:
        return self._accounts.get(account_id)

    async def update(self, account: LiangzidpAccount) -> None:
        """Update an account in the local table."""
        async with self._lock:
            self._accounts[account.id] = account
            self._dirty = True

    @property
    def count(self) -> int:
        return len(self._accounts)

    @property
    def available_count(self) -> int:
        return len(self.list_available())
