"""Local SQLite account repository."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from app.control.account.backends.base import AccountRepository
from app.platform.paths import data_path


class LocalAccountRepository(AccountRepository):
    """SQLite-based account storage (default backend)."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or str(data_path("accounts.db"))
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                op TEXT NOT NULL,
                data TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
        """)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def list_all(self) -> list[dict[str, Any]]:
        assert self._conn
        async with self._conn.execute("SELECT data FROM accounts") as cursor:
            rows = await cursor.fetchall()
            return [json.loads(row["data"]) for row in rows]

    async def get(self, account_id: str) -> dict[str, Any] | None:
        assert self._conn
        async with self._conn.execute(
            "SELECT data FROM accounts WHERE id = ?", (account_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return json.loads(row["data"]) if row else None

    async def upsert(self, account: dict[str, Any]) -> None:
        assert self._conn
        now = datetime.now(UTC).isoformat()
        data = json.dumps(account, ensure_ascii=False)
        await self._conn.execute(
            """INSERT INTO accounts (id, data, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   data = excluded.data,
                   updated_at = excluded.updated_at""",
            (account["id"], data, now),
        )
        await self._conn.execute(
            "INSERT INTO change_log (account_id, op, data, created_at) "
            "VALUES (?, 'upsert', ?, ?)",
            (account["id"], data, now),
        )
        await self._conn.commit()

    async def delete(self, account_id: str) -> None:
        assert self._conn
        now = datetime.now(UTC).isoformat()
        await self._conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        await self._conn.execute(
            "INSERT INTO change_log (account_id, op, data, created_at) "
            "VALUES (?, 'delete', NULL, ?)",
            (account_id, now),
        )
        await self._conn.commit()

    async def scan_changes(self, since: datetime | None) -> list[dict[str, Any]]:
        """Get changes since the given timestamp."""
        assert self._conn
        if since is None:
            # Return all accounts as upserts
            accounts = await self.list_all()
            return [{"op": "upsert", "id": a["id"], "data": a} for a in accounts]

        if since.tzinfo is None:
            since = since.replace(tzinfo=UTC)
        since_str = since.astimezone(UTC).isoformat()
        async with self._conn.execute(
            """SELECT account_id, op, data FROM change_log
               WHERE created_at > ? ORDER BY id ASC""",
            (since_str,),
        ) as cursor:
            rows = await cursor.fetchall()

        changes = []
        for row in rows:
            data = json.loads(row["data"]) if row["data"] else None
            changes.append(
                {
                    "op": row["op"],
                    "id": row["account_id"],
                    "data": data,
                }
            )
        return changes
