"""PostgreSQL account repository."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.control.account.backends.base import AccountRepository


class PostgreSQLAccountRepository(AccountRepository):
    """PostgreSQL-based account storage."""

    def __init__(self, database_url: str):
        self._pool = None
        self._url = database_url

    async def initialize(self) -> None:
        import asyncpg
        self._pool = await asyncpg.create_pool(self._url, min_size=1, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id VARCHAR(255) PRIMARY KEY,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS change_log (
                    id SERIAL PRIMARY KEY,
                    account_id VARCHAR(255) NOT NULL,
                    op VARCHAR(20) NOT NULL,
                    data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_changelog_created ON change_log(created_at)"
            )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def list_all(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT data FROM accounts")
            return [json.loads(row["data"]) for row in rows]

    async def get(self, account_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data FROM accounts WHERE id = $1", account_id
            )
            return json.loads(row["data"]) if row else None

    async def upsert(self, account: dict[str, Any]) -> None:
        data = json.dumps(account, ensure_ascii=False)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO accounts (id, data) VALUES ($1, $2::jsonb)
                   ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data""",
                account["id"], data,
            )
            await conn.execute(
                "INSERT INTO change_log (account_id, op, data) VALUES ($1, 'upsert', $2::jsonb)",
                account["id"], data,
            )

    async def delete(self, account_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM accounts WHERE id = $1", account_id)
            await conn.execute(
                "INSERT INTO change_log (account_id, op, data) VALUES ($1, 'delete', NULL)",
                account_id,
            )

    async def scan_changes(self, since: datetime | None) -> list[dict[str, Any]]:
        if since is None:
            return [{"op": "upsert", "id": a["id"], "data": a}
                    for a in await self.list_all()]
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT account_id, op, data FROM change_log WHERE created_at > $1",
                since,
            )
            return [
                {"op": r["op"], "id": r["account_id"], "data": json.loads(r["data"]) if r["data"] else None}
                for r in rows
            ]
