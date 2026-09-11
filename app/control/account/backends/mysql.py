"""MySQL account repository."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlparse

from app.control.account.backends.base import AccountRepository


class MySQLAccountRepository(AccountRepository):
    """MySQL-based account storage."""

    def __init__(self, database_url: str):
        self._pool = None
        self._dict_cursor = None
        self._url = database_url

    async def initialize(self) -> None:
        import aiomysql
        parsed = urlparse(self._url.replace("mysql+aiomysql://", "mysql://", 1))
        if not parsed.hostname or not parsed.path.lstrip("/"):
            raise ValueError("DATABASE_URL must include a MySQL host and database name")
        self._dict_cursor = aiomysql.DictCursor
        self._pool = await aiomysql.create_pool(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            db=unquote(parsed.path.lstrip("/")),
            charset="utf8mb4",
            autocommit=True,
            minsize=1,
            maxsize=10,
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        id VARCHAR(255) PRIMARY KEY,
                        data JSON NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_updated (updated_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS change_log (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        account_id VARCHAR(255) NOT NULL,
                        op VARCHAR(20) NOT NULL,
                        data JSON,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    async def _execute(self, query: str, *args):
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, args)
                return cur

    async def list_all(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor) as cur:
                await cur.execute("SELECT data FROM accounts")
                rows = await cur.fetchall()
                return [json.loads(row["data"]) for row in rows]

    async def get(self, account_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor) as cur:
                await cur.execute("SELECT data FROM accounts WHERE id = %s", (account_id,))
                row = await cur.fetchone()
                return json.loads(row["data"]) if row else None

    async def upsert(self, account: dict[str, Any]) -> None:
        data = json.dumps(account, ensure_ascii=False)
        await self._execute(
            """INSERT INTO accounts (id, data) VALUES (%s, %s)
               ON DUPLICATE KEY UPDATE data = VALUES(data)""",
            account["id"], data,
        )
        await self._execute(
            "INSERT INTO change_log (account_id, op, data) VALUES (%s, 'upsert', %s)",
            account["id"], data,
        )

    async def delete(self, account_id: str) -> None:
        await self._execute("DELETE FROM accounts WHERE id = %s", account_id)
        await self._execute(
            "INSERT INTO change_log (account_id, op, data) VALUES (%s, 'delete', NULL)",
            account_id,
        )

    async def scan_changes(self, since: datetime | None) -> list[dict[str, Any]]:
        if since is None:
            return [{"op": "upsert", "id": a["id"], "data": a}
                    for a in await self.list_all()]
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT account_id, op, data FROM change_log WHERE created_at > %s",
                    (since.isoformat(),),
                )
                rows = await cur.fetchall()
                return [
                    {"op": r[1], "id": r[0], "data": json.loads(r[2]) if r[2] else None}
                    for r in rows
                ]
