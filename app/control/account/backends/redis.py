"""Redis account repository."""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from app.control.account.backends.base import AccountRepository


class RedisAccountRepository(AccountRepository):
    """Redis-based account storage."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        import redis.asyncio as aioredis

        self._client = aioredis.from_url(redis_url, decode_responses=True)
        self._prefix = "liangzidp:account:"
        self._changelog_key = "liangzidp:changelog:v2"

    async def initialize(self) -> None:
        # Redis doesn't need table creation
        pass

    async def close(self) -> None:
        await self._client.aclose()

    async def list_all(self) -> list[dict[str, Any]]:
        accounts = []
        async for key in self._client.scan_iter(match=f"{self._prefix}*", count=200):
            data = await self._client.get(key)
            if data:
                accounts.append(json.loads(data))
        return accounts

    async def get(self, account_id: str) -> dict[str, Any] | None:
        data = await self._client.get(f"{self._prefix}{account_id}")
        return json.loads(data) if data else None

    async def upsert(self, account: dict[str, Any]) -> None:
        data = json.dumps(account, ensure_ascii=False)
        now = time.time()
        change = json.dumps(
            {
                "op": "upsert",
                "id": account["id"],
                "data": account,
                "created_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "event_id": uuid.uuid4().hex,
            },
            ensure_ascii=False,
        )
        async with self._client.pipeline(transaction=True) as pipeline:
            pipeline.set(f"{self._prefix}{account['id']}", data)
            pipeline.zadd(self._changelog_key, {change: now})
            await pipeline.execute()

    async def delete(self, account_id: str) -> None:
        now = time.time()
        change = json.dumps(
            {
                "op": "delete",
                "id": account_id,
                "created_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "event_id": uuid.uuid4().hex,
            }
        )
        async with self._client.pipeline(transaction=True) as pipeline:
            pipeline.delete(f"{self._prefix}{account_id}")
            pipeline.zadd(self._changelog_key, {change: now})
            await pipeline.execute()

    async def scan_changes(self, since: datetime | None) -> list[dict[str, Any]]:
        if since is None:
            return [
                {"op": "upsert", "id": a["id"], "data": a}
                for a in await self.list_all()
            ]
        if since.tzinfo is None:
            since = since.replace(tzinfo=UTC)
        rows = await self._client.zrangebyscore(
            self._changelog_key,
            f"({since.astimezone(UTC).timestamp()}",
            "+inf",
        )
        changes = []
        for row in rows:
            change = json.loads(row)
            change.pop("created_at", None)
            change.pop("event_id", None)
            changes.append(change)
        return changes
