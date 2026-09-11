from __future__ import annotations

import tomllib
from datetime import UTC, datetime, timedelta

import pytest

from app.control.account.backends.local import LocalAccountRepository
from app.control.account.backends.redis import RedisAccountRepository
from app.control.account.refresh.auth import LiangzidpAuthRefresher
from app.platform.config.snapshot import ConfigSnapshot


class _ProfileResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            "data": {
                "name": "updated-name",
                "is_plus": True,
                "is_pro": True,
                "plan_type": "pro",
            }
        }


class _ProfileSession:
    async def get(self, *_args, **_kwargs):
        return _ProfileResponse()


@pytest.mark.asyncio
async def test_account_refresh_preserves_scheduler_metadata():
    refresher = LiangzidpAuthRefresher(
        {
            "id": "account-1",
            "metadata": {
                "priority": 80,
                "weight": 40,
                "max_inflight": 1,
                "daily_request_limit": 100,
            },
        }
    )

    await refresher._fetch_user_info(_ProfileSession(), {})

    updated = refresher.get_updated_account()
    assert updated["metadata"] == {
        "priority": 80,
        "weight": 40,
        "max_inflight": 1,
        "daily_request_limit": 100,
        "username": "updated-name",
        "is_plus": True,
        "is_pro": True,
        "plan_type": "pro",
    }
    assert updated["tier"] == "pro"


@pytest.mark.asyncio
async def test_local_changelog_uses_comparable_utc_timestamps(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    await repo.initialize()
    try:
        before = datetime.now(UTC) - timedelta(seconds=1)
        await repo.upsert({"id": "account-1", "session_token": "token"})
        changes = await repo.scan_changes(before)
        assert [change["op"] for change in changes] == ["upsert"]

        row = await (
            await repo._conn.execute(
                "SELECT created_at FROM change_log ORDER BY id DESC LIMIT 1"
            )
        ).fetchone()
        created_at = datetime.fromisoformat(row["created_at"])
        assert created_at.tzinfo is not None
        assert created_at.utcoffset() == timedelta(0)

        await repo.delete("account-1")
        changes = await repo.scan_changes(before)
        assert [change["op"] for change in changes] == ["upsert", "delete"]
    finally:
        await repo.close()


def test_config_save_is_atomic_and_leaves_no_temporary_file(tmp_path):
    snapshot = ConfigSnapshot()
    snapshot._data = {"account": {"selection": {"max_inflight": 1}}}
    destination = tmp_path / "config.toml"

    snapshot._save(destination)

    with destination.open("rb") as file:
        assert tomllib.load(file) == snapshot._data
    assert list(tmp_path.glob(".config.toml.*.tmp")) == []


class _FakePipeline:
    def __init__(self, client):
        self.client = client
        self.operations = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def set(self, key, value):
        self.operations.append(("set", key, value))

    def delete(self, key):
        self.operations.append(("delete", key))

    def zadd(self, key, values):
        self.operations.append(("zadd", key, values))

    async def execute(self):
        for operation in self.operations:
            if operation[0] == "set":
                self.client.values[operation[1]] = operation[2]
            elif operation[0] == "delete":
                self.client.values.pop(operation[1], None)
            else:
                self.client.sorted_sets.setdefault(operation[1], {}).update(
                    operation[2]
                )


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.sorted_sets = {}

    def pipeline(self, transaction=True):
        assert transaction is True
        return _FakePipeline(self)

    async def zrangebyscore(self, key, minimum, _maximum):
        threshold = float(str(minimum).removeprefix("("))
        values = self.sorted_sets.get(key, {})
        return [
            member
            for member, score in sorted(values.items(), key=lambda item: item[1])
            if score > threshold
        ]


@pytest.mark.asyncio
async def test_redis_changelog_returns_incremental_changes():
    repo = object.__new__(RedisAccountRepository)
    repo._client = _FakeRedis()
    repo._prefix = "liangzidp:account:"
    repo._changelog_key = "liangzidp:changelog"
    since = datetime.now(UTC) - timedelta(seconds=1)

    await repo.upsert({"id": "account-1", "session_token": "token"})
    await repo.delete("account-1")

    assert await repo.scan_changes(since) == [
        {
            "op": "upsert",
            "id": "account-1",
            "data": {"id": "account-1", "session_token": "token"},
        },
        {"op": "delete", "id": "account-1"},
    ]
