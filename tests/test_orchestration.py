from __future__ import annotations

import asyncio
import sqlite3
import time
from types import SimpleNamespace

import pytest

from app.control.account.backends.local import LocalAccountRepository
from app.control.orchestration import AccountLease, OrchestrationStore, SessionOrchestrator
from app.control.orchestration import service as orchestration_service
from app.dataplane.account.model import LiangzidpAccount


def account(account_id: str, *, tier: str = "basic", status: str = "active", priority: int = 100) -> dict:
    return {
        "id": account_id,
        "email": account_id,
        "session_token": f"token-{account_id}",
        "status": status,
        "tier": tier,
        "metadata": {"priority": priority, "weight": 100, "max_inflight": 2},
    }


@pytest.mark.asyncio
async def test_lease_heartbeat_has_a_hard_lifetime(monkeypatch):
    class HeartbeatStore:
        def __init__(self):
            self.metrics: list[str] = []
            self.heartbeats = 0

        async def increment_metric(self, name: str, _amount: int = 1):
            self.metrics.append(name)

        async def heartbeat_lease(self, _lease_id: str, _lease_seconds: int):
            self.heartbeats += 1
            return True

    async def no_wait(_seconds: float):
        return None

    store = HeartbeatStore()
    orchestrator = SessionOrchestrator(store)  # type: ignore[arg-type]
    raw = account("stale")
    lease = AccountLease(
        account=LiangzidpAccount.from_dict(raw),
        raw=raw,
        session_key="task-stale",
        conversation={},
        sticky_hit=False,
        lease_id="lease-stale",
    )
    lease.acquired_at = time.monotonic() - 120
    monkeypatch.setattr(orchestration_service.asyncio, "sleep", no_wait)
    monkeypatch.setattr(
        orchestration_service.config,
        "get_int",
        lambda key, default=0: 30 if key in {
            "account.lease.stale_seconds",
            "account.lease.max_heartbeat_seconds",
        } else default,
    )

    await orchestrator._heartbeat_loop(lease)

    assert store.heartbeats == 0
    assert store.metrics == ["lease_heartbeat_hard_expired"]


@pytest.mark.asyncio
async def test_legacy_database_migrates_without_losing_session_state(tmp_path):
    path = tmp_path / "orchestration.db"
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE conversations(
            session_key TEXT PRIMARY KEY, account_id TEXT NOT NULL,
            model TEXT NOT NULL, egress_proxy TEXT NOT NULL DEFAULT '',
            chat_session_id TEXT NOT NULL DEFAULT '', parent_message_id INTEGER,
            generation INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'active',
            checkpoint_json TEXT NOT NULL DEFAULT '{}', last_error TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL, updated_at REAL NOT NULL, expires_at REAL NOT NULL
        )
        """
    )
    now = time.time()
    db.execute(
        "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("legacy", "alpha", "deepseek-v4-pro", "", "chat-1", 42, 3, "active", "{}", "", now, now, now + 3600),
    )
    db.commit()
    db.close()

    store = OrchestrationStore(path)
    await store.initialize()
    try:
        row = await store.get_conversation("legacy")
        assert row
        assert row["chat_session_id"] == "chat-1"
        assert row["parent_message_id"] == 42
        assert row["last_activity_at"] == pytest.approx(now)
        columns = {
            item[1]
            for item in await (await store._db().execute("PRAGMA table_info(conversations)")).fetchall()
        }
        assert {"client_identity", "identity_source", "last_activity_at"} <= columns
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_content_session_key_is_stable_for_growing_history(tmp_path):
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await store.initialize()
    try:
        service = SessionOrchestrator(store)
        first = {
            "_client_identity": "key-1",
            "model": "deepseek-v4-flash",
            "messages": [
                {"role": "system", "content": "work carefully"},
                {"role": "user", "content": "inspect project"},
            ],
            "tools": [{"type": "function", "function": {"name": "Bash"}}],
        }
        later = {
            **first,
            "messages": [
                *first["messages"],
                {"role": "assistant", "content": "working"},
                {"role": "user", "content": "continue"},
            ],
        }
        assert service.session_key(first) == service.session_key(later)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_account_is_selected_once_then_sticks_to_conversation(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("primary", priority=10))
        await repo.upsert(account("secondary", priority=100))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "conversation-1",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "hello"}],
        }
        first = await service.acquire(repo, request)
        assert first and first.account.id == "primary"
        assert first.sticky_hit is False
        await service.release(first, success=True)

        second = await service.acquire(repo, {**request, "messages": [
            *request["messages"],
            {"role": "assistant", "content": "hi"},
            {"role": "user", "content": "continue"},
        ]})
        assert second and second.account.id == "primary"
        assert second.sticky_hit is True
        await service.release(second, success=True)
        metrics = await store.metrics()
        assert metrics["new_sessions"] == 1
        assert metrics["sticky_hits"] == 1
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_new_conversations_spread_across_accounts_with_fewer_active_sessions(
    tmp_path,
):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        alpha = account("alpha")
        beta = account("beta")
        alpha["metadata"].update({"max_inflight": 8, "weight": 1})
        beta["metadata"].update({"max_inflight": 8, "weight": 1})
        await repo.upsert(alpha)
        await repo.upsert(beta)
        service = SessionOrchestrator(store)
        first = await service.acquire(repo, {
            "conversation_id": "window-one",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "first task"}],
        })
        assert first
        await service.release(first, success=True)

        second = await service.acquire(repo, {
            "conversation_id": "window-two",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "second task"}],
        })
        assert second
        assert second.account.id != first.account.id
        await service.release(second, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_busy_sticky_conversation_is_not_migrated_to_another_account(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        primary = account("primary", priority=10)
        fallback = account("fallback", priority=10)
        primary["metadata"]["max_inflight"] = 1
        fallback["metadata"]["max_inflight"] = 1
        await repo.upsert(primary)
        await repo.upsert(fallback)
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "one-sticky-window",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "long task"}],
        }

        first = await service.acquire(repo, request)
        assert first
        request["_queue_timeout_seconds"] = 2
        waiting = asyncio.create_task(service.acquire(repo, request))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        persisted = await store.get_conversation(first.session_key)
        assert persisted and persisted["account_id"] == first.account.id

        await service.release(first, success=True)
        resumed = await waiting
        assert resumed and resumed.account.id == first.account.id
        assert resumed.sticky_hit is True
        await service.release(resumed, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_request_account_hint_cannot_rebind_existing_conversation(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("primary", priority=10))
        await repo.upsert(account("secondary", priority=20))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "fixed-account",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "start"}],
        }

        first = await service.acquire(repo, request, requested_account_id="primary")
        assert first and first.account.id == "primary"
        await service.release(first, success=True)

        second = await service.acquire(repo, request, requested_account_id="secondary")
        assert second and second.account.id == "primary"
        assert second.sticky_hit is True
        await service.release(second, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_scheduler_filters_rate_limited_and_model_tier(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("limited", tier="pro", status="rate_limited", priority=1))
        basic = account("basic", tier="basic", priority=2)
        basic["metadata"]["pro_access"] = False
        await repo.upsert(basic)
        await repo.upsert(account("pro", tier="pro", priority=3))
        service = SessionOrchestrator(store)
        lease = await service.acquire(repo, {
            "conversation_id": "pro-task",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "complex task"}],
        })
        assert lease and lease.account.id == "pro"
        await service.release(lease, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_failed_account_cools_down_and_conversation_recovers(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("first", priority=1))
        await repo.upsert(account("fallback", priority=2))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "recover-task",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "work"}],
        }
        first = await service.acquire(repo, request)
        assert first and first.account.id == "first"
        await service.release(first, success=False, error="rate limited")

        failed = await repo.get(first.account.id)
        failed["status"] = "expired"
        await repo.upsert(failed)
        fallback = await service.acquire(repo, request)
        assert fallback and fallback.account.id == "fallback"
        assert fallback.recovered is True
        assert int(fallback.conversation["generation"]) == 2
        await service.release(fallback, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_transient_failures_respect_configured_threshold(tmp_path, monkeypatch):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    monkeypatch.setenv("LIANGZIDP_UNUSED", "1")
    try:
        await repo.upsert(account("primary", priority=1))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "threshold-task",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "work"}],
        }
        for attempt in range(3):
            lease = await service.acquire(repo, request)
            assert lease
            await service.release(lease, success=False, error="network reset", error_kind="upstream_error")
            runtime = (await store.runtime_snapshot(["primary"]))["primary"]
            assert bool(runtime["cooling_down"]) is (attempt == 2)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_global_upstream_busy_does_not_penalize_account(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("primary", priority=1))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": "busy-pro",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "work"}],
        }
        lease = await service.acquire(repo, request)
        assert lease
        await service.release(
            lease,
            success=False,
            error="服务器繁忙，请稍后再试，或使用快速模式",
            error_kind="upstream_busy",
        )
        runtime = (await store.runtime_snapshot(["primary"]))["primary"]
        assert runtime["inflight"] == 0
        assert runtime["failure_count"] == 0
        assert runtime["consecutive_failures"] == 0
        assert runtime["cooling_down"] is False
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_kind", ["incomplete_stream", "account_busy", "session_state"])
async def test_session_scoped_failures_do_not_cool_account(tmp_path, error_kind):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("primary", priority=1))
        service = SessionOrchestrator(store)
        request = {
            "conversation_id": f"neutral-{error_kind}",
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "work"}],
        }
        lease = await service.acquire(repo, request)
        assert lease
        await service.release(
            lease,
            success=False,
            error="turn ended in state incomplete",
            error_kind=error_kind,
        )
        runtime = (await store.runtime_snapshot(["primary"]))["primary"]
        assert runtime["failure_count"] == 0
        assert runtime["consecutive_failures"] == 0
        assert runtime["cooling_down"] is False
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_two_workers_atomically_claim_one_conversation_account(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store_a = OrchestrationStore(tmp_path / "orchestration.db")
    store_b = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store_a.initialize()
    await store_b.initialize()
    try:
        await repo.upsert(account("alpha", priority=10))
        await repo.upsert(account("beta", priority=10))
        service_a = SessionOrchestrator(store_a)
        service_b = SessionOrchestrator(store_b)
        request = {
            "_client_identity": "same-client",
            "conversation_id": "cross-worker-race",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "start"}],
        }
        lease_a, lease_b = await asyncio.gather(
            service_a.acquire(repo, dict(request)),
            service_b.acquire(repo, dict(request)),
        )
        leases = [lease for lease in (lease_a, lease_b) if lease]
        assert len(leases) == 1
        persisted = await store_a.get_conversation(leases[0].session_key)
        assert persisted and persisted["account_id"] == leases[0].account.id
        await service_a.release(leases[0], success=True)
    finally:
        await store_a.close()
        await store_b.close()
        await repo.close()


@pytest.mark.asyncio
async def test_same_api_key_and_metadata_user_id_create_distinct_tasks(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        for name in ("alpha", "beta"):
            item = account(name)
            item["metadata"]["max_inflight"] = 1
            await repo.upsert(item)
        service = SessionOrchestrator(store)
        common = {
            "_client_identity": "api-key:shared",
            "metadata": {"user_id": "same-user"},
            "model": "deepseek-v4-pro",
        }
        first = await service.acquire(repo, {
            **common,
            "messages": [{"role": "user", "content": "task one"}],
        })
        assert first
        await service.release(first, success=True)
        second = await service.acquire(repo, {
            **common,
            "messages": [{"role": "user", "content": "task two"}],
        })
        assert second
        assert second.session_key != first.session_key
        assert second.account.id != first.account.id
        await service.release(second, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_lineage_resumes_original_account_after_idle_scoring_window(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        await repo.upsert(account("alpha"))
        await repo.upsert(account("beta"))
        service = SessionOrchestrator(store)
        first_request = {
            "_client_identity": "api-key:shared",
            "metadata": {"user_id": "same-user"},
            "model": "deepseek-v4-pro",
            "messages": [{"role": "user", "content": "inspect project"}],
        }
        first = await service.acquire(repo, first_request)
        assert first
        response = {"role": "assistant", "content": "inspection complete"}
        await service.register_response_lineage(first_request, response)
        await service.release(first, success=True)
        await store._db().execute(
            "UPDATE conversations SET last_activity_at=last_activity_at-130 "
            "WHERE session_key=?",
            (first.session_key,),
        )
        await store._db().commit()

        resumed = await service.acquire(repo, {
            "_client_identity": "api-key:shared",
            "metadata": {"user_id": "same-user"},
            "model": "deepseek-v4-pro",
            "messages": [
                {"role": "user", "content": "inspect project"},
                response,
                {"role": "user", "content": "continue"},
            ],
        })
        assert resumed
        assert resumed.session_key == first.session_key
        assert resumed.account.id == first.account.id
        assert resumed.identity_source == "lineage"
        await service.release(resumed, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_fifty_accounts_accept_fifty_distinct_tasks(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        for index in range(50):
            item = account(f"account-{index:02d}")
            item["metadata"]["max_inflight"] = 1
            await repo.upsert(item)
        service = SessionOrchestrator(store)
        leases = []
        for index in range(50):
            lease = await service.acquire(repo, {
                "_client_identity": "api-key:shared",
                "model": "deepseek-v4-flash",
                "messages": [{"role": "user", "content": f"task {index}"}],
            })
            assert lease
            leases.append(lease)
        assert len({lease.session_key for lease in leases}) == 50
        assert len({lease.account.id for lease in leases}) == 50
        await asyncio.gather(*(service.release(lease, success=True) for lease in leases))
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_two_accounts_queue_and_complete_six_tasks(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        for name in ("alpha", "beta"):
            item = account(name)
            item["metadata"]["max_inflight"] = 1
            await repo.upsert(item)
        service = SessionOrchestrator(store)
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def run(index: int) -> str:
            nonlocal active, peak
            lease = await service.acquire(repo, {
                "_client_identity": "api-key:shared",
                "_queue_timeout_seconds": 3,
                "model": "deepseek-v4-flash",
                "messages": [{"role": "user", "content": f"queued task {index}"}],
            })
            assert lease
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.04)
            async with lock:
                active -= 1
            await service.release(lease, success=True)
            return lease.account.id

        accounts = await asyncio.gather(*(run(index) for index in range(6)))
        assert peak == 2
        assert set(accounts) == {"alpha", "beta"}
        assert await service.queue_depth() == 0
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_cancelled_waiter_is_removed_and_lease_is_reclaimed(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        item = account("only")
        item["metadata"]["max_inflight"] = 1
        await repo.upsert(item)
        service = SessionOrchestrator(store)
        first = await service.acquire(repo, {
            "conversation_id": "first",
            "model": "deepseek-v4-flash",
        })
        assert first
        waiting = asyncio.create_task(service.acquire(repo, {
            "conversation_id": "second",
            "_queue_timeout_seconds": 10,
            "model": "deepseek-v4-flash",
        }))
        for _ in range(20):
            if await service.queue_depth():
                break
            await asyncio.sleep(0.01)
        assert await service.queue_depth() == 1
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert await service.queue_depth() == 0

        if first.heartbeat_task:
            first.heartbeat_task.cancel()
            await asyncio.gather(first.heartbeat_task, return_exceptions=True)
            first.heartbeat_task = None
        await store._db().execute(
            "UPDATE account_leases SET expires_at=0 WHERE lease_id=?",
            (first.lease_id,),
        )
        await store._db().commit()
        replacement = await service.acquire(repo, {
            "conversation_id": "replacement",
            "model": "deepseek-v4-flash",
        })
        assert replacement and replacement.account.id == "only"
        await service.release(replacement, success=True)
        runtime = (await store.runtime_snapshot(["only"]))["only"]
        assert runtime["inflight"] == 0
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_nonstream_singleflight_executes_identical_request_once(tmp_path):
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await store.initialize()
    try:
        service = SessionOrchestrator(store)
        request = {
            "_client_identity": "api-key:shared",
            "model": "deepseek-v4-flash",
            "stream": False,
            "messages": [{"role": "user", "content": "same request"}],
        }
        assert service.request_digest(request) != service.request_digest({
            **request,
            "_force_fresh_upstream_session": True,
        })
        calls = 0

        async def invoke():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.03)
            return {"ok": True}

        results = await asyncio.gather(
            *(service.run_singleflight(dict(request), invoke) for _ in range(10))
        )
        assert calls == 1
        assert results == [{"ok": True}] * 10
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_stream_singleflight_replays_one_upstream_to_all_subscribers(tmp_path):
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await store.initialize()
    try:
        service = SessionOrchestrator(store)
        request = {
            "_client_identity": "api-key:shared",
            "model": "deepseek-v4-flash",
            "stream": True,
            "messages": [{"role": "user", "content": "same stream"}],
        }
        calls = 0

        async def invoke():
            nonlocal calls
            calls += 1

            async def source():
                for value in ("one", "two", "three"):
                    await asyncio.sleep(0.01)
                    yield value

            return source()

        streams = await asyncio.gather(
            *(service.run_singleflight(dict(request), invoke) for _ in range(5))
        )
        output = await asyncio.gather(*(
            asyncio.create_task(_collect_stream(stream))
            for stream in streams
        ))
        assert calls == 1
        assert output == [["one", "two", "three"]] * 5
    finally:
        await store.close()


async def _collect_stream(stream):
    return [item async for item in stream]


@pytest.mark.asyncio
async def test_fixed_quota_window_is_anchored_to_pool_entry(tmp_path, monkeypatch):
    clock = {"now": 1_000.0}
    monkeypatch.setattr(
        "app.control.orchestration.store.time",
        SimpleNamespace(time=lambda: clock["now"]),
    )
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await store.initialize()
    try:
        await store.configure_account(
            "limited",
            pool_added_at=1_000.0,
            request_limit=1,
        )
        assert await store.try_acquire_slot("limited", 8) is True
        await store.release_slot("limited", success=True)
        first = (await store.runtime_snapshot(["limited"]))["limited"]
        assert first["quota_requests_used"] == 1
        assert first["quota_exhausted"] is True
        assert first["quota_reset_at"] == 87_400.0

        clock["now"] = 87_401.0
        reset = (await store.runtime_snapshot(["limited"]))["limited"]
        assert reset["quota_window_started_at"] == 87_400.0
        assert reset["quota_reset_at"] == 173_800.0
        assert reset["quota_requests_used"] == 0
        assert reset["quota_exhausted"] is False
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_quota_exhausted_account_is_skipped_by_scheduler(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    try:
        primary = account("primary", priority=10)
        primary["metadata"]["daily_request_limit"] = 1
        await repo.upsert(primary)
        await repo.upsert(account("secondary", priority=20))
        service = SessionOrchestrator(store)

        first = await service.acquire(repo, {
            "conversation_id": "quota-first",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "first"}],
        })
        assert first and first.account.id == "primary"
        await service.release(first, success=True)
        runtime = (await store.runtime_snapshot(["primary"]))["primary"]
        assert runtime["quota_exhausted"] is True

        second = await service.acquire(repo, {
            "conversation_id": "quota-second",
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "second"}],
        })
        assert second and second.account.id == "secondary"
        await service.release(second, success=True)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_last_request_quota_slot_is_atomic_across_workers(tmp_path):
    path = tmp_path / "orchestration.db"
    store_a = OrchestrationStore(path)
    store_b = OrchestrationStore(path)
    await store_a.initialize()
    await store_b.initialize()
    try:
        await store_a.configure_account("limited", request_limit=1)
        await store_b.configure_account("limited", request_limit=1)
        acquired = await asyncio.gather(
            store_a.try_acquire_slot("limited", 8),
            store_b.try_acquire_slot("limited", 8),
        )
        assert acquired.count(True) == 1
        assert acquired.count(False) == 1
        snapshot = (await store_a.runtime_snapshot(["limited"]))["limited"]
        assert snapshot["quota_requests_used"] == 1
        assert snapshot["quota_exhausted"] is True
        winner = store_a if acquired[0] else store_b
        await winner.release_slot("limited", success=True)
    finally:
        await store_a.close()
        await store_b.close()


@pytest.mark.asyncio
async def test_token_cost_budget_exhausts_and_manual_reset_keeps_schedule(tmp_path):
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await store.initialize()
    try:
        await store.configure_account(
            "budgeted",
            budget_microusd=1_000_000,
            rate_microusd_per_million=2_000_000,
        )
        before = (await store.runtime_snapshot(["budgeted"]))["budgeted"]
        await store.record_quota_usage("budgeted", 500_000)
        exhausted = (await store.runtime_snapshot(["budgeted"]))["budgeted"]
        assert exhausted["quota_cost_usd"] == 1.0
        assert exhausted["quota_exhausted"] is True

        reset = await store.reset_quota("budgeted")
        assert reset is not None
        assert reset["quota_cost_usd"] == 0
        assert reset["quota_exhausted"] is False
        assert reset["quota_reset_at"] == before["quota_reset_at"]
    finally:
        await store.close()
