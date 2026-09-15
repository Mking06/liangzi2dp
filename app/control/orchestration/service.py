from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.control.orchestration.store import OrchestrationStore
from app.dataplane.account.model import AccountTier, LiangzidpAccount
from app.platform.config.snapshot import config
from app.platform.errors import AccountError
from app.platform.logging.logger import logger


@dataclass
class AccountLease:
    account: LiangzidpAccount
    raw: dict[str, Any]
    session_key: str
    conversation: dict[str, Any]
    sticky_hit: bool
    recovered: bool = False
    egress_proxy: str = ""
    lease_id: str = ""
    identity_source: str = "legacy"
    queue_wait_ms: int = 0
    heartbeat_task: asyncio.Task[None] | None = None
    acquired_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class TaskResolution:
    session_key: str
    source: str
    client_identity: str
    existing: bool = False


@dataclass
class QueueTicket:
    ticket_id: str
    client_identity: str
    continuation: bool
    created_at: float
    event: asyncio.Event
    cancelled: bool = False


@dataclass
class StreamFlight:
    source: Any
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    history: list[Any] = field(default_factory=list)
    done: bool = False
    error: BaseException | None = None
    subscribers: int = 0
    task: asyncio.Task[None] | None = None

    async def pump(self) -> None:
        try:
            async for chunk in self.source:
                async with self.condition:
                    self.history.append(chunk)
                    self.condition.notify_all()
        except BaseException as exc:
            async with self.condition:
                self.error = exc
                self.done = True
                self.condition.notify_all()
            return
        async with self.condition:
            self.done = True
            self.condition.notify_all()

    async def subscribe(self):
        index = 0
        async with self.condition:
            self.subscribers += 1
        try:
            while True:
                async with self.condition:
                    await self.condition.wait_for(lambda: index < len(self.history) or self.done)
                    pending = self.history[index:]
                    index = len(self.history)
                    done = self.done
                    error = self.error
                for chunk in pending:
                    yield chunk
                if done:
                    if error:
                        raise error
                    return
        finally:
            async with self.condition:
                self.subscribers = max(0, self.subscribers - 1)
                if self.subscribers == 0 and not self.done and self.task:
                    self.task.cancel()


class SessionOrchestrator:
    """Sticky conversation routing and load-aware account scheduling."""

    def __init__(self, store: OrchestrationStore):
        self.store = store
        self._selection_lock = asyncio.Lock()
        self._policy_cache: dict[str, dict[str, int | float]] = {}
        self._owner_id = f"{os.getpid()}:{uuid.uuid4().hex[:12]}"
        self._queue_lock = asyncio.Lock()
        self._waiters: list[QueueTicket] = []
        self._continuation_turns = 0
        self._last_tenant = ""
        self._singleflight_lock = asyncio.Lock()
        self._singleflight: dict[str, tuple[float, asyncio.Future[Any]]] = {}
        self._stream_singleflight: dict[
            str,
            tuple[float, asyncio.Future[StreamFlight]],
        ] = {}

    @staticmethod
    def _content_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict):
                    value = block.get("text") or block.get("content")
                    if isinstance(value, str):
                        parts.append(value)
            return "\n".join(parts)
        return ""

    def session_key(self, request_data: dict[str, Any]) -> tuple[str, str]:
        resolved = str(request_data.get("_resolved_session_key") or "")
        if resolved:
            return resolved, str(request_data.get("_identity_source") or "resolved")
        explicit = (
            request_data.get("conversation_id")
            or request_data.get("session_id")
            or request_data.get("thread_id")
        )
        client_identity = str(request_data.get("_client_identity") or "anonymous")
        if explicit:
            seed = f"explicit|{client_identity}|{explicit}"
            return hashlib.sha256(seed.encode()).hexdigest(), "explicit"

        messages = [item for item in request_data.get("messages") or [] if isinstance(item, dict)]
        system = [
            self._content_text(item.get("content"))
            for item in messages
            if item.get("role") == "system"
        ]
        first_user = next(
            (self._content_text(item.get("content")) for item in messages if item.get("role") == "user"),
            "",
        )
        tools = request_data.get("tools") or request_data.get("functions") or []
        anchor = {
            "client": client_identity,
            "model": str(request_data.get("model") or ""),
            "system": system,
            "tools": tools,
            "first_user": first_user,
        }
        raw = json.dumps(anchor, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest(), "root_candidate"

    @staticmethod
    def _canonical_message(message: dict[str, Any]) -> dict[str, Any]:
        calls = []
        for call in message.get("tool_calls") or []:
            if not isinstance(call, dict):
                continue
            function = call.get("function") if isinstance(call.get("function"), dict) else {}
            calls.append({
                "id": str(call.get("id") or ""),
                "name": str(function.get("name") or call.get("name") or ""),
                "arguments": str(function.get("arguments") or call.get("input") or ""),
            })
        return {
            "role": str(message.get("role") or ""),
            "content": SessionOrchestrator._content_text(message.get("content")),
            "reasoning": str(message.get("reasoning_content") or ""),
            "tool_call_id": str(message.get("tool_call_id") or ""),
            "tool_calls": calls,
        }

    @staticmethod
    def _digest(value: Any) -> str:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _lineage_alias(self, request_data: dict[str, Any]) -> str:
        messages = [item for item in request_data.get("messages") or [] if isinstance(item, dict)]
        for index in range(len(messages) - 1, -1, -1):
            if messages[index].get("role") != "assistant":
                continue
            previous = next(
                (
                    messages[pos]
                    for pos in range(index - 1, -1, -1)
                    if messages[pos].get("role") != "system"
                ),
                {},
            )
            return self._digest({
                "previous": self._canonical_message(previous),
                "assistant": self._canonical_message(messages[index]),
            })
        return ""

    def _root_alias(self, request_data: dict[str, Any]) -> str:
        if any(
            request_data.get(name)
            for name in ("conversation_id", "session_id", "thread_id")
        ):
            return ""
        messages = [item for item in request_data.get("messages") or [] if isinstance(item, dict)]
        return self._digest({
            "model": str(request_data.get("model") or ""),
            "system": [
                self._canonical_message(item)
                for item in messages
                if item.get("role") == "system"
            ],
            "first_user": next(
                (self._canonical_message(item) for item in messages if item.get("role") == "user"),
                {},
            ),
            "tools": request_data.get("tools") or request_data.get("functions") or [],
        })

    async def resolve_task(self, request_data: dict[str, Any]) -> TaskResolution:
        existing_key = str(request_data.get("_resolved_session_key") or "")
        client_identity = str(request_data.get("_client_identity") or "anonymous")
        if existing_key:
            conversation = await self.store.get_conversation(existing_key)
            return TaskResolution(
                existing_key,
                str(request_data.get("_identity_source") or "resolved"),
                client_identity,
                conversation is not None,
            )

        tool_call_id = self._tool_result_call_id(request_data)
        continuation = await self.store.get_tool_continuation(tool_call_id) if tool_call_id else None
        if continuation:
            await self.store.increment_metric("identity_tool")
            return TaskResolution(
                str(continuation["session_key"]),
                "tool",
                client_identity,
                True,
            )

        explicit_key, source = self.session_key(request_data)
        if source == "explicit":
            conversation = await self.store.get_conversation(explicit_key)
            await self.store.increment_metric("identity_explicit")
            return TaskResolution(explicit_key, source, client_identity, conversation is not None)

        lineage = self._lineage_alias(request_data)
        if lineage:
            conversation = await self.store.resolve_task_alias(client_identity, lineage)
            if conversation:
                await self.store.increment_metric("identity_lineage")
                return TaskResolution(
                    str(conversation["session_key"]),
                    "lineage",
                    client_identity,
                    True,
                )

        root_alias = self._root_alias(request_data)
        if root_alias:
            conversation = await self.store.resolve_task_alias(client_identity, root_alias)
            if conversation:
                await self.store.increment_metric("identity_retry")
                return TaskResolution(
                    str(conversation["session_key"]),
                    "retry",
                    client_identity,
                    True,
                )

        session_key = hashlib.sha256(
            f"task|{client_identity}|{uuid.uuid4().hex}".encode()
        ).hexdigest()
        await self.store.increment_metric("identity_new")
        return TaskResolution(session_key, "new", client_identity, False)

    @staticmethod
    def _tool_result_call_id(request_data: dict[str, Any]) -> str:
        for message in reversed(request_data.get("messages") or []):
            if not isinstance(message, dict):
                continue
            if message.get("role") == "tool" and message.get("tool_call_id"):
                return str(message["tool_call_id"])
        return ""

    @staticmethod
    def _supports_model(account: LiangzidpAccount, model: str, raw: dict[str, Any] | None = None) -> bool:
        name = model.lower().removesuffix("[1m]")
        metadata = (raw or {}).get("metadata") if isinstance((raw or {}).get("metadata"), dict) else {}
        denied = {str(item).lower() for item in metadata.get("denied_models") or []}
        if name in denied or (name == "deepseek-v4-pro" and metadata.get("pro_access") is False):
            return False
        if name == "deepseek-v4-pro":
            return (
                account.tier == AccountTier.PRO
                or not config.get_bool("account.selection.require_pro_tier", False)
            )
        return True

    @staticmethod
    def _account_limits(raw: dict[str, Any]) -> tuple[int, int, int]:
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        priority = int(metadata.get("priority", 100))
        weight = max(1, int(metadata.get("weight", 100)))
        max_inflight = max(
            1,
            int(metadata.get("max_inflight") or config.get_int("account.selection.max_inflight", 1)),
        )
        return priority, weight, max_inflight

    @staticmethod
    def _quota_policy(raw: dict[str, Any]) -> dict[str, int | float]:
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        pool_added_at: float
        value = metadata.get("pool_added_at") or raw.get("pool_added_at")
        try:
            pool_added_at = float(value)
        except (TypeError, ValueError):
            try:
                pool_added_at = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
            except (TypeError, ValueError):
                pool_added_at = time.time()

        def non_negative_int(name: str) -> int:
            try:
                return max(0, int(metadata.get(name) or 0))
            except (TypeError, ValueError):
                return 0

        def non_negative_float(name: str) -> float:
            try:
                return max(0.0, float(metadata.get(name) or 0))
            except (TypeError, ValueError):
                return 0.0

        return {
            "pool_added_at": pool_added_at,
            "request_limit": non_negative_int("daily_request_limit"),
            "token_limit": non_negative_int("daily_token_limit"),
            "budget_microusd": round(non_negative_float("daily_budget_usd") * 1_000_000),
            "rate_microusd_per_million": round(
                non_negative_float("usd_per_million_tokens") * 1_000_000
            ),
        }

    async def sync_account_policy(self, raw: dict[str, Any]) -> None:
        account_id = str(raw.get("id") or "")
        if not account_id:
            return
        policy = self._quota_policy(raw)
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        if not (metadata.get("pool_added_at") or raw.get("pool_added_at")):
            cached = self._policy_cache.get(account_id)
            if cached:
                policy["pool_added_at"] = cached["pool_added_at"]
        if self._policy_cache.get(account_id) == policy:
            return
        await self.store.configure_account(account_id, **policy)
        self._policy_cache[account_id] = policy

    async def sync_account_policies(self, rows: list[dict[str, Any]]) -> None:
        for raw in rows:
            await self.sync_account_policy(raw)

    async def _eligible(
        self,
        repo: Any,
        model: str,
        excluded: set[str],
    ) -> list[tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]]]:
        rows = await repo.list_all()
        await self.sync_account_policies(rows)
        runtime = await self.store.runtime_snapshot([str(row.get("id")) for row in rows])
        output: list[tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]]] = []
        for raw in rows:
            account = LiangzidpAccount.from_dict(raw)
            state = runtime.get(account.id, {"account_id": account.id, "inflight": 0, "active_sessions": 0})
            _, _, max_inflight = self._account_limits(raw)
            if account.id in excluded:
                continue
            if not account.is_available() or not self._supports_model(account, model, raw):
                continue
            if (
                state.get("cooling_down")
                or state.get("quota_exhausted")
                or int(state.get("inflight") or 0) >= max_inflight
                or str(state.get("health_status") or "unknown") not in {"healthy", "unknown"}
            ):
                continue
            output.append((account, raw, state))
        return output

    def _ranked_choice(
        self,
        candidates: list[tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]]],
        session_key: str,
    ) -> list[tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]]]:
        buckets: dict[int, list[tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]]]] = {}
        for item in candidates:
            priority, _, _ = self._account_limits(item[1])
            buckets.setdefault(priority, []).append(item)
        if not buckets:
            return []
        pool = buckets[min(buckets)]

        def load(
            item: tuple[LiangzidpAccount, dict[str, Any], dict[str, Any]],
        ) -> tuple[int, int, float, int, float]:
            _, weight, _ = self._account_limits(item[1])
            state = item[2]
            failure_rate = float(state.get("error_ewma") or 0)
            tie = int(
                hashlib.sha256(f"{session_key}|{item[0].id}".encode()).hexdigest()[:16],
                16,
            ) / weight
            return (
                int(state.get("inflight") or 0),
                int(state.get("active_sessions") or 0),
                failure_rate,
                int(state.get("latency_ewma_ms") or state.get("health_latency_ms") or 0),
                tie,
            )

        return sorted(pool, key=load)

    async def _enqueue(self, client_identity: str, continuation: bool) -> QueueTicket:
        ticket = QueueTicket(
            ticket_id=uuid.uuid4().hex,
            client_identity=client_identity,
            continuation=continuation,
            created_at=time.monotonic(),
            event=asyncio.Event(),
        )
        async with self._queue_lock:
            self._waiters = [item for item in self._waiters if not item.cancelled]
            limit = config.get_int("account.queue.max_size", 200)
            if len(self._waiters) >= max(1, limit):
                raise AccountError(
                    "Account queue is full",
                    429,
                    "queue_full",
                    {"retry_after": 5},
                )
            self._waiters.append(ticket)
        return ticket

    async def _remove_ticket(self, ticket: QueueTicket) -> None:
        ticket.cancelled = True
        async with self._queue_lock:
            self._waiters = [item for item in self._waiters if item.ticket_id != ticket.ticket_id]

    async def _wake_next(self) -> None:
        async with self._queue_lock:
            live = [item for item in self._waiters if not item.cancelled]
            self._waiters = live
            if not live:
                return
            continuations = [item for item in live if item.continuation]
            new_tasks = [item for item in live if not item.continuation]
            if continuations and (self._continuation_turns < 3 or not new_tasks):
                pool = continuations
                self._continuation_turns += 1
            else:
                pool = new_tasks or continuations
                self._continuation_turns = 0
            tenant_pool = [item for item in pool if item.client_identity != self._last_tenant]
            selected = min(tenant_pool or pool, key=lambda item: item.created_at)
            self._last_tenant = selected.client_identity
            self._waiters = [item for item in live if item.ticket_id != selected.ticket_id]
            selected.event.set()

    async def _heartbeat_loop(self, lease: AccountLease) -> None:
        interval = config.get_int("account.lease.heartbeat_seconds", 15)
        lease_seconds = config.get_int("account.lease.stale_seconds", 90)
        max_heartbeat_seconds = config.get_int("account.lease.max_heartbeat_seconds", 600)
        deadline = lease.acquired_at + max(lease_seconds, max_heartbeat_seconds)
        try:
            while True:
                await asyncio.sleep(max(5, interval))
                if time.monotonic() >= deadline:
                    await self.store.increment_metric("lease_heartbeat_hard_expired")
                    logger.error(
                        "lease heartbeat hard limit reached: lease={} session={} account={} age_seconds={}",
                        lease.lease_id[:12],
                        lease.session_key[:16],
                        lease.account.id,
                        round(time.monotonic() - lease.acquired_at),
                    )
                    return
                if not await self.store.heartbeat_lease(lease.lease_id, lease_seconds):
                    return
        except asyncio.CancelledError:
            raise

    def _start_heartbeat(self, lease: AccountLease) -> AccountLease:
        if lease.lease_id:
            lease.heartbeat_task = asyncio.create_task(self._heartbeat_loop(lease))
        return lease

    async def queue_depth(self) -> int:
        async with self._queue_lock:
            return sum(1 for item in self._waiters if not item.cancelled)

    def request_digest(self, request_data: dict[str, Any]) -> str:
        idempotency_key = str(request_data.get("_idempotency_key") or "")
        client_identity = str(request_data.get("_client_identity") or "anonymous")
        if idempotency_key:
            return self._digest({
                "client": client_identity,
                "idempotency_key": idempotency_key,
            })
        stable = {
            key: value
            for key, value in request_data.items()
            if not key.startswith("_")
            and key not in {"chat_session_id", "parent_message_id", "pow_response"}
        }
        return self._digest({
            "client": client_identity,
            "request": stable,
            "force_fresh_upstream_session": bool(
                request_data.get("_force_fresh_upstream_session")
            ),
        })

    async def run_singleflight(
        self,
        request_data: dict[str, Any],
        invoke: Any,
    ) -> Any:
        """Coalesce identical buffered/non-stream requests for 30 seconds."""
        if bool(request_data.get("stream")):
            return await self._run_stream_singleflight(request_data, invoke)
        key = self.request_digest(request_data)
        now = time.monotonic()
        leader = False
        async with self._singleflight_lock:
            expired = [
                name
                for name, (until, item) in self._singleflight.items()
                if until <= now and item.done()
            ]
            for name in expired:
                self._singleflight.pop(name, None)
            entry = self._singleflight.get(key)
            if entry is None:
                future = asyncio.get_running_loop().create_future()
                self._singleflight[key] = (now + 86400, future)
                leader = True
            else:
                future = entry[1]
        if not leader:
            await self.store.increment_metric("singleflight_hits")
            result, metadata = await asyncio.shield(future)
            request_data.update(metadata)
            return copy.deepcopy(result)
        try:
            result = await invoke()
            if not future.done():
                metadata = {
                    key: request_data[key]
                    for key in (
                        "_resolved_session_key",
                        "_identity_source",
                        "_queue_wait_ms",
                        "_selected_account_hash",
                    )
                    if key in request_data
                }
                future.set_result((copy.deepcopy(result), metadata))
                async with self._singleflight_lock:
                    current = self._singleflight.get(key)
                    if current and current[1] is future:
                        self._singleflight[key] = (time.monotonic() + 30, future)
            await self.store.increment_metric("singleflight_leaders")
            try:
                size = len(json.dumps(result, ensure_ascii=False))
            except (TypeError, ValueError):
                size = 9 * 1024 * 1024
            if size > 8 * 1024 * 1024:
                async with self._singleflight_lock:
                    self._singleflight.pop(key, None)
            return result
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
                future.add_done_callback(lambda item: item.exception())
            async with self._singleflight_lock:
                self._singleflight.pop(key, None)
            raise

    async def _run_stream_singleflight(self, request_data: dict[str, Any], invoke: Any) -> Any:
        key = self.request_digest(request_data)
        now = time.monotonic()
        leader = False
        async with self._singleflight_lock:
            expired = [
                name
                for name, (until, future) in self._stream_singleflight.items()
                if until <= now and future.done()
            ]
            for name in expired:
                self._stream_singleflight.pop(name, None)
            entry = self._stream_singleflight.get(key)
            if entry is None:
                future: asyncio.Future[StreamFlight] = asyncio.get_running_loop().create_future()
                self._stream_singleflight[key] = (now + 86400, future)
                leader = True
            else:
                future = entry[1]
        if not leader:
            await self.store.increment_metric("singleflight_stream_hits")
            flight = await asyncio.shield(future)
            return flight.subscribe()
        try:
            source = await invoke()
            if not hasattr(source, "__aiter__"):
                raise RuntimeError("stream singleflight received a non-stream response")
            flight = StreamFlight(source)

            async def pump() -> None:
                await flight.pump()
                async with self._singleflight_lock:
                    current = self._stream_singleflight.get(key)
                    if current and current[1] is future:
                        if isinstance(flight.error, asyncio.CancelledError):
                            self._stream_singleflight.pop(key, None)
                        else:
                            self._stream_singleflight[key] = (time.monotonic() + 30, future)

            flight.task = asyncio.create_task(pump())
            future.set_result(flight)
            await self.store.increment_metric("singleflight_stream_leaders")
            return flight.subscribe()
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
                future.add_done_callback(lambda item: item.exception())
            async with self._singleflight_lock:
                self._stream_singleflight.pop(key, None)
            raise

    async def acquire(
        self,
        repo: Any,
        request_data: dict[str, Any],
        requested_account_id: str | None = None,
        excluded: set[str] | None = None,
        egress_proxy: str = "",
    ) -> AccountLease | None:
        excluded = set(excluded or ())
        model = str(request_data.get("model") or "deepseek-v4-flash")
        wait_timeout = max(0.0, float(request_data.get("_queue_timeout_seconds") or 0))
        deadline = time.monotonic() + wait_timeout
        queued_at: float | None = None
        resolution: TaskResolution | None = None

        while True:
            async with self._selection_lock:
                reaped = await self.store.reap_stale_leases()
                if reaped:
                    await self.store.increment_metric("leases_reclaimed", reaped)
                resolution = resolution or await self.resolve_task(request_data)
                session_key = resolution.session_key
                request_data["_resolved_session_key"] = session_key
                request_data["_identity_source"] = resolution.source
                conversation = await self.store.get_conversation(session_key)
                sticky_account_id = (
                    str(conversation["account_id"])
                    if conversation
                    else (requested_account_id or "")
                )
                sticky_existing = conversation is not None
                sticky_temporarily_blocked = False

                if sticky_account_id and sticky_account_id not in excluded:
                    raw = await repo.get(sticky_account_id)
                    if raw:
                        await self.sync_account_policy(raw)
                        account = LiangzidpAccount.from_dict(raw)
                        _, _, max_inflight = self._account_limits(raw)
                        runtime = (await self.store.runtime_snapshot([account.id])).get(account.id, {})
                        permanent_failure = (
                            not account.is_available()
                            or not self._supports_model(account, model, raw)
                        )
                        sticky_temporarily_blocked = not permanent_failure and bool(
                            runtime.get("cooling_down")
                            or runtime.get("quota_exhausted")
                            or int(runtime.get("inflight") or 0) >= max_inflight
                            or str(runtime.get("health_status") or "unknown")
                            not in {"healthy", "unknown"}
                        )
                        if not permanent_failure and not sticky_temporarily_blocked:
                            lease_id = await self.store.try_acquire_lease(
                                account.id,
                                session_key,
                                max_inflight,
                                self._owner_id,
                                config.get_int("account.lease.stale_seconds", 90),
                            )
                            if lease_id:
                                ttl = config.get_int("account.selection.sticky_ttl_seconds", 3600)
                                if conversation is None:
                                    conversation = await self.store.bind_conversation(
                                        session_key,
                                        account.id,
                                        model,
                                        ttl,
                                        egress_proxy=egress_proxy,
                                        client_identity=resolution.client_identity,
                                        identity_source=resolution.source,
                                    )
                                else:
                                    await self.store.touch_conversation(session_key, ttl)
                                    if not conversation.get("egress_proxy") and egress_proxy:
                                        await self.store.update_conversation(
                                            session_key,
                                            egress_proxy=egress_proxy,
                                            ttl_seconds=ttl,
                                        )
                                    conversation = await self.store.get_conversation(session_key) or conversation
                                root_alias = self._root_alias(request_data)
                                if root_alias and resolution.source == "new":
                                    await self.store.register_task_alias(
                                        resolution.client_identity,
                                        root_alias,
                                        session_key,
                                        "retry",
                                        config.get_int("account.identity.retry_ttl_seconds", 30),
                                    )
                                if resolution.source == "tool":
                                    await self.store.update_conversation(session_key, state="resuming")
                                    conversation = await self.store.get_conversation(session_key) or conversation
                                await self.store.increment_metric(
                                    "sticky_hits" if sticky_existing else "new_sessions"
                                )
                                queue_wait_ms = round((time.monotonic() - queued_at) * 1000) if queued_at else 0
                                if queue_wait_ms:
                                    await self.store.increment_metric("queue_wait_ms", queue_wait_ms)
                                request_data["_queue_wait_ms"] = queue_wait_ms
                                request_data["_selected_account_hash"] = hashlib.sha256(
                                    account.id.encode()
                                ).hexdigest()[:12]
                                return self._start_heartbeat(AccountLease(
                                    account,
                                    raw,
                                    session_key,
                                    conversation,
                                    sticky_hit=sticky_existing,
                                    egress_proxy=str(conversation.get("egress_proxy") or egress_proxy),
                                    lease_id=lease_id,
                                    identity_source=resolution.source,
                                    queue_wait_ms=queue_wait_ms,
                                ))

                candidates = [] if sticky_temporarily_blocked else await self._eligible(
                    repo,
                    model,
                    excluded | ({sticky_account_id} if sticky_account_id else set()),
                )
                for account, raw, _ in self._ranked_choice(candidates, session_key):
                    _, _, max_inflight = self._account_limits(raw)
                    lease_id = await self.store.try_acquire_lease(
                        account.id,
                        session_key,
                        max_inflight,
                        self._owner_id,
                        config.get_int("account.lease.stale_seconds", 90),
                    )
                    if not lease_id:
                        continue
                    recovered = conversation is not None
                    ttl = config.get_int("account.selection.sticky_ttl_seconds", 3600)
                    conversation = await self.store.bind_conversation(
                        session_key,
                        account.id,
                        model,
                        ttl,
                        state="recovering" if recovered else "active",
                        egress_proxy=str((conversation or {}).get("egress_proxy") or egress_proxy),
                        replace_account_id=str(conversation["account_id"]) if recovered else None,
                        client_identity=resolution.client_identity,
                        identity_source=resolution.source,
                    )
                    if str(conversation["account_id"]) != account.id:
                        await self.store.release_slot(
                            account.id,
                            success=None,
                            error="conversation claim lost",
                            lease_id=lease_id,
                        )
                        continue
                    root_alias = self._root_alias(request_data)
                    if root_alias and resolution.source == "new":
                        await self.store.register_task_alias(
                            resolution.client_identity,
                            root_alias,
                            session_key,
                            "retry",
                            config.get_int("account.identity.retry_ttl_seconds", 30),
                        )
                    if recovered:
                        await self.store.update_conversation(
                            session_key,
                            chat_session_id="",
                            parent_message_id=None,
                            state="recovering",
                            increment_generation=True,
                        )
                        conversation = await self.store.get_conversation(session_key) or conversation
                        await self.store.increment_metric("recoveries")
                        await self.store.increment_metric("cache_resets")
                    else:
                        await self.store.increment_metric("new_sessions")
                        logger.info(
                            "new conversation routed: session={} source={} account={} model={}",
                            session_key[:16],
                            resolution.source,
                            account.id,
                            model,
                        )
                    queue_wait_ms = round((time.monotonic() - queued_at) * 1000) if queued_at else 0
                    if queue_wait_ms:
                        await self.store.increment_metric("queue_wait_ms", queue_wait_ms)
                    request_data["_queue_wait_ms"] = queue_wait_ms
                    request_data["_selected_account_hash"] = hashlib.sha256(
                        account.id.encode()
                    ).hexdigest()[:12]
                    return self._start_heartbeat(AccountLease(
                        account,
                        raw,
                        session_key,
                        conversation,
                        sticky_hit=False,
                        recovered=recovered,
                        egress_proxy=str(conversation.get("egress_proxy") or egress_proxy),
                        lease_id=lease_id,
                        identity_source=resolution.source,
                        queue_wait_ms=queue_wait_ms,
                    ))

            if wait_timeout <= 0:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                await self.store.increment_metric("queue_timeouts")
                raise AccountError(
                    "Timed out waiting for an account",
                    429,
                    "queue_timeout",
                    {"retry_after": 5},
                )
            if queued_at is not None:
                # This ticket was woken for a capacity change but its sticky
                # account may still be busy. Let another compatible waiter try.
                await self._wake_next()
            if queued_at is None:
                queued_at = time.monotonic()
                await self.store.increment_metric("queue_enqueued")
            ticket = await self._enqueue(
                resolution.client_identity,
                resolution.existing or resolution.source in {"tool", "lineage", "retry"},
            )
            try:
                await asyncio.wait_for(ticket.event.wait(), timeout=min(remaining, 5.0))
            except TimeoutError as exc:
                await self._remove_ticket(ticket)
                if deadline - time.monotonic() > 0:
                    continue
                await self.store.increment_metric("queue_timeouts")
                raise AccountError(
                    "Timed out waiting for an account",
                    429,
                    "queue_timeout",
                    {"retry_after": 5},
                ) from exc
            except asyncio.CancelledError:
                await self._remove_ticket(ticket)
                await self.store.increment_metric("queue_cancelled")
                raise

    async def release(
        self,
        lease: AccountLease,
        *,
        success: bool | None,
        error: str = "",
        error_kind: str = "",
        usage_tokens: int = 0,
    ) -> None:
        if lease.heartbeat_task and lease.heartbeat_task is not asyncio.current_task():
            lease.heartbeat_task.cancel()
            await asyncio.gather(lease.heartbeat_task, return_exceptions=True)
            lease.heartbeat_task = None
        neutral = {
            "client_cancelled",
            "invalid_pow",
            "client_error",
            "upstream_busy",
            "context_length",
            "incomplete_stream",
            "account_busy",
            "session_state",
            "pow_infrastructure",
        }
        if error_kind in neutral:
            success = None
        cooldown = config.get_int("account.selection.cooldown_seconds", 300) if success is False else 0
        threshold = config.get_int("account.selection.failure_threshold", 3)
        # Account-level / transport failures cool down immediately (threshold 1)
        # with a kind-specific duration, so a muted/limited/stalled account stops
        # absorbing traffic instead of being retried every cooldown window.
        if error_kind == "account_muted":
            threshold = 1
            cooldown = config.get_int("account.selection.muted_cooldown_seconds", 10800)
        elif error_kind == "invalid_token":
            threshold = 1
            cooldown = config.get_int("account.selection.invalid_cooldown_seconds", 3600)
        elif error_kind == "rate_limit" or "rate limit" in error.lower():
            threshold = 1
            cooldown = config.get_int("account.selection.rate_limit_cooldown_seconds", cooldown)
        elif error_kind == "stream_idle":
            threshold = 1
            cooldown = config.get_int("account.selection.stall_cooldown_seconds", 30)
        await self.store.release_slot(
            lease.account.id,
            success=success,
            error=error,
            cooldown_seconds=cooldown,
            failure_threshold=threshold,
            lease_id=lease.lease_id,
            latency_ms=round((time.monotonic() - lease.acquired_at) * 1000),
        )
        if success is True and usage_tokens > 0:
            await self.store.record_quota_usage(lease.account.id, usage_tokens)
        await self._wake_next()

    async def register_response_lineage(
        self,
        request_data: dict[str, Any],
        assistant_message: dict[str, Any],
    ) -> None:
        session_key, _ = self.session_key(request_data)
        conversation = await self.store.get_conversation(session_key)
        if not conversation:
            return
        messages = [item for item in request_data.get("messages") or [] if isinstance(item, dict)]
        previous = next(
            (item for item in reversed(messages) if item.get("role") != "system"),
            {},
        )
        alias_hash = self._digest({
            "previous": self._canonical_message(previous),
            "assistant": self._canonical_message({"role": "assistant", **assistant_message}),
        })
        await self.store.register_task_alias(
            str(request_data.get("_client_identity") or "anonymous"),
            alias_hash,
            session_key,
            "lineage",
            config.get_int("account.selection.sticky_ttl_seconds", 3600),
        )
        await self.store.increment_metric("lineage_aliases")

    async def register_tool_calls(
        self,
        request_data: dict[str, Any],
        calls: list[dict[str, Any]],
    ) -> None:
        if not calls:
            return
        session_key, _ = self.session_key(request_data)
        source_call_id = self._tool_result_call_id(request_data)
        if source_call_id:
            continuation = await self.store.get_tool_continuation(source_call_id)
            if continuation:
                session_key = str(continuation["session_key"])
        conversation = await self.store.get_conversation(session_key)
        if not conversation:
            raise RuntimeError("tool call has no active orchestrated conversation")
        await self.store.update_conversation(session_key, state="waiting_tool")
        await self.register_response_lineage(
            request_data,
            {"content": None, "tool_calls": calls},
        )

        ttl = config.get_int("account.selection.sticky_ttl_seconds", 3600)
        for call in calls:
            call_id = str(call.get("id") or "")
            if not call_id:
                continue
            aliases = {call_id}
            if call_id.startswith("call_"):
                aliases.add("toolu_" + call_id[5:])
            elif call_id.startswith("toolu_"):
                aliases.add("call_" + call_id[6:])
            for alias in aliases:
                await self.store.register_tool_continuation(
                    alias,
                    session_key,
                    str(conversation["account_id"]),
                    str(conversation.get("chat_session_id") or ""),
                    conversation.get("parent_message_id"),
                    ttl,
                )

    async def pool_overview(self, repo: Any) -> dict[str, Any]:
        rows = await repo.list_all()
        await self.sync_account_policies(rows)
        runtime = await self.store.runtime_snapshot([str(row.get("id")) for row in rows])
        accounts: list[dict[str, Any]] = []
        for raw in rows:
            account = LiangzidpAccount.from_dict(raw)
            priority, weight, max_inflight = self._account_limits(raw)
            state = runtime.get(account.id, {"inflight": 0, "active_sessions": 0})
            accounts.append({
                "id": account.id,
                "email": account.email,
                "status": account.status.value,
                "tier": account.tier.value,
                "eligible": (
                    account.is_available()
                    and not bool(state.get("cooling_down"))
                    and not bool(state.get("quota_exhausted"))
                ),
                "priority": priority,
                "weight": weight,
                "max_inflight": max_inflight,
                **state,
            })
        conversations = await self.store.list_conversations(1000)
        metrics = await self.store.metrics()
        routed = sum(metrics.get(name, 0) for name in ("new_sessions", "sticky_hits", "recoveries"))
        metrics["session_reuse_rate"] = round(
            metrics.get("sticky_hits", 0) / routed,
            4,
        ) if routed else 0.0
        queued = await self.queue_depth()
        metrics["average_queue_wait_ms"] = round(
            metrics.get("queue_wait_ms", 0) / max(1, metrics.get("queue_enqueued", 0)),
        )
        return {
            "settings": {
                "strategy": config.get_str("account.selection.strategy", "least_loaded_weighted"),
                "max_inflight": config.get_int("account.selection.max_inflight", 1),
                "sticky_ttl_seconds": config.get_int("account.selection.sticky_ttl_seconds", 3600),
                "cooldown_seconds": config.get_int("account.selection.cooldown_seconds", 300),
                "failure_threshold": config.get_int("account.selection.failure_threshold", 3),
                "queue_max_size": config.get_int("account.queue.max_size", 200),
                "queue_stream_timeout_seconds": config.get_int(
                    "account.queue.stream_timeout_seconds",
                    300,
                ),
                "queue_nonstream_timeout_seconds": config.get_int(
                    "account.queue.nonstream_timeout_seconds",
                    30,
                ),
                "lease_heartbeat_seconds": config.get_int(
                    "account.lease.heartbeat_seconds",
                    15,
                ),
                "lease_stale_seconds": config.get_int("account.lease.stale_seconds", 90),
            },
            "summary": {
                "accounts_total": len(accounts),
                "accounts_eligible": sum(1 for item in accounts if item["eligible"]),
                "accounts_cooling_down": sum(1 for item in accounts if item.get("cooling_down")),
                "accounts_quota_exhausted": sum(
                    1 for item in accounts if item.get("quota_exhausted")
                ),
                "inflight": sum(int(item.get("inflight") or 0) for item in accounts),
                "queued": queued,
                "active_sessions": sum(
                    int(item.get("active_sessions") or 0) for item in accounts
                ),
                "sticky_sessions": len(conversations),
                "session_reuse_rate": metrics["session_reuse_rate"],
            },
            "metrics": metrics,
            "accounts": accounts,
        }
