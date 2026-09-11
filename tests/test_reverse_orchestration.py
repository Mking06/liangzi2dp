from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from app.control.account.backends.local import LocalAccountRepository
from app.control.model import list_models
from app.control.orchestration import OrchestrationStore, SessionOrchestrator
from app.dataplane.proxy import reverse as reverse_module
from app.dataplane.proxy.liangzidp_client import LiangzidpAPIError
from app.dataplane.proxy.reverse import ReverseProxy
from app.platform.errors import AccountError, ProxyError


def account(account_id: str, priority: int) -> dict[str, Any]:
    return {
        "id": account_id,
        "email": f"{account_id}@example.test",
        "session_token": f"token-{account_id}",
        "status": "active",
        "tier": "pro",
        "metadata": {"priority": priority, "weight": 100, "max_inflight": 2},
    }


class FakeLiangzidpClient:
    calls: list[dict[str, Any]] = []
    next_message_id = 100

    def __init__(self, session_token: str = "", **_: Any):
        self.session_token = session_token

    async def create_chat_session(self) -> str:
        return f"session-{self.session_token}"

    async def chat_completion(self, **payload: Any) -> dict[str, Any]:
        type(self).next_message_id += 1
        message_id = type(self).next_message_id
        type(self).calls.append({"token": self.session_token, **payload})
        return {
            "id": f"chatcmpl-{message_id}",
            "object": "chat.completion",
            "model": payload.get("response_model"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 1},
            "liangzidp": {
                "chat_session_id": payload["chat_session_id"],
                "response_message_id": message_id,
                "status": "FINISHED",
            },
        }


@pytest.fixture
async def orchestration(tmp_path):
    repo = LocalAccountRepository(str(tmp_path / "accounts.db"))
    store = OrchestrationStore(tmp_path / "orchestration.db")
    await repo.initialize()
    await store.initialize()
    await repo.upsert(account("primary", 1))
    await repo.upsert(account("secondary", 2))
    try:
        yield repo, store, SessionOrchestrator(store)
    finally:
        await store.close()
        await repo.close()


@pytest.mark.asyncio
async def test_real_proxy_path_sticks_account_session_parent_and_proxy(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    FakeLiangzidpClient.calls = []
    FakeLiangzidpClient.next_message_id = 100
    monkeypatch.setattr(reverse_module, "LiangzidpClient", FakeLiangzidpClient)
    proxy = ReverseProxy()
    first_request = {
        "_client_identity": "api-key:test",
        "conversation_id": "conversation-1",
        "model": "deepseek-v4-flash",
        "stream": False,
        "messages": [{"role": "user", "content": "inspect project"}],
    }
    await proxy.chat_completion(
        first_request,
        repository=repo,
        orchestrator=orchestrator,
        proxy="http://proxy-one",
    )
    second_request = {
        **first_request,
        "messages": [
            *first_request["messages"],
            {"role": "assistant", "content": "working"},
            {"role": "user", "content": "continue now"},
        ],
    }
    await proxy.chat_completion(
        second_request,
        repository=repo,
        orchestrator=orchestrator,
        proxy="http://proxy-two",
    )

    assert [item["token"] for item in FakeLiangzidpClient.calls] == ["token-primary", "token-primary"]
    assert FakeLiangzidpClient.calls[1]["chat_session_id"] == "session-token-primary"
    assert FakeLiangzidpClient.calls[1]["parent_message_id"] == 101
    assert "continue now" in FakeLiangzidpClient.calls[1]["prompt"]
    assert "inspect project" not in FakeLiangzidpClient.calls[1]["prompt"]
    session_key, _ = orchestrator.session_key(first_request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["account_id"] == "primary"
    assert conversation["egress_proxy"] == "http://proxy-one"
    assert conversation["parent_message_id"] == 102
    runtime = await store.runtime_snapshot(["primary"])
    assert runtime["primary"]["inflight"] == 0
    assert runtime["primary"]["success_count"] == 2


@pytest.mark.asyncio
async def test_incomplete_conversation_starts_a_fresh_upstream_session(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    FakeLiangzidpClient.calls = []
    FakeLiangzidpClient.next_message_id = 300
    monkeypatch.setattr(reverse_module, "LiangzidpClient", FakeLiangzidpClient)
    request = {
        "_client_identity": "api-key:incomplete",
        "conversation_id": "incomplete-tool-call",
        "model": "deepseek-v4-flash",
        "stream": False,
        "messages": [{"role": "user", "content": "scan the target"}],
    }
    first = await ReverseProxy().chat_completion(
        request,
        repository=repo,
        orchestrator=orchestrator,
    )
    assert first["liangzidp"]["status"] == "FINISHED"
    session_key, _ = orchestrator.session_key(request)
    await store.update_conversation(
        session_key,
        chat_session_id="stale-upstream-session",
        parent_message_id=999,
        state="incomplete",
        last_error="invalid tool arguments",
    )

    recovery_request = {
        **request,
        "messages": [
            *request["messages"],
            {"role": "assistant", "content": "FAKE_SCAN_REPORT_SHOULD_NOT_SURVIVE"},
            {"role": "user", "content": "continue the real scan"},
        ],
    }
    await ReverseProxy().chat_completion(
        recovery_request,
        repository=repo,
        orchestrator=orchestrator,
    )
    assert FakeLiangzidpClient.calls[-1]["chat_session_id"] == "session-token-primary"
    assert FakeLiangzidpClient.calls[-1]["parent_message_id"] is None
    assert "continue the real scan" in FakeLiangzidpClient.calls[-1]["prompt"]
    assert "FAKE_SCAN_REPORT_SHOULD_NOT_SURVIVE" not in FakeLiangzidpClient.calls[-1]["prompt"]


class FakeStreamingClient(FakeLiangzidpClient):
    async def chat_completion(self, **payload: Any):
        type(self).calls.append({"token": self.session_token, **payload})

        async def stream():
            base = {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "model": payload.get("response_model"),
            }
            yield "data: " + json.dumps({
                **base,
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
                "liangzidp": {"chat_session_id": payload["chat_session_id"]},
            }) + "\n\n"
            yield "data: " + json.dumps({
                **base,
                "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": None}],
                "liangzidp": {"response_message_id": 777},
            }) + "\n\n"
            yield "data: " + json.dumps({
                **base,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                "liangzidp": {"response_message_id": 777, "status": "FINISHED"},
            }) + "\n\n"
            yield "data: [DONE]\n\n"

        return stream()


class SessionErrorStreamingClient(FakeLiangzidpClient):
    async def chat_completion(self, **payload: Any):
        async def stream():
            yield "data: " + json.dumps({
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
                "liangzidp": {"chat_session_id": payload["chat_session_id"]},
            }) + "\n\n"
            raise LiangzidpAPIError(
                "Chat rejected: biz_code=1 msg=invalid chat session id",
                409,
                "session_state_invalid",
            )

        return stream()


class SessionErrorOnceClient(FakeLiangzidpClient):
    calls: list[dict[str, Any]] = []
    create_count = 0

    async def create_chat_session(self) -> str:
        type(self).create_count += 1
        return f"rebuilt-session-{type(self).create_count}"

    async def chat_completion(self, **payload: Any) -> dict[str, Any]:
        if not getattr(self, "failed_once", False):
            self.failed_once = True
            type(self).calls.append({"token": self.session_token, **payload})
            raise LiangzidpAPIError(
                "Chat rejected: biz_code=26 msg=invalid message id",
                409,
                "session_state_invalid",
            )
        return await super().chat_completion(**payload)


def test_only_flash_and_pro_models_are_exposed():
    assert [item["id"] for item in list_models()] == [
        "deepseek-v4-flash",
        "deepseek-v4-pro",
    ]


def test_unknown_model_is_rejected_instead_of_falling_back_to_flash():
    with pytest.raises(ProxyError) as raised:
        ReverseProxy()._convert_request({
            "model": "liangzidp-chat",
            "messages": [{"role": "user", "content": "hello"}],
        })
    assert raised.value.status == 400
    assert raised.value.code == "model_not_found"


def test_pro_prompt_is_limited_below_web_character_ceiling(monkeypatch):
    monkeypatch.setenv("LIANGZIDP_PRO_INPUT_CHAR_LIMIT", "120000")
    payload = ReverseProxy()._convert_request({
        "model": "deepseek-v4-pro",
        "messages": [
            {"role": "system", "content": "S" * 150000},
            {"role": "user", "content": "U" * 100000 + "FINAL_TASK"},
        ],
    })
    assert len(payload["prompt"]) <= 120000
    assert "context truncated" in payload["prompt"]
    assert payload["prompt"].endswith("FINAL_TASK")


@pytest.mark.asyncio
async def test_nonstream_session_error_rebuilds_once_without_penalizing_account(
    monkeypatch,
    orchestration,
):
    repo, store, orchestrator = orchestration
    SessionErrorOnceClient.calls = []
    SessionErrorOnceClient.create_count = 0
    SessionErrorOnceClient.next_message_id = 500
    monkeypatch.setattr(reverse_module, "LiangzidpClient", SessionErrorOnceClient)
    request = {
        "_client_identity": "api-key:session-rebuild",
        "conversation_id": "session-rebuild",
        "model": "deepseek-v4-pro",
        "stream": False,
        "messages": [{"role": "user", "content": "continue safely"}],
    }

    result = await ReverseProxy().chat_completion(
        request,
        repository=repo,
        orchestrator=orchestrator,
    )
    assert result["choices"][0]["message"]["content"] == "ok"
    assert len(SessionErrorOnceClient.calls) == 2
    assert SessionErrorOnceClient.calls[1]["chat_session_id"] == "rebuilt-session-2"
    assert SessionErrorOnceClient.calls[1]["parent_message_id"] is None
    runtime = (await store.runtime_snapshot(["primary"]))["primary"]
    assert runtime["failure_count"] == 0
    assert runtime["consecutive_failures"] == 0
    assert runtime["cooling_down"] is False
    session_key, _ = orchestrator.session_key(request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["generation"] == 2
    assert conversation["account_id"] == "primary"


@pytest.mark.asyncio
async def test_stream_consumption_commits_parent_and_releases_slot(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    FakeStreamingClient.calls = []
    monkeypatch.setattr(reverse_module, "LiangzidpClient", FakeStreamingClient)
    request = {
        "_client_identity": "api-key:stream",
        "conversation_id": "stream-1",
        "model": "deepseek-v4-flash",
        "stream": True,
        "messages": [{"role": "user", "content": "stream"}],
    }
    result = await ReverseProxy().chat_completion(
        request,
        repository=repo,
        orchestrator=orchestrator,
    )
    output = [part async for part in result]
    assert output[-1] == "data: [DONE]\n\n"
    session_key, _ = orchestrator.session_key(request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["parent_message_id"] == 777
    assert conversation["state"] == "active"
    runtime = await store.runtime_snapshot(["primary"])
    assert runtime["primary"]["inflight"] == 0


@pytest.mark.asyncio
async def test_stream_session_error_does_not_penalize_account(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    monkeypatch.setattr(reverse_module, "LiangzidpClient", SessionErrorStreamingClient)
    request = {
        "_client_identity": "api-key:stream-session-error",
        "conversation_id": "stream-session-error",
        "model": "deepseek-v4-pro",
        "stream": True,
        "messages": [{"role": "user", "content": "continue"}],
    }
    result = await ReverseProxy().chat_completion(
        request,
        repository=repo,
        orchestrator=orchestrator,
    )
    with pytest.raises(LiangzidpAPIError, match="invalid chat session id"):
        _ = [part async for part in result]

    runtime = await store.runtime_snapshot(["primary"])
    assert runtime["primary"]["inflight"] == 0
    assert runtime["primary"]["failure_count"] == 0
    assert runtime["primary"]["consecutive_failures"] == 0
    assert runtime["primary"]["cooling_down"] is False
    session_key, _ = orchestrator.session_key(request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["state"] == "incomplete"


@pytest.mark.asyncio
async def test_tool_call_alias_resolves_same_conversation(orchestration):
    repo, store, orchestrator = orchestration
    request = {
        "_client_identity": "api-key:tools",
        "conversation_id": "tool-loop",
        "model": "deepseek-v4-flash",
        "messages": [{"role": "user", "content": "inspect"}],
    }
    lease = await orchestrator.acquire(repo, request)
    assert lease
    await store.update_conversation(
        lease.session_key,
        chat_session_id="upstream-1",
        parent_message_id=44,
    )
    await orchestrator.release(lease, success=True)
    await orchestrator.register_tool_calls(request, [{"id": "call_abc"}])

    continuation = await orchestrator.acquire(repo, {
        "_client_identity": "api-key:tools",
        "model": "deepseek-v4-flash",
        "messages": [{"role": "tool", "tool_call_id": "toolu_abc", "content": "done"}],
    })
    assert continuation
    assert continuation.session_key == lease.session_key
    assert continuation.account.id == lease.account.id
    assert continuation.conversation["chat_session_id"] == "upstream-1"
    await orchestrator.release(continuation, success=True)


class FailingPrimaryClient(FakeLiangzidpClient):
    async def chat_completion(self, **payload: Any) -> dict[str, Any]:
        type(self).calls.append({"token": self.session_token, **payload})
        if self.session_token == "token-primary":
            raise LiangzidpAPIError("INVALID_TOKEN", 401, "invalid_api_key")
        return await super().chat_completion(**payload)


class BusyThenSuccessClient(FakeLiangzidpClient):
    attempts = 0
    failures_before_success = 2
    payloads: list[dict[str, Any]] = []

    async def chat_completion(self, **payload: Any) -> dict[str, Any]:
        type(self).attempts += 1
        type(self).payloads.append({"token": self.session_token, **payload})
        if type(self).attempts <= type(self).failures_before_success:
            raise LiangzidpAPIError(
                "server busy",
                503,
                "upstream_busy",
            )
        type(self).next_message_id += 1
        message_id = type(self).next_message_id
        return {
            "id": f"chatcmpl-{message_id}",
            "object": "chat.completion",
            "model": payload.get("response_model"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "recovered"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 1},
            "liangzidp": {
                "chat_session_id": payload["chat_session_id"],
                "response_message_id": message_id,
                "status": "FINISHED",
            },
        }


@pytest.mark.asyncio
async def test_upstream_busy_retries_same_account_and_session(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    BusyThenSuccessClient.attempts = 0
    BusyThenSuccessClient.failures_before_success = 2
    BusyThenSuccessClient.payloads = []
    monkeypatch.setattr(reverse_module, "LiangzidpClient", BusyThenSuccessClient)
    monkeypatch.setenv("LIANGZIDP_UPSTREAM_BUSY_ATTEMPTS", "3")

    async def no_wait(_seconds: float):
        return None

    monkeypatch.setattr(reverse_module.asyncio, "sleep", no_wait)
    request = {
        "_client_identity": "api-key:busy-recovery",
        "conversation_id": "busy-recovery",
        "model": "deepseek-v4-pro",
        "stream": False,
        "messages": [{"role": "user", "content": "continue the task"}],
    }
    result = await ReverseProxy().chat_completion(
        request,
        repository=repo,
        orchestrator=orchestrator,
    )

    assert result["choices"][0]["message"]["content"] == "recovered"
    assert BusyThenSuccessClient.attempts == 3
    assert len({item["token"] for item in BusyThenSuccessClient.payloads}) == 1
    assert len({item["chat_session_id"] for item in BusyThenSuccessClient.payloads}) == 1
    assert len({item["parent_message_id"] for item in BusyThenSuccessClient.payloads}) == 1
    metrics = await store.metrics()
    assert metrics["upstream_busy_retries"] == 2
    assert metrics["upstream_busy_recovered"] == 1


@pytest.mark.asyncio
async def test_upstream_busy_exhaustion_is_neutral_and_keeps_session_active(
    monkeypatch,
    orchestration,
):
    repo, store, orchestrator = orchestration
    BusyThenSuccessClient.attempts = 0
    BusyThenSuccessClient.failures_before_success = 10
    BusyThenSuccessClient.payloads = []
    monkeypatch.setattr(reverse_module, "LiangzidpClient", BusyThenSuccessClient)
    monkeypatch.setenv("LIANGZIDP_UPSTREAM_BUSY_ATTEMPTS", "3")

    async def no_wait(_seconds: float):
        return None

    monkeypatch.setattr(reverse_module.asyncio, "sleep", no_wait)
    request = {
        "_client_identity": "api-key:busy-exhausted",
        "conversation_id": "busy-exhausted",
        "model": "deepseek-v4-pro",
        "stream": False,
        "messages": [{"role": "user", "content": "continue the task"}],
    }
    with pytest.raises(LiangzidpAPIError) as caught:
        await ReverseProxy().chat_completion(
            request,
            repository=repo,
            orchestrator=orchestrator,
        )
    assert caught.value.code == "upstream_busy"
    assert BusyThenSuccessClient.attempts == 3
    account_id = BusyThenSuccessClient.payloads[0]["token"].removeprefix("token-")
    runtime = await store.runtime_snapshot([account_id])
    assert runtime[account_id]["failure_count"] == 0
    assert runtime[account_id]["cooling_down"] is False
    session_key, _ = orchestrator.session_key(request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["state"] == "active"
    metrics = await store.metrics()
    assert metrics["upstream_busy_retries"] == 2
    assert metrics["upstream_busy_exhausted"] == 1


@pytest.mark.asyncio
async def test_cancellation_during_busy_backoff_releases_lease(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    BusyThenSuccessClient.attempts = 0
    BusyThenSuccessClient.failures_before_success = 10
    BusyThenSuccessClient.payloads = []
    monkeypatch.setattr(reverse_module, "LiangzidpClient", BusyThenSuccessClient)
    monkeypatch.setenv("LIANGZIDP_UPSTREAM_BUSY_ATTEMPTS", "3")

    async def cancel_wait(_seconds: float):
        raise asyncio.CancelledError

    monkeypatch.setattr(reverse_module.asyncio, "sleep", cancel_wait)
    request = {
        "_client_identity": "api-key:busy-cancel",
        "conversation_id": "busy-cancel",
        "model": "deepseek-v4-pro",
        "stream": False,
        "messages": [{"role": "user", "content": "continue the task"}],
    }
    with pytest.raises(asyncio.CancelledError):
        await ReverseProxy().chat_completion(
            request,
            repository=repo,
            orchestrator=orchestrator,
        )
    account_id = BusyThenSuccessClient.payloads[0]["token"].removeprefix("token-")
    runtime = await store.runtime_snapshot([account_id])
    assert runtime[account_id]["inflight"] == 0
    assert runtime[account_id]["failure_count"] == 0


@pytest.mark.asyncio
async def test_invalid_account_fails_over_with_new_upstream_session(monkeypatch, orchestration):
    repo, store, orchestrator = orchestration
    FailingPrimaryClient.calls = []
    FailingPrimaryClient.next_message_id = 900
    monkeypatch.setattr(reverse_module, "LiangzidpClient", FailingPrimaryClient)
    request = {
        "_client_identity": "api-key:recovery",
        "conversation_id": "recover-1",
        "model": "deepseek-v4-flash",
        "stream": False,
        "messages": [{"role": "user", "content": "recover this task"}],
    }
    with pytest.raises(AccountError):
        await ReverseProxy().chat_completion(request, repository=repo, orchestrator=orchestrator)

    result = await ReverseProxy().chat_completion(request, repository=repo, orchestrator=orchestrator)
    assert result["choices"][0]["message"]["content"] == "ok"
    assert FailingPrimaryClient.calls[-1]["token"] == "token-secondary"
    assert FailingPrimaryClient.calls[-1]["chat_session_id"] == "session-token-secondary"
    assert FailingPrimaryClient.calls[-1]["parent_message_id"] is None
    assert "recover this task" in FailingPrimaryClient.calls[-1]["prompt"]
    session_key, _ = orchestrator.session_key(request)
    conversation = await store.get_conversation(session_key)
    assert conversation and conversation["account_id"] == "secondary"
    assert conversation["generation"] == 2
    assert (await repo.get("primary"))["status"] == "expired"
