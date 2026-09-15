"""Reverse proxy 鈥?core request forwarding logic."""



from __future__ import annotations



import asyncio

import json

import os

import random

import re

from typing import Any, AsyncGenerator



from app.control.model import get_model_info

from app.dataplane.proxy.liangzidp_client import LiangzidpAPIError, LiangzidpClient

from app.dataplane.proxy.context_policy import fit_rendered_parts, govern_tool_result

from app.dataplane.proxy.tool_calls import (

    assistant_tool_history,

    is_continuation_only,

    tool_prompt_for_request,

    tool_prompt_guard_for_request,

)

from app.dataplane.proxy.turn_integrity import TurnIntegrityGuard, TurnState

from app.platform.errors import AccountError, ProxyError

from app.platform.logging.logger import logger





LIANGZIDP_ERROR_KINDS = {
    "invalid_api_key": "invalid_token",
    "rate_limit_exceeded": "rate_limit",
    "invalid_pow": "invalid_pow",
    "incomplete_stream": "incomplete_stream",
    "upstream_busy": "upstream_busy",
    "context_length_exceeded": "context_length",
    "parallel_chat_limit": "account_busy",
    "session_state_invalid": "session_state",
    "pow_service_unavailable": "pow_infrastructure",
    "pow_solver_error": "pow_infrastructure",
    "pow_solver_no_answer": "pow_infrastructure",
    "account_muted": "account_muted",
    "stream_idle_timeout": "stream_idle",
}

# Kinds that mean "this account cannot serve this request right now": cool it
# down and, before any downstream byte is produced, try another account. The
# outer failover loop in chat_completion bounds how many accounts are tried.
ACCOUNT_FAILOVER_KINDS = {
    "account_muted",
    "rate_limit",
    "invalid_token",
    "account_busy",
    "stream_idle",
    "incomplete_stream",
}

# Generic upstream HTTP statuses that also justify switching account.
FAILOVER_UPSTREAM_STATUSES = {500, 502, 503, 504}


class _SwitchAccount(Exception):
    """Internal signal: release/cooldown done; outer loop should switch account."""

    def __init__(self, account_id: str, kind: str, exc: Exception):
        self.account_id = account_id
        self.kind = kind
        self.exc = exc
        super().__init__(f"switch away from {account_id} after {kind}")





class ReverseProxy:

    """Forwards API requests to Liangzidp Web API."""



    def __init__(self):

        self._client = LiangzidpClient()



    async def _invoke_with_busy_retry(

        self,

        client: LiangzidpClient,

        payload: dict[str, Any],

        orchestrator: Any,

        lease: Any,

        model: Any,

    ) -> dict | AsyncGenerator[str, None]:

        attempts = max(

            1, min(int(os.getenv("LIANGZIDP_UPSTREAM_BUSY_ATTEMPTS", "3")), 5)

        )

        base_delay = max(

            0.1, float(os.getenv("LIANGZIDP_UPSTREAM_BUSY_BACKOFF_SECONDS", "3"))

        )

        for attempt in range(attempts):

            try:

                result = await client.chat_completion(**payload)

                if attempt:

                    await orchestrator.store.increment_metric("upstream_busy_recovered")

                return result

            except LiangzidpAPIError as exc:

                retryable = exc.code == "upstream_busy" and not payload.get("stream")

                if not retryable or attempt >= attempts - 1:

                    if retryable:

                        await orchestrator.store.increment_metric(

                            "upstream_busy_exhausted"

                        )

                    raise

                await orchestrator.store.increment_metric("upstream_busy_retries")

                delay = base_delay * (2**attempt) * random.uniform(0.85, 1.15)

                logger.warning(

                    "liangzidp upstream busy; retrying same session: session={} account={} model={} "

                    "attempt={}/{} delay_seconds={:.1f}",

                    lease.session_key[:16],

                    lease.account.id,

                    model,

                    attempt + 2,

                    attempts,

                    delay,

                )

                await asyncio.sleep(delay)

        raise RuntimeError("unreachable upstream busy retry state")



    async def chat_completion(
        self,
        request_data: dict[str, Any],
        account_id: str | None = None,
        pow_response: str = "",
        proxy: str | None = None,
        repository: Any | None = None,
        orchestrator: Any | None = None,
    ) -> dict | AsyncGenerator[str, None]:
        """Handle a chat completion request with bounded cross-account failover."""
        from app.control.account.backends.factory import create_repository

        repo = repository or create_repository()
        if repository is None:
            await repo.initialize()
        if orchestrator is None:
            raise AccountError(
                "Session orchestrator unavailable", 503, "orchestrator_unavailable"
            )
        max_attempts = max(
            1,
            min(int(os.getenv("LIANGZIDP_ACCOUNT_FAILOVER_ATTEMPTS", "5")), 6),
        )
        tried: set[str] = set()
        attempt_no = 0
        try:
            while True:
                attempt_no += 1
                try:
                    return await self._chat_attempt(
                        request_data,
                        repo=repo,
                        orchestrator=orchestrator,
                        requested_account_id=account_id,
                        egress_proxy=proxy or "",
                        pow_response=pow_response,
                        excluded=tried,
                        allow_failover=not account_id,
                        attempt_no=attempt_no,
                        max_attempts=max_attempts,
                    )
                except _SwitchAccount as switch:
                    tried.add(switch.account_id)
                    await orchestrator.store.increment_metric("account_failovers")
                    if attempt_no >= max_attempts:
                        logger.warning(
                            "account failover exhausted: tried={} last_kind={}",
                            sorted(tried),
                            switch.kind,
                        )
                        if isinstance(
                            switch.exc, (LiangzidpAPIError, AccountError, ProxyError)
                        ):
                            raise switch.exc
                        raise ProxyError(str(switch.exc)) from switch.exc
                    logger.warning(
                        "failing over to another account: failed={} kind={} attempt={}/{}",
                        switch.account_id,
                        switch.kind,
                        attempt_no,
                        max_attempts,
                    )
                    continue
        finally:
            if repository is None:
                await repo.close()

    async def _chat_attempt(
        self,
        request_data: dict[str, Any],
        *,
        repo: Any,
        orchestrator: Any,
        requested_account_id: str | None,
        egress_proxy: str,
        pow_response: str,
        excluded: set[str],
        allow_failover: bool,
        attempt_no: int,
        max_attempts: int,
    ) -> dict | AsyncGenerator[str, None]:
        lease = await orchestrator.acquire(
            repo,
            request_data,
            requested_account_id=requested_account_id,
            excluded=excluded,
            egress_proxy=egress_proxy,
        )
        if lease is None:
            raise AccountError("No available accounts", 503)
        account_data = lease.raw
        account_id = lease.account.id

        session_token = account_data.get("session_token") or account_data.get(

            "bearer_token", ""

        )

        csrf_token = account_data.get("csrf_token", "")

        if not session_token:

            await orchestrator.release(

                lease,

                success=False,

                error="account has no bearer/session token",

                error_kind="invalid_token",

            )

            raise AccountError("Account not found or expired", 404)



        # Build client with account credentials

        client = LiangzidpClient(

            session_token=session_token,

            csrf_token=csrf_token,

            pow_response=pow_response,

            proxy=lease.egress_proxy or egress_proxy or "",

            timeout=int(os.getenv("LIANGZIDP_CHAT_TIMEOUT", "300")),

        )



        # Convert OpenAI format to Liangzidp format

        ds_payload = self._convert_request(request_data)

        session_key = lease.session_key

        reuse_enabled = os.getenv("LIANGZIDP_ENABLE_SESSION_REUSE", "true").lower() in {

            "1",

            "true",

            "yes",

            "on",

        }

        conversation = lease.conversation

        force_fresh_session = bool(request_data.get("_force_fresh_upstream_session"))

        persisted_session = str(conversation.get("chat_session_id") or "")

        persisted_parent = self._normalize_parent_message_id(

            conversation.get("parent_message_id")

        )

        reset_incomplete_session = str(conversation.get("state") or "") == "incomplete"

        had_upstream_history = bool(

            persisted_session

            and persisted_parent is not None

            and not lease.recovered

            and not reset_incomplete_session

            and not force_fresh_session

        )

        if (

            reuse_enabled

            and persisted_session

            and not reset_incomplete_session

            and not force_fresh_session

        ):

            ds_payload["chat_session_id"] = persisted_session

            ds_payload["parent_message_id"] = persisted_parent

        elif (

            ds_payload.get("chat_session_id")

            and not reset_incomplete_session

            and not force_fresh_session

        ):

            await orchestrator.store.update_conversation(

                session_key,

                chat_session_id=str(ds_payload["chat_session_id"]),

                parent_message_id=ds_payload.get("parent_message_id"),

                state="active",

            )

            had_upstream_history = ds_payload.get("parent_message_id") is not None

        else:

            try:

                ds_payload["chat_session_id"] = await client.create_chat_session()

            except LiangzidpAPIError as exc:

                error_kind = self._error_kind(exc)

                await orchestrator.release(

                    lease,

                    success=False,

                    error=exc.message,

                    error_kind=error_kind,

                )

                if error_kind == "invalid_token":

                    account_data["status"] = "expired"

                    await repo.upsert(account_data)

                if self._should_failover(error_kind, exc, allow_failover):

                    raise _SwitchAccount(account_id, error_kind, exc)

                if error_kind == "invalid_token":

                    raise AccountError(

                        "Session expired", 401, "invalid_api_key"

                    ) from exc

                raise

            except _SwitchAccount:

                raise

            except Exception as exc:

                await orchestrator.release(

                    lease,

                    success=False,

                    error=str(exc),

                    error_kind="upstream_error",

                )

                if allow_failover:

                    raise _SwitchAccount(account_id, "upstream_error", exc)

                raise ProxyError(str(exc)) from exc

            ds_payload["parent_message_id"] = None

            await orchestrator.store.update_conversation(

                session_key,

                chat_session_id=str(ds_payload["chat_session_id"]),

                parent_message_id=None,

                state="recovering" if lease.recovered else "active",

                increment_generation=force_fresh_session,

            )

            if force_fresh_session:

                await orchestrator.store.increment_metric("cache_resets")



        if reset_incomplete_session or force_fresh_session:

            # The client transcript may already contain prose hallucinated

            # after a failed tool call. A fresh upstream Session must not be

            # seeded with those stale assistant messages. Keep instructions,

            # the latest user request, and genuine tool results only.

            ds_payload["prompt"] = self._build_clean_prompt(

                request_data,

                str(ds_payload.get("model_type") or "default"),

            )



        if had_upstream_history and os.getenv(

            "LIANGZIDP_SESSION_REUSE_SEND_DELTA", "true"

        ).lower() in {"1", "true", "yes", "on"}:

            # The upstream Liangzidp session already owns prior turns. Uploading

            # the complete OpenAI transcript again duplicates context and can

            # resurrect stale assistant/tool output.

            prompt_parts = self._messages_to_prompt_parts(

                request_data.get("messages", []),

                mode_override="delta",

            )

            tool_prompt = self._tools_to_prompt(request_data)

            if tool_prompt:

                prompt_parts.insert(0, tool_prompt)

            tool_guard = self._tools_guard(request_data)

            if tool_guard:

                prompt_parts.append(tool_guard)

            ds_payload["prompt"] = self._limit_prompt_chars(

                "\n".join(prompt_parts),

                str(ds_payload.get("model_type") or "default"),

            )



        try:

            try:

                result = await self._invoke_with_busy_retry(

                    client,

                    ds_payload,

                    orchestrator,

                    lease,

                    request_data.get("model"),

                )

            except LiangzidpAPIError as exc:

                recovery_kind = self._error_kind(exc)

                if recovery_kind not in {

                    "session_state",

                    "context_length",

                } or ds_payload.get("stream"):

                    raise

                logger.warning(

                    "rebuilding upstream session after recoverable error: session={} account={} "

                    "kind={} error={}",

                    lease.session_key[:16],

                    account_id,

                    recovery_kind,

                    exc.message,

                )

                if recovery_kind == "context_length":

                    await orchestrator.store.increment_metric(

                        "context_recovery_attempts"

                    )

                ds_payload["chat_session_id"] = await client.create_chat_session()

                ds_payload["parent_message_id"] = None

                ds_payload["prompt"] = self._build_clean_prompt(

                    request_data,

                    str(ds_payload.get("model_type") or "default"),

                    compact=recovery_kind == "context_length",

                )

                await orchestrator.store.update_conversation(

                    lease.session_key,

                    chat_session_id=str(ds_payload["chat_session_id"]),

                    parent_message_id=None,

                    state="recovering",

                    last_error="",

                    increment_generation=True,

                )

                await orchestrator.store.increment_metric("cache_resets")

                try:

                    result = await self._invoke_with_busy_retry(

                        client,

                        ds_payload,

                        orchestrator,

                        lease,

                        request_data.get("model"),

                    )

                except LiangzidpAPIError as retry_error:

                    if (

                        recovery_kind == "context_length"

                        and self._error_kind(retry_error) == "context_length"

                    ):

                        await orchestrator.store.increment_metric(

                            "context_recovery_exhausted"

                        )

                    raise

                else:

                    if recovery_kind == "context_length":

                        await orchestrator.store.increment_metric(

                            "context_recovery_succeeded"

                        )

            if not isinstance(result, dict):

                # The stream generator has not touched the network yet. Prime its

                # first frame so account-level start errors surface here, before

                # any downstream byte, and can trigger cross-account failover;

                # once a real frame is produced we never retry mid-stream.

                try:

                    first_frame = await result.__anext__()

                except StopAsyncIteration:

                    first_frame = None

                except Exception:

                    await result.aclose()

                    raise

                async def _prepended_stream(_gen=result, _first=first_frame):

                    if _first is not None:

                        yield _first

                    async for _part in _gen:

                        yield _part

                result = _prepended_stream()

            if isinstance(result, dict):

                await self._commit_orchestration_result(

                    orchestrator,

                    lease,

                    result.get("liangzidp")

                    if isinstance(result.get("liangzidp"), dict)

                    else {},

                    success=True,

                )

                usage = (

                    result.get("usage") if isinstance(result.get("usage"), dict) else {}

                )

                message = (result.get("choices") or [{}])[0].get("message") or {}

                if isinstance(message, dict):

                    await orchestrator.register_response_lineage(request_data, message)

                await orchestrator.release(

                    lease,

                    success=True,

                    usage_tokens=int(usage.get("total_tokens") or 0),

                )

                return result

            return self._orchestrated_stream(result, orchestrator, lease, request_data)

        except asyncio.CancelledError:

            await orchestrator.store.update_conversation(

                lease.session_key,

                state="incomplete",

                last_error="client cancelled request",

            )

            await orchestrator.release(

                lease,

                success=None,

                error="client cancelled request",

                error_kind="client_cancelled",

            )

            raise

        except LiangzidpAPIError as exc:

            logger.warning(

                "liangzidp upstream error: account={} model={} status={} code={} prompt_chars={} error={}",

                account_id,

                request_data.get("model"),

                exc.status_code,

                exc.code,

                len(str(ds_payload.get("prompt") or "")),

                exc.message,

            )

            error_kind = self._error_kind(exc)

            await orchestrator.release(

                lease, success=False, error=exc.message, error_kind=error_kind

            )

            await orchestrator.store.update_conversation(

                lease.session_key,

                state=(

                    "recovering"

                    if error_kind in {"invalid_token", "rate_limit"}

                    else (

                        "active"

                        if error_kind

                        in {"account_busy", "pow_infrastructure", "upstream_busy"}

                        else "incomplete"

                    )

                ),

                last_error=exc.message,

            )

            if error_kind == "invalid_token":

                account_data["status"] = "expired"

                await repo.upsert(account_data)

            if self._should_failover(error_kind, exc, allow_failover):

                raise _SwitchAccount(account_id, error_kind, exc)

            if exc.status_code == 401 or "INVALID_TOKEN" in exc.message:

                logger.warning("session expired: account={}", account_id)

                raise AccountError("Session expired", 401, "invalid_api_key")

            elif exc.code == "invalid_pow" or "INVALID_POW" in exc.message:

                raise AccountError("POW challenge failed", 403, "invalid_pow")

            raise

        except _SwitchAccount:

            raise

        except Exception as exc:

            await orchestrator.release(

                lease,

                success=False,

                error=str(exc),

                error_kind="upstream_error",

            )

            logger.error("proxy error: error={}", exc)

            if allow_failover:

                raise _SwitchAccount(account_id, "upstream_error", exc)

            raise ProxyError(str(exc))




    async def _commit_orchestration_result(

        self,

        orchestrator: Any,

        lease: Any,

        metadata: dict[str, Any],

        *,

        success: bool | None,

    ) -> None:

        chat_session_id = str(

            metadata.get("chat_session_id")

            or lease.conversation.get("chat_session_id")

            or ""

        )

        parent_message_id = self._normalize_parent_message_id(

            metadata.get("response_message_id") or metadata.get("message_id")

        )

        await orchestrator.store.update_conversation(

            lease.session_key,

            chat_session_id=chat_session_id,

            parent_message_id=(

                parent_message_id

                if parent_message_id is not None

                else lease.conversation.get("parent_message_id")

            ),

            state="active"

            if success is True

            else ("incomplete" if success is False else None),

            last_error=(

                ""

                if success is True

                else (

                    str(metadata.get("error") or "incomplete stream")

                    if success is False

                    else None

                )

            ),

        )



    def _orchestrated_stream(

        self,

        source: AsyncGenerator[str, None],

        orchestrator: Any,

        lease: Any,

        request_data: dict[str, Any],

    ) -> AsyncGenerator[str, None]:

        async def generate() -> AsyncGenerator[str, None]:

            guard = TurnIntegrityGuard()

            latest_metadata: dict[str, Any] = {}

            assistant_message: dict[str, Any] = {

                "content": "",

                "reasoning_content": "",

                "tool_calls": [],

            }

            try:

                async for part in source:

                    for block in str(part).split("\n\n"):

                        data = "\n".join(

                            line[5:].strip()

                            for line in block.splitlines()

                            if line.startswith("data:")

                        )

                        if not data or data == "[DONE]":

                            continue

                        try:

                            event = json.loads(data)

                        except json.JSONDecodeError:

                            continue

                        metadata = event.get("liangzidp")

                        guard.observe(event)

                        choice = (event.get("choices") or [{}])[0]

                        delta = choice.get("delta") if isinstance(choice, dict) else {}

                        if isinstance(delta, dict):

                            if delta.get("content"):

                                assistant_message["content"] += str(delta["content"])

                            if delta.get("reasoning_content"):

                                assistant_message["reasoning_content"] += str(

                                    delta["reasoning_content"]

                                )

                            if delta.get("tool_calls"):

                                assistant_message["tool_calls"].extend(

                                    delta["tool_calls"]

                                )

                        if isinstance(metadata, dict):

                            latest_metadata.update(metadata)

                            if metadata.get("response_message_id") is not None:

                                await self._commit_orchestration_result(

                                    orchestrator,

                                    lease,

                                    latest_metadata,

                                    success=(

                                        True

                                        if guard.state == TurnState.COMPLETED

                                        else (

                                            False

                                            if guard.state == TurnState.INCOMPLETE

                                            else None

                                        )

                                    ),

                                )

                    yield part

            except asyncio.CancelledError:

                guard.cancelled()

                await orchestrator.store.update_conversation(

                    lease.session_key,

                    state="incomplete",

                    last_error="client cancelled stream",

                )

                await orchestrator.release(

                    lease,

                    success=None,

                    error="client cancelled stream",

                    error_kind="client_cancelled",

                )

                raise

            except Exception as exc:

                guard.failed()

                error_kind = self._error_kind(exc)

                await orchestrator.store.update_conversation(

                    lease.session_key,

                    state="incomplete",

                    last_error=str(exc),

                )

                await orchestrator.release(

                    lease,

                    success=False,

                    error=str(exc),

                    error_kind=error_kind,

                )

                raise

            else:

                guard.transport_ended()

                success = guard.successful

                error_kind = str(

                    latest_metadata.get("error_code") or "incomplete_stream"

                )

                if error_kind == "rate_limit_exceeded":

                    error_kind = "rate_limit"

                await self._commit_orchestration_result(

                    orchestrator,

                    lease,

                    latest_metadata,

                    success=success,

                )

                if success:

                    await orchestrator.register_response_lineage(

                        request_data,

                        assistant_message,

                    )

                await orchestrator.release(

                    lease,

                    success=success,

                    error="" if success else f"turn ended in state {guard.state.value}",

                    error_kind=("" if success else error_kind),

                    usage_tokens=int(latest_metadata.get("usage_total") or 0),

                )



        return generate()



    @staticmethod

    def _error_kind(exc: Exception) -> str:

        if isinstance(exc, LiangzidpAPIError):

            return LIANGZIDP_ERROR_KINDS.get(exc.code, "upstream_error")

        return "upstream_error"

    @staticmethod

    def _should_failover(kind: str, exc: Exception, allow_failover: bool) -> bool:

        if not allow_failover:

            return False

        if kind in ACCOUNT_FAILOVER_KINDS:

            return True

        if kind == "upstream_error" and isinstance(exc, LiangzidpAPIError):

            return int(getattr(exc, "status_code", 0) or 0) in FAILOVER_UPSTREAM_STATUSES

        return False



    def _build_clean_prompt(

        self,

        request_data: dict[str, Any],

        model_type: str,

        *,

        compact: bool = False,

    ) -> str:

        """Build a fresh-session prompt, optionally with a recovery budget."""

        prompt_parts = self._messages_to_prompt_parts(

            request_data.get("messages", []),

            mode_override="clean",

        )

        tool_prompt = self._tools_to_prompt(request_data)

        if tool_prompt:

            prompt_parts.insert(0, tool_prompt)

        tool_guard = self._tools_guard(request_data)

        if tool_guard:

            prompt_parts.append(tool_guard)

        if compact:

            limit = max(

                8_192,

                int(os.getenv("LIANGZIDP_CONTEXT_RECOVERY_TOKEN_LIMIT", "65536")),

            )

            prompt_parts = fit_rendered_parts(prompt_parts, limit=limit)

        return self._limit_prompt_chars("\n".join(prompt_parts), model_type)



    def _convert_request(self, openai_request: dict) -> dict:

        """Convert OpenAI chat completion request to Liangzidp format."""

        messages = openai_request.get("messages", [])

        stream = openai_request.get("stream", False)



        # Convert messages to a coherent role-labelled transcript. Removing

        # normal assistant turns leaves every historical user request looking

        # unanswered, which makes the model resume stale tasks unexpectedly.

        prompt_parts = self._messages_to_prompt_parts(messages)



        tool_prompt = self._tools_to_prompt(openai_request)

        tool_mode = bool(tool_prompt)

        if tool_prompt:

            prompt_parts.insert(0, tool_prompt)

        tool_guard = self._tools_guard(openai_request)

        if tool_guard:

            prompt_parts.append(tool_guard)



        prompt = "\n".join(prompt_parts)

        has_url = self._contains_url(prompt)



        # Map model names

        model = openai_request.get("model", "deepseek-v4-flash")

        model_key = str(model or "deepseek-v4-flash").lower()

        if model_key.endswith("[1m]"):

            model_key = model_key[:-4]

        model_info = get_model_info(model_key)

        if model_info is None:

            raise ProxyError(

                f"Unsupported model: {model_key}",

                400,

                "model_not_found",

            )

        model_key = model_info.id

        requested_thinking = openai_request.get("thinking_enabled")

        if requested_thinking is None:

            requested_thinking = openai_request.get("enable_thinking")

        if requested_thinking is None and isinstance(

            openai_request.get("thinking"), dict

        ):

            requested_thinking = openai_request["thinking"].get("type") == "enabled"



        if model_info.internal_id == "expert":
            # Model selection is explicit. URL presence must never silently
            # downgrade a requested Pro turn to Flash.
            model_type = "expert"
            thinking_enabled = bool(requested_thinking)
        elif model_info.internal_id == "vision":
            # Vision mode supports images plus search; pass through as-is.
            model_type = "vision"
            thinking_enabled = bool(requested_thinking)
        else:
            # The current Liangzidp Web bundle uses "default" as the normal

            # chat model_type. Passing null worked in older captures but now

            # makes model/session behavior less deterministic.

            model_type = "default"

            thinking_enabled = (

                bool(requested_thinking) if requested_thinking is not None else False

            )



        prompt = self._limit_prompt_chars(prompt, model_type)



        return {

            "prompt": prompt,

            "model_type": model_type,

            "response_model": model_key,

            "chat_session_id": str(

                openai_request.get("liangzidp_chat_session_id")

                or openai_request.get("chat_session_id")

                or ""

            ),

            "parent_message_id": self._normalize_parent_message_id(

                openai_request.get("parent_message_id")

            ),

            "ref_file_ids": [],

            "thinking_enabled": thinking_enabled,

            "search_enabled": bool(

                openai_request.get("search_enabled", False if tool_mode else has_url)

            ),

            "action": None,

            "preempt": False,

            "stream": stream,

        }



    @staticmethod
    def _limit_prompt_chars(prompt: str, model_type: str) -> str:
        env_name = (
            "LIANGZIDP_PRO_INPUT_CHAR_LIMIT"
            if model_type == "expert"
            else (
                "LIANGZIDP_VISION_INPUT_CHAR_LIMIT"
                if model_type == "vision"
                else "LIANGZIDP_FLASH_INPUT_CHAR_LIMIT"
            )
        )
        default_limit = (
            163_840
            if model_type == "expert"
            else 2_621_440
        )
        limit = max(1_000, int(os.getenv(env_name, str(default_limit))))

        if len(prompt) <= limit:

            return prompt

        marker = f"\n...[context truncated: original_chars={len(prompt)}]...\n"

        available = max(1, limit - len(marker))

        head = min(available // 3, 50_000)

        tail = available - head

        return prompt[:head] + marker + prompt[-tail:]



    def _messages_to_prompt_parts(

        self,

        messages: list[dict[str, Any]],

        mode_override: str | None = None,

    ) -> list[str]:

        mode = (

            (mode_override or os.getenv("LIANGZIDP_HISTORY_MODE", "full"))

            .strip()

            .lower()

        )

        # A complete transcript is the only coherent stateless representation.

        # Keep an opt-out for deployments that intentionally used the old lossy

        # behavior, but make correctness the default.

        include_assistant = os.getenv(

            "LIANGZIDP_INCLUDE_ASSISTANT_HISTORY", "true"

        ).lower() in {"1", "true", "yes", "on"}



        def render(msg: dict[str, Any]) -> str:

            role = str(msg.get("role", "user"))

            content = self._content_to_text(msg.get("content", ""))

            if role == "system":

                return f"System instruction:\n{content}"

            if role == "tool":

                call_id = msg.get("tool_call_id") or ""

                name = msg.get("name") or "tool"

                return f"Tool result (name={name}, call_id={call_id}):\n{govern_tool_result(content)}"

            if role == "assistant":

                history = assistant_tool_history(msg)

                # Tool-call assistant messages and their reasoning are required

                # for the next tool-result request. Normal assistant prose

                # remains opt-in to avoid duplicating a reused Web session.

                if msg.get("tool_calls"):

                    return history

                if not include_assistant:

                    return ""

                return f"Assistant response:\n{content}" if content else ""

            return f"User:\n{content}" if mode == "full" else content



        system_parts: list[str] = []

        user_parts: list[str] = []

        tool_parts: list[str] = []

        full_parts: list[str] = []



        for msg in messages or []:

            if not isinstance(msg, dict):

                continue

            role = str(msg.get("role", "user"))

            text = render(msg)

            if not text:

                continue

            if mode == "full":

                full_parts.append(text)

            elif role == "system":

                system_parts.append(text)

            elif role == "tool":

                tool_parts.append(text)

            elif role == "user":

                user_parts.append(text)

            elif role == "assistant" and include_assistant:

                full_parts.append(text)



        if mode == "full":

            parts = full_parts

        elif mode == "delta":

            last_assistant = max(

                (

                    index

                    for index, item in enumerate(messages or [])

                    if isinstance(item, dict) and item.get("role") == "assistant"

                ),

                default=-1,

            )

            tail = [

                render(item)

                for item in (messages or [])[last_assistant + 1 :]

                if isinstance(item, dict) and item.get("role") in {"user", "tool"}

            ]

            parts = [item for item in tail if item]

            if not parts and user_parts:

                parts = [user_parts[-1]]

        else:

            # Clean default: system + most recent user + recent tool outputs.

            parts = []

            if system_parts:

                parts.extend(system_parts[-2:])

            meaningful_user = next(

                (

                    render(item)

                    for item in reversed(messages or [])

                    if isinstance(item, dict)

                    and item.get("role") == "user"

                    and not is_continuation_only(

                        self._content_to_text(item.get("content", ""))

                    )

                ),

                "",

            )

            if meaningful_user:

                parts.append(meaningful_user)

            elif user_parts:

                parts.append(user_parts[-1])

            if tool_parts:

                parts.append(

                    "Use the following tool results to answer the last user request."

                )

                parts.extend(tool_parts[-4:])

            if not parts:

                parts = [

                    render(m)

                    for m in (messages or [])[-4:]

                    if isinstance(m, dict) and render(m)

                ]



        return fit_rendered_parts(parts)



    def _contains_url(self, text: str) -> bool:

        return bool(re.search(r"https?://|www\.[A-Za-z0-9.-]+", text or "", re.I))



    def _tools_to_prompt(self, openai_request: dict) -> str:

        """Expose tool schemas using the Web tool-emulation protocol."""

        return tool_prompt_for_request(openai_request)



    def _tools_guard(self, openai_request: dict) -> str:

        """Place a compact authoritative tool contract after client history."""

        return tool_prompt_guard_for_request(openai_request)



    def _content_to_text(self, content: Any) -> str:

        """Normalize OpenAI/Anthropic content blocks to text."""

        if content is None:

            return ""

        if isinstance(content, str):

            return content

        if isinstance(content, list):

            parts: list[str] = []

            for item in content:

                if isinstance(item, str):

                    parts.append(item)

                elif isinstance(item, dict):

                    item_type = item.get("type")

                    if item_type in {"text", "input_text"}:

                        parts.append(str(item.get("text", "")))

                    elif item_type in {"thinking", "redacted_thinking"}:

                        # Do not feed prior hidden/reasoning blocks back into

                        # Liangzidp; that pollutes future answers and tool calls.

                        continue

                    elif item_type == "tool_result":

                        parts.append(self._content_to_text(item.get("content", "")))

                    elif item_type in {"image", "image_url", "input_image"}:

                        parts.append("[image]")

                    else:

                        text = item.get("text") or item.get("content")

                        parts.append(

                            self._content_to_text(text)

                            if text is not None

                            else json.dumps(item, ensure_ascii=False)

                        )

            return "\n".join(part for part in parts if part)

        if isinstance(content, dict):

            if content.get("type") in {"thinking", "redacted_thinking"}:

                return ""

            if content.get("type") in {"text", "input_text"}:

                return str(content.get("text", ""))

            if "content" in content:

                return self._content_to_text(content.get("content"))

            # cache_control and other provider metadata are control fields, not

            # prompt text.  Keep unknown payloads only after removing them.

            cleaned = {k: v for k, v in content.items() if k not in {"cache_control"}}

            return json.dumps(cleaned, ensure_ascii=False) if cleaned else ""

        return str(content)



    def _normalize_parent_message_id(self, value: Any) -> Any:

        if isinstance(value, str):

            value = value.strip()

            if value == "":

                return None

            if value.isdigit():

                return int(value)

        return value



    async def get_models_list(self) -> list[dict]:

        """Fetch available models."""

        client = LiangzidpClient()

        return await client.get_models()

