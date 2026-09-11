"""Anthropic Messages compatibility for the Liangzidp Web backend."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.control.proxy import get_proxy_directory
from app.dataplane.proxy.reverse import ReverseProxy
from app.dataplane.proxy.liangzidp_client import LiangzidpAPIError
from app.dataplane.proxy.tool_calls import (
    ToolProtocolError,
    anthropic_sse_from_response,
    convert_openai_tool_response_with_repair,
    has_tools,
    openai_to_anthropic_stream,
    resolve_tool_policy,
    stream_with_keepalive,
)
from app.platform.config.snapshot import config
from app.platform.errors import AccountError, AppError, ProxyError
from app.platform.logging.logger import logger


router = APIRouter()
_proxy = ReverseProxy()


def _text_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text") or ""))
            elif isinstance(item, dict) and isinstance(item.get("content"), str):
                parts.append(str(item["content"]))
        return "\n".join(part for part in parts if part)
    if isinstance(content, dict):
        return _text_content(content.get("text") or content.get("content") or "")
    return str(content)


def _map_model(model: Any) -> str:
    name = str(model or "deepseek-v4-flash")
    if name.endswith("[1m]"):
        name = name[:-4]
    return name


def _convert_tool_choice(choice: Any) -> Any:
    if choice is None:
        return None
    if isinstance(choice, str):
        return {"any": "required"}.get(choice, choice)
    if not isinstance(choice, dict):
        return choice
    kind = str(choice.get("type") or "auto")
    if kind == "any":
        return "required"
    if kind in {"auto", "none"}:
        return kind
    if kind == "tool":
        return {"type": "function", "function": {"name": str(choice.get("name") or "")}}
    return choice


def _assistant_message(message: dict[str, Any]) -> dict[str, Any]:
    text_parts: list[str] = []
    thinking_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    content = message.get("content")
    if isinstance(content, str):
        text_parts.append(content)
    elif isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                text_parts.append(str(block.get("text") or ""))
            elif block_type == "thinking":
                thinking_parts.append(str(block.get("thinking") or ""))
            elif block_type == "tool_use":
                tool_calls.append({
                    "id": str(block.get("id") or ""),
                    "type": "function",
                    "function": {
                        "name": str(block.get("name") or ""),
                        "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False, separators=(",", ":")),
                    },
                })
    output: dict[str, Any] = {
        "role": "assistant",
        "content": "\n".join(part for part in text_parts if part) or None,
    }
    if thinking_parts:
        output["reasoning_content"] = "".join(thinking_parts)
    if tool_calls:
        output["tool_calls"] = tool_calls
    return output


def _convert_anthropic_to_openai(body: dict[str, Any]) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    system = body.get("system")
    if system:
        messages.append({"role": "system", "content": _text_content(system)})

    for message in body.get("messages") or []:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "user")
        content = message.get("content")
        if role == "assistant":
            messages.append(_assistant_message(message))
            continue
        if isinstance(content, list):
            user_text: list[str] = []
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "tool_result":
                    messages.append({
                        "role": "tool",
                        "tool_call_id": str(block.get("tool_use_id") or ""),
                        "content": _text_content(block.get("content")),
                    })
                elif block_type == "text":
                    user_text.append(str(block.get("text") or ""))
            if user_text:
                messages.append({"role": "user", "content": "\n".join(user_text)})
            continue
        messages.append({"role": role, "content": _text_content(content)})

    tools = []
    for tool in body.get("tools") or []:
        if not isinstance(tool, dict) or not tool.get("name"):
            continue
        function = {
            "name": str(tool["name"]),
            "description": str(tool.get("description") or ""),
            "parameters": tool.get("input_schema") or {"type": "object", "properties": {}},
        }
        if tool.get("strict") is not None:
            function["strict"] = bool(tool["strict"])
        tools.append({"type": "function", "function": function})

    thinking = body.get("thinking")
    thinking_enabled = isinstance(thinking, dict) and thinking.get("type") == "enabled"
    output = {
        "model": _map_model(body.get("model")),
        "messages": messages,
        "max_tokens": body.get("max_tokens", 8192),
        "stream": bool(body.get("stream", False)),
        "thinking": thinking,
        "thinking_enabled": thinking_enabled,
        "reasoning_effort": (body.get("output_config") or {}).get("effort") if isinstance(body.get("output_config"), dict) else None,
        "_client_identity": body.get("_client_identity"),
        "conversation_id": body.get("conversation_id"),
        "session_id": body.get("session_id"),
        "thread_id": body.get("thread_id"),
        "metadata": body.get("metadata"),
        "chat_session_id": body.get("chat_session_id"),
        "parent_message_id": body.get("parent_message_id"),
    }
    if tools:
        output["tools"] = tools
    tool_choice = _convert_tool_choice(body.get("tool_choice"))
    if tool_choice is not None:
        output["tool_choice"] = tool_choice
    return output


def _convert_openai_to_anthropic(response: dict[str, Any], model: str) -> dict[str, Any]:
    choice = (response.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    blocks: list[dict[str, Any]] = []
    if message.get("reasoning_content"):
        blocks.append({"type": "thinking", "thinking": message["reasoning_content"]})
    if message.get("content"):
        blocks.append({"type": "text", "text": message["content"]})
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        try:
            tool_input = json.loads(function.get("arguments") or "{}")
        except json.JSONDecodeError:
            tool_input = {}
        call_id = str(call.get("id") or "")
        if call_id.startswith("call_"):
            call_id = "toolu_" + call_id[5:]
        blocks.append({
            "type": "tool_use", "id": call_id,
            "name": function.get("name") or "", "input": tool_input,
        })
    usage = response.get("usage") or {}
    finish = str(choice.get("finish_reason") or "stop")
    return {
        "id": str(response.get("id") or "msg_liangzidp").replace("chatcmpl-", "msg_"),
        "type": "message", "role": "assistant", "content": blocks, "model": model,
        "stop_reason": "tool_use" if message.get("tool_calls") else {"length": "max_tokens"}.get(finish, "end_turn"),
        "stop_sequence": None,
        "usage": {
            "input_tokens": int(usage.get("prompt_tokens") or 0),
            "output_tokens": int(usage.get("completion_tokens") or 0),
        },
    }


@router.post("/v1/messages", tags=["Anthropic - Messages"])
async def anthropic_messages(request: Request) -> Any:
    body = await request.json()
    body["_client_identity"] = getattr(request.state, "client_identity", "anonymous")
    if request.headers.get("X-Conversation-Id") and not body.get("conversation_id"):
        body["conversation_id"] = request.headers["X-Conversation-Id"]
    if request.headers.get("X-DeepSeek-Chat-Session-Id") and not body.get("chat_session_id"):
        body["chat_session_id"] = request.headers["X-DeepSeek-Chat-Session-Id"]
    if request.headers.get("X-DeepSeek-Parent-Message-Id") and body.get("parent_message_id") is None:
        body["parent_message_id"] = request.headers["X-DeepSeek-Parent-Message-Id"]

    openai_body = _convert_anthropic_to_openai(body)
    try:
        resolve_tool_policy(openai_body)
        tool_mode = has_tools(openai_body)
    except ToolProtocolError as exc:
        raise AppError(str(exc), exc.status, exc.code) from exc

    proxy_dir = get_proxy_directory()
    proxy = proxy_dir.get_proxy()
    stream = bool(body.get("stream", False))
    model = str(body.get("model") or "deepseek-v4-flash")
    openai_body["_queue_timeout_seconds"] = config.get_int(
        "account.queue.stream_timeout_seconds" if stream else "account.queue.nonstream_timeout_seconds",
        300 if stream else 30,
    )
    if request.headers.get("Idempotency-Key"):
        openai_body["_idempotency_key"] = request.headers["Idempotency-Key"]

    try:
        account_id = request.headers.get("X-DeepSeek-Account-Id") or request.headers.get("X-Account-Id")
        pow_response = request.headers.get("X-Ds-Pow-Response", "")

        async def invoke(payload: dict[str, Any]) -> Any:
            return await _proxy.chat_completion(
                request_data=payload,
                account_id=account_id,
                pow_response=pow_response,
                proxy=proxy,
                repository=request.app.state.repository,
                orchestrator=request.app.state.orchestrator,
            )

        async def mark_tool_protocol_failure(exc: ToolProtocolError) -> None:
            await request.app.state.orchestrator.store.increment_metric("tool_protocol_failures")
            session_key, _ = request.app.state.orchestrator.session_key(openai_body)
            await request.app.state.orchestrator.store.update_conversation(
                session_key,
                state="incomplete",
                last_error=f"tool protocol: {exc}",
            )

        async def invoke_validated_tool(payload: dict[str, Any]) -> dict[str, Any]:
            result = await invoke(payload)
            if not isinstance(result, dict):
                raise ToolProtocolError("buffered tool request unexpectedly returned a stream")

            async def retry_tool_call(repair_body: dict[str, Any]) -> dict[str, Any]:
                await request.app.state.orchestrator.store.increment_metric("tool_protocol_repairs")
                retry_result = await invoke(repair_body)
                if not isinstance(retry_result, dict):
                    raise ToolProtocolError("tool protocol repair unexpectedly returned a stream")
                return retry_result

            result, tool_request = await convert_openai_tool_response_with_repair(
                result,
                payload,
                retry_tool_call,
            )
            await request.app.state.orchestrator.register_tool_calls(
                tool_request,
                (result.get("choices") or [{}])[0].get("message", {}).get("tool_calls") or [],
            )
            return result

        if stream and tool_mode:
            buffered_body = {**openai_body, "stream": False}

            async def buffered_source():
                result = await request.app.state.orchestrator.run_singleflight(
                    buffered_body,
                    lambda: invoke_validated_tool(buffered_body),
                )
                yield _convert_openai_to_anthropic(result, model)

            async def buffered_tool_stream():
                try:
                    async for item in stream_with_keepalive(buffered_source()):
                        if item is None:
                            yield ": keep-alive\n\n"
                            continue
                        for event in anthropic_sse_from_response(item):
                            yield event
                except ToolProtocolError as exc:
                    await mark_tool_protocol_failure(exc)
                    error = {"type": "error", "error": {"type": exc.code, "message": str(exc)}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                except LiangzidpAPIError as exc:
                    error = {"type": "error", "error": {"type": exc.code, "message": exc.message}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                except AppError as exc:
                    error = {"type": "error", "error": {"type": exc.code, "message": exc.message}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                except Exception as exc:
                    logger.error("buffered anthropic tool stream error: error={}", exc)
                    error = {"type": "error", "error": {"type": "server_error", "message": str(exc)}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"

            return StreamingResponse(
                buffered_tool_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache, no-transform",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        if stream:
            async def deferred_source():
                yield await request.app.state.orchestrator.run_singleflight(
                    openai_body,
                    lambda: invoke(openai_body),
                )

            async def deferred_openai_stream():
                async for upstream in stream_with_keepalive(deferred_source()):
                    if upstream is None:
                        yield None
                        continue
                    async for part in stream_with_keepalive(upstream):
                        yield part

            async def deferred_anthropic_stream():
                try:
                    async for part in openai_to_anthropic_stream(deferred_openai_stream(), model):
                        yield part
                except LiangzidpAPIError as exc:
                    error = {"type": "error", "error": {"type": exc.code, "message": exc.message}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                except AppError as exc:
                    error = {"type": "error", "error": {"type": exc.code, "message": exc.message}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                except Exception as exc:
                    logger.error("deferred anthropic stream error: error={}", exc)
                    error = {"type": "error", "error": {"type": "server_error", "message": str(exc)}}
                    yield "event: error\ndata: " + json.dumps(error, ensure_ascii=False, separators=(",", ":")) + "\n\n"

            return StreamingResponse(
                deferred_anthropic_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache, no-transform",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        result = await request.app.state.orchestrator.run_singleflight(
            openai_body,
            lambda: invoke_validated_tool(openai_body) if tool_mode else invoke(openai_body),
        )
        payload = _convert_openai_to_anthropic(result, model)
        return JSONResponse(
            content=payload,
            media_type="application/json; charset=utf-8",
            headers={
                "X-Task-Id": str(openai_body.get("_resolved_session_key") or "")[:16],
                "X-Queue-Wait-Ms": str(int(openai_body.get("_queue_wait_ms") or 0)),
                "X-Account-Hash": str(openai_body.get("_selected_account_hash") or ""),
            },
        )
    except ToolProtocolError as exc:
        await request.app.state.orchestrator.store.increment_metric("tool_protocol_failures")
        session_key, _ = request.app.state.orchestrator.session_key(openai_body)
        await request.app.state.orchestrator.store.update_conversation(
            session_key,
            state="incomplete",
            last_error=f"tool protocol: {exc}",
        )
        raise AppError(str(exc), exc.status, exc.code) from exc
    except LiangzidpAPIError as exc:
        raise AppError(exc.message, exc.status_code, exc.code) from exc
    except (AccountError, ProxyError, AppError):
        raise
    except Exception as exc:
        logger.error("anthropic messages error: error={}", exc)
        raise AppError(str(exc), 500) from exc
