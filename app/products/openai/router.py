"""OpenAI-compatible API routes."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.control.model import list_models
from app.control.proxy import get_proxy_directory
from app.dataplane.proxy.reverse import ReverseProxy
from app.dataplane.proxy.liangzidp_client import LiangzidpAPIError
from app.dataplane.proxy.tool_calls import (
    ToolProtocolError,
    convert_openai_tool_response_with_repair,
    has_tools,
    openai_sse_from_response,
    resolve_tool_policy,
    stream_with_keepalive,
)
from app.platform.config.snapshot import config
from app.platform.errors import AppError
from app.platform.logging.logger import logger

router = APIRouter()
_proxy = ReverseProxy()


@router.get("/v1/models", tags=["OpenAI - Models"])
async def list_openai_models(request: Request) -> dict:
    """List available models in OpenAI format."""
    return {
        "object": "list",
        "data": list_models(),
    }


@router.post("/v1/chat/completions", tags=["OpenAI - Chat"])
async def chat_completions(request: Request) -> Any:
    """OpenAI-compatible chat completions endpoint."""
    body = await request.json()
    body["_client_identity"] = getattr(request.state, "client_identity", "anonymous")
    # Optional sticky conversation controls for Liangzidp Web session reuse.
    # Normal OpenAI clients can ignore these; advanced clients may send headers
    # to avoid re-uploading huge histories every turn.
    if request.headers.get("X-Conversation-Id") and not body.get("conversation_id"):
        body["conversation_id"] = request.headers["X-Conversation-Id"]
    if request.headers.get("X-DeepSeek-Chat-Session-Id") and not body.get("chat_session_id"):
        body["chat_session_id"] = request.headers["X-DeepSeek-Chat-Session-Id"]
    if request.headers.get("X-DeepSeek-Parent-Message-Id") and body.get("parent_message_id") is None:
        body["parent_message_id"] = request.headers["X-DeepSeek-Parent-Message-Id"]
    stream = bool(body.get("stream", False))
    body["_queue_timeout_seconds"] = config.get_int(
        "account.queue.stream_timeout_seconds" if stream else "account.queue.nonstream_timeout_seconds",
        300 if stream else 30,
    )
    if request.headers.get("Idempotency-Key"):
        body["_idempotency_key"] = request.headers["Idempotency-Key"]
    try:
        resolve_tool_policy(body)
        tool_mode = has_tools(body)
    except ToolProtocolError as exc:
        raise AppError(str(exc), exc.status, exc.code) from exc

    # Get optional proxy
    proxy_dir = get_proxy_directory()
    proxy = proxy_dir.get_proxy()

    # Forward request
    try:
        account_id = (
            request.headers.get("X-DeepSeek-Account-Id")
            or request.headers.get("X-Account-Id")
            or body.get("account_id")
        )
        pow_response = (
            request.headers.get("X-Ds-Pow-Response")
            or body.get("pow_response", "")
        )
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
            session_key, _ = request.app.state.orchestrator.session_key(body)
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
            buffered_body = {**body, "stream": False}

            async def buffered_source():
                yield await request.app.state.orchestrator.run_singleflight(
                    buffered_body,
                    lambda: invoke_validated_tool(buffered_body),
                )

            async def buffered_tool_stream():
                try:
                    async for item in stream_with_keepalive(buffered_source()):
                        if item is None:
                            yield ": keep-alive\n\n"
                            continue
                        for chunk in openai_sse_from_response(item):
                            yield chunk
                except ToolProtocolError as exc:
                    await mark_tool_protocol_failure(exc)
                    payload = AppError(str(exc), exc.status, exc.code).to_dict()
                    yield "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"
                except LiangzidpAPIError as exc:
                    payload = AppError(exc.message, exc.status_code, exc.code).to_dict()
                    yield "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"
                except AppError as exc:
                    yield "data: " + json.dumps(exc.to_dict(), ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"
                except Exception as exc:
                    logger.error("buffered tool stream error: error={}", exc)
                    payload = AppError(str(exc), 500).to_dict()
                    yield "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"

            return StreamingResponse(
                buffered_tool_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        if stream:
            async def deferred_source():
                yield await request.app.state.orchestrator.run_singleflight(
                    body,
                    lambda: invoke(body),
                )

            async def deferred_plain_stream():
                try:
                    async for upstream in stream_with_keepalive(deferred_source()):
                        if upstream is None:
                            yield ": keep-alive\n\n"
                            continue
                        async for part in stream_with_keepalive(upstream):
                            yield ": keep-alive\n\n" if part is None else part
                except LiangzidpAPIError as exc:
                    payload = AppError(exc.message, exc.status_code, exc.code).to_dict()
                    yield "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"
                except AppError as exc:
                    yield "data: " + json.dumps(exc.to_dict(), ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"
                except Exception as exc:
                    logger.error("deferred stream error: error={}", exc)
                    payload = AppError(str(exc), 500).to_dict()
                    yield "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                    yield "data: [DONE]\n\n"

            return StreamingResponse(
                deferred_plain_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        result = await request.app.state.orchestrator.run_singleflight(
            body,
            lambda: invoke_validated_tool(body) if tool_mode else invoke(body),
        )
        return JSONResponse(
            content=result,
            media_type="application/json; charset=utf-8",
            headers={
                "X-Task-Id": str(body.get("_resolved_session_key") or "")[:16],
                "X-Queue-Wait-Ms": str(int(body.get("_queue_wait_ms") or 0)),
                "X-Account-Hash": str(body.get("_selected_account_hash") or ""),
            },
        )
    except ToolProtocolError as exc:
        await request.app.state.orchestrator.store.increment_metric("tool_protocol_failures")
        session_key, _ = request.app.state.orchestrator.session_key(body)
        await request.app.state.orchestrator.store.update_conversation(
            session_key,
            state="incomplete",
            last_error=f"tool protocol: {exc}",
        )
        raise AppError(str(exc), exc.status, exc.code) from exc
    except LiangzidpAPIError as exc:
        raise AppError(exc.message, exc.status_code, exc.code) from exc
    except AppError:
        raise
    except Exception as exc:
        logger.error("chat completions error: error={}", exc)
        raise AppError(str(exc), 500)


@router.post("/v1/images/generations", tags=["OpenAI - Images"])
async def generate_images(request: Request) -> dict:
    """Image generation endpoint (placeholder)."""
    return {
        "created": 0,
        "data": [],
        "error": "Image generation not yet implemented for Liangzidp",
    }


@router.get("/v1/files", tags=["OpenAI - Files"])
async def list_files(request: Request) -> dict:
    """List cached files."""
    return {"object": "list", "data": []}
