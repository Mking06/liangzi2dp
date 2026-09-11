"""OpenAI Responses API compatibility for the Liangzidp Web backend.

Implements POST /v1/responses by translating between the Responses API
format (input/output item arrays) and the internal Chat Completions flow.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.control.proxy import get_proxy_directory
from app.dataplane.proxy.reverse import ReverseProxy
from app.dataplane.proxy.liangzidp_client import LiangzidpAPIError
from app.dataplane.proxy.tool_calls import (
    ToolProtocolError,
    convert_openai_tool_response_with_repair,
    has_tools,
    resolve_tool_policy,
    stream_with_keepalive,
)
from app.platform.config.snapshot import config
from app.platform.errors import AppError
from app.platform.logging.logger import logger

router = APIRouter()
_proxy = ReverseProxy()


# ---------------------------------------------------------------------------
# Request conversion: Responses -> Chat Completions
# ---------------------------------------------------------------------------


def _content_to_text(content: Any) -> str:
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
                itype = item.get("type")
                if itype in {"input_text", "text", "output_text"}:
                    parts.append(str(item.get("text") or item.get("content") or ""))
                elif itype in {"input_image", "image", "image_url"}:
                    parts.append("[image]")
                elif itype == "input_file":
                    parts.append(f"[file:{item.get('filename', 'attachment')}]")
                elif isinstance(item.get("text"), str):
                    parts.append(item["text"])
        return "\n".join(p for p in parts if p)
    if isinstance(content, dict):
        return _content_to_text(content.get("text") or content.get("content"))
    return str(content)


def _flatten_namespace_tools(
    tools_list: list[Any], freeform: set[str] | None = None,
    custom: set[str] | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Recursively flatten Codex-style namespace/custom tool structures.

    Returns (tools, freeform_names). A freeform custom tool (no JSON parameter
    schema, e.g. Codex ``exec`` which accepts raw source text) receives a
    synthesized ``{code: string}`` schema so the upstream model knows to emit
    a source/command string.
    """
    result: list[dict[str, Any]] = []
    if freeform is None:
        freeform = set()
    if custom is None:
        custom = set()
    for t in (tools_list or []):
        if not isinstance(t, dict):
            continue
        ttype = t.get("type")
        if ttype == "namespace":
            sub, _ = _flatten_namespace_tools(t.get("tools") or [], freeform, custom)
            result.extend(sub)
        elif ttype == "custom":
            name = t.get("name")
            if name:
                name = str(name)
                custom.add(name)
                params = t.get("parameters") or t.get("input_schema")
                is_freeform = not params or (
                    isinstance(params, dict) and not params.get("properties")
                )
                if is_freeform:
                    freeform.add(name)
                    params = {
                        "type": "object",
                        "properties": {
                            "code": {
                                "type": "string",
                                "description": "Raw source code or command text passed verbatim to this freeform tool.",
                            },
                        },
                        "required": ["code"],
                    }
                result.append({
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": str(t.get("description") or ""),
                        "parameters": params,
                    },
                })
        elif ttype == "function":
            fn = t.get("function")
            if isinstance(fn, dict):
                result.append({"type": "function", "function": fn})
            elif t.get("name"):
                result.append({
                    "type": "function",
                    "function": {
                        "name": str(t["name"]),
                        "description": str(t.get("description") or ""),
                        "parameters": t.get("parameters") or {"type": "object", "properties": {}},
                    },
                })
    return result, freeform


def _extract_additional_tools(
    input_data: Any,
) -> tuple[list[dict[str, Any]], set[str], set[str]]:
    """Extract tools from Codex-style additional_tools input items.

    Returns (tools, freeform_names, custom_names). Every tool declared as a
    Responses ``custom`` tool (Codex namespace tools) is recorded in
    custom_names so the response emits ``custom_tool_call`` items.
    """
    extracted: list[dict[str, Any]] = []
    freeform: set[str] = set()
    custom: set[str] = set()
    if not isinstance(input_data, list):
        return extracted, freeform, custom
    for item in input_data:
        if isinstance(item, dict) and item.get("type") == "additional_tools":
            tools, ff = _flatten_namespace_tools(item.get("tools") or [], freeform, custom)
            extracted.extend(tools)
            freeform.update(ff)
    return extracted, freeform, custom


def _convert_tools(tools: list[Any] | None) -> list[dict[str, Any]]:
    if not tools:
        return []
    result: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        ttype = tool.get("type")
        if ttype in {"function_search", "web_search", "computer_use", "code_interpreter"}:
            continue
        if ttype == "function":
            # Support both formats:
            # 1. Chat Completions: {"type": "function", "function": {"name": ..., "parameters": ...}}
            # 2. Responses API: {"type": "function", "name": ..., "parameters": ...}
            fn = tool.get("function")
            if isinstance(fn, dict):
                name = fn.get("name")
                params = fn.get("parameters") or {"type": "object", "properties": {}}
                desc = fn.get("description", "")
            else:
                name = tool.get("name")
                params = tool.get("parameters") or {"type": "object", "properties": {}}
                desc = tool.get("description", "")
            if name:
                result.append({
                    "type": "function",
                    "function": {"name": name, "description": desc, "parameters": params},
                })
    return result


def _convert_tool_choice(choice: Any) -> Any:
    if choice is None:
        return None
    if isinstance(choice, str):
        if choice in {"auto", "none", "required"}:
            return choice
        return "auto"
    if isinstance(choice, dict):
        ctype = choice.get("type")
        if ctype == "function":
            name = choice.get("name")
            if name:
                return {"type": "function", "function": {"name": name}}
        return "auto"
    return "auto"


def _input_to_messages(input_data: Any, instructions: str = "") -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    if instructions:
        messages.append({"role": "system", "content": instructions})

    if input_data is None:
        return messages
    if isinstance(input_data, str):
        messages.append({"role": "user", "content": input_data})
        return messages
    if not isinstance(input_data, list):
        return messages

    for item in input_data:
        if not isinstance(item, dict):
            continue
        itype = item.get("type")

        if itype == "message":
            role = str(item.get("role") or "user")
            content = item.get("content")
            if role == "assistant":
                text_parts: list[str] = []
                if isinstance(content, list):
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        btype = block.get("type")
                        if btype in {"output_text", "text"}:
                            text_parts.append(str(block.get("text") or ""))
                msg: dict[str, Any] = {"role": "assistant"}
                if text_parts:
                    msg["content"] = "\n".join(text_parts)
                else:
                    msg["content"] = _content_to_text(content)
                messages.append(msg)
            else:
                messages.append({"role": role, "content": _content_to_text(content)})

        elif itype == "function_call":
            call_id = str(item.get("call_id") or f"call_{uuid.uuid4().hex[:24]}")
            name = str(item.get("name") or "")
            arguments = item.get("arguments")
            if isinstance(arguments, dict):
                arguments = json.dumps(arguments, ensure_ascii=False)
            else:
                arguments = str(arguments or "{}")
            if messages and messages[-1].get("role") == "assistant":
                assistant = messages[-1]
            else:
                assistant = {"role": "assistant", "content": ""}
                messages.append(assistant)
            calls = assistant.setdefault("tool_calls", [])
            calls.append({
                "id": call_id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            })

        elif itype == "function_call_output":
            call_id = str(item.get("call_id") or "")
            output = item.get("output")
            if isinstance(output, (dict, list)):
                output = json.dumps(output, ensure_ascii=False)
            else:
                output = str(output or "")
            messages.append({
                "role": "tool",
                "tool_call_id": call_id,
                "content": output,
            })

        elif itype == "custom_tool_call":
            # Echo of a custom (Codex) tool call -> assistant tool_calls entry
            call_id = str(item.get("call_id") or f"call_{uuid.uuid4().hex[:24]}")
            name = str(item.get("name") or "")
            raw_input = item.get("input")
            if isinstance(raw_input, str):
                # freeform custom tool: re-wrap raw source into the synthesized
                # {code: string} shape used on the upstream request
                arguments = json.dumps({"code": raw_input}, ensure_ascii=False)
            else:
                arguments = json.dumps(raw_input if raw_input is not None else {},
                                       ensure_ascii=False)
            if messages and messages[-1].get("role") == "assistant":
                assistant = messages[-1]
            else:
                assistant = {"role": "assistant", "content": ""}
                messages.append(assistant)
            assistant.setdefault("tool_calls", []).append({
                "id": call_id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            })

        elif itype == "custom_tool_call_output":
            call_id = str(item.get("call_id") or "")
            output = item.get("output")
            if isinstance(output, (dict, list)):
                output = json.dumps(output, ensure_ascii=False)
            else:
                output = str(output or "")
            messages.append({
                "role": "tool",
                "tool_call_id": call_id,
                "content": output,
            })

        elif itype in {"file", "input_file"}:
            filename = item.get("filename") or item.get("file_id") or "attachment"
            messages.append({"role": "user", "content": f"[file:{filename}]"})

        elif itype == "video":
            messages.append({"role": "user", "content": "[video]"})

        elif itype == "reasoning":
            continue

    return messages


def _responses_to_chat_request(body: dict[str, Any]) -> dict[str, Any]:
    input_data = body.get("input")
    instructions = body.get("instructions") or ""
    # Merge top-level tools with tools embedded in additional_tools input items (Codex style)
    # Merge tools from three sources, de-duplicated by function name:
    #  1) top-level function tools (Chat-Completions-nested or Responses-flat)
    #  2) top-level Codex custom/namespace tools (free-form, e.g. "exec")
    #  3) tools embedded in additional_tools input items (Codex style)
    tools = _convert_tools(body.get("tools"))
    _top_freeform, _top_custom = set(), set()
    namespace_tools, _ = _flatten_namespace_tools(
        body.get("tools") or [], _top_freeform, _top_custom
    )
    additional_tools, add_freeform, add_custom = _extract_additional_tools(input_data)
    existing_names = {t["function"]["name"] for t in tools}
    for _t in [*namespace_tools, *additional_tools]:
        _name = _t["function"]["name"]
        if _name not in existing_names:
            tools.append(_t)
            existing_names.add(_name)
    freeform_names = _top_freeform | add_freeform
    custom_names = _top_custom | add_custom
    tool_choice = _convert_tool_choice(body.get("tool_choice"))
    messages = _input_to_messages(input_data, instructions)

    chat_body: dict[str, Any] = {
        "model": str(body.get("model") or "deepseek-v4-flash"),
        "messages": messages,
        "stream": bool(body.get("stream", False)),
    }
    if tools:
        chat_body["tools"] = tools
        if tool_choice is not None:
            chat_body["tool_choice"] = tool_choice
    if freeform_names:
        chat_body["_freeform_tools"] = sorted(freeform_names)
    if custom_names:
        chat_body["_custom_tools"] = sorted(custom_names)
    if body.get("temperature") is not None:
        chat_body["temperature"] = body["temperature"]
    if body.get("top_p") is not None:
        chat_body["top_p"] = body["top_p"]
    if body.get("max_output_tokens") is not None:
        chat_body["max_tokens"] = body["max_output_tokens"]
    if body.get("user") is not None:
        chat_body["user"] = body["user"]
    for key in ("conversation_id", "chat_session_id", "parent_message_id", "account_id"):
        if body.get(key) is not None:
            chat_body[key] = body[key]
    # Codex/Responses clients scope automatic prefix caching with a stable
    # ``prompt_cache_key`` but send no conversation_id. Reuse it as a stable
    # conversation identity so consecutive turns share one upstream Liangzidp
    # session (and parent_message_id), enabling context/cache reuse instead of
    # opening a brand-new upstream session on every turn.
    if not chat_body.get("conversation_id"):
        cache_key = body.get("prompt_cache_key")
        if cache_key:
            chat_body["conversation_id"] = f"pcache:{cache_key}"
    return chat_body


# ---------------------------------------------------------------------------
# Response conversion: Chat Completions -> Responses
# ---------------------------------------------------------------------------


def _sse_event(event_type: str, data: dict[str, Any], seq: int) -> str:
    payload = {"type": event_type, "sequence_number": seq, **data}
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n"


def _build_response_object(
    resp_id: str,
    model: str,
    created_at: int,
    status: str,
    output: list[dict[str, Any]],
    usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": resp_id,
        "object": "response",
        "created_at": created_at,
        "status": status,
        "model": model,
        "output": output,
        "usage": usage or {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        "error": None,
        "incomplete_details": None,
    }


_EXEC_JS_RE = re.compile(
        r"\btools\.[A-Za-z_]|\bawait\b|\btext\s*\(|\bexit\s*\(|"
        r"\b(?:const|let|var|function|return|require|console|async|import|export|new|throw|try|catch|if|for|while)\b|=>"
    )


def _wrap_exec_command(raw: str) -> str:
    """Wrap a bare shell command as Codex exec JavaScript.

    exec runs JS in a V8 isolate where ``tools.exec_command(cmd)`` invokes a
    shell. If the model already produced JavaScript (uses await/tools/text),
    leave it untouched.
    """
    text = (raw or "").strip()
    if not text:
        return raw or ""
    if _EXEC_JS_RE.search(text):
        return raw
    escaped = text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    return f"text(await tools.exec_command(`{escaped}`))"


from app.dataplane.proxy.context_policy import estimate_tokens

# Liangzidp Web never reports cached token counts. Providers only cache a stable
# prefix once it reaches this minimum; below it neither write nor read is billed.
_CACHE_MIN_TOKENS = 1024


def _estimate_input_tokens(chat_body: dict[str, Any]) -> int:
    """Estimate total input tokens of the full transcript Codex actually sent."""
    total = 0
    for msg in chat_body.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        total += estimate_tokens(str(msg.get("role") or ""))
        content = msg.get("content")
        if isinstance(content, (dict, list)):
            content = json.dumps(content, ensure_ascii=False)
        total += estimate_tokens(str(content or ""))
        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function") if isinstance(tc, dict) else None
            if isinstance(fn, dict):
                total += estimate_tokens(str(fn.get("name") or ""))
                total += estimate_tokens(str(fn.get("arguments") or ""))
        if msg.get("tool_call_id"):
            total += estimate_tokens(str(msg.get("tool_call_id")))
    for tool in chat_body.get("tools") or []:
        total += estimate_tokens(json.dumps(tool, ensure_ascii=False))
    return total


def _estimate_output_tokens(chat_response: dict[str, Any]) -> int:
    """Estimate generated tokens (visible text + tool call arguments)."""
    message = ((chat_response.get("choices") or [{}])[0].get("message") or {})
    total = estimate_tokens(str(message.get("content") or ""))
    for tc in message.get("tool_calls") or []:
        fn = tc.get("function") if isinstance(tc, dict) else None
        if isinstance(fn, dict):
            total += estimate_tokens(str(fn.get("name") or ""))
            total += estimate_tokens(str(fn.get("arguments") or ""))
    return total


def _cache_accounting(full_input: int, prev_input: int) -> tuple[int, int]:
    """Return (cached_read_tokens, cache_write_tokens).

    First turn for a stable identity writes the prefix; later turns read the
    previous turn's prefix from cache. Below the provider minimum nothing is
    cacheable.
    """
    if full_input < _CACHE_MIN_TOKENS:
        return 0, 0
    if prev_input >= _CACHE_MIN_TOKENS:
        return min(prev_input, full_input), 0
    return 0, full_input


def _responses_usage(chat_response: dict[str, Any], usage_hint: dict[str, Any] | None) -> dict[str, Any]:
    """Build the Responses usage object, with estimated cache accounting."""
    if usage_hint:
        inp = int(usage_hint.get("input_tokens") or 0)
        cached = int(usage_hint.get("cached_tokens") or 0)
        written = int(usage_hint.get("cache_write_tokens") or 0)
        out = _estimate_output_tokens(chat_response)
        return {
            "input_tokens": inp,
            "input_tokens_details": {
                "cached_tokens": cached,
                "cache_write_tokens": written,
            },
            "output_tokens": out,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": inp + out,
        }
    usage = chat_response.get("usage") or {}
    return {
        "input_tokens": int(usage.get("prompt_tokens") or 0),
        "output_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


def _freeform_arguments(name: str, args_json: str, freeform_names: set[str]) -> str:
    """Freeform tools (Codex exec) receive raw text, not a JSON object."""
    if not freeform_names or name not in freeform_names:
        return args_json or "{}"
    raw = args_json or ""
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            for value in obj.values():
                if isinstance(value, str):
                    raw = value
                    break
    except json.JSONDecodeError:
        pass
    if name == "exec":
        raw = _wrap_exec_command(raw)
    return raw


def _custom_tool_input(
    name: str, args_json: str, freeform_names: set[str],
) -> Any:
    """Build the ``input`` field of a custom_tool_call item.

    Freeform custom tools (no schema, e.g. exec) carry a raw source string;
    structured custom tools carry the parsed JSON object.
    """
    if name in freeform_names:
        return _freeform_arguments(name, args_json, freeform_names)
    try:
        return json.loads(args_json or "{}")
    except (json.JSONDecodeError, TypeError):
        return args_json or ""


def _make_tool_output_item(
    call: dict[str, Any],
    freeform_names: set[str],
    custom_names: set[str],
) -> dict[str, Any]:
    """Convert one Chat Completions tool_call to a Responses output item.

    Custom-declared tools (Codex) become ``custom_tool_call`` (ctc_ id, input
    field); standard function tools become ``function_call`` (fc_ id, arguments).
    """
    fn = call.get("function") or {}
    call_id = str(call.get("id") or f"call_{uuid.uuid4().hex[:24]}")
    name = str(fn.get("name") or "")
    args_json = str(fn.get("arguments") or "{}")
    if name in custom_names:
        return {
            "id": f"ctc_{uuid.uuid4().hex[:24]}",
            "type": "custom_tool_call",
            "call_id": call_id,
            "name": name,
            "input": _custom_tool_input(name, args_json, freeform_names),
            "status": "completed",
        }
    return {
        "id": f"fc_{uuid.uuid4().hex[:24]}",
        "type": "function_call",
        "call_id": call_id,
        "name": name,
        "arguments": _freeform_arguments(name, args_json, freeform_names),
        "status": "completed",
    }


def _tool_item_events(
    item: dict[str, Any], output_index: int, seq: int,
) -> tuple[list[str], int]:
    """SSE events for one tool output item (function_call or custom_tool_call)."""
    events: list[str] = []
    item_id = item.get("id")
    if item["type"] == "custom_tool_call":
        value = item.get("input", "")
        delta = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        initial = "" if isinstance(value, str) else {}
        events.append(_sse_event("response.output_item.added", {
            "item_id": item_id, "output_index": output_index,
            "item": {
                "id": item_id, "type": "custom_tool_call",
                "call_id": item.get("call_id", item_id),
                "name": item.get("name", ""), "input": initial,
                "status": "in_progress",
            },
        }, seq)); seq += 1
        if delta:
            events.append(_sse_event("response.custom_tool_call_input.delta", {
                "item_id": item_id, "output_index": output_index, "delta": delta,
            }, seq)); seq += 1
        events.append(_sse_event("response.custom_tool_call_input.done", {
            "item_id": item_id, "output_index": output_index, "input": value,
        }, seq)); seq += 1
    else:
        fc_name = item.get("name", "")
        args = item.get("arguments", "")
        events.append(_sse_event("response.output_item.added", {
            "item_id": item_id, "output_index": output_index,
            "item": {
                "id": item_id, "type": "function_call",
                "call_id": item.get("call_id", item_id),
                "name": fc_name, "arguments": "", "status": "in_progress",
            },
        }, seq)); seq += 1
        if args:
            events.append(_sse_event("response.function_call_arguments.delta", {
                "item_id": item_id, "output_index": output_index, "delta": args,
            }, seq)); seq += 1
        events.append(_sse_event("response.function_call_arguments.done", {
            "item_id": item_id, "output_index": output_index,
            "arguments": args, "name": fc_name,
        }, seq)); seq += 1
    events.append(_sse_event("response.output_item.done", {
        "item_id": item_id, "output_index": output_index, "item": item,
    }, seq)); seq += 1
    return events, seq


def _chat_to_responses_response(
    chat_response: dict[str, Any],
    resp_id: str | None = None,
    freeform_names: set[str] | None = None,
    custom_names: set[str] | None = None,
    usage_hint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    freeform_names = freeform_names or set()
    custom_names = custom_names or set()
    choice = (chat_response.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    usage = chat_response.get("usage") or {}

    output: list[dict[str, Any]] = []
    content_text = message.get("content")
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"

    if content_text:
        output.append({
            "id": msg_id,
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": content_text,
                "annotations": [],
            }],
        })

    for call in message.get("tool_calls") or []:
        output.append(_make_tool_output_item(call, freeform_names, custom_names))

    finish_reason = choice.get("finish_reason") or "stop"
    status = "completed" if finish_reason != "length" else "incomplete"

    return _build_response_object(
        resp_id=resp_id or str(chat_response.get("id") or f"resp_{uuid.uuid4().hex[:24]}"),
        model=str(chat_response.get("model") or "deepseek-v4-flash"),
        created_at=int(chat_response.get("created") or time.time()),
        status=status,
        output=output,
        usage=_responses_usage(chat_response, usage_hint),
    )


def _responses_sse_events(
    chat_response: dict[str, Any],
    resp_id: str,
    model: str,
    created_at: int,
    skip_initial: bool = True,
    freeform_names: set[str] | None = None,
    custom_names: set[str] | None = None,
    usage_hint: dict[str, Any] | None = None,
) -> list[str]:
    """Convert a Chat Completions response to Responses SSE events.

    Every event includes type and sequence_number; response lifecycle events
    wrap the response object under a "response" key.
    """
    freeform_names = freeform_names or set()
    custom_names = custom_names or set()
    resp = _chat_to_responses_response(
        chat_response, resp_id=resp_id,
        freeform_names=freeform_names, custom_names=custom_names,
        usage_hint=usage_hint,
    )
    events: list[str] = []
    seq = 0

    if not skip_initial:
        events.append(_sse_event("response.created", {"response": _build_response_object(
            resp_id, model, created_at, "in_progress", [], None,
        )}, seq)); seq += 1
        events.append(_sse_event("response.in_progress", {"response": _build_response_object(
            resp_id, model, created_at, "in_progress", [], None,
        )}, seq)); seq += 1

    output_index = 0
    for item in resp["output"]:
        if item["type"] == "message":
            item_id = item["id"]
            events.append(_sse_event("response.output_item.added", {
                "item_id": item_id, "output_index": output_index,
                "item": {
                    "id": item_id, "type": "message", "role": "assistant",
                    "status": "in_progress", "content": [],
                },
            }, seq)); seq += 1
            content_index = 0
            for block in item.get("content", []):
                text = block.get("text", "")
                events.append(_sse_event("response.content_part.added", {
                    "item_id": item_id, "output_index": output_index,
                    "content_index": content_index,
                    "part": {"type": "output_text", "text": text, "annotations": []},
                }, seq)); seq += 1
                if text:
                    events.append(_sse_event("response.output_text.delta", {
                        "item_id": item_id, "output_index": output_index,
                        "content_index": content_index, "delta": text,
                    }, seq)); seq += 1
                    events.append(_sse_event("response.output_text.done", {
                        "item_id": item_id, "output_index": output_index,
                        "content_index": content_index, "text": text,
                    }, seq)); seq += 1
                events.append(_sse_event("response.content_part.done", {
                    "item_id": item_id, "output_index": output_index,
                    "content_index": content_index,
                    "part": {"type": "output_text", "text": text, "annotations": []},
                }, seq)); seq += 1
                content_index += 1
            events.append(_sse_event("response.output_item.done", {
                "item_id": item_id, "output_index": output_index, "item": item,
            }, seq)); seq += 1
            output_index += 1

        elif item["type"] in ("function_call", "custom_tool_call"):
            tool_evts, seq = _tool_item_events(item, output_index, seq)
            events.extend(tool_evts)
            output_index += 1

    events.append(_sse_event("response.completed", {"response": resp}, seq)); seq += 1
    events.append("data: [DONE]\n\n")
    return events


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------


@router.post("/v1/responses", tags=["OpenAI - Responses"])
async def create_response(request: Request) -> Any:
    """OpenAI-compatible Responses API endpoint."""
    body = await request.json()
    body["_client_identity"] = getattr(request.state, "client_identity", "anonymous")

    if request.headers.get("X-Conversation-Id") and not body.get("conversation_id"):
        body["conversation_id"] = request.headers["X-Conversation-Id"]
    if request.headers.get("X-DeepSeek-Chat-Session-Id") and not body.get("chat_session_id"):
        body["chat_session_id"] = request.headers["X-DeepSeek-Chat-Session-Id"]

    stream = bool(body.get("stream", False))
    chat_body = _responses_to_chat_request(body)

    try:
        resolve_tool_policy(chat_body)
        tool_mode = has_tools(chat_body)
    except ToolProtocolError as exc:
        raise AppError(str(exc), exc.status, exc.code) from exc

    proxy_dir = get_proxy_directory()
    proxy = proxy_dir.get_proxy()

    account_id = (
        request.headers.get("X-DeepSeek-Account-Id")
        or request.headers.get("X-Account-Id")
        or body.get("account_id")
    )
    pow_response = (
        request.headers.get("X-Ds-Pow-Response")
        or body.get("pow_response", "")
    )

    chat_body["_queue_timeout_seconds"] = config.get_int(
        "account.queue.stream_timeout_seconds" if stream else "account.queue.nonstream_timeout_seconds",
        300 if stream else 30,
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

    async def invoke_validated_tool(payload: dict[str, Any]) -> dict[str, Any]:
        result = await invoke(payload)
        if not isinstance(result, dict):
            raise ToolProtocolError("buffered tool request unexpectedly returned a stream")

        def _msg(r):
            return ((r.get("choices") or [{}])[0].get("message") or {})

        try:
            _m = _msg(result)
            logger.info("[calls-diag] BEFORE content={!r}", str(_m.get("content"))[:1800])
        except Exception:
            pass

        async def retry_tool_call(repair_body: dict[str, Any]) -> dict[str, Any]:
            retry_result = await invoke(repair_body)
            if not isinstance(retry_result, dict):
                raise ToolProtocolError("tool protocol repair unexpectedly returned a stream")
            try:
                logger.info("[calls-diag] RETRY content={!r}", str(_msg(retry_result).get("content"))[:1800])
            except Exception:
                pass
            return retry_result

        try:
            result, _ = await convert_openai_tool_response_with_repair(
                result, payload, retry_tool_call,
            )
        except Exception as _exc:
            logger.info("[calls-diag] FINAL-FAIL {}: {}", type(_exc).__name__, _exc)
            raise
        try:
            _m = _msg(result)
            logger.info("[calls-diag] AFTER content={!r} tool_calls={}", str(_m.get("content"))[:300],
                        [(c.get("function") or {}).get("name") for c in (_m.get("tool_calls") or [])])
        except Exception:
            pass
        return result

    # Common IDs for consistency across all events
    resp_id = f"resp_{uuid.uuid4().hex[:24]}"
    model_name = str(chat_body.get("model") or "deepseek-v4-flash")
    created_at = int(time.time())

    # --- Estimated prompt-cache accounting (Liangzidp Web exposes no cached count) ---
    input_tokens_est = _estimate_input_tokens(chat_body)
    usage_hint: dict[str, Any] = {
        "input_tokens": input_tokens_est,
        "cached_tokens": 0,
        "cache_write_tokens": 0,
    }
    _orchestrator = request.app.state.orchestrator
    _cache_session_key, _ = _orchestrator.session_key(chat_body)
    try:
        _prior = await _orchestrator.store.get_conversation(_cache_session_key)
        _prev_input = int((_prior or {}).get("checkpoint", {}).get("resp_last_input_tokens") or 0)
        _has_history = bool(
            _prior and _prior.get("state") == "active" and _prior.get("parent_message_id") is not None
        )
        _cached, _written = _cache_accounting(input_tokens_est, _prev_input if _has_history else 0)
        usage_hint["cached_tokens"] = _cached
        usage_hint["cache_write_tokens"] = _written
    except Exception:
        pass

    async def _persist_cache_checkpoint() -> None:
        try:
            _store = _orchestrator.store
            _cur = await _store.get_conversation(_cache_session_key)
            if not _cur:
                return
            _ckpt = dict(_cur.get("checkpoint") or {})
            _ckpt["resp_last_input_tokens"] = input_tokens_est
            await _store.update_conversation(_cache_session_key, checkpoint=_ckpt)
        except Exception:
            pass

    def _error_events(exc: Exception) -> list[str]:
        if isinstance(exc, ToolProtocolError):
            payload = AppError(str(exc), exc.status, exc.code).to_dict()
        elif isinstance(exc, LiangzidpAPIError):
            payload = AppError(exc.message, exc.status_code, exc.code).to_dict()
        elif isinstance(exc, AppError):
            payload = exc.to_dict()
        else:
            logger.error("responses stream error: error={}", exc)
            payload = AppError(str(exc), 500).to_dict()
        failed_resp = _build_response_object(resp_id, model_name, created_at, "failed", [])
        failed_resp["error"] = payload.get("error", payload)
        seq = 0
        evts = [
            _sse_event("response.created", {"response": _build_response_object(
                resp_id, model_name, created_at, "in_progress", [], None,
            )}, seq),
        ]
        seq += 1
        evts.append(_sse_event("response.failed", {"response": failed_resp}, seq))
        evts.append("data: [DONE]\n\n")
        return evts

    # --- Streaming (real progressive) ---
    if stream:
        # For tool mode, use buffered approach (tools need full response)
        if tool_mode:
            buffered_body = {**chat_body, "stream": False}

            async def buffered_tool_source():
                yield await request.app.state.orchestrator.run_singleflight(
                    buffered_body, lambda: invoke_validated_tool(buffered_body),
                )

            async def tool_responses_stream():
                seq = 0
                yield _sse_event("response.created", {"response": _build_response_object(
                    resp_id, model_name, created_at, "in_progress", [], None,
                )}, seq); seq += 1
                yield _sse_event("response.in_progress", {"response": _build_response_object(
                    resp_id, model_name, created_at, "in_progress", [], None,
                )}, seq); seq += 1
                try:
                    async for item in stream_with_keepalive(buffered_tool_source()):
                        if item is None:
                            yield ": keep-alive\n\n"
                            continue
                        for event in _responses_sse_events(
                            item, resp_id, model_name, created_at, skip_initial=True,
                            freeform_names=set(chat_body.get("_freeform_tools") or []),
                            custom_names=set(chat_body.get("_custom_tools") or []),
                            usage_hint=usage_hint,
                        ):
                            yield event
                        await _persist_cache_checkpoint()
                except Exception as exc:
                    for event in _error_events(exc):
                        yield event

            return StreamingResponse(
                tool_responses_stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
            )

        # Plain streaming: real progressive conversion
        async def progressive_stream():
            # Send initial events immediately
            seq = 0
            yield _sse_event("response.created", {"response": _build_response_object(
                resp_id, model_name, created_at, "in_progress", [], None,
            )}, seq); seq += 1
            yield _sse_event("response.in_progress", {"response": _build_response_object(
                resp_id, model_name, created_at, "in_progress", [], None,
            )}, seq); seq += 1

            item_id = f"msg_{uuid.uuid4().hex[:24]}"
            output_index = 0
            content_index = 0
            reasoning_index = 0
            item_added = False
            content_added = False
            reasoning_added = False
            full_text = ""
            full_reasoning = ""
            finish_reason = "stop"
            tool_calls_accum: list[dict[str, Any]] = []
            usage_info = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

            try:
                stream_result = await request.app.state.orchestrator.run_singleflight(
                    chat_body, lambda: invoke(chat_body),
                )

                # Consume upstream stream with real in_progress heartbeat (not just SSE comments)
                # This prevents Cloudflare Tunnel / proxies from closing idle connections
                upstream_iter = stream_result.__aiter__()
                while True:
                    try:
                        raw = await asyncio.wait_for(upstream_iter.__anext__(), timeout=15.0)
                    except asyncio.TimeoutError:
                        # No data for 15s - send a real in_progress event to keep connection alive
                        yield _sse_event("response.in_progress", {"response": _build_response_object(
                            resp_id, model_name, created_at, "in_progress", [], None,
                        )}, seq); seq += 1
                        continue
                    except StopAsyncIteration:
                        break

                    # Parse SSE chunk(s)
                    raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                    for block in raw_str.split("\n\n"):
                        if not block.strip():
                            continue
                        data_lines = [
                            line[5:].lstrip() for line in block.splitlines()
                            if line.startswith("data:")
                        ]
                        data = "\n".join(data_lines)
                        if not data or data == "[DONE]":
                            continue
                        try:
                            event = json.loads(data)
                        except json.JSONDecodeError:
                            continue

                        choice = (event.get("choices") or [{}])[0]
                        delta = choice.get("delta") or {}

                        # Accumulate usage if present
                        if event.get("usage"):
                            u = event["usage"]
                            usage_info["prompt_tokens"] = int(u.get("prompt_tokens") or 0)
                            usage_info["completion_tokens"] = int(u.get("completion_tokens") or 0)
                            usage_info["total_tokens"] = int(u.get("total_tokens") or 0)

                        if choice.get("finish_reason"):
                            finish_reason = str(choice["finish_reason"])

                        # Handle tool calls in delta
                        if delta.get("tool_calls"):
                            for tc in delta["tool_calls"]:
                                idx = tc.get("index", len(tool_calls_accum))
                                while len(tool_calls_accum) <= idx:
                                    tool_calls_accum.append({
                                        "id": "", "type": "function",
                                        "function": {"name": "", "arguments": ""},
                                    })
                                if tc.get("id"):
                                    tool_calls_accum[idx]["id"] = tc["id"]
                                fn = tc.get("function") or {}
                                if fn.get("name"):
                                    tool_calls_accum[idx]["function"]["name"] = fn["name"]
                                if fn.get("arguments"):
                                    tool_calls_accum[idx]["function"]["arguments"] += fn["arguments"]

                        # Handle reasoning content delta (keeps stream alive during thinking)
                        reasoning_delta = delta.get("reasoning_content")
                        if reasoning_delta:
                            if not item_added:
                                yield _sse_event("response.output_item.added", {
                                    "item_id": item_id, "output_index": output_index,
                                    "item": {
                                        "id": item_id, "type": "message", "role": "assistant",
                                        "status": "in_progress", "content": [],
                                    },
                                }, seq); seq += 1
                                item_added = True
                            if not reasoning_added:
                                yield _sse_event("response.content_part.added", {
                                    "item_id": item_id, "output_index": output_index,
                                    "content_index": reasoning_index,
                                    "part": {"type": "reasoning", "text": ""},
                                }, seq); seq += 1
                                reasoning_added = True
                            full_reasoning += str(reasoning_delta)
                            yield _sse_event("response.reasoning.delta", {
                                "item_id": item_id, "output_index": output_index,
                                "content_index": reasoning_index,
                                "delta": str(reasoning_delta),
                            }, seq); seq += 1

                        # Handle text content delta
                        text_delta = delta.get("content")
                        if text_delta:
                            if not item_added:
                                yield _sse_event("response.output_item.added", {
                                    "item_id": item_id, "output_index": output_index,
                                    "item": {
                                        "id": item_id, "type": "message", "role": "assistant",
                                        "status": "in_progress", "content": [],
                                    },
                                }, seq); seq += 1
                                item_added = True
                            if not content_added:
                                yield _sse_event("response.content_part.added", {
                                    "item_id": item_id, "output_index": output_index,
                                    "content_index": content_index,
                                    "part": {"type": "output_text", "text": "", "annotations": []},
                                }, seq); seq += 1
                                content_added = True
                            full_text += str(text_delta)
                            yield _sse_event("response.output_text.delta", {
                                "item_id": item_id, "output_index": output_index,
                                "content_index": content_index,
                                "delta": str(text_delta),
                            }, seq); seq += 1
                            yield _sse_event("response.output_text.done", {
                                "item_id": item_id, "output_index": output_index,
                                "content_index": content_index, "text": full_text,
                            }, seq); seq += 1

                # Stream finished - send completion events
                if reasoning_added:
                    yield _sse_event("response.content_part.done", {
                        "item_id": item_id, "output_index": output_index,
                        "content_index": reasoning_index,
                        "part": {"type": "reasoning", "text": full_reasoning},
                    }, seq); seq += 1
                if content_added:
                    yield _sse_event("response.content_part.done", {
                        "item_id": item_id, "output_index": output_index,
                        "content_index": content_index,
                        "part": {"type": "output_text", "text": full_text, "annotations": []},
                    }, seq); seq += 1
                if item_added:
                    msg_content = []
                    if reasoning_added:
                        msg_content.append({"type": "reasoning", "text": full_reasoning})
                    if content_added:
                        msg_content.append({"type": "output_text", "text": full_text, "annotations": []})
                    yield _sse_event("response.output_item.done", {
                        "item_id": item_id, "output_index": output_index,
                        "item": {
                            "id": item_id, "type": "message", "role": "assistant",
                            "status": "completed", "content": msg_content,
                        },
                    }, seq); seq += 1

                # Handle tool calls in output
                ff_names = set(chat_body.get("_freeform_tools") or [])
                ct_names = set(chat_body.get("_custom_tools") or [])
                fc_output_index = output_index + 1 if item_added else 0
                for tc in tool_calls_accum:
                    if not tc["function"]["name"]:
                        continue
                    tool_item = _make_tool_output_item(tc, ff_names, ct_names)
                    tool_evts, seq = _tool_item_events(tool_item, fc_output_index, seq)
                    for ev in tool_evts:
                        yield ev
                    fc_output_index += 1

                # Build final output array for completed event
                final_output: list[dict[str, Any]] = []
                if item_added:
                    final_content = []
                    if reasoning_added:
                        final_content.append({"type": "reasoning", "text": full_reasoning})
                    if content_added:
                        final_content.append({"type": "output_text", "text": full_text, "annotations": []})
                    final_output.append({
                        "id": item_id, "type": "message", "role": "assistant",
                        "status": "completed", "content": final_content,
                    })
                for tc in tool_calls_accum:
                    if not tc["function"]["name"]:
                        continue
                    final_output.append(_make_tool_output_item(tc, ff_names, ct_names))

                status = "completed" if finish_reason != "length" else "incomplete"
                _prog_reason = estimate_tokens(full_reasoning)
                _prog_out = estimate_tokens(full_text) + _prog_reason
                for _tc in tool_calls_accum:
                    _prog_out += estimate_tokens(json.dumps(_tc, ensure_ascii=False))
                completed_resp = _build_response_object(
                    resp_id, model_name, created_at, status, final_output,
                    usage={
                        "input_tokens": int(usage_hint["input_tokens"]),
                        "input_tokens_details": {
                            "cached_tokens": int(usage_hint["cached_tokens"]),
                            "cache_write_tokens": int(usage_hint["cache_write_tokens"]),
                        },
                        "output_tokens": _prog_out,
                        "output_tokens_details": {"reasoning_tokens": _prog_reason},
                        "total_tokens": int(usage_hint["input_tokens"]) + _prog_out,
                    },
                )
                await _persist_cache_checkpoint()
                yield _sse_event("response.completed", {"response": completed_resp}, seq); seq += 1
                yield "data: [DONE]\n\n"

            except Exception as exc:
                for event in _error_events(exc):
                    yield event

        return StreamingResponse(
            progressive_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    # --- Non-streaming ---
    result = await request.app.state.orchestrator.run_singleflight(
        chat_body,
        lambda: invoke_validated_tool(chat_body) if tool_mode else invoke(chat_body),
    )
    responses_result = _chat_to_responses_response(
        result, resp_id=resp_id,
        freeform_names=set(chat_body.get("_freeform_tools") or []),
        custom_names=set(chat_body.get("_custom_tools") or []),
        usage_hint=usage_hint,
    )
    await _persist_cache_checkpoint()
    return JSONResponse(
        content=responses_result,
        media_type="application/json; charset=utf-8",
    )
