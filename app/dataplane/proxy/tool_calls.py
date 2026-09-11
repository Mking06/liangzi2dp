from __future__ import annotations

import asyncio
import copy
import html
import json
import os
import re
import time
import uuid

# Liangzidp DSML (Liangzidp Markup Language) section markers.
# Must be stripped from client-visible text; matches one-or-more
# pipe delimiters with arbitrary whitespace.
_DSML_MARKER_RE = re.compile(r"<[^>]*DSML[^>]*>", re.IGNORECASE)
# Liangzidp wraps structural tags with pipe-separated DSML prefixes, using ASCII
# or full-width pipes, e.g.:
#   <｜｜DSML｜｜invoke name="exec">   ->  <invoke name="exec">
#   </｜｜DSML｜｜tool_calls>          ->  </tool_calls>
# Normalize them to standard XML so ToolSieve parses invoke/parameter natively.
_DSML_SEP = r"[|\u2502\uff5c\u2223\s]*"
_DSML_TAG_RE = re.compile(
    r"<\s*(/)?\s*" + _DSML_SEP + r"DSML" + _DSML_SEP +
    r"([a-zA-Z_][a-zA-Z0-9_:-]*)((?:[^>]*?))\s*>",
    re.IGNORECASE,
)


def _dsml_tag_replacer(m: "re.Match[str]") -> str:
    closing = m.group(1) or ""
    tag = (m.group(2) or "").strip()
    attrs = m.group(3) or ""
    if not tag:
        return ""  # bare section marker without a tag name -> drop
    return f"<{closing}{tag}{attrs}>"


def _strip_dsml(text: str) -> str:
    """Normalize Liangzidp DSML markup.

    DSML-prefixed structural tags (invoke / parameter / tool_calls) are rewritten
    to standard XML tags so the tool parser understands them; any remaining bare
    DSML section marker (no recognizable tag) is removed.
    """
    if not text:
        return text or ""
    text = _DSML_TAG_RE.sub(_dsml_tag_replacer, text)
    text = _DSML_MARKER_RE.sub("", text)
    return text

from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, AsyncGenerator, AsyncIterable


_CAPTURE_PAIRS = (
    ("<tool_calls", "</tool_calls>"),
    ("<function_calls", "</function_calls>"),
    ("<tool_call", "</tool_call>"),
    ("<function_call", "</function_call>"),
    ("<invoke", "</invoke>"),
    ("<tool_use", "</tool_use>"),
    ("<calls", "</calls>"),
)
_CALL_RE = re.compile(
    r"<(?:[a-z0-9_:-]+:)?(?:tool_call|function_call|invoke|tool_use|call)\b([^>]*)>"
    r"([\s\S]*?)</(?:[a-z0-9_:-]+:)?(?:tool_call|function_call|invoke|tool_use|call)>",
    re.I,
)
_SELF_CLOSING_INVOKE_RE = re.compile(r"<(?:[a-z0-9_:-]+:)?invoke\b([^>]*)/>", re.I)
_NAME_RES = (
    re.compile(r"<(?:[a-z0-9_:-]+:)?tool_name\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?tool_name>", re.I),
    re.compile(r"<(?:[a-z0-9_:-]+:)?function_name\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?function_name>", re.I),
    re.compile(r"<(?:[a-z0-9_:-]+:)?name\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?name>", re.I),
)
_ARGS_RES = tuple(
    re.compile(
        rf"<(?:[a-z0-9_:-]+:)?{name}\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?{name}>",
        re.I,
    )
    for name in ("parameters", "arguments", "input", "args", "params")
)
# Lenient fallbacks: Liangzidp sometimes opens a plural wrapper (<parameters>)
# but closes it with the singular form (</parameter>). Match open/close tag
# names independently so such generations still parse. Strict matchers run
# first; these are used only when strict matching finds nothing.
_ARG_TAG_NAMES = r"parameters|parameter|arguments|argument|input|args|params|param"
_NAME_TAG_NAMES = r"tool_name|function_name|name"
_ARGS_RELAXED_RE = re.compile(
    r"<(?:[a-z0-9_:-]+:)?(?:" + _ARG_TAG_NAMES + r")\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?(?:" + _ARG_TAG_NAMES + r")>",
    re.I,
)
_NAME_RELAXED_RE = re.compile(
    r"<(?:[a-z0-9_:-]+:)?(?:" + _NAME_TAG_NAMES + r")\b[^>]*>([\s\S]*?)</(?:[a-z0-9_:-]+:)?(?:" + _NAME_TAG_NAMES + r")>",
    re.I,
)
_ATTR_NAME_RE = re.compile(r"(?:name|function|tool)\s*=\s*[\"']([^\"']+)[\"']", re.I)
# Hermes-style parameter element: <parameter name="foo" type="string">value</parameter>
_PARAM_EL_RE = re.compile(
    r"<parameter\b([^>]*)>([\s\S]*?)</parameter\s*>",
    re.I,
)
_PARAM_NAME_RE = re.compile(r"name\s*=\s*[\"']([^\"']+)[\"']", re.I)
_PARAM_TYPE_RE = re.compile(r"type\s*=\s*[\"']([^\"']+)[\"']", re.I)
_CONTINUATION_ONLY = frozenset({
    "continue",
    "continueplease",
    "goon",
    "retry",
    "tryagain",
    "\u7ee7\u7eed",
    "\u7ee7\u7eed\u5427",
    "\u7ee7\u7eed\u554a",
    "\u8bf7\u7ee7\u7eed",
    "\u91cd\u8bd5",
    "\u518d\u8bd5",
})
_PENDING_ACTION_MARKERS = (
    "i will ",
    "i'll ",
    "let me ",
    "next i ",
    "continuing with ",
    "\u73b0\u5728\u7ee7\u7eed",
    "\u63a5\u4e0b\u6765",
    "\u8fd9\u5c31",
    "\u8ba9\u6211",
    "\u5148\u628a",
    "\u5148\u6765",
    "\u7ee7\u7eed\u521b\u5efa",
    "\u7ee7\u7eed\u68c0\u67e5",
    "\u7ee7\u7eed\u8bfb\u53d6",
    "\u7ee7\u7eed\u6267\u884c",
    "\u7ee7\u7eed\u4fee\u6539",
    "\u7ee7\u7eed\u6574\u5408",
    "\u7ee7\u7eed\u63d0\u4ea4",
    "\u7ee7\u7eed\u626b\u63cf",
    "\u7ee7\u7eed\u6d4b\u8bd5",
)


class ToolProtocolError(ValueError):
    def __init__(self, message: str, status: int = 422, code: str = "tool_protocol_error"):
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class ToolPolicy:
    mode: str
    definitions: dict[str, dict[str, Any]]
    allowed_names: tuple[str, ...]
    forced_name: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.allowed_names) and self.mode != "none"


def _tool_function(tool: Any) -> dict[str, Any] | None:
    if not isinstance(tool, dict):
        return None
    fn = tool.get("function") if tool.get("type") == "function" else tool
    return fn if isinstance(fn, dict) else None


def _tool_definitions(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    definitions: dict[str, dict[str, Any]] = {}
    for tool in body.get("tools") or []:
        fn = _tool_function(tool)
        if fn and fn.get("name"):
            name = str(fn["name"]).strip()
            definitions[name] = {
                "name": name,
                "description": str(fn.get("description") or ""),
                "parameters": fn.get("parameters") or fn.get("input_schema") or {"type": "object", "properties": {}},
                "strict": bool(fn.get("strict")),
            }
    for fn in body.get("functions") or []:
        if isinstance(fn, dict) and fn.get("name"):
            name = str(fn["name"]).strip()
            definitions[name] = {
                "name": name,
                "description": str(fn.get("description") or ""),
                "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
                "strict": bool(fn.get("strict")),
            }
    return definitions


def resolve_tool_policy(body: dict[str, Any]) -> ToolPolicy:
    definitions = _tool_definitions(body)
    names = tuple(definitions)
    choice = body.get("tool_choice")
    if not definitions:
        if choice not in (None, "none"):
            raise ToolProtocolError("tool_choice requires non-empty tools", 400, "invalid_request_error")
        return ToolPolicy("none", definitions, ())

    if choice is None or choice == "auto":
        return ToolPolicy("auto", definitions, names)
    if choice == "none":
        return ToolPolicy("none", definitions, ())
    if isinstance(choice, str) and choice in {"required", "any"}:
        return ToolPolicy("required", definitions, names)
    if isinstance(choice, dict):
        choice_type = str(choice.get("type") or "function")
        if choice_type not in {"function", "tool"}:
            raise ToolProtocolError(f"unsupported tool_choice type: {choice_type}", 400, "invalid_request_error")
        fn = choice.get("function") if isinstance(choice.get("function"), dict) else choice
        forced = str(fn.get("name") or "").strip()
        if not forced or forced not in definitions:
            raise ToolProtocolError(f"tool_choice references undeclared tool: {forced or '-'}", 400, "invalid_request_error")
        return ToolPolicy("forced", definitions, (forced,), forced)
    raise ToolProtocolError(f"unsupported tool_choice: {choice}", 400, "invalid_request_error")


def openai_tool_names(body: dict[str, Any]) -> set[str]:
    return set(_tool_definitions(body))


def anthropic_tool_names(body: dict[str, Any]) -> set[str]:
    return openai_tool_names(body)


def has_tools(body: dict[str, Any]) -> bool:
    return resolve_tool_policy(body).enabled


def is_continuation_only(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    compact = re.sub(r"[\W_]+", "", value.casefold(), flags=re.UNICODE)
    return compact in _CONTINUATION_ONLY


def _latest_user_text(body: dict[str, Any]) -> str:
    for item in reversed(body.get("messages") or []):
        if not isinstance(item, dict) or item.get("role") != "user":
            continue
        content = item.get("content")
        if isinstance(content, list):
            return "\n".join(
                str(block.get("text") or block.get("content") or "")
                for block in content
                if isinstance(block, dict)
            )
        return str(content or "")
    return ""


def _is_progress_only_continuation(content: str, body: dict[str, Any]) -> bool:
    if not is_continuation_only(_latest_user_text(body)):
        return False
    normalized = " ".join((content or "").strip().casefold().split())
    if not normalized or len(normalized) > 800:
        return False
    return any(marker in normalized for marker in _PENDING_ACTION_MARKERS)


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip() or "{}"
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def tool_prompt_for_request(body: dict[str, Any]) -> str:
    policy = resolve_tool_policy(body)
    if not policy.enabled:
        return ""
    schemas = [policy.definitions[name] for name in policy.allowed_names]
    instruction = [
        "[System] External tool mode is enabled.",
        "Decide whether a tool is needed. The application executes tools; you never execute them yourself.",
        "When calling tools, emit raw XML outside markdown fences using exactly this format:",
        "<tool_calls>",
        "  <tool_call>",
        "    <tool_name>TOOL_NAME</tool_name>",
        "    <parameters>{\"schema_field\":\"value\"}</parameters>",
        "  </tool_call>",
        "</tool_calls>",
        "The parameters element must contain one strict JSON object with double-quoted keys.",
        "Use only declared tool names and exact schema fields. Multiple calls belong in one tool_calls block.",
        _required_arguments_instruction(policy),
        "After a tool result, either call another tool with the same format or provide the final answer.",
        "Never repeat a completed tool call unless its result explicitly says it failed and retry is necessary.",
        "A tool call is not a tool result. If no Tool result message follows a prior call, call the tool again; "
        "never infer, summarize, or invent its output.",
    ]
    if policy.mode == "required":
        instruction.append("For this response, at least one tool call is required.")
    elif policy.mode == "forced":
        instruction.append(f"For this response, call the tool {policy.forced_name}.")
    instruction.append("Available tools:\n" + json.dumps(schemas, ensure_ascii=False, separators=(",", ":")))
    return "\n".join(instruction)


def _required_arguments_instruction(policy: ToolPolicy) -> str:
    signatures: list[str] = []
    for name in policy.allowed_names:
        schema = policy.definitions[name].get("parameters")
        required = schema.get("required") if isinstance(schema, dict) else None
        if required:
            signatures.append(f"{name}({', '.join(str(item) for item in required)})")
    if not signatures:
        return "Tools without required fields still require one valid JSON object in parameters."
    return (
        "Required top-level arguments: " + "; ".join(signatures) + ". "
        "Never emit an empty parameters object for a tool with required arguments."
    )


def tool_prompt_guard_for_request(body: dict[str, Any]) -> str:
    """Repeat the tool wire contract after the conversation.

    The full schemas stay at the prompt prefix so upstream prefix caching still
    has a stable target. Long client system prompts can otherwise override that
    early instruction, so this compact suffix makes the required wire format
    the final instruction seen by the model.
    """
    policy = resolve_tool_policy(body)
    if not policy.enabled:
        return ""
    names = ", ".join(policy.allowed_names)
    requirement = (
        "You must call a tool in this response."
        if policy.mode in {"required", "forced"}
        else "Call a tool when the request requires external execution or observation."
    )
    if policy.mode == "forced":
        requirement = f"You must call {policy.forced_name} in this response."
    return (
        "[System tool contract - final and authoritative]\n"
        f"Allowed tools: {names}. {requirement}\n"
        f"{_required_arguments_instruction(policy)}\n"
        "A tool call is valid only as raw <tool_calls><tool_call><tool_name>NAME</tool_name>"
        "<parameters>{strict JSON object}</parameters></tool_call></tool_calls>.\n"
        "Never simulate execution with labels such as Bash, IN, OUT, Calling, Web Fetch, or code blocks. "
        "Never claim a tool result before the application returns one. A prior tool call without a following "
        "Tool result message has not run; call it again instead of inventing output. "
        "When the user says continue, retry, or go on, resume the actual work: call the required tool or return "
        "a concrete completed result, never progress narration by itself."
    )


def assistant_tool_history(message: dict[str, Any]) -> str:
    blocks: list[str] = []
    reasoning = message.get("reasoning_content")
    if reasoning:
        blocks.append(f"Assistant reasoning:\n{reasoning}")
    content = message.get("content")
    if content:
        blocks.append(f"Assistant response:\n{content}")
    calls = message.get("tool_calls") or []
    rendered: list[str] = []
    for call in calls:
        if not isinstance(call, dict):
            continue
        fn = _tool_function(call) or {}
        name = str(fn.get("name") or call.get("name") or "").strip()
        if not name:
            continue
        call_id = str(call.get("id") or "")
        args = _json_text(fn.get("arguments", call.get("input", {})))
        rendered.append(
            "<tool_call>\n"
            f"  <call_id>{html.escape(call_id)}</call_id>\n"
            f"  <tool_name>{html.escape(name)}</tool_name>\n"
            f"  <parameters><![CDATA[{args.replace(']]>', ']]]]><![CDATA[>')}]]></parameters>\n"
            "</tool_call>"
        )
    if rendered:
        blocks.append("<tool_calls>\n" + "\n".join(rendered) + "\n</tool_calls>")
    return "\n\n".join(blocks)


def _decode_xml_text(value: str) -> str:
    raw = (value or "").strip()
    match = re.fullmatch(r"<!\[CDATA\[([\s\S]*?)]]>", raw, re.I)
    return html.unescape(match.group(1) if match else raw)


def _find_tag_value(text: str, patterns: tuple[re.Pattern[str], ...]) -> str:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return _decode_xml_text(match.group(1))
    return ""


def _validate_schema(value: Any, schema: Any, path: str = "arguments") -> None:
    if not isinstance(schema, dict) or not schema:
        return
    if "anyOf" in schema:
        errors: list[str] = []
        for candidate in schema.get("anyOf") or []:
            try:
                _validate_schema(value, candidate, path)
                return
            except ToolProtocolError as exc:
                errors.append(str(exc))
        raise ToolProtocolError(f"{path} does not match any allowed schema: {'; '.join(errors[:2])}")
    expected = schema.get("type")
    type_ok = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }
    if isinstance(expected, list):
        if not any(type_ok.get(item, True) for item in expected):
            raise ToolProtocolError(f"{path} has the wrong type; expected one of {expected}")
    elif expected and not type_ok.get(expected, True):
        raise ToolProtocolError(f"{path} has the wrong type; expected {expected}")
    if "enum" in schema and value not in schema["enum"]:
        raise ToolProtocolError(f"{path} must be one of {schema['enum']}")
    if isinstance(value, dict):
        properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        missing = [name for name in schema.get("required") or [] if name not in value]
        if missing:
            raise ToolProtocolError(f"{path} is missing required fields: {', '.join(missing)}")
        if schema.get("additionalProperties") is False:
            extras = [name for name in value if name not in properties]
            if extras:
                raise ToolProtocolError(f"{path} has undeclared fields: {', '.join(extras)}")
        for name, item in value.items():
            if name in properties:
                _validate_schema(item, properties[name], f"{path}.{name}")
    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for index, item in enumerate(value):
            _validate_schema(item, schema["items"], f"{path}[{index}]")
    if isinstance(value, str) and schema.get("pattern"):
        if not re.search(str(schema["pattern"]), value):
            raise ToolProtocolError(f"{path} does not match required pattern")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ToolProtocolError(f"{path} is below minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise ToolProtocolError(f"{path} exceeds maximum {schema['maximum']}")


def _hermes_params_to_json(inner: str) -> str | None:
    """Convert Hermes-style <parameter name=...>value</parameter> to a JSON object.

    Hermes (and some other clients) emit tool calls as:
      <tool_call name="skill_view">
        <parameter name="name">script-breakdown-a</parameter>
      </tool_call>
    This builds a JSON object {"name":"script-breakdown-a", ...}, preserving
    simple scalar types via the optional type attribute. Returns None when the
    block has no <parameter> elements (so callers fall back to normal parsing).
    """
    params = list(_PARAM_EL_RE.finditer(inner or ""))
    if not params:
        return None
    obj: dict[str, Any] = {}
    for m in params:
        attr, raw = m.group(1), m.group(2)
        key = _PARAM_NAME_RE.search(attr or "")
        if not key:
            continue
        name = key.group(1).strip()
        type_attr = _PARAM_TYPE_RE.search(attr or "")
        val = raw.strip()
        t = (type_attr.group(1).lower() if type_attr else "string")
        if t == "integer" or t == "int":
            try:
                obj[name] = int(val)
            except (TypeError, ValueError):
                obj[name] = val
        elif t in ("number", "float", "double"):
            try:
                obj[name] = float(val)
            except (TypeError, ValueError):
                obj[name] = val
        elif t in ("boolean", "bool"):
            obj[name] = val.lower() in {"1", "true", "yes", "on"}
        elif t in ("object", "array", "json"):
            try:
                obj[name] = json.loads(val)
            except json.JSONDecodeError:
                obj[name] = val
        else:
            obj[name] = val
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _normalize_hermes_call(attrs: str, inner: str) -> str | None:
    """Rewrite a Hermes-style attribute call into gateway canonical form.

    Returns canonical XML (tool_name + parameters JSON) when the block uses
    ``<tool_call name=...>`` plus ``<parameter name=...>`` children; otherwise
    None so existing parsing logic proceeds unchanged.
    """
    if not re.search(r"\b(?:name|function|tool)\s*=", attrs or ""):
        return None
    name_match = _ATTR_NAME_RE.search(attrs or "")
    if not name_match:
        return None
    name = name_match.group(1).strip()
    if not name:
        return None
    json_args = _hermes_params_to_json(inner)
    if json_args is None:
        return None
    return (
        "<tool_call>"
        f"<tool_name>{name}</tool_name>"
        f"<parameters>{json_args}</parameters>"
        "</tool_call>"
    )


def _parse_call(attrs: str, inner: str, policy: ToolPolicy) -> dict[str, Any]:
    # Hermes-style: <tool_call name="X"><parameter name="a">v</parameter></tool_call>
    canonical = _normalize_hermes_call(attrs, inner)
    if canonical is not None:
        m = _CALL_RE.search(canonical)
        if m:
            attrs, inner = m.group(1), m.group(2)

    name_match = _ATTR_NAME_RE.search(attrs or "")
    if name_match:
        name = name_match.group(1)
    else:
        name = _find_tag_value(inner, _NAME_RES)
        if not name:
            _rm = _NAME_RELAXED_RE.search(inner or "")
            if _rm:
                name = _decode_xml_text(_rm.group(1))
    name = name.strip()
    if not name:
        raise ToolProtocolError("tool call is missing tool_name")
    if name not in policy.allowed_names:
        raise ToolProtocolError(f"model requested undeclared tool: {name}")
    raw_args = _find_tag_value(inner, _ARGS_RES)
    if not raw_args:
        _am = _ARGS_RELAXED_RE.search(inner or "")
        if _am:
            raw_args = _decode_xml_text(_am.group(1))
    raw_args = raw_args or "{}"
    try:
        arguments = json.loads(raw_args)
    except json.JSONDecodeError as exc:
        raise ToolProtocolError(f"tool {name} returned invalid JSON arguments: {exc.msg}") from exc
    if not isinstance(arguments, dict):
        raise ToolProtocolError(f"tool {name} arguments must be a JSON object")
    # Some Liangzidp generations mirror the XML field name inside JSON, e.g.
    # <parameters>{"parameters":{"command":"dir"}}</parameters>.
    # Unwrap only this exact single-key shape, then apply the normal schema.
    for wrapper in ("parameters", "arguments", "input", "args", "params"):
        if set(arguments) == {wrapper} and isinstance(arguments[wrapper], dict):
            arguments = arguments[wrapper]
            break
    _validate_schema(arguments, policy.definitions[name].get("parameters"), f"{name}.arguments")
    return {
        "id": "call_" + uuid.uuid4().hex,
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments, ensure_ascii=False, separators=(",", ":")),
        },
    }


def _parse_tool_block(block: str, policy: ToolPolicy) -> list[dict[str, Any]]:
    calls = [_parse_call(match.group(1), match.group(2), policy) for match in _CALL_RE.finditer(block)]
    calls.extend(_parse_call(match.group(1), "", policy) for match in _SELF_CLOSING_INVOKE_RE.finditer(block))
    if not calls and _find_tag_value(block, _NAME_RES):
        # Some generations omit the inner <tool_call> wrapper but still emit
        # one unambiguous tool_name/parameters pair inside <tool_calls>.
        calls.append(_parse_call("", block, policy))
    if not calls:
        inner = re.sub(r"^<[^>]+>|</[^>]+>$", "", block.strip()).strip()
        try:
            structured = json.loads(html.unescape(inner))
        except json.JSONDecodeError:
            structured = None
        items = structured if isinstance(structured, list) else [structured]
        for item in items:
            if not isinstance(item, dict):
                continue
            function = item.get("function") if isinstance(item.get("function"), dict) else item
            name = str(
                function.get("name")
                or function.get("tool_name")
                or function.get("tool")
                or ""
            ).strip()
            arguments = (
                function.get("arguments")
                or function.get("parameters")
                or function.get("input")
                or {}
            )
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    continue
            if name not in policy.allowed_names or not isinstance(arguments, dict):
                continue
            _validate_schema(
                arguments,
                policy.definitions[name].get("parameters"),
                f"{name}.arguments",
            )
            calls.append({
                "id": "call_" + uuid.uuid4().hex,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": json.dumps(
                        arguments,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            })
    if not calls:
        # Lenient fallback for freeform tools (e.g. Codex "exec"): a model may
        # emit a bare command / source string inside <tool_calls> with no
        # tool_name/parameters tags. Prefer "exec", else the sole allowed tool.
        inner_raw = re.sub(r"^<[^>]+>|</[^>]+>$", "", block.strip()).strip()
        candidate = ""
        if inner_raw:
            if "exec" in policy.allowed_names:
                candidate = "exec"
            elif len(policy.allowed_names) == 1:
                candidate = next(iter(policy.allowed_names))
        if candidate:
            tool_schema = policy.definitions[candidate].get("parameters") or {}
            props = tool_schema.get("properties") if isinstance(tool_schema, dict) else None
            if isinstance(props, dict) and props:
                field = next(iter(props))
                arguments = {field: inner_raw}
            else:
                arguments = {"code": inner_raw}
            _validate_schema(arguments, tool_schema, f"{candidate}.arguments")
            calls.append({
                "id": "call_" + uuid.uuid4().hex,
                "type": "function",
                "function": {
                    "name": candidate,
                    "arguments": json.dumps(arguments, ensure_ascii=False, separators=(",", ":")),
                },
            })
    if not calls:
        raise ToolProtocolError("tool XML block did not contain a valid tool call")
    return calls


class ToolSieve:
    def __init__(self, policy: ToolPolicy):
        self.policy = policy
        self.capture = ""
        self.capturing = False
        self.emitted_text = ""
        self.pending = ""

    def _inside_fence(self, prefix: str) -> bool:
        return (self.emitted_text + prefix).count("```") % 2 == 1

    def _segment_start(self, text: str) -> int:
        lower = text.lower()
        offset = 0
        while offset < len(lower):
            matches = [(lower.find(open_tag, offset), open_tag) for open_tag, _ in _CAPTURE_PAIRS]
            matches = [(index, tag) for index, tag in matches if index >= 0]
            if not matches:
                return -1
            index, tag = min(matches, key=lambda item: item[0])
            if not self._inside_fence(text[:index]):
                return index
            offset = index + len(tag)
        return -1

    def _partial_start(self, text: str) -> int:
        index = text.rfind("<")
        if index < 0 or ">" in text[index:]:
            return -1
        tail = text[index:].lower()
        return index if any(open_tag.startswith(tail) for open_tag, _ in _CAPTURE_PAIRS) else -1

    def _consume_capture(self) -> tuple[bool, str, list[dict[str, Any]], str]:
        lower = self.capture.lower()
        complete: list[tuple[int, int, int]] = []
        waiting: tuple[int, int] | None = None
        for idx, (open_tag, close_tag) in enumerate(_CAPTURE_PAIRS):
            start = lower.find(open_tag)
            if start < 0:
                continue
            end = lower.rfind(close_tag)
            if end > start:
                complete.append((start, idx, end + len(close_tag)))
            elif waiting is None or (start, idx) < waiting:
                waiting = (start, idx)
        if not complete:
            # An opener is present but its close has not arrived yet (streaming),
            # or there is no recognizable wrapper at all.
            return (False, "", [], "") if waiting is not None else (True, self.capture, [], "")
        # Consume the OUTERMOST complete wrapper: earliest open tag; on a prefix
        # tie (e.g. <tool_call> vs <tool_calls>) prefer the earlier pair index.
        start, _idx, close_end = min(complete, key=lambda item: (item[0], item[1]))
        if waiting is not None and waiting < (start, _idx):
            # An even earlier wrapper is still unclosed -> keep buffering.
            return False, "", [], ""
        return (
            True,
            self.capture[:start],
            _parse_tool_block(self.capture[start:close_end], self.policy),
            self.capture[close_end:],
        )

    def _text_event(self, events: list[dict[str, Any]], text: str) -> None:
        if text:
            self.emitted_text += text
            events.append({"type": "text", "text": text})

    def _drain(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        while True:
            if self.capturing:
                if self.pending:
                    self.capture += self.pending
                    self.pending = ""
                ready, prefix, calls, suffix = self._consume_capture()
                if not ready:
                    break
                self.capture = ""
                self.capturing = False
                self._text_event(events, prefix)
                if calls:
                    events.append({"type": "tool_calls", "calls": calls})
                self.pending = suffix + self.pending
                continue
            if not self.pending:
                break
            start = self._segment_start(self.pending)
            if start >= 0:
                self._text_event(events, self.pending[:start])
                self.capture = self.pending[start:]
                self.pending = ""
                self.capturing = True
                continue
            partial = self._partial_start(self.pending)
            if partial >= 0 and not self._inside_fence(self.pending[:partial]):
                self._text_event(events, self.pending[:partial])
                self.pending = self.pending[partial:]
            else:
                self._text_event(events, self.pending)
                self.pending = ""
            break
        return events

    def push(self, chunk: str) -> list[dict[str, Any]]:
        self.pending += _strip_dsml(str(chunk or ""))
        return self._drain()

    def _lenient_close(self) -> tuple[str, list[dict[str, Any]]] | None:
        """Flush-time recovery for an outer tool block missing its close tag.

        Liangzidp occasionally opens ``<tool_calls>`` and emits one or more
        complete inner ``<tool_call>...</tool_call>`` calls but never writes the
        matching outer close (sometimes emitting a duplicated inner close
        instead). At end-of-stream we can still extract the complete inner
        calls instead of leaking the whole markup block as visible text.
        Returns None when no parseable inner call exists (caller keeps the old
        text passthrough behaviour).
        """
        lower = self.capture.lower()
        best = -1
        best_open = ""
        for open_tag, _ in _CAPTURE_PAIRS:
            idx = lower.find(open_tag)
            if idx >= 0 and (best < 0 or idx < best):
                best, best_open = idx, open_tag
        if best < 0:
            return None
        opener_end = lower.find(">", best + len(best_open))
        body = self.capture[opener_end + 1:] if opener_end >= 0 else self.capture[best:]
        try:
            calls = _parse_tool_block(body, self.policy)
        except ToolProtocolError:
            return None
        if not calls:
            return None
        return self.capture[:best], calls

    def flush(self) -> list[dict[str, Any]]:
        events = self._drain()
        if self.capturing:
            ready, prefix, calls, suffix = self._consume_capture()
            if ready:
                self._text_event(events, prefix)
                if calls:
                    events.append({"type": "tool_calls", "calls": calls})
                self._text_event(events, suffix)
            else:
                recovered = self._lenient_close()
                if recovered is not None:
                    r_prefix, r_calls = recovered
                    self._text_event(events, r_prefix)
                    if r_calls:
                        events.append({"type": "tool_calls", "calls": r_calls})
                else:
                    self._text_event(events, self.capture)
        self._text_event(events, self.pending)
        self.capture = ""
        self.pending = ""
        self.capturing = False
        return events


def _ensure_choice(policy: ToolPolicy, calls: list[dict[str, Any]]) -> None:
    if policy.mode == "required" and not calls:
        raise ToolProtocolError("tool_choice=required but the model did not produce a valid tool call")
    if policy.mode == "forced":
        names = {call["function"]["name"] for call in calls}
        if policy.forced_name not in names:
            raise ToolProtocolError(f"tool_choice requires tool {policy.forced_name}")


def extract_tool_aware_output(text: str, body: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    policy = resolve_tool_policy(body)
    if not policy.enabled:
        return _strip_dsml(text), []
    # Strip Liangzidp DSML markers from complete text
    text = _strip_dsml(text)
    sieve = ToolSieve(policy)
    events = [*sieve.push(text or ""), *sieve.flush()]
    content = "".join(event.get("text", "") for event in events if event["type"] == "text")
    calls = [call for event in events if event["type"] == "tool_calls" for call in event["calls"]]
    _ensure_choice(policy, calls)
    if not calls and _is_progress_only_continuation(content, body):
        raise ToolProtocolError(
            "continuation ended with progress narration instead of a tool call or completed result"
        )
    return content, calls


def convert_openai_tool_response(response: dict[str, Any], request_body: dict[str, Any]) -> dict[str, Any]:
    choices = response.get("choices") or []
    if not choices:
        return response
    message = choices[0].get("message") or {}
    content, calls = extract_tool_aware_output(str(message.get("content") or ""), request_body)
    if message.get("reasoning_content"):
        message["reasoning_content"] = _strip_dsml(str(message["reasoning_content"]))
    message["content"] = content or (None if calls else "")
    if calls:
        message["tool_calls"] = calls
        choices[0]["finish_reason"] = "tool_calls"
    else:
        message.pop("tool_calls", None)
    choices[0]["message"] = message
    return response


def build_tool_repair_request(
    request_body: dict[str, Any],
    response: dict[str, Any],
    error: ToolProtocolError,
    *,
    clean: bool = False,
) -> dict[str, Any]:
    """Continue the same upstream conversation with a schema correction."""
    repaired = copy.deepcopy(request_body)
    choice = (response.get("choices") or [{}])[0]
    message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
    raw_content = str(message.get("content") or "")
    messages = [item for item in repaired.get("messages") or [] if isinstance(item, dict)]
    task_context = ""
    for item in reversed(messages):
        if item.get("role") != "user":
            continue
        content = item.get("content")
        if isinstance(content, list):
            text = "\n".join(
                str(block.get("text") or block.get("content") or "")
                for block in content
                if isinstance(block, dict)
            )
        else:
            text = str(content or "")
        stripped = text.strip()
        if not stripped or stripped.startswith("[System tool protocol repair]"):
            continue
        if is_continuation_only(stripped):
            continue
        task_context = stripped
        break
    if len(task_context) > 12000:
        task_context = task_context[:4000] + "\n...[task context truncated]...\n" + task_context[-8000:]
    if clean:
        system = [item for item in messages if item.get("role") == "system"]
        latest_user_index = next(
            (
                index
                for index in range(len(messages) - 1, -1, -1)
                if messages[index].get("role") == "user"
                and not is_continuation_only(str(messages[index].get("content") or ""))
            ),
            -1,
        )
        clean_tail: list[dict[str, Any]] = []
        if latest_user_index >= 0:
            clean_tail.append(messages[latest_user_index])
        for item in messages[latest_user_index + 1:]:
            role = item.get("role")
            if role == "tool" or (role == "assistant" and item.get("tool_calls")):
                clean_tail.append(item)
        if not clean_tail:
            latest = next(
                (item for item in reversed(messages) if item.get("role") in {"user", "tool"}),
                None,
            )
            clean_tail = [latest] if latest else []
        messages = [*system, *clean_tail[-7:]]
    messages.extend([
        {
            "role": "assistant",
            "content": raw_content or "[Invalid tool call omitted by protocol validator]",
        },
        {
            "role": "user",
            "content": (
                "[System tool protocol repair]\n"
                f"The previous tool call was rejected: {error}.\n"
                + (f"Current task context:\n{task_context}\n" if task_context else "")
                + "Retry the tool call now. Output only one corrected raw XML tool_calls block. "
                + "Use the exact declared field names and include every required argument. "
                + "Do not explain the error and do not claim the tool ran."
            ),
        },
    ])
    repaired["messages"] = messages
    repaired["stream"] = False
    policy = resolve_tool_policy(repaired)
    if len(policy.allowed_names) == 1:
        repaired["tool_choice"] = {
            "type": "function",
            "function": {"name": policy.allowed_names[0]},
        }
    elif policy.allowed_names:
        repaired["tool_choice"] = "required"
    if clean:
        repaired["_force_fresh_upstream_session"] = True
    return repaired


async def convert_openai_tool_response_with_repair(
    response: dict[str, Any],
    request_body: dict[str, Any],
    retry_invoke: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate a model tool call and retry malformed generations in-session."""
    retries = max(0, min(int(os.getenv("LIANGZIDP_TOOL_PROTOCOL_RETRIES", "2")), 2))
    current_body = request_body
    current_response = response
    for attempt in range(retries + 1):
        try:
            return convert_openai_tool_response(current_response, current_body), current_body
        except ToolProtocolError as exc:
            if attempt >= retries:
                raise
            clean = attempt >= 1
            current_body = build_tool_repair_request(
                request_body if clean else current_body,
                current_response,
                exc,
                clean=clean,
            )
            current_response = await retry_invoke(current_body)
    raise RuntimeError("unreachable tool protocol repair state")


def _openai_chunk(stream_id: str, model: str, delta: dict[str, Any], finish: str | None = None) -> str:
    return "data: " + json.dumps({
        "id": stream_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }, ensure_ascii=False, separators=(",", ":")) + "\n\n"


def openai_sse_from_response(response: dict[str, Any]) -> list[str]:
    choice = (response.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    stream_id = response.get("id") or "chatcmpl-" + uuid.uuid4().hex
    model = response.get("model") or "deepseek-v4-flash"
    output = [_openai_chunk(stream_id, model, {"role": "assistant"})]
    if message.get("reasoning_content"):
        output.append(_openai_chunk(stream_id, model, {"reasoning_content": message["reasoning_content"]}))
    if message.get("content"):
        output.append(_openai_chunk(stream_id, model, {"content": message["content"]}))
    for index, call in enumerate(message.get("tool_calls") or []):
        output.append(_openai_chunk(stream_id, model, {"tool_calls": [{
            "index": index,
            "id": call["id"],
            "type": "function",
            "function": call["function"],
        }]}))
    output.append(_openai_chunk(stream_id, model, {}, choice.get("finish_reason") or "stop"))
    output.append("data: [DONE]\n\n")
    return output


async def stream_with_keepalive(source: AsyncIterable[str], interval: float = 10.0) -> AsyncGenerator[str | None, None]:
    queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

    async def consume() -> None:
        try:
            async for item in source:
                await queue.put(("item", item))
        except BaseException as exc:
            await queue.put(("error", exc))
        finally:
            await queue.put(("done", None))

    task = asyncio.create_task(consume())
    try:
        while True:
            try:
                kind, value = await asyncio.wait_for(queue.get(), timeout=interval)
            except TimeoutError:
                yield None
                continue
            if kind == "item":
                yield value
            elif kind == "error":
                raise value
            else:
                break
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _sse_json_blocks(raw: Any) -> list[dict[str, Any] | str]:
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw or "")
    output: list[dict[str, Any] | str] = []
    for block in re.split(r"\r?\n\r?\n", text):
        stripped = block.strip()
        if not stripped:
            continue
        if stripped.startswith(":"):
            output.append(stripped)
            continue
        data = "\n".join(line[5:].lstrip() for line in stripped.splitlines() if line.startswith("data:"))
        if not data or data == "[DONE]":
            continue
        try:
            output.append(json.loads(data))
        except json.JSONDecodeError:
            continue
    return output


async def openai_tool_stream(
    source: AsyncIterable[str],
    request_body: dict[str, Any],
    on_tool_calls: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
    on_protocol_error: Callable[[ToolProtocolError], Awaitable[None]] | None = None,
) -> AsyncGenerator[str, None]:
    policy = resolve_tool_policy(request_body)
    sieve = ToolSieve(policy)
    stream_id = "chatcmpl-" + uuid.uuid4().hex
    model = str(request_body.get("model") or "deepseek-v4-flash")
    role_sent = False
    calls: list[dict[str, Any]] = []
    upstream_finish = "stop"

    async def emit_events(events: list[dict[str, Any]]) -> AsyncGenerator[str, None]:
        nonlocal calls
        for event in events:
            if event["type"] == "text" and event.get("text"):
                yield _openai_chunk(stream_id, model, {"content": event["text"]})
            elif event["type"] == "tool_calls":
                start = len(calls)
                if on_tool_calls:
                    await on_tool_calls(event["calls"])
                calls.extend(event["calls"])
                payload = []
                for offset, call in enumerate(event["calls"]):
                    payload.append({
                        "index": start + offset,
                        "id": call["id"],
                        "type": "function",
                        "function": call["function"],
                    })
                yield _openai_chunk(stream_id, model, {"tool_calls": payload})

    try:
        async for raw in stream_with_keepalive(source):
            if raw is None:
                yield ": keep-alive\n\n"
                continue
            for event in _sse_json_blocks(raw):
                if isinstance(event, str):
                    yield event + "\n\n"
                    continue
                stream_id = str(event.get("id") or stream_id)
                model = str(event.get("model") or model)
                choice = (event.get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
                if not role_sent:
                    yield _openai_chunk(stream_id, model, {"role": "assistant"})
                    role_sent = True
                if delta.get("reasoning_content"):
                    yield _openai_chunk(stream_id, model, {"reasoning_content": delta["reasoning_content"]})
                if delta.get("content"):
                    async for item in emit_events(sieve.push(str(delta["content"]))):
                        yield item
                if choice.get("finish_reason"):
                    upstream_finish = str(choice["finish_reason"])
        if not role_sent:
            yield _openai_chunk(stream_id, model, {"role": "assistant"})
        async for item in emit_events(sieve.flush()):
            yield item
        _ensure_choice(policy, calls)
        if calls and on_tool_calls:
            await on_tool_calls(calls)
        yield _openai_chunk(stream_id, model, {}, "tool_calls" if calls else upstream_finish)
        yield "data: [DONE]\n\n"
    except ToolProtocolError as exc:
        if on_protocol_error:
            await on_protocol_error(exc)
        yield "data: " + json.dumps({"error": {
            "message": str(exc), "type": "tool_protocol_error", "code": exc.code,
        }}, ensure_ascii=False, separators=(",", ":")) + "\n\n"
        yield "data: [DONE]\n\n"


def convert_anthropic_tool_response(response: dict[str, Any], request_body: dict[str, Any]) -> dict[str, Any]:
    # Kept as a compatibility helper for callers that already have Anthropic text blocks.
    text = "".join(
        str(block.get("text") or "")
        for block in response.get("content") or []
        if isinstance(block, dict) and block.get("type") == "text"
    )
    content, calls = extract_tool_aware_output(text, request_body)
    blocks: list[dict[str, Any]] = []
    if content:
        blocks.append({"type": "text", "text": content})
    for call in calls:
        blocks.append({
            "type": "tool_use",
            "id": call["id"].replace("call_", "toolu_", 1),
            "name": call["function"]["name"],
            "input": json.loads(call["function"]["arguments"]),
        })
    response["content"] = blocks
    response["stop_reason"] = "tool_use" if calls else response.get("stop_reason") or "end_turn"
    return response


def anthropic_sse_from_response(response: dict[str, Any]) -> list[str]:
    def emit(event: str, payload: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n"

    model = response.get("model") or "deepseek-v4-flash"
    message = {
        "id": response.get("id") or "msg_" + uuid.uuid4().hex,
        "type": "message", "role": "assistant", "model": model, "content": [],
        "stop_reason": None, "stop_sequence": None,
        "usage": response.get("usage") or {"input_tokens": 0, "output_tokens": 0},
    }
    output = [emit("message_start", {"type": "message_start", "message": message})]
    for index, block in enumerate(response.get("content") or []):
        if block.get("type") == "tool_use":
            output.append(emit("content_block_start", {
                "type": "content_block_start", "index": index,
                "content_block": {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}},
            }))
            output.append(emit("content_block_delta", {
                "type": "content_block_delta", "index": index,
                "delta": {"type": "input_json_delta", "partial_json": json.dumps(block.get("input") or {}, ensure_ascii=False, separators=(",", ":"))},
            }))
        else:
            block_type = block.get("type") or "text"
            start = {"type": block_type, "text": ""} if block_type == "text" else {"type": block_type, "thinking": ""}
            output.append(emit("content_block_start", {"type": "content_block_start", "index": index, "content_block": start}))
            key = "text" if block_type == "text" else "thinking"
            delta_type = "text_delta" if block_type == "text" else "thinking_delta"
            if block.get(key):
                output.append(emit("content_block_delta", {
                    "type": "content_block_delta", "index": index,
                    "delta": {"type": delta_type, key: block[key]},
                }))
        output.append(emit("content_block_stop", {"type": "content_block_stop", "index": index}))
    output.append(emit("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": response.get("stop_reason") or "end_turn", "stop_sequence": None},
        "usage": {"output_tokens": (response.get("usage") or {}).get("output_tokens", 0)},
    }))
    output.append(emit("message_stop", {"type": "message_stop"}))
    return output


async def openai_to_anthropic_stream(source: AsyncIterable[str], model: str) -> AsyncGenerator[str, None]:
    def emit(event: str, payload: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n"

    message_id = "msg_" + uuid.uuid4().hex
    yield emit("message_start", {"type": "message_start", "message": {
        "id": message_id, "type": "message", "role": "assistant", "model": model,
        "content": [], "stop_reason": None, "stop_sequence": None,
        "usage": {"input_tokens": 0, "output_tokens": 0},
    }})
    index = -1
    open_type = ""
    saw_tool = False
    saw_error = False
    finish = "end_turn"

    async def close_open() -> AsyncGenerator[str, None]:
        nonlocal open_type
        if open_type:
            yield emit("content_block_stop", {"type": "content_block_stop", "index": index})
            open_type = ""

    async def ensure_block(block_type: str) -> AsyncGenerator[str, None]:
        nonlocal index, open_type
        if open_type == block_type:
            return
        async for item in close_open():
            yield item
        index += 1
        open_type = block_type
        content_block = {"type": "text", "text": ""} if block_type == "text" else {"type": "thinking", "thinking": ""}
        yield emit("content_block_start", {"type": "content_block_start", "index": index, "content_block": content_block})

    async for raw in source:
        if raw is None or str(raw).lstrip().startswith(":"):
            yield emit("ping", {"type": "ping"})
            continue
        for event in _sse_json_blocks(raw):
            if isinstance(event, str):
                yield emit("ping", {"type": "ping"})
                continue
            if event.get("error"):
                saw_error = True
                yield emit("error", {"type": "error", "error": event["error"]})
                continue
            choice = (event.get("choices") or [{}])[0]
            delta = choice.get("delta") or {}
            if delta.get("reasoning_content"):
                async for item in ensure_block("thinking"):
                    yield item
                yield emit("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {
                    "type": "thinking_delta", "thinking": delta["reasoning_content"],
                }})
            if delta.get("content"):
                async for item in ensure_block("text"):
                    yield item
                yield emit("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {
                    "type": "text_delta", "text": delta["content"],
                }})
            for call in delta.get("tool_calls") or []:
                async for item in close_open():
                    yield item
                saw_tool = True
                index += 1
                function = call.get("function") or {}
                call_id = str(call.get("id") or "call_" + uuid.uuid4().hex).replace("call_", "toolu_", 1)
                yield emit("content_block_start", {"type": "content_block_start", "index": index, "content_block": {
                    "type": "tool_use", "id": call_id, "name": function.get("name") or "", "input": {},
                }})
                yield emit("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {
                    "type": "input_json_delta", "partial_json": function.get("arguments") or "{}",
                }})
                yield emit("content_block_stop", {"type": "content_block_stop", "index": index})
            if choice.get("finish_reason"):
                reason = str(choice["finish_reason"])
                if reason == "error":
                    saw_error = True
                    yield emit("error", {"type": "error", "error": {
                        "type": "upstream_incomplete",
                        "message": "Upstream model stream ended before a successful terminal status",
                    }})
                    continue
                finish = {"length": "max_tokens", "tool_calls": "tool_use"}.get(reason, "end_turn")
    async for item in close_open():
        yield item
    if saw_tool:
        finish = "tool_use"
    if saw_error:
        return
    yield emit("message_delta", {"type": "message_delta", "delta": {
        "stop_reason": finish, "stop_sequence": None,
    }, "usage": {"output_tokens": 0}})
    yield emit("message_stop", {"type": "message_stop"})
