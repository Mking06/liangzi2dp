"""Canonical Liangzidp Web SSE event decoding.

The Web endpoint emits a mixture of SSE event names, full response snapshots,
JSON-patch operations, and short bare deltas.  This module is the only place
that assigns those transport events semantic meaning.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

# Liangzidp internal DSML (Liangzidp Markup Language) section markers.
# These are transport-level delimiters emitted by the Web frontend model
# (e.g. "< | DSML | tool_calls>" or "< | | DSML | | tool_calls>") and must never reach the client as
# visible text.  Strip them at the reducer source so both streaming and
# buffered paths are covered.
_DSML_MARKER_RE = re.compile(r"<[^>]*DSML[^>]*>", re.IGNORECASE)
_DSML_SEP = r"[|\u2502\uff5c\u2223\s]*"
_DSML_TAG_RE = re.compile(
    r"<\s*(/)?\s*" + _DSML_SEP + r"DSML" + _DSML_SEP +
    r"([a-zA-Z_][a-zA-Z0-9_:-]*)((?:[^>]*?))\s*>",
    re.IGNORECASE,
)


def _dsml_tag_replacer(m):
    closing = m.group(1) or ""
    tag = (m.group(2) or "").strip()
    attrs = m.group(3) or ""
    if not tag:
        return ""
    return f"<{closing}{tag}{attrs}>"
# A partial DSML marker start at end of text: "<", optional spaces,
# one-or-more pipes+spaces, then optional partial letters.
_DSML_PARTIAL_RE = re.compile(r"<\s*[|\u2502\uff5c\u2223][^>]*$")
_DSML_HOLD_LIMIT = 48


class DSMLStreamStripper:
    """Stateful DSML marker stripper across streamed text deltas."""
    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = ""

    def feed(self, text: str) -> str:
        if not text:
            return ""
        combined = self._buf + text
        stripped = _DSML_TAG_RE.sub(_dsml_tag_replacer, combined)
        stripped = _DSML_MARKER_RE.sub("", stripped)
        last_lt = stripped.rfind("<")
        if last_lt >= 0:
            tail = stripped[last_lt:]
            if len(tail) <= _DSML_HOLD_LIMIT and _DSML_PARTIAL_RE.match(tail):
                self._buf = tail
                return stripped[:last_lt]
        self._buf = ""
        return stripped

    def flush(self) -> str:
        result = self._buf
        self._buf = ""
        # A tag held mid-stream at EOF must still be normalized so raw DSML
        # never leaks into the assembled content.
        if result:
            result = _strip_dsml_markers(result)
        return result



def _strip_dsml_markers(text: str) -> str:
    """Normalize DSML: tool_calls opener -> <tool_calls>, others removed."""
    if not text:
        return text
    text = _DSML_TAG_RE.sub(_dsml_tag_replacer, text)
    return _DSML_MARKER_RE.sub("", text)
from typing import Any, Iterable


SUCCESS_STATUSES = {"FINISHED", "SUCCESS", "COMPLETED"}
LENGTH_STATUSES = {"CONTEXT_LENGTH_EXCEEDED", "LENGTH", "MAX_TOKENS"}
FILTER_STATUSES = {"CONTENT_FILTER", "FILTERED"}
INCOMPLETE_STATUSES = {
    "INCOMPLETE",
    "TIMEOUT",
    "CANCELLED",
    "FAILED",
    "ERROR",
    "RATE_LIMITED",
}


@dataclass(slots=True)
class CanonicalEvent:
    kind: str
    text: str = ""
    status: str = ""
    finish_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def finish_reason_for_status(status: str | None) -> str | None:
    normalized = str(status or "").upper()
    if normalized in SUCCESS_STATUSES:
        return "stop"
    if normalized in LENGTH_STATUSES:
        return "length"
    if normalized in FILTER_STATUSES:
        return "content_filter"
    if normalized in INCOMPLETE_STATUSES:
        return "error"
    return None


def parse_sse_blocks(text: str) -> Iterable[tuple[str, Any]]:
    """Yield `(event_name, decoded_data)` from complete SSE text."""
    event_name = "message"
    data_lines: list[str] = []

    def flush() -> tuple[str, Any] | None:
        nonlocal event_name, data_lines
        if not data_lines:
            event_name = "message"
            return None
        raw = "\n".join(data_lines).strip()
        name = event_name
        event_name = "message"
        data_lines = []
        if raw == "[DONE]":
            return "done", None
        try:
            return name, json.loads(raw)
        except json.JSONDecodeError:
            return "malformed", raw

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r")
        if line == "":
            item = flush()
            if item is not None:
                yield item
            continue
        if line.startswith("event:"):
            event_name = line[6:].strip() or "message"
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    item = flush()
    if item is not None:
        yield item


class LiangzidpEventReducer:
    """Reduce Liangzidp transport events into stable semantic events."""

    def __init__(self) -> None:
        self.current_fragment_type = "RESPONSE"
        self.current_fragment_key = "fragment:0"
        self.fragment_text: dict[str, str] = {}
        self.content_parts: list[str] = []
        self.reasoning_parts: list[str] = []
        self.status = ""
        self.usage_total = 0
        self.metadata: dict[str, Any] = {}
        self.transport_closed = False
        self.terminal_emitted = False
        self.error_code = ""
        self.error_message = ""
        self._dsml = DSMLStreamStripper()

    @property
    def content(self) -> str:
        return "".join(self.content_parts)

    @property
    def reasoning_content(self) -> str:
        return "".join(self.reasoning_parts)

    @property
    def finish_reason(self) -> str | None:
        return finish_reason_for_status(self.status)

    @staticmethod
    def _fragment_key(path: str, value: dict[str, Any], fallback: str) -> str:
        if value.get("id") is not None:
            return f"id:{value['id']}"
        match = re.search(r"fragments/(?:-|)(\d+|-1)", path)
        if match and match.group(1) != "-1":
            return f"index:{match.group(1)}"
        return fallback

    def _emit_text(self, text: Any, fragment_type: str, key: str, *, snapshot: bool) -> list[CanonicalEvent]:
        if not isinstance(text, str) or not text:
            return []
        normalized_type = str(fragment_type or "RESPONSE").upper()
        previous = self.fragment_text.get(key, "")
        delta = text
        if snapshot:
            if text == previous or previous.startswith(text):
                self.fragment_text[key] = previous
                return []
            if text.startswith(previous):
                delta = text[len(previous):]
            elif previous:
                # A reconnect snapshot can replay or revise already streamed
                # text. SSE cannot retract prior bytes, so retain the emitted
                # prefix and suppress an ambiguous replay.
                self.fragment_text[key] = text if len(text) > len(previous) else previous
                return []
            self.fragment_text[key] = text
        else:
            self.fragment_text[key] = previous + text

        if not delta:
            return []
        # Strip DSML markers with a stateful stripper that catches markers
        # split across multiple SSE text deltas.
        delta = self._dsml.feed(delta)
        if not delta:
            return []
        if normalized_type == "THINK":
            self.reasoning_parts.append(delta)
            return [CanonicalEvent("reasoning_delta", text=delta)]
        self.content_parts.append(delta)
        return [CanonicalEvent("text_delta", text=delta)]

    def _set_status(self, status: Any) -> list[CanonicalEvent]:
        if not status:
            return []
        self.status = str(status).upper()
        reason = finish_reason_for_status(self.status)
        if reason is None or self.terminal_emitted:
            return []
        self.terminal_emitted = True
        return [CanonicalEvent("terminal", status=self.status, finish_reason=reason)]

    def feed(self, event_name: str, payload: Any) -> list[CanonicalEvent]:
        output: list[CanonicalEvent] = []
        name = str(event_name or "message").lower()
        if name == "done":
            self.transport_closed = True
            return output
        if name == "close":
            self.transport_closed = True
        if not isinstance(payload, dict):
            return output

        if name == "hint" and str(payload.get("type") or "").lower() == "error":
            reason = str(payload.get("finish_reason") or "").lower()
            self.error_message = str(payload.get("content") or "Liangzidp upstream error")
            if reason in {"rate_limit_reached", "rate_limited"}:
                self.error_code = "rate_limit_exceeded"
                output.extend(self._set_status("RATE_LIMITED"))
            else:
                self.error_code = reason or "upstream_error"
                output.extend(self._set_status("ERROR"))
            output.append(CanonicalEvent(
                "error",
                status=self.status,
                metadata={"code": self.error_code, "message": self.error_message},
            ))
            return output

        metadata = {
            key: payload[key]
            for key in ("request_message_id", "response_message_id", "model_type", "updated_at")
            if key in payload
        }
        if metadata:
            self.metadata.update(metadata)
            output.append(CanonicalEvent("metadata", metadata=metadata))

        if set(payload) == {"v"} and isinstance(payload.get("v"), str):
            return output + self._emit_text(
                payload["v"],
                self.current_fragment_type,
                self.current_fragment_key,
                snapshot=False,
            )

        op = str(payload.get("op", payload.get("o", ""))).upper()
        path = str(payload.get("path", payload.get("p", "")))
        value = payload.get("value", payload.get("v"))

        snapshots: list[dict[str, Any]] = []
        if isinstance(value, dict) and isinstance(value.get("response"), dict):
            snapshots.append(value["response"])
        if isinstance(payload.get("response"), dict):
            snapshots.append(payload["response"])

        for response in snapshots:
            if response.get("parent_id") is not None:
                self.metadata["request_message_id"] = response["parent_id"]
            if response.get("message_id") is not None:
                self.metadata["response_message_id"] = response["message_id"]
            self.usage_total = int(response.get("accumulated_token_usage") or self.usage_total or 0)
            fragments = response.get("fragments") or []
            for index, fragment in enumerate(fragments):
                if not isinstance(fragment, dict):
                    continue
                key = self._fragment_key(f"response/fragments/{index}", fragment, f"index:{index}")
                fragment_type = str(fragment.get("type") or self.current_fragment_type)
                output.extend(self._emit_text(fragment.get("content"), fragment_type, key, snapshot=True))
                self.current_fragment_key = key
                self.current_fragment_type = fragment_type
            output.extend(self._set_status(response.get("status") or response.get("quasi_status")))

        if isinstance(value, dict) and "type" in value and "fragments" in path:
            key = self._fragment_key(path, value, self.current_fragment_key)
            self.current_fragment_key = key
            self.current_fragment_type = str(value.get("type") or self.current_fragment_type)
            output.extend(self._emit_text(value.get("content"), self.current_fragment_type, key, snapshot=True))

        if op == "APPEND" and path.rstrip("/").endswith("response/fragments") and isinstance(value, list):
            for index, fragment in enumerate(value):
                if not isinstance(fragment, dict):
                    continue
                key = self._fragment_key(path, fragment, f"append:{index}")
                self.current_fragment_key = key
                self.current_fragment_type = str(fragment.get("type") or self.current_fragment_type)
                output.extend(
                    self._emit_text(
                        fragment.get("content"),
                        self.current_fragment_type,
                        key,
                        snapshot=True,
                    )
                )

        if op == "APPEND" and isinstance(value, str):
            if path.endswith("/reasoning_content"):
                output.extend(self._emit_text(value, "THINK", self.current_fragment_key, snapshot=False))
            elif path.endswith("/content"):
                output.extend(self._emit_text(value, self.current_fragment_type, self.current_fragment_key, snapshot=False))
        elif op == "SET" and path in {"response/status", "response/quasi_status"}:
            output.extend(self._set_status(value))
        elif op == "BATCH" and path == "response" and isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    continue
                item_path = str(item.get("path", item.get("p", "")))
                item_value = item.get("value", item.get("v"))
                if item_path == "accumulated_token_usage":
                    self.usage_total = int(item_value or 0)
                elif item_path in {"status", "quasi_status"}:
                    output.extend(self._set_status(item_value))
        return output

    def finalize(self, transport_reason: str = "eof") -> CanonicalEvent:
        leftover = self._dsml.flush()
        if leftover:
            self.content_parts.append(leftover)
        if self.terminal_emitted:
            return CanonicalEvent(
                "terminal",
                status=self.status,
                finish_reason=self.finish_reason,
                metadata={"duplicate": True},
            )
        reason = self.finish_reason
        if reason is not None:
            self.terminal_emitted = True
            return CanonicalEvent("terminal", status=self.status, finish_reason=reason)
        self.status = "INCOMPLETE"
        self.terminal_emitted = True
        return CanonicalEvent(
            "terminal",
            status="INCOMPLETE",
            finish_reason="error",
            metadata={"transport_reason": transport_reason},
        )
