"""Lightweight turn lifecycle validation for translated model streams."""

from __future__ import annotations

from enum import Enum
from typing import Any


class TurnState(str, Enum):
    RUNNING = "running"
    WAITING_TOOL = "waiting_tool"
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TurnIntegrityGuard:
    def __init__(self) -> None:
        self.state = TurnState.RUNNING
        self.upstream_status = ""
        self.finish_reason = ""
        self.response_message_id: int | str | None = None
        self.saw_output = False

    def observe(self, event: dict[str, Any]) -> None:
        liangzidp = event.get("liangzidp")
        if isinstance(liangzidp, dict):
            self.upstream_status = str(liangzidp.get("status") or self.upstream_status).upper()
            if liangzidp.get("response_message_id") is not None:
                self.response_message_id = liangzidp["response_message_id"]

        choice = (event.get("choices") or [{}])[0]
        delta = choice.get("delta") or {}
        if delta.get("content") or delta.get("reasoning_content") or delta.get("tool_calls"):
            self.saw_output = True
        if choice.get("finish_reason"):
            self.finish_reason = str(choice["finish_reason"])

        if self.finish_reason == "tool_calls":
            self.state = TurnState.WAITING_TOOL
        elif self.upstream_status in {"FINISHED", "SUCCESS", "COMPLETED"}:
            self.state = TurnState.COMPLETED
        elif self.finish_reason == "error" or self.upstream_status in {
            "INCOMPLETE",
            "TIMEOUT",
            "CANCELLED",
            "FAILED",
            "ERROR",
        }:
            self.state = TurnState.INCOMPLETE

    def transport_ended(self) -> TurnState:
        if self.state == TurnState.RUNNING:
            self.state = TurnState.INCOMPLETE
        return self.state

    def cancelled(self) -> None:
        self.state = TurnState.CANCELLED

    def failed(self) -> None:
        self.state = TurnState.FAILED

    @property
    def successful(self) -> bool:
        return self.state in {TurnState.COMPLETED, TurnState.WAITING_TOOL}
