"""Optional sanitized JSONL tracing for Liangzidp Web protocol events."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any


_SECRET_MARKERS = ("authorization", "token", "csrf", "cookie", "pow", "password")


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if any(marker in str(key).lower() for marker in _SECRET_MARKERS) else _sanitize(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


class ProtocolTrace:
    def __init__(self, metadata: dict[str, Any] | None = None):
        root = os.getenv("LIANGZIDP_PROTOCOL_TRACE_DIR", "").strip()
        self.enabled = bool(root)
        self.trace_id = uuid.uuid4().hex
        self._lock = threading.Lock()
        self.path: Path | None = None
        if not self.enabled:
            return
        directory = Path(root).expanduser().resolve()
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{int(time.time() * 1000)}-{self.trace_id}.jsonl"
        self.record("trace_start", metadata or {})

    @staticmethod
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16] if value else ""

    def record(self, kind: str, data: Any) -> None:
        if not self.enabled or self.path is None:
            return
        item = {
            "time": time.time(),
            "trace_id": self.trace_id,
            "kind": kind,
            "data": _sanitize(data),
        }
        encoded = json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(encoded)
