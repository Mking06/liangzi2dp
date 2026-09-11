"""API-key enforcement, request accounting, and admin security headers."""

from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send


PUBLIC_API_PATHS = {
    "/v1/models",
    "/v1/chat/completions",
    "/v1/messages",
    "/v1/images/generations",
    "/v1/files",
}
ADMIN_PREFIXES = (
    "/api/admin",
    "/v1/admin",
    "/v1/accounts",
    "/v1/pow",
    "/v1/chat/test",
    "/v1/logs",
    "/v1/status",
)


def _header(scope: Scope, name: bytes) -> str:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return ""


def _raw_api_key(scope: Scope) -> str:
    value = _header(scope, b"x-api-key").strip()
    if value:
        return value
    authorization = _header(scope, b"authorization").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


class AdminGatewayMiddleware:
    """Pure ASGI middleware that never buffers response chunks."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        is_admin = path == "/admin" or path.startswith("/admin/") or path.startswith("/api/admin")
        is_admin_asset = path.startswith("/static/admin/")
        is_public_api = path in PUBLIC_API_PATHS and not path.startswith(ADMIN_PREFIXES)
        store = getattr(scope.get("app").state, "admin_store", None) if scope.get("app") else None

        api_key: dict[str, Any] | None = None
        raw_key = ""
        if is_public_api and store:
            raw_key = _raw_api_key(scope)
            if raw_key:
                api_key = await store.verify_api_key(raw_key)
            enforced = bool(await store.get_setting("api_key_enforced", False))
            if enforced and not api_key:
                payload = json.dumps(
                    {"error": {"message": "Invalid or missing API key", "type": "authentication_error"}}
                ).encode("utf-8")
                try:
                    await store.log_request({
                        "api_key_id": None,
                        "method": scope.get("method", ""),
                        "path": path,
                        "model": None,
                        "stream": False,
                        "status_code": 401,
                        "latency_ms": 0,
                        "request_bytes": 0,
                        "response_bytes": len(payload),
                        "error": "HTTP 401",
                    })
                except Exception:
                    pass
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(payload)).encode())],
                })
                await send({"type": "http.response.body", "body": payload})
                return

        if is_public_api:
            if api_key:
                client_identity = f"api-key:{api_key['id']}"
            elif raw_key:
                client_identity = "bearer:" + hashlib.sha256(raw_key.encode()).hexdigest()[:24]
            else:
                peer = scope.get("client") or ("unknown", 0)
                user_agent = _header(scope, b"user-agent")
                seed = f"{peer[0]}|{user_agent}"
                client_identity = "anonymous:" + hashlib.sha256(seed.encode()).hexdigest()[:24]
            scope.setdefault("state", {})["client_identity"] = client_identity

        captured = bytearray()
        request_bytes = 0

        async def receive_wrapper() -> Message:
            nonlocal request_bytes
            message = await receive()
            if is_public_api and message["type"] == "http.request":
                body = message.get("body", b"")
                request_bytes += len(body)
                if len(captured) < 65536:
                    captured.extend(body[: 65536 - len(captured)])
            return message

        status_code = 500
        response_bytes = 0
        started = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_bytes
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                if is_admin or is_admin_asset:
                    headers = list(message.get("headers", []))
                    headers.extend([
                        (b"content-security-policy", b"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"),
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                        (b"referrer-policy", b"no-referrer"),
                        (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
                    ])
                    message["headers"] = headers
            elif message["type"] == "http.response.body":
                response_bytes += len(message.get("body", b""))
            await send(message)

        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        finally:
            if is_public_api and store:
                raw = captured.decode("utf-8", errors="ignore")
                model_match = re.search(r'"model"\s*:\s*"([^"\\]{1,120})"', raw)
                stream_match = re.search(r'"stream"\s*:\s*true', raw, re.IGNORECASE)
                try:
                    await store.log_request({
                        "api_key_id": api_key.get("id") if api_key else None,
                        "method": scope.get("method", ""),
                        "path": path,
                        "model": model_match.group(1) if model_match else None,
                        "stream": bool(stream_match),
                        "status_code": status_code,
                        "latency_ms": round((time.perf_counter() - started) * 1000),
                        "request_bytes": request_bytes,
                        "response_bytes": response_bytes,
                        "error": f"HTTP {status_code}" if status_code >= 400 else None,
                    })
                except Exception:
                    # Operational accounting must never alter a completed API response.
                    pass
