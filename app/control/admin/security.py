"""Authentication helpers for the administration console."""

from __future__ import annotations

import hmac
import os
from typing import Any
from urllib.parse import urlsplit

from fastapi import Request

from app.control.admin.store import AdminStore, digest
from app.platform.errors import AppError

SESSION_COOKIE = "ds2api_admin"
CSRF_COOKIE = "ds2api_csrf"


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def admin_store(request: Request) -> AdminStore:
    store = getattr(request.app.state, "admin_store", None)
    if not store:
        raise AppError("Admin service unavailable", 503, "admin_unavailable")
    return store


async def require_admin(request: Request) -> dict[str, Any]:
    session = await admin_store(request).get_session(request.cookies.get(SESSION_COOKIE, ""))
    if not session:
        raise AppError("Authentication required", 401, "unauthorized")
    request.state.admin = session
    return session


async def require_csrf(request: Request) -> dict[str, Any]:
    session = getattr(request.state, "admin", None) or await require_admin(request)
    header = request.headers.get("X-CSRF-Token", "")
    cookie = request.cookies.get(CSRF_COOKIE, "")
    if not header or not cookie or not hmac.compare_digest(header, cookie):
        raise AppError("Invalid CSRF token", 403, "csrf_failed")
    if not hmac.compare_digest(digest(header), session["csrf_hash"]):
        raise AppError("Invalid CSRF token", 403, "csrf_failed")

    origin = request.headers.get("Origin")
    if origin:
        origin_host = urlsplit(origin).netloc.lower()
        request_host = request.headers.get("host", "").lower()
        if not origin_host or not hmac.compare_digest(origin_host, request_host):
            raise AppError("Cross-origin admin request denied", 403, "origin_denied")
    return session


def cookie_secure(request: Request) -> bool:
    configured = os.getenv("ADMIN_COOKIE_SECURE", "auto").strip().lower()
    if configured in {"1", "true", "yes", "on"}:
        return True
    if configured in {"0", "false", "no", "off"}:
        return False
    return request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https"
