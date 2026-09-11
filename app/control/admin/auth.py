"""Single-administrator authentication endpoints."""

from __future__ import annotations

import os

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, Field

from app.control.admin.security import (
    CSRF_COOKIE,
    SESSION_COOKIE,
    admin_store,
    client_ip,
    cookie_secure,
    require_admin,
    require_csrf,
)
from app.platform.errors import AppError

router = APIRouter(prefix="/api/admin/auth", tags=["Admin - Auth"])


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=500)


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(min_length=12, max_length=500)


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response) -> dict:
    store = admin_store(request)
    ip = client_ip(request)
    if not await store.login_allowed(ip):
        raise AppError("Too many login attempts", 429, "login_rate_limited")
    admin = await store.verify_admin(body.username, body.password)
    await store.record_login(ip, bool(admin))
    if not admin:
        await store.audit("auth.login", "admin", body.username, ip, False, "invalid credentials")
        raise AppError("Invalid username or password", 401, "invalid_credentials")

    ttl = max(1, min(int(os.getenv("ADMIN_SESSION_TTL_HOURS", "12")), 168))
    token, csrf, expires = await store.create_session(
        admin["id"], ip, request.headers.get("user-agent", ""), ttl
    )
    secure = cookie_secure(request)
    response.set_cookie(
        SESSION_COOKIE, token, max_age=ttl * 3600, httponly=True,
        secure=secure, samesite="strict", path="/",
    )
    response.set_cookie(
        CSRF_COOKIE, csrf, max_age=ttl * 3600, httponly=False,
        secure=secure, samesite="strict", path="/",
    )
    await store.audit("auth.login", "admin", str(admin["id"]), ip)
    return {"username": admin["username"], "expires_at": expires}


@router.get("/me")
async def me(request: Request) -> dict:
    session = await require_admin(request)
    return {"id": session["admin_id"], "username": session["username"], "expires_at": session["expires_at"]}


@router.post("/logout")
async def logout(request: Request, response: Response) -> dict:
    session = await require_csrf(request)
    store = admin_store(request)
    await store.delete_session(request.cookies.get(SESSION_COOKIE, ""))
    await store.audit("auth.logout", "admin", str(session["admin_id"]), client_ip(request))
    response.delete_cookie(SESSION_COOKIE, path="/")
    response.delete_cookie(CSRF_COOKIE, path="/")
    return {"ok": True}


@router.post("/change-password")
async def change_password(body: ChangePasswordRequest, request: Request, response: Response) -> dict:
    session = await require_csrf(request)
    store = admin_store(request)
    changed = await store.change_password(session["admin_id"], body.current_password, body.new_password)
    if not changed:
        raise AppError("Current password is incorrect", 400, "invalid_password")
    await store.audit("auth.password_changed", "admin", str(session["admin_id"]), client_ip(request))
    response.delete_cookie(SESSION_COOKIE, path="/")
    response.delete_cookie(CSRF_COOKIE, path="/")
    return {"ok": True, "reauthenticate": True}
