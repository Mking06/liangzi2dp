"""Operational dashboard, settings, request history, and audit endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.control.admin.security import admin_store, client_ip, require_csrf
from app.control.admin.system import get_pow_status, get_status, tail_logs
from app.platform.config.snapshot import config

router = APIRouter(prefix="/api/admin", tags=["Admin - Operations"])


class SettingsUpdate(BaseModel):
    api_key_enforced: bool | None = None
    request_log_retention_days: int | None = Field(default=None, ge=1, le=90)
    account_refresh_enabled: bool | None = None
    retry_max_retries: int | None = Field(default=None, ge=0, le=10)
    timeout_chat_seconds: int | None = Field(default=None, ge=10, le=1800)
    logging_file_level: str | None = None


@router.get("/dashboard")
async def dashboard(request: Request) -> dict[str, Any]:
    status = await get_status(request)
    store = admin_store(request)
    return {
        "status": status.model_dump(),
        "pow": await get_pow_status(),
        "pool": await request.app.state.orchestrator.pool_overview(request.app.state.repository),
        "traffic": await store.dashboard(),
        "api_key_enforced": bool(await store.get_setting("api_key_enforced", False)),
    }


@router.get("/requests")
async def requests(request: Request, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    return await admin_store(request).list_requests(limit, offset)


@router.get("/audit")
async def audit(request: Request, limit: int = 100) -> dict[str, Any]:
    return {"items": await admin_store(request).list_audit(limit)}


@router.get("/settings")
async def settings(request: Request) -> dict[str, Any]:
    store = admin_store(request)
    return {
        "api_key_enforced": bool(await store.get_setting("api_key_enforced", False)),
        "request_log_retention_days": int(await store.get_setting("request_log_retention_days", 7)),
        "account_refresh_enabled": config.get_bool("account.refresh.enabled", True),
        "retry_max_retries": config.get_int("retry.max_retries", 1),
        "timeout_chat_seconds": config.get_int("timeouts.chat", 60),
        "logging_file_level": config.get_str("logging.file_level", "INFO"),
    }


@router.patch("/settings")
async def update_settings(body: SettingsUpdate, request: Request) -> dict[str, Any]:
    await require_csrf(request)
    store = admin_store(request)
    changed: list[str] = []
    values = body.model_dump(exclude_none=True)
    if "api_key_enforced" in values:
        await store.set_setting("api_key_enforced", values["api_key_enforced"])
        changed.append("api_key_enforced")
    if "request_log_retention_days" in values:
        await store.set_setting("request_log_retention_days", values["request_log_retention_days"])
        changed.append("request_log_retention_days")

    config_map = {
        "account_refresh_enabled": "account.refresh.enabled",
        "retry_max_retries": "retry.max_retries",
        "timeout_chat_seconds": "timeouts.chat",
        "logging_file_level": "logging.file_level",
    }
    if "logging_file_level" in values:
        level = str(values["logging_file_level"]).upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            from app.platform.errors import AppError
            raise AppError("Invalid logging level", 400, "invalid_setting")
        values["logging_file_level"] = level
    for input_key, config_key in config_map.items():
        if input_key in values:
            config.set(config_key, values[input_key])
            changed.append(input_key)
    await config.save()
    await store.audit("settings.updated", "settings", "runtime", client_ip(request), detail=",".join(changed))
    return await settings(request)


@router.post("/maintenance/cleanup")
async def cleanup(request: Request) -> dict[str, Any]:
    await require_csrf(request)
    store = admin_store(request)
    result = await store.cleanup()
    await store.audit("maintenance.cleanup", "system", "logs", client_ip(request), detail=str(result))
    return {"ok": True, "deleted": result}


@router.get("/system/logs")
async def system_logs(lines: int = 200) -> dict[str, Any]:
    return await tail_logs(lines)
