from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.control.admin.security import admin_store, client_ip, require_csrf
from app.platform.config.snapshot import config
from app.platform.errors import AccountError

router = APIRouter(prefix="/api/admin/pool", tags=["Admin - Account Pool"])


class PoolSettingsUpdate(BaseModel):
    strategy: str | None = None
    max_inflight: int | None = Field(default=None, ge=1, le=256)
    sticky_ttl_seconds: int | None = Field(default=None, ge=300, le=86400)
    cooldown_seconds: int | None = Field(default=None, ge=1, le=86400)
    failure_threshold: int | None = Field(default=None, ge=1, le=20)
    queue_max_size: int | None = Field(default=None, ge=1, le=5000)
    queue_stream_timeout_seconds: int | None = Field(default=None, ge=10, le=1800)
    queue_nonstream_timeout_seconds: int | None = Field(default=None, ge=1, le=300)
    lease_heartbeat_seconds: int | None = Field(default=None, ge=5, le=60)
    lease_stale_seconds: int | None = Field(default=None, ge=30, le=600)


class AccountSchedulingUpdate(BaseModel):
    priority: int | None = Field(default=None, ge=0, le=10000)
    weight: int | None = Field(default=None, ge=1, le=10000)
    max_inflight: int | None = Field(default=None, ge=1, le=256)


class CooldownRequest(BaseModel):
    seconds: int = Field(default=300, ge=1, le=86400)
    reason: str = Field(default="manual", max_length=500)


@router.get("")
async def pool_overview(request: Request) -> dict[str, Any]:
    return await request.app.state.orchestrator.pool_overview(request.app.state.repository)


@router.get("/sessions")
async def pool_sessions(request: Request, limit: int = 200) -> dict[str, Any]:
    items = await request.app.state.orchestration_store.list_conversations(limit)
    return {"items": items, "total": len(items)}


@router.patch("/settings")
async def update_pool_settings(request: Request, body: PoolSettingsUpdate) -> dict[str, Any]:
    await require_csrf(request)
    values = body.model_dump(exclude_none=True)
    if values.get("strategy") not in {None, "least_loaded_weighted"}:
        raise AccountError("Unsupported account selection strategy", 400)
    mapping = {
        "strategy": "account.selection.strategy",
        "max_inflight": "account.selection.max_inflight",
        "sticky_ttl_seconds": "account.selection.sticky_ttl_seconds",
        "cooldown_seconds": "account.selection.cooldown_seconds",
        "failure_threshold": "account.selection.failure_threshold",
        "queue_max_size": "account.queue.max_size",
        "queue_stream_timeout_seconds": "account.queue.stream_timeout_seconds",
        "queue_nonstream_timeout_seconds": "account.queue.nonstream_timeout_seconds",
        "lease_heartbeat_seconds": "account.lease.heartbeat_seconds",
        "lease_stale_seconds": "account.lease.stale_seconds",
    }
    for key, value in values.items():
        config.set(mapping[key], value)
    await config.save()
    await admin_store(request).audit(
        "pool.settings_updated",
        "pool",
        "runtime",
        client_ip(request),
        detail=",".join(values),
    )
    return await pool_overview(request)


@router.patch("/accounts/{account_id}")
async def update_account_scheduling(
    request: Request,
    account_id: str,
    body: AccountSchedulingUpdate,
) -> dict[str, Any]:
    await require_csrf(request)
    repo = request.app.state.repository
    raw = await repo.get(account_id)
    if not raw:
        raise AccountError("Account not found", 404)
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    metadata = dict(metadata)
    metadata.update(body.model_dump(exclude_none=True))
    raw["metadata"] = metadata
    await repo.upsert(raw)
    await admin_store(request).audit(
        "pool.account_scheduling_updated",
        "account",
        account_id,
        client_ip(request),
        detail=",".join(body.model_dump(exclude_none=True)),
    )
    return await pool_overview(request)


@router.post("/accounts/{account_id}/cooldown")
async def set_account_cooldown(
    request: Request,
    account_id: str,
    body: CooldownRequest,
) -> dict[str, Any]:
    await require_csrf(request)
    if not await request.app.state.repository.get(account_id):
        raise AccountError("Account not found", 404)
    await request.app.state.orchestration_store.set_cooldown(account_id, body.seconds, body.reason)
    await admin_store(request).audit(
        "pool.account_cooled_down",
        "account",
        account_id,
        client_ip(request),
        detail=f"seconds={body.seconds};reason={body.reason}",
    )
    return {"ok": True, "account_id": account_id}


@router.post("/accounts/{account_id}/clear-cooldown")
async def clear_account_cooldown(request: Request, account_id: str) -> dict[str, Any]:
    await require_csrf(request)
    await request.app.state.orchestration_store.clear_cooldown(account_id)
    await admin_store(request).audit(
        "pool.account_cooldown_cleared",
        "account",
        account_id,
        client_ip(request),
    )
    return {"ok": True, "account_id": account_id}


@router.delete("/sessions/{session_key}")
async def delete_pool_session(request: Request, session_key: str) -> dict[str, Any]:
    await require_csrf(request)
    await request.app.state.orchestration_store.delete_conversation(session_key)
    await admin_store(request).audit(
        "pool.session_deleted",
        "conversation",
        session_key[:16],
        client_ip(request),
    )
    return {"deleted": session_key}
