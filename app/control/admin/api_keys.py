"""Client API-key lifecycle endpoints."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, field_validator

from app.control.admin.security import admin_store, client_ip, require_csrf
from app.platform.errors import AppError

router = APIRouter(prefix="/api/admin/api-keys", tags=["Admin - API Keys"])


class CreateKeyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    expires_at: str | None = None

    @field_validator("expires_at")
    @classmethod
    def validate_expiry(cls, value: str | None) -> str | None:
        if value:
            datetime.fromisoformat(value)
        return value


class UpdateKeyRequest(BaseModel):
    enabled: bool


@router.get("")
async def list_keys(request: Request) -> list[dict]:
    return await admin_store(request).list_api_keys()


@router.post("")
async def create_key(body: CreateKeyRequest, request: Request) -> dict:
    await require_csrf(request)
    store = admin_store(request)
    item, raw = await store.create_api_key(body.name.strip(), body.expires_at)
    await store.audit("api_key.created", "api_key", item["id"], client_ip(request), detail=body.name)
    return {**item, "key": raw}


@router.patch("/{key_id}")
async def update_key(key_id: str, body: UpdateKeyRequest, request: Request) -> dict:
    await require_csrf(request)
    store = admin_store(request)
    if not await store.update_api_key(key_id, body.enabled):
        raise AppError("API key not found", 404, "not_found")
    await store.audit("api_key.updated", "api_key", key_id, client_ip(request), detail=f"enabled={body.enabled}")
    return {"id": key_id, "enabled": body.enabled}


@router.delete("/{key_id}")
async def delete_key(key_id: str, request: Request) -> dict:
    await require_csrf(request)
    store = admin_store(request)
    if not await store.delete_api_key(key_id):
        raise AppError("API key not found", 404, "not_found")
    await store.audit("api_key.deleted", "api_key", key_id, client_ip(request))
    return {"deleted": key_id}
