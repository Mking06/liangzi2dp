"""Admin batch operations."""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.platform.errors import AccountError
from app.control.admin.security import admin_store, client_ip, require_csrf

router = APIRouter()


class BatchRefreshRequest(BaseModel):
    count: int = 5


class BatchRefreshResponse(BaseModel):
    total: int
    success: int
    failed: int


@router.post(
    "/api/admin/accounts/refresh/batch",
    response_model=BatchRefreshResponse,
    tags=["Admin - Batch"],
)
@router.post(
    "/v1/accounts/refresh/batch",
    response_model=BatchRefreshResponse,
    tags=["Admin - Batch"],
)
async def batch_refresh(request: Request, body: BatchRefreshRequest) -> BatchRefreshResponse:
    """Refresh a batch of random accounts."""
    await require_csrf(request)
    refresh_svc = request.app.state.refresh_service
    if not refresh_svc:
        raise AccountError("Refresh service not available", 503)

    stats = await refresh_svc.refresh_random_batch(body.count)
    await admin_store(request).audit("account.batch_refresh", "account", str(body.count), client_ip(request), detail=str(stats))
    return BatchRefreshResponse(**stats)


@router.post("/api/admin/accounts/refresh/all", tags=["Admin - Batch"])
@router.post("/v1/accounts/refresh/all", tags=["Admin - Batch"])
async def refresh_all(request: Request) -> dict:
    """Refresh all accounts."""
    await require_csrf(request)
    refresh_svc = request.app.state.refresh_service
    if not refresh_svc:
        raise AccountError("Refresh service not available", 503)

    stats = await refresh_svc.refresh_all(max_concurrency=10)
    await admin_store(request).audit("account.refresh_all", "account", "all", client_ip(request), detail=str(stats))
    return stats
