"""Admin API router — combines all admin endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.control.admin.api_keys import router as api_keys_router
from app.control.admin.auth import router as auth_router
from app.control.admin.system import router as system_router
from app.control.admin.tokens import router as tokens_router
from app.control.admin.batch import router as batch_router
from app.control.admin.ops import router as ops_router
from app.control.admin.pool import router as pool_router
from app.control.admin.security import require_admin

router = APIRouter()
router.include_router(auth_router)

protected = APIRouter(dependencies=[Depends(require_admin)])
protected.include_router(system_router)
protected.include_router(tokens_router)
protected.include_router(batch_router)
protected.include_router(api_keys_router)
protected.include_router(ops_router)
protected.include_router(pool_router)
router.include_router(protected)
