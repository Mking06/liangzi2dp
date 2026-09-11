"""Web UI routes — authentication and chat."""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.platform.config.snapshot import config
from app.platform.errors import AppError

router = APIRouter()


class WebLoginRequest(BaseModel):
    key: str


class WebLoginResponse(BaseModel):
    token: str
    username: str


@router.post("/webui/login", response_model=WebLoginResponse, tags=["WebUI - System"])
async def webui_login(body: WebLoginRequest) -> WebLoginResponse:
    """Web UI login with access key."""
    expected_key = config.get_str("app.webui_key", "")
    if not expected_key:
        raise AppError("WebUI not configured", 503)
    if body.key != expected_key:
        raise AppError("Invalid key", 401)

    # In production, return a JWT or session token
    import secrets
    token = secrets.token_urlsafe(32)
    return WebLoginResponse(token=token, username="admin")


@router.get("/webui/config", tags=["WebUI - System"])
async def webui_config(request: Request) -> dict:
    """Get WebUI configuration."""
    return {
        "service": "liangzidp",
        "version": "0.1.0",
        "features": {
            "chat": True,
            "stream": config.get_bool("features.stream", True),
            "thinking": config.get_bool("features.thinking", True),
        },
    }
