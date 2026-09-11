"""Admin system routes."""

from __future__ import annotations

import time
from pathlib import Path
import re
from typing import Any

import curl_cffi.requests
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.dataplane.proxy.liangzidp_client import POW_SERVICE_URL
from app.dataplane.proxy.reverse import ReverseProxy
from app.platform.logging.logger import logger
from app.platform.paths import log_path
from app.control.admin.security import require_csrf

router = APIRouter()


class StatusResponse(BaseModel):
    status: str
    service: str
    version: str
    accounts_total: int
    accounts_available: int


class PowTestRequest(BaseModel):
    algorithm: str = "LiangzidpHashV1"
    challenge: str = "1ed5f5e350e1f351556b8a3ac4330a5ff93aa12a0c2a338dc4da82057856d5d5"
    salt: str = "2f2c3e3b24783a1a9649"
    difficulty: int = 144000
    expireAt: int = 1780000000000


class ChatTestRequest(BaseModel):
    account_id: str = ""
    prompt: str = "ping"
    model: str = "deepseek-v4-flash"


_proxy = ReverseProxy()


@router.get("/v1/status", tags=["Admin - System"], response_model=StatusResponse)
async def get_status(request: Request) -> StatusResponse:
    """Get system status."""
    directory = request.app.state.directory

    from app.platform.meta import get_project_version

    return StatusResponse(
        status="ok",
        service="liangzidp",
        version=get_project_version(),
        accounts_total=directory.count if directory else 0,
        accounts_available=directory.available_count if directory else 0,
    )


@router.get("/api/admin/summary", tags=["Admin - System"])
@router.get("/v1/admin/summary", tags=["Admin - System"])
async def admin_summary(request: Request) -> dict[str, Any]:
    """Small aggregate payload for the static admin panel."""
    status = await get_status(request)
    pow_status = await get_pow_status()
    return {
        "status": status.model_dump(),
        "pow": pow_status,
    }


@router.get("/api/admin/pow/status", tags=["Admin - System"])
@router.get("/v1/pow/status", tags=["Admin - System"])
async def get_pow_status() -> dict[str, Any]:
    """Check local Node POW solver health."""
    started = time.perf_counter()
    try:
        async with curl_cffi.requests.AsyncSession(timeout=5) as session:
            resp = await session.get(f"{POW_SERVICE_URL}/health")
        payload: Any
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text[:500]
        return {
            "ok": 200 <= resp.status_code < 300,
            "url": POW_SERVICE_URL,
            "status_code": resp.status_code,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "response": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": POW_SERVICE_URL,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }


@router.post("/api/admin/pow/test", tags=["Admin - System"])
@router.post("/v1/pow/test", tags=["Admin - System"])
async def test_pow_solver(request: Request, body: PowTestRequest | None = None) -> dict[str, Any]:
    """Solve a deterministic POW sample. Expected answer for default payload: 42."""
    await require_csrf(request)
    payload = (body or PowTestRequest()).model_dump()
    started = time.perf_counter()
    try:
        async with curl_cffi.requests.AsyncSession(timeout=30) as session:
            resp = await session.post(f"{POW_SERVICE_URL}/solve", json=payload)
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:1000]}
        answer = data.get("answer") if isinstance(data, dict) else None
        return {
            "ok": 200 <= resp.status_code < 300 and bool(data.get("ok")) if isinstance(data, dict) else False,
            "status_code": resp.status_code,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "answer": answer,
            "expected": 42 if payload["challenge"].startswith("1ed5f5e350e1f351") else None,
            "response": data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }


@router.post("/api/admin/chat/test", tags=["Admin - System"])
@router.post("/v1/chat/test", tags=["Admin - System"])
async def test_chat(request: Request, body: ChatTestRequest) -> dict[str, Any]:
    """Run a minimal non-streaming chat request through the proxy."""
    await require_csrf(request)
    started = time.perf_counter()
    try:
        result = await _proxy.chat_completion(
            request_data={
                "model": body.model,
                "stream": False,
                "_client_identity": "admin-chat-test",
                "conversation_id": f"admin-test:{body.account_id or 'auto'}:{time.time_ns()}",
                "messages": [{"role": "user", "content": body.prompt}],
            },
            account_id=body.account_id or None,
            repository=request.app.state.repository,
            orchestrator=request.app.state.orchestrator,
        )
        content = ""
        try:
            content = result["choices"][0]["message"]["content"]
        except Exception:
            content = str(result)[:500]
        return {
            "ok": True,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "content_preview": content[:500],
            "raw": result,
        }
    except Exception as exc:
        logger.warning("admin chat test failed: error={}", exc)
        return {
            "ok": False,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }


@router.get("/api/admin/system/legacy-logs", tags=["Admin - System"], include_in_schema=False)
@router.get("/v1/logs", tags=["Admin - System"])
async def tail_logs(lines: int = 200) -> dict[str, Any]:
    """Return tail of recent log files for the panel."""
    lines = max(1, min(lines, 1000))
    root = log_path()
    candidates: list[Path] = []
    if root.exists():
        candidates.extend(sorted(root.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True))

    # The local dev launcher in this workspace also writes a few root-level logs.
    project_root = Path(__file__).resolve().parents[3]
    candidates.extend(sorted(project_root.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True))

    seen: set[str] = set()
    files = []
    for path in candidates:
        key = str(path.resolve())
        if key in seen or not path.is_file():
            continue
        seen.add(key)
        try:
            text_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            redacted_lines = [
                re.sub(
                    r"(?i)(authorization|bearer|session_token|csrf_token|password|cookie)([\s:=]+)([^\s,;]+)",
                    r"\1\2[REDACTED]",
                    re.sub(
                        r"(?i)Bearer\s+[A-Za-z0-9+/_=.~-]+",
                        "Bearer [REDACTED]",
                        line,
                    ),
                )
                for line in text_lines[-lines:]
            ]
            files.append({
                "file": path.name,
                "updated_at": path.stat().st_mtime,
                "lines": redacted_lines,
            })
        except Exception as exc:
            files.append({"file": path.name, "error": str(exc), "lines": []})
        if len(files) >= 5:
            break

    return {"ok": True, "files": files}
