"""Admin token management routes."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.control.admin.security import admin_store, client_ip, require_csrf

from app.dataplane.account.model import LiangzidpAccount
from app.dataplane.proxy.liangzidp_client import LiangzidpClient
from app.platform.errors import AccountError
from app.platform.logging.logger import logger
from app.platform.config.snapshot import config

router = APIRouter()


class AccountAddRequest(BaseModel):
    email: str = ""
    password: str = ""
    bearer_token: str = ""
    session_token: str = ""
    csrf_token: str = ""
    is_temporary: bool = False
    tier: str = "basic"
    priority: int = Field(default=100, ge=0, le=10000)
    weight: int = Field(default=100, ge=1, le=10000)
    max_inflight: int = Field(default=1, ge=1, le=256)
    daily_request_limit: int = Field(default=0, ge=0, le=1_000_000_000)
    daily_token_limit: int = Field(default=0, ge=0, le=100_000_000_000)
    daily_budget_usd: float = Field(default=0, ge=0, le=1_000_000)
    usd_per_million_tokens: float = Field(default=0, ge=0, le=1_000_000)


class AccountResponse(BaseModel):
    id: str
    email: str
    status: str
    tier: str
    is_available: bool
    has_token: bool = False
    is_temporary: bool = False
    use_count: int = 0
    error_count: int = 0
    last_used: str | None = None
    last_refresh: str | None = None
    priority: int = 100
    weight: int = 100
    max_inflight: int = 1
    inflight: int = 0
    active_sessions: int = 0
    cooling_down: bool = False
    cooldown_until: float | None = None
    cooldown_reason: str = ""
    success_count: int = 0
    failure_count: int = 0
    last_error: str = ""
    pool_added_at: float | None = None
    quota_window_started_at: float | None = None
    quota_reset_at: float | None = None
    quota_request_limit: int = 0
    quota_requests_used: int = 0
    quota_token_limit: int = 0
    quota_tokens_used: int = 0
    quota_budget_usd: float = 0
    quota_cost_usd: float = 0
    quota_usd_per_million_tokens: float = 0
    quota_exhausted: bool = False
    last_health_check_at: float | None = None
    last_health_ok_at: float | None = None
    health_status: str = "unknown"
    health_latency_ms: int | None = None
    health_error: str = ""
    health_consecutive_failures: int = 0
    pro_access: bool | None = None
    pro_access_checked_at: float | None = None


class AccountTestRequest(BaseModel):
    verify_session: bool = True
    test_pow: bool = True
    check_models: bool = True


class AccountUpdateRequest(BaseModel):
    email: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    session_token: str | None = None
    csrf_token: str | None = None
    is_temporary: bool | None = None
    status: str | None = None
    tier: str | None = None
    priority: int | None = Field(default=None, ge=0, le=10000)
    weight: int | None = Field(default=None, ge=1, le=10000)
    max_inflight: int | None = Field(default=None, ge=1, le=256)
    daily_request_limit: int | None = Field(default=None, ge=0, le=1_000_000_000)
    daily_token_limit: int | None = Field(default=None, ge=0, le=100_000_000_000)
    daily_budget_usd: float | None = Field(default=None, ge=0, le=1_000_000)
    usd_per_million_tokens: float | None = Field(default=None, ge=0, le=1_000_000)


class AccountImportRequest(BaseModel):
    accounts: list[AccountAddRequest] = Field(max_length=500)


def _validate_quota_policy(metadata: dict[str, Any]) -> None:
    budget = float(metadata.get("daily_budget_usd") or 0)
    rate = float(metadata.get("usd_per_million_tokens") or 0)
    if budget > 0 and rate <= 0:
        raise AccountError(
            "usd_per_million_tokens is required when daily_budget_usd is enabled",
            400,
        )


def _account_to_response(
    acc: LiangzidpAccount,
    raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
) -> AccountResponse:
    raw = raw or acc.to_dict()
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    runtime = runtime or {}
    return AccountResponse(
        id=acc.id,
        email=acc.email,
        status=acc.status.value,
        tier=acc.tier.value,
        is_available=(
            acc.is_available()
            and not bool(runtime.get("cooling_down"))
            and not bool(runtime.get("quota_exhausted"))
        ),
        has_token=bool(raw.get("bearer_token") or raw.get("session_token")),
        is_temporary=acc.is_temporary,
        use_count=acc.use_count,
        error_count=acc.error_count,
        last_used=acc.last_used.isoformat() if acc.last_used else None,
        last_refresh=acc.last_refresh.isoformat() if acc.last_refresh else None,
        priority=int(metadata.get("priority", 100)),
        weight=int(metadata.get("weight", 100)),
        max_inflight=int(
            metadata.get("max_inflight")
            or config.get_int("account.selection.max_inflight", 1)
        ),
        inflight=int(runtime.get("inflight") or 0),
        active_sessions=int(runtime.get("active_sessions") or 0),
        cooling_down=bool(runtime.get("cooling_down")),
        cooldown_until=runtime.get("cooldown_until"),
        cooldown_reason=str(runtime.get("cooldown_reason") or ""),
        success_count=int(runtime.get("success_count") or 0),
        failure_count=int(runtime.get("failure_count") or 0),
        last_error=str(runtime.get("last_error") or ""),
        pool_added_at=runtime.get("pool_added_at"),
        quota_window_started_at=runtime.get("quota_window_started_at"),
        quota_reset_at=runtime.get("quota_reset_at"),
        quota_request_limit=int(runtime.get("quota_request_limit") or 0),
        quota_requests_used=int(runtime.get("quota_requests_used") or 0),
        quota_token_limit=int(runtime.get("quota_token_limit") or 0),
        quota_tokens_used=int(runtime.get("quota_tokens_used") or 0),
        quota_budget_usd=float(runtime.get("quota_budget_usd") or 0),
        quota_cost_usd=float(runtime.get("quota_cost_usd") or 0),
        quota_usd_per_million_tokens=float(
            runtime.get("quota_usd_per_million_tokens") or 0
        ),
        quota_exhausted=bool(runtime.get("quota_exhausted")),
        last_health_check_at=runtime.get("last_health_check_at"),
        last_health_ok_at=runtime.get("last_health_ok_at"),
        health_status=str(runtime.get("health_status") or "unknown"),
        health_latency_ms=runtime.get("health_latency_ms"),
        health_error=str(runtime.get("health_error") or ""),
        health_consecutive_failures=int(runtime.get("health_consecutive_failures") or 0),
        pro_access=(
            bool(metadata.get("pro_access"))
            if "pro_access" in metadata
            else None
        ),
        pro_access_checked_at=metadata.get("pro_access_checked_at"),
    )


@router.get("/api/admin/accounts", response_model=list[AccountResponse], tags=["Admin - Tokens"])
@router.get("/v1/accounts", response_model=list[AccountResponse], tags=["Admin - Tokens"])
async def list_accounts(request: Request) -> list[AccountResponse]:
    """List all accounts."""
    repo = request.app.state.repository
    accounts = await repo.list_all()
    await request.app.state.orchestrator.sync_account_policies(accounts)
    runtime = await request.app.state.orchestration_store.runtime_snapshot(
        [str(account.get("id")) for account in accounts]
    )
    return [
        _account_to_response(LiangzidpAccount.from_dict(account), account, runtime.get(str(account.get("id"))))
        for account in accounts
    ]


@router.post("/api/admin/accounts", response_model=AccountResponse, tags=["Admin - Tokens"])
@router.post("/v1/accounts", response_model=AccountResponse, tags=["Admin - Tokens"])
async def add_account(
    request: Request,
    body: AccountAddRequest,
) -> AccountResponse:
    """Add a new account."""
    import secrets

    await require_csrf(request)

    token_value = body.session_token or body.bearer_token
    if not token_value and not (body.email and body.password):
        raise AccountError("Provide a session token or email and password", 400)

    repo = request.app.state.repository
    account_id = secrets.token_hex(8)

    account_data = {
        "id": account_id,
        "email": body.email,
        "password": body.password,
        "session_token": token_value,
        "bearer_token": token_value,
        "csrf_token": body.csrf_token,
        "status": "unknown",
        "tier": body.tier if body.tier in {"basic", "plus", "pro"} else "basic",
        "is_temporary": body.is_temporary,
        "metadata": {
            "priority": body.priority,
            "weight": body.weight,
            "pool_added_at": time.time(),
            "daily_request_limit": body.daily_request_limit,
            "daily_token_limit": body.daily_token_limit,
            "daily_budget_usd": body.daily_budget_usd,
            "usd_per_million_tokens": body.usd_per_million_tokens,
            **({"max_inflight": body.max_inflight} if body.max_inflight else {}),
        },
    }

    account_data["session_token"] = account_data["session_token"].strip().removeprefix("Bearer ").removeprefix("bearer ")
    account_data["bearer_token"] = account_data["session_token"]
    _validate_quota_policy(account_data["metadata"])
    await repo.upsert(account_data)
    await request.app.state.orchestrator.sync_account_policy(account_data)
    await admin_store(request).audit("account.created", "account", account_id, client_ip(request), detail=body.email[:100])
    logger.info("account added: id={} email={}", account_id, body.email[:20])

    runtime = await request.app.state.orchestration_store.runtime_snapshot([account_id])
    return _account_to_response(
        LiangzidpAccount.from_dict(account_data),
        account_data,
        runtime.get(account_id),
    )


@router.patch("/api/admin/accounts/{account_id}", response_model=AccountResponse, tags=["Admin - Tokens"])
async def update_account(request: Request, account_id: str, body: AccountUpdateRequest) -> AccountResponse:
    await require_csrf(request)
    repo = request.app.state.repository
    data = await repo.get(account_id)
    if not data:
        raise AccountError("Account not found", 404)
    values = body.model_dump(exclude_none=True)
    if values.get("status") not in {None, "active", "expired", "rate_limited", "banned", "unknown"}:
        raise AccountError("Invalid account status", 400)
    if values.get("tier") not in {None, "basic", "plus", "pro"}:
        raise AccountError("Invalid account tier", 400)
    scheduling = {
        key: values.pop(key)
        for key in (
            "priority",
            "weight",
            "max_inflight",
            "daily_request_limit",
            "daily_token_limit",
            "daily_budget_usd",
            "usd_per_million_tokens",
        )
        if key in values
    }
    token = values.pop("bearer_token", None) or values.pop("session_token", None)
    data.update(values)
    if scheduling:
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        data["metadata"] = {**metadata, **scheduling}
    if token is not None:
        token = token.strip().removeprefix("Bearer ").removeprefix("bearer ")
        data["session_token"] = token
        data["bearer_token"] = token
    _validate_quota_policy(
        data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    )
    await repo.upsert(data)
    await request.app.state.orchestrator.sync_account_policy(data)
    await admin_store(request).audit("account.updated", "account", account_id, client_ip(request), detail=",".join(values.keys()))
    runtime = await request.app.state.orchestration_store.runtime_snapshot([account_id])
    return _account_to_response(
        LiangzidpAccount.from_dict(data),
        data,
        runtime.get(account_id),
    )


@router.post("/api/admin/accounts/batch-import", tags=["Admin - Tokens"])
async def import_accounts(request: Request, body: AccountImportRequest) -> dict:
    await require_csrf(request)
    created: list[str] = []
    failed: list[dict[str, str]] = []
    for index, account in enumerate(body.accounts):
        try:
            result = await add_account(request, account)
            created.append(result.id)
        except Exception as exc:
            failed.append({"index": str(index), "error": str(exc)})
    await admin_store(request).audit("account.batch_import", "account", str(len(created)), client_ip(request), detail=f"failed={len(failed)}")
    return {"total": len(body.accounts), "created": created, "failed": failed}


@router.get("/api/admin/accounts/export", tags=["Admin - Tokens"])
async def export_accounts(request: Request) -> dict:
    rows = await request.app.state.repository.list_all()
    accounts = []
    for row in rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        accounts.append({
            "id": row.get("id"),
            "email": row.get("email", ""),
            "status": row.get("status", "unknown"),
            "tier": row.get("tier", "basic"),
            "is_temporary": bool(row.get("is_temporary")),
            "has_token": bool(row.get("bearer_token") or row.get("session_token")),
            "priority": int(metadata.get("priority") or 100),
            "weight": int(metadata.get("weight") or 100),
            "max_inflight": int(
                metadata.get("max_inflight")
                or config.get_int("account.selection.max_inflight", 1)
            ),
            "daily_request_limit": int(metadata.get("daily_request_limit") or 0),
            "daily_token_limit": int(metadata.get("daily_token_limit") or 0),
            "daily_budget_usd": float(metadata.get("daily_budget_usd") or 0),
            "usd_per_million_tokens": float(
                metadata.get("usd_per_million_tokens") or 0
            ),
        })
    return {"accounts": accounts, "secrets_included": False}


@router.delete("/api/admin/accounts/{account_id}", tags=["Admin - Tokens"])
@router.delete("/v1/accounts/{account_id}", tags=["Admin - Tokens"])
async def delete_account(request: Request, account_id: str) -> dict:
    """Delete an account."""
    await require_csrf(request)
    repo = request.app.state.repository
    await repo.delete(account_id)
    await request.app.state.orchestration_store.delete_account_runtime(account_id)
    await admin_store(request).audit("account.deleted", "account", account_id, client_ip(request))
    logger.info("account deleted: id={}", account_id)
    return {"deleted": account_id}


@router.post("/api/admin/accounts/{account_id}/refresh", tags=["Admin - Tokens"])
@router.post("/v1/accounts/{account_id}/refresh", tags=["Admin - Tokens"])
async def refresh_account(request: Request, account_id: str) -> dict:
    """Force refresh an account's session."""
    await require_csrf(request)
    refresh_svc = request.app.state.refresh_service
    if not refresh_svc:
        raise AccountError("Refresh service not available", 503)

    success = await refresh_svc.refresh_account(account_id)
    await admin_store(request).audit("account.refreshed", "account", account_id, client_ip(request), success=success)
    return {"id": account_id, "refreshed": success}


@router.post("/api/admin/accounts/{account_id}/test", tags=["Admin - Tokens"])
@router.post("/v1/accounts/{account_id}/test", tags=["Admin - Tokens"])
async def test_account(
    request: Request,
    account_id: str,
    body: AccountTestRequest | None = None,
) -> dict[str, Any]:
    """Verify one account token and optionally exercise the POW path."""
    await require_csrf(request)
    body = body or AccountTestRequest()
    repo = request.app.state.repository
    account_data = await repo.get(account_id)
    if not account_data:
        raise AccountError("Account not found", 404)

    token = account_data.get("bearer_token") or account_data.get("session_token") or ""
    csrf_token = account_data.get("csrf_token") or ""
    if not token:
        raise AccountError("Account has no bearer/session token", 400)

    client = LiangzidpClient(
        bearer_token=token,
        session_token=token,
        csrf_token=csrf_token,
        timeout=30,
    )

    started = time.perf_counter()
    checks: dict[str, Any] = {}
    ok = True

    if body.verify_session:
        session_detail = await client.verify_session_detail()
        session_ok = bool(session_detail.get("ok"))
        checks["session"] = {
            "ok": session_ok,
            "status": session_detail.get("status"),
            "http_status": session_detail.get("http_status"),
            "latency_ms": session_detail.get("latency_ms"),
            **(
                {"error": session_detail.get("error")}
                if session_detail.get("error")
                else {}
            ),
        }
        ok = ok and session_ok

    if body.test_pow:
        pow_started = time.perf_counter()
        try:
            pow_header = await client.build_pow_response("/api/v0/chat/completion")
            checks["pow"] = {
                "ok": True,
                "latency_ms": round((time.perf_counter() - pow_started) * 1000),
                "header_len": len(pow_header),
            }
        except Exception as exc:
            checks["pow"] = {
                "ok": False,
                "latency_ms": round((time.perf_counter() - pow_started) * 1000),
                "error": str(exc),
            }
            ok = False

    if body.check_models and checks.get("session", {}).get("ok"):
        model_started = time.perf_counter()
        try:
            models = await client.get_web_model_configs()
            summary = [
                {
                    "model_type": str(item.get("model_type") or ""),
                    "enabled": bool(item.get("enabled")),
                    "switchable": bool(item.get("switchable")),
                    "input_character_limit": int(item.get("input_character_limit") or 0),
                    "file_token_limit": int(
                        (item.get("file_feature") or {}).get("token_limit") or 0
                    ),
                }
                for item in models
            ]
            pro_access = any(
                item["model_type"] == "expert"
                and item["enabled"]
                and item["switchable"]
                for item in summary
            )
            checks["models"] = {
                "ok": True,
                "latency_ms": round((time.perf_counter() - model_started) * 1000),
                "pro_access": pro_access,
                "models": summary,
            }
            metadata = (
                account_data.get("metadata")
                if isinstance(account_data.get("metadata"), dict)
                else {}
            )
            account_data["metadata"] = {
                **metadata,
                "pro_access": pro_access,
                "pro_access_checked_at": time.time(),
            }
        except Exception as exc:
            checks["models"] = {
                "ok": False,
                "latency_ms": round((time.perf_counter() - model_started) * 1000),
                "error": str(exc),
            }

    if body.verify_session:
        session_status = str(checks["session"].get("status") or "unknown")
        if checks["session"]["ok"] and account_data.get("status") != "banned":
            account_data["status"] = "active"
            account_data["error_count"] = 0
        elif session_status == "invalid_token" and account_data.get("status") != "banned":
            account_data["status"] = "expired"
            account_data["error_count"] = int(account_data.get("error_count") or 0) + 1
        await request.app.state.orchestration_store.record_health(
            account_id,
            status=session_status,
            ok=bool(checks["session"]["ok"]),
            latency_ms=int(checks["session"].get("latency_ms") or 0),
            error=str(checks["session"].get("error") or ""),
        )
        account_data["last_refresh"] = datetime.now(UTC).isoformat()
        await repo.upsert(account_data)
        if session_status == "invalid_token":
            await request.app.state.orchestration_store.set_cooldown(
                account_id,
                config.get_int("account.selection.cooldown_seconds", 300),
                "session verification failed",
            )
    await admin_store(request).audit(
        "account.tested",
        "account",
        account_id,
        client_ip(request),
        success=ok,
        detail=",".join(f"{name}={bool(value.get('ok'))}" for name, value in checks.items()),
    )

    return {
        "ok": ok,
        "id": account_id,
        "latency_ms": round((time.perf_counter() - started) * 1000),
        "checks": checks,
    }


@router.post("/api/admin/accounts/{account_id}/quota/reset", tags=["Admin - Tokens"])
async def reset_account_quota(request: Request, account_id: str) -> dict[str, Any]:
    """Reset local usage counters without changing the insertion-anchored schedule."""
    await require_csrf(request)
    account_data = await request.app.state.repository.get(account_id)
    if not account_data:
        raise AccountError("Account not found", 404)
    await request.app.state.orchestrator.sync_account_policy(account_data)
    runtime = await request.app.state.orchestration_store.reset_quota(account_id)
    await admin_store(request).audit(
        "account.quota_reset",
        "account",
        account_id,
        client_ip(request),
    )
    return {"ok": True, "id": account_id, "quota": runtime or {}}
