"""Liangzidp - OpenAI/Anthropic 兼容 API 网关 for Liangzidp Web

启动方式:
  uv run granian --interface asgi --host 0.0.0.0 --port 8000 app.main:app
"""

import asyncio
import os
import platform
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.platform.logging.logger import logger, setup_logging, reload_logging
from app.platform.config.snapshot import config as _config
from app.platform.errors import AppError
from app.platform.meta import get_project_version
from app.platform.paths import data_path
from app.platform.storage import reconcile_local_media_cache_async

load_dotenv()

# ---------------------------------------------------------------------------
# Scheduler leader-election via advisory file lock
# ---------------------------------------------------------------------------

_LOCK_FILE = data_path(".scheduler.lock")
_lock_fd = None


def _try_acquire_scheduler_lock() -> bool:
    """Non-blocking attempt to become the scheduler leader."""
    global _lock_fd
    _LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        import fcntl

        fd = os.open(str(_LOCK_FILE), os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd = fd
        return True
    except BlockingIOError:
        return False
    except ImportError:
        try:
            import msvcrt

            fd = os.open(str(_LOCK_FILE), os.O_CREAT | os.O_RDWR)
            if os.path.getsize(_LOCK_FILE) == 0:
                os.write(fd, b"0")
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            _lock_fd = fd
            return True
        except OSError:
            try:
                os.close(fd)
            except (OSError, UnboundLocalError):
                pass
            return False
    except (OSError, AttributeError):
        return False


def _release_scheduler_lock() -> None:
    global _lock_fd
    if _lock_fd is None:
        return
    try:
        if os.name == "nt":
            import msvcrt

            os.lseek(_lock_fd, 0, os.SEEK_SET)
            msvcrt.locking(_lock_fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
    except (ImportError, OSError):
        pass
    try:
        os.close(_lock_fd)
    except OSError:
        pass
    _lock_fd = None


# ---------------------------------------------------------------------------
# Early logging setup
# ---------------------------------------------------------------------------

setup_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    file_logging=os.getenv("LOG_FILE_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"},
)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Load configuration.
    await _config.load()
    reload_logging(
        level=os.getenv("LOG_LEVEL", "INFO"),
        file_level=_config.get_str("logging.file_level", "") or None,
        max_files=_config.get_int("logging.max_files", 7),
    )
    logger.info(
        "application startup: service=liangzidp python={} platform={}",
        sys.version.split()[0],
        platform.system(),
    )

    # 1a. Initialize the isolated administration database and first admin.
    from app.control.admin.store import AdminStore

    admin_store = AdminStore(os.getenv("ADMIN_DB_PATH") or None)
    await admin_store.initialize()
    admin_username = os.getenv("ADMIN_USERNAME", "").strip()
    admin_password = os.getenv("ADMIN_PASSWORD", "")
    if not await admin_store.has_admin():
        if not admin_username or len(admin_password) < 12:
            await admin_store.close()
            raise RuntimeError(
                "Admin is not initialized. Set ADMIN_USERNAME and an "
                "ADMIN_PASSWORD of at least 12 characters for the first start."
            )
        await admin_store.bootstrap_admin(admin_username, admin_password)
        logger.info("administration account initialized: username={}", admin_username)
    app.state.admin_store = admin_store

    # 2. Initialise account repository and bootstrap runtime table.
    from app.control.account.backends.factory import (
        create_repository,
        describe_repository_target,
    )
    from app.control.account.runtime import (
        reconcile_refresh_runtime,
        set_refresh_scheduler,
        set_refresh_scheduler_leader,
        set_refresh_service,
    )
    from app.control.account.scheduler import get_account_refresh_scheduler
    from app.dataplane.account import get_account_directory

    storage_backend, storage_target = describe_repository_target()
    logger.info(
        "account storage configured: backend={} target={}",
        storage_backend,
        storage_target,
    )

    repo = create_repository()
    await repo.initialize()

    from app.control.orchestration import OrchestrationStore, SessionOrchestrator

    orchestration_store = OrchestrationStore(os.getenv("ORCHESTRATION_DB_PATH") or None)
    await orchestration_store.initialize()
    orchestrator = SessionOrchestrator(orchestration_store)
    app.state.orchestration_store = orchestration_store
    app.state.orchestrator = orchestrator

    # 2a. First-boot migrations
    from app.platform.startup import run_startup_migrations

    await run_startup_migrations(
        config_backend=_config._get_backend(),
        account_repo=repo,
    )
    await _config.load()
    await reconcile_local_media_cache_async()

    # Existing accounts predate quota metadata. Persist a one-time migration
    # anchor; all new accounts receive their exact insertion timestamp.
    account_rows = await repo.list_all()
    anchor_now = time.time()
    for account_data in account_rows:
        metadata = (
            account_data.get("metadata")
            if isinstance(account_data.get("metadata"), dict)
            else {}
        )
        if not metadata.get("pool_added_at"):
            account_data["metadata"] = {**metadata, "pool_added_at": anchor_now}
            await repo.upsert(account_data)
    await orchestrator.sync_account_policies(account_rows)

    directory = await get_account_directory(repo)

    # Expose repository on app.state for admin handlers.
    app.state.repository = repo
    app.state.directory = directory

    # 3. Account directory sync loop — all workers.
    _SYNC_IDLE_INTERVAL = int(os.getenv("ACCOUNT_SYNC_INTERVAL", "30"))
    _SYNC_ACTIVE_INTERVAL = int(os.getenv("ACCOUNT_SYNC_ACTIVE_INTERVAL", "3"))
    _SYNC_IDLE_AFTER = 5

    async def _sync_loop() -> None:
        idle_streak = 0
        while True:
            interval = (
                _SYNC_ACTIVE_INTERVAL
                if idle_streak < _SYNC_IDLE_AFTER
                else _SYNC_IDLE_INTERVAL
            )
            await asyncio.sleep(interval)
            try:
                changed = await directory.sync_if_changed(repo)
                idle_streak = 0 if changed else min(idle_streak + 1, _SYNC_IDLE_AFTER)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("account directory sync error: error={}", exc)
                idle_streak = _SYNC_IDLE_AFTER

    sync_task = asyncio.create_task(_sync_loop(), name="account-dir-sync")

    async def _admin_cleanup_loop() -> None:
        while True:
            try:
                await admin_store.cleanup()
                await orchestration_store.cleanup()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("admin data cleanup failed: error={}", exc)
            await asyncio.sleep(3600)

    admin_cleanup_task = asyncio.create_task(_admin_cleanup_loop(), name="admin-data-cleanup")

    # 4. Account refresh scheduler — leader only.
    from app.control.account.refresh import AccountRefreshService

    refresh_enabled = _config.get_bool("account.refresh.enabled", False)

    refresh_svc = AccountRefreshService(repo)
    set_refresh_service(refresh_svc)
    app.state.refresh_service = refresh_svc

    is_leader = _try_acquire_scheduler_lock()
    scheduler = get_account_refresh_scheduler(refresh_svc)
    set_refresh_scheduler(scheduler)
    set_refresh_scheduler_leader(is_leader)
    app.state.account_refresh_scheduler = scheduler
    app.state.account_refresh_is_leader = is_leader

    strategy_name = reconcile_refresh_runtime(refresh_enabled)
    if is_leader and strategy_name == "quota":
        logger.info(
            "scheduler leader: pid={} strategy=quota active_sync_s={} idle_sync_s={}",
            os.getpid(),
            _SYNC_ACTIVE_INTERVAL,
            _SYNC_IDLE_INTERVAL,
        )
    elif is_leader:
        logger.info(
            "scheduler leader: pid={} strategy=random (scheduler idle) "
            "active_sync_s={} idle_sync_s={}",
            os.getpid(),
            _SYNC_ACTIVE_INTERVAL,
            _SYNC_IDLE_INTERVAL,
        )
    else:
        logger.info(
            "scheduler follower: pid={} strategy={} active_sync_s={} idle_sync_s={}",
            os.getpid(),
            strategy_name,
            _SYNC_ACTIVE_INTERVAL,
            _SYNC_IDLE_INTERVAL,
        )

    # 4a. Lightweight credential health checks — leader only. This probes the
    # authenticated session endpoint and never sends a model generation.
    from app.control.account.health import AccountHealthMonitor

    health_monitor = AccountHealthMonitor(repo, orchestration_store)
    app.state.account_health_monitor = health_monitor
    if is_leader and _config.get_bool("account.health.enabled", True):
        health_monitor.start()

    # 5. Initialise proxy directory and start clearance refresh scheduler.
    from app.control.proxy import get_proxy_directory
    from app.control.proxy.scheduler import ProxyClearanceScheduler

    proxy_dir = get_proxy_directory()
    proxy_scheduler = ProxyClearanceScheduler(proxy_dir)
    if is_leader:
        proxy_scheduler.start()

    logger.info("application startup completed")
    yield

    # -----------
    # Shutdown
    # -----------
    logger.info("application shutdown started")
    sync_task.cancel()
    admin_cleanup_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass
    try:
        await admin_cleanup_task
    except asyncio.CancelledError:
        pass

    if is_leader:
        await health_monitor.stop()
        scheduler.stop()
        proxy_scheduler.stop()
        _release_scheduler_lock()

    set_refresh_scheduler(None)
    set_refresh_scheduler_leader(False)
    set_refresh_service(None)
    await repo.close()
    await orchestration_store.close()
    await admin_store.close()
    logger.info("application shutdown completed")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    openapi_tags = [
        {"name": "OpenAI - Models", "description": "Model discovery endpoints."},
        {
            "name": "OpenAI - Chat",
            "description": "Chat Completions compatible endpoints.",
        },
        {
            "name": "OpenAI - Responses",
            "description": "Responses API compatible endpoints.",
        },
        {
            "name": "OpenAI - Images",
            "description": "Image generation endpoints.",
        },
        {
            "name": "OpenAI - Files",
            "description": "Locally cached media file endpoints.",
        },
        {"name": "Anthropic - Messages", "description": "Anthropic Messages API compatible endpoints."},
        {"name": "Admin - System", "description": "Admin system status and runtime configuration endpoints."},
        {"name": "Admin - Tokens", "description": "Admin account token management endpoints."},
        {"name": "Admin - Batch", "description": "Admin batch operation endpoints."},
        {"name": "Admin - Auth", "description": "Private console authentication endpoints."},
        {"name": "Admin - API Keys", "description": "Client API key lifecycle endpoints."},
        {"name": "Admin - Operations", "description": "Operational metrics, settings, and audit endpoints."},
        {"name": "WebUI - System", "description": "WebUI authentication and system endpoints."},
        {"name": "WebUI - Chat", "description": "WebUI chat API endpoints."},
    ]

    app = FastAPI(
        title="Liangzidp",
        version=get_project_version(),
        description="OpenAI-compatible API gateway for Liangzidp Web",
        openapi_tags=openapi_tags,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.control.admin.middleware import AdminGatewayMiddleware

    app.add_middleware(AdminGatewayMiddleware)

    # Ensure config is loaded on every request.
    @app.middleware("http")
    async def _ensure_config(request: Request, call_next):
        from app.control.account.runtime import reconcile_refresh_runtime

        await _config.load()
        reconcile_refresh_runtime(_config.get_bool("account.refresh.enabled", False))
        return await call_next(request)

    # Global exception handler
    @app.exception_handler(AppError)
    async def _app_error_handler(request: Request, exc: AppError):
        headers = {}
        if exc.status == 429 and exc.details and exc.details.get("retry_after"):
            headers["Retry-After"] = str(exc.details["retry_after"])
        return JSONResponse(exc.to_dict(), status_code=exc.status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(request: Request, exc: RequestValidationError):
        errors = exc.errors()
        first = errors[0] if errors else {}
        loc = tuple(first.get("loc") or ())
        param_parts = [
            str(part)
            for part in loc
            if str(part) not in {"body", "query", "path", "header", "cookie"}
        ]
        param = ".".join(param_parts)
        message = first.get("msg") or "Request validation failed"
        logger.warning(
            "request validation failed: method={} path={} param={} errors={}",
            request.method,
            request.url.path,
            param or "-",
            errors,
        )
        payload = {
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "code": "invalid_value",
            }
        }
        if param:
            payload["error"]["param"] = param
        return JSONResponse(payload, status_code=400)

    @app.exception_handler(Exception)
    async def _generic_error_handler(request: Request, exc: Exception):
        logger.exception("unhandled application exception: error={}", exc)
        return JSONResponse(
            {"error": {"message": "Internal server error", "type": "server_error"}},
            status_code=500,
        )

    # Routers
    from app.products.web import router as web_router
    from app.products.openai.router import router as openai_router
    from app.products.openai.responses import router as responses_router
    from app.products.anthropic.router import router as anthropic_router
    from app.control.admin import router as admin_router

    app.include_router(web_router)
    app.include_router(admin_router)
    app.include_router(openai_router)
    app.include_router(responses_router)
    app.include_router(anthropic_router)

    # Static assets
    _statics_dir = Path(__file__).resolve().parent / "statics"
    if _statics_dir.is_dir():
        app.mount("/static", StaticFiles(directory=_statics_dir), name="static")

    @app.get("/admin", include_in_schema=False)
    @app.get("/admin/", include_in_schema=False)
    @app.get("/admin/{path:path}", include_in_schema=False)
    def admin_panel(path: str = ""):
        _index = _statics_dir / "admin" / "index.html"
        if _index.exists():
            return FileResponse(_index)
        return JSONResponse({"error": "admin panel not found"}, status_code=404)

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon():
        from fastapi.responses import FileResponse as _FR
        _ico = _statics_dir / "favicon.ico"
        if _ico.exists():
            return _FR(_ico)
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.get("/health", include_in_schema=False)
    def health():
        return {"status": "ok"}

    return app


app = create_app()

if __name__ == "__main__":
    logger.error(
        "direct startup is disabled: use "
        "uv run granian --interface asgi --host 0.0.0.0 --port 8000 app.main:app"
    )
    raise SystemExit(1)
