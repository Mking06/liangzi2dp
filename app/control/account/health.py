"""Lightweight account credential health monitoring."""

from __future__ import annotations

import asyncio
import random
from typing import Any

from app.dataplane.proxy.liangzidp_client import LiangzidpClient
from app.platform.config.snapshot import config
from app.platform.logging.logger import logger


class AccountHealthMonitor:
    """Checks account authentication without sending a model generation."""

    def __init__(self, repository: Any, orchestration_store: Any):
        self._repo = repository
        self._store = orchestration_store
        self._task: asyncio.Task[None] | None = None
        self._running = False

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="account-health-monitor")

    async def stop(self) -> None:
        self._running = False
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def run_once(self) -> dict[str, int]:
        rows = await self._repo.list_all()
        random.shuffle(rows)
        concurrency = max(1, config.get_int("account.health.concurrency", 5))
        timeout = max(5, config.get_int("account.health.timeout_seconds", 15))
        semaphore = asyncio.Semaphore(concurrency)
        stats = {
            "total": len(rows),
            "healthy": 0,
            "invalid": 0,
            "degraded": 0,
            "skipped": 0,
        }

        async def check(raw: dict[str, Any]) -> None:
            account_id = str(raw.get("id") or "")
            token = str(raw.get("bearer_token") or raw.get("session_token") or "")
            if not account_id or not token:
                stats["skipped"] += 1
                if account_id:
                    await self._store.record_health(
                        account_id,
                        status="missing_token",
                        ok=False,
                        latency_ms=0,
                        error="account has no bearer/session token",
                    )
                return

            async with semaphore:
                client = LiangzidpClient(
                    bearer_token=token,
                    session_token=token,
                    csrf_token=str(raw.get("csrf_token") or ""),
                    timeout=timeout,
                )
                detail = await client.verify_session_detail()

            ok = bool(detail.get("ok"))
            status = str(detail.get("status") or "unknown")
            await self._store.record_health(
                account_id,
                status=status,
                ok=ok,
                latency_ms=int(detail.get("latency_ms") or 0),
                error=str(detail.get("error") or ""),
            )
            current_status = str(raw.get("status") or "unknown")
            if ok:
                stats["healthy"] += 1
                if current_status in {"unknown", "expired"}:
                    raw["status"] = "active"
                    raw["error_count"] = 0
                    await self._repo.upsert(raw)
            elif status == "invalid_token":
                stats["invalid"] += 1
                if current_status != "banned":
                    raw["status"] = "expired"
                    raw["error_count"] = int(raw.get("error_count") or 0) + 1
                    await self._repo.upsert(raw)
            else:
                # Network and upstream outages are not account failures.
                stats["degraded"] += 1

        await asyncio.gather(*(check(row) for row in rows))
        return stats

    async def _loop(self) -> None:
        while self._running:
            try:
                stats = await self.run_once()
                logger.info(
                    "account health cycle complete: total={} healthy={} invalid={} degraded={} skipped={}",
                    stats["total"],
                    stats["healthy"],
                    stats["invalid"],
                    stats["degraded"],
                    stats["skipped"],
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("account health cycle failed: error={}", exc)
            await asyncio.sleep(max(30, config.get_int("account.health.interval_seconds", 120)))
