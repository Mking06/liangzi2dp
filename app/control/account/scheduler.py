"""Account refresh scheduler — periodic token refresh."""

from __future__ import annotations

from app.platform.logging.logger import logger

_scheduler: "AccountRefreshScheduler | None" = None


class AccountRefreshScheduler:
    """Manages periodic account token refresh."""

    def __init__(self, refresh_service):
        self._svc = refresh_service
        self._task = None
        self._running = False
        self._strategy = "quota"

    def set_strategy(self, strategy: str) -> None:
        """Set refresh strategy: 'quota' or 'random'."""
        self._strategy = strategy

    def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        import asyncio
        self._task = asyncio.create_task(self._scheduler_loop())

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                if self._strategy == "quota":
                    await self._quota_cycle()
                else:
                    import asyncio
                    await asyncio.sleep(3600)
            except Exception as exc:
                logger.error("scheduler error: error={}", exc)
                import asyncio
                await asyncio.sleep(60)

    async def _quota_cycle(self) -> None:
        """Full quota check cycle."""
        stats = await self._svc.refresh_all(max_concurrency=10)
        logger.info(
            "refresh cycle complete: total={} success={} failed={}",
            stats["total"],
            stats["success"],
            stats["failed"],
        )
        import asyncio
        await asyncio.sleep(1800)


def get_account_refresh_scheduler(refresh_service) -> AccountRefreshScheduler:
    """Get or create the process-local account refresh scheduler."""
    global _scheduler
    if _scheduler is None or _scheduler._svc is not refresh_service:
        _scheduler = AccountRefreshScheduler(refresh_service)
    return _scheduler
