"""Account refresh service — keeps session tokens alive."""

from __future__ import annotations

import asyncio
import random

from app.control.account.backends.base import AccountRepository
from app.control.account.refresh.auth import LiangzidpAuthRefresher
from app.platform.logging.logger import logger


class AccountRefreshService:
    """Manages account session refresh cycles."""

    def __init__(self, repo: AccountRepository):
        self._repo = repo
        self._running = False

    async def refresh_account(self, account_id: str) -> bool:
        """Refresh a single account's session token.

        Returns True if successful, False otherwise.
        """
        account_data = await self._repo.get(account_id)
        if not account_data:
            return False

        refresher = LiangzidpAuthRefresher(account_data)
        success = await refresher.refresh()

        if success:
            await self._repo.upsert(refresher.get_updated_account())
            logger.info("account refreshed: id={}", account_id)
        else:
            logger.warning("account refresh failed: id={}", account_id)

        return success

    async def refresh_all(self, max_concurrency: int = 10) -> dict[str, int]:
        """Refresh all accounts concurrently.

        Returns stats dict with success/failure counts.
        """
        accounts = await self._repo.list_all()
        semaphore = asyncio.Semaphore(max_concurrency)

        stats = {"total": len(accounts), "success": 0, "failed": 0}

        async def _refresh_one(acc_data: dict):
            async with semaphore:
                if await self.refresh_account(acc_data["id"]):
                    stats["success"] += 1
                else:
                    stats["failed"] += 1

        tasks = [_refresh_one(a) for a in accounts]
        await asyncio.gather(*tasks, return_exceptions=True)
        return stats

    async def refresh_random_batch(self, count: int = 5) -> dict[str, int]:
        """Refresh a random batch of accounts."""
        accounts = await self._repo.list_all()
        if not accounts:
            return {"total": 0, "success": 0, "failed": 0}

        batch = random.sample(accounts, min(count, len(accounts)))
        stats = {"total": len(batch), "success": 0, "failed": 0}

        for acc in batch:
            if await self.refresh_account(acc["id"]):
                stats["success"] += 1
            else:
                stats["failed"] += 1

        return stats
