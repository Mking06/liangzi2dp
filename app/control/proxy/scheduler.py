"""Proxy clearance scheduler — tests proxies for health."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.platform.logging.logger import logger

if TYPE_CHECKING:
    from app.control.proxy.directory import ProxyDirectory


class ProxyClearanceScheduler:
    """Periodically tests proxy health."""

    def __init__(self, proxy_dir: "ProxyDirectory"):
        self._dir = proxy_dir
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self, interval: int = 300) -> None:
        """Start the clearance loop."""
        self._running = True
        self._task = asyncio.create_task(self._clearance_loop(interval))

    def stop(self) -> None:
        """Stop the clearance loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _clearance_loop(self, interval: int) -> None:
        """Periodically test all proxies."""
        while self._running:
            await asyncio.sleep(interval)
            if not self._running:
                break
            await self._test_all_proxies()

    async def _test_all_proxies(self) -> None:
        """Test all proxies for connectivity."""
        import curl_cffi.requests

        for address in list(self._dir._proxies.keys()):
            try:
                async with curl_cffi.requests.AsyncSession(
                    impersonate="chrome137",
                    proxies={"http": address, "https": address},
                    timeout=10,
                ) as session:
                    resp = await session.get("https://httpbin.org/ip", timeout=10)
                    if resp.status_code == 200:
                        self._dir.record_result(address, True)
                    else:
                        self._dir.record_result(address, False)
            except Exception:
                self._dir.record_result(address, False)

        logger.info(
            "proxy clearance complete: total={} healthy={}",
            self._dir.total_count,
            self._dir.healthy_count,
        )
