"""Proxy directory — manages egress proxy pool."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any



class ProxyEntry:
    """A single proxy entry with health tracking."""

    def __init__(self, address: str, enabled: bool = True):
        self.address = address
        self.enabled = enabled
        self.last_check: datetime | None = None
        self.last_status: str = "unknown"  # ok | fail | unknown
        self.consecutive_fails = 0
        self.total_requests = 0
        self.failed_requests = 0

    @property
    def is_healthy(self) -> bool:
        if not self.enabled:
            return False
        if self.consecutive_fails >= 3:
            return False
        return True

    def record_success(self) -> None:
        self.last_status = "ok"
        self.last_check = datetime.utcnow()
        self.consecutive_fails = 0
        self.total_requests += 1

    def record_failure(self) -> None:
        self.last_status = "fail"
        self.last_check = datetime.utcnow()
        self.consecutive_fails += 1
        self.total_requests += 1
        self.failed_requests += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "enabled": self.enabled,
            "is_healthy": self.is_healthy,
            "last_status": self.last_status,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "consecutive_fails": self.consecutive_fails,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
        }


class ProxyDirectory:
    """Manages a pool of egress proxies."""

    def __init__(self):
        self._proxies: dict[str, ProxyEntry] = {}
        self._lock = asyncio.Lock()
        self._index = 0

    async def load(self) -> None:
        """Load proxies from config."""
        # Proxies can be added via config or admin API
        pass

    def add_proxy(self, address: str, enabled: bool = True) -> None:
        """Add a proxy to the pool."""
        self._proxies[address] = ProxyEntry(address, enabled)

    def remove_proxy(self, address: str) -> None:
        """Remove a proxy from the pool."""
        self._proxies.pop(address, None)

    def list_proxies(self) -> list[dict[str, Any]]:
        """List all proxies with their health status."""
        return [p.to_dict() for p in self._proxies.values()]

    def get_proxy(self) -> str | None:
        """Get a healthy proxy using round-robin."""
        healthy = [p for p in self._proxies.values() if p.is_healthy]
        if not healthy:
            return None

        # Round-robin through healthy proxies
        proxy = healthy[self._index % len(healthy)]
        self._index += 1
        return proxy.address

    def record_result(self, address: str, success: bool) -> None:
        """Record the result of a proxy request."""
        proxy = self._proxies.get(address)
        if proxy:
            if success:
                proxy.record_success()
            else:
                proxy.record_failure()

    @property
    def healthy_count(self) -> int:
        return sum(1 for p in self._proxies.values() if p.is_healthy)

    @property
    def total_count(self) -> int:
        return len(self._proxies)
