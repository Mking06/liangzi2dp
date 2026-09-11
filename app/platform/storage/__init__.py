"""Local media cache reconciliation."""

from __future__ import annotations

import asyncio

from app.platform.paths import data_path


async def reconcile_local_media_cache_async() -> None:
    """Clean up expired local cache entries. Runs in background."""
    cache_dir = data_path("cache")
    if not cache_dir.exists():
        return

    # Simple cleanup: remove files older than 24h
    cutoff = asyncio.get_event_loop().time() - 86400 if hasattr(asyncio, 'get_event_loop') else 0

    try:
        for f in cache_dir.iterdir():
            if f.is_file():
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                except OSError:
                    pass
    except Exception:
        pass
