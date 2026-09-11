"""Proxy directory singleton."""

from __future__ import annotations

from app.control.proxy.directory import ProxyDirectory

_proxy_dir: ProxyDirectory | None = None


def get_proxy_directory() -> ProxyDirectory:
    """Get the global proxy directory instance."""
    global _proxy_dir
    if _proxy_dir is None:
        _proxy_dir = ProxyDirectory()
    return _proxy_dir
