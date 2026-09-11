"""Account runtime — module-level state for refresh service."""

from __future__ import annotations

from typing import Any

_refresh_service: Any = None
_refresh_scheduler: Any = None
_is_scheduler_leader: bool = False


def set_refresh_service(svc) -> None:
    global _refresh_service
    _refresh_service = svc


def get_refresh_service():
    return _refresh_service


def set_refresh_scheduler(scheduler) -> None:
    global _refresh_scheduler
    _refresh_scheduler = scheduler


def get_refresh_scheduler():
    return _refresh_scheduler


def set_refresh_scheduler_leader(is_leader: bool) -> None:
    global _is_scheduler_leader
    _is_scheduler_leader = is_leader


def get_refresh_scheduler_leader() -> bool:
    return _is_scheduler_leader


def reconcile_refresh_runtime(refresh_enabled: bool) -> str:
    """Reconcile refresh runtime strategy based on config."""
    strategy = "quota" if refresh_enabled else "random"
    if _refresh_scheduler:
        _refresh_scheduler.set_strategy(strategy)
    return strategy
