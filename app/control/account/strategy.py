"""Account selection policy."""

from __future__ import annotations

from app.dataplane.account.strategy import (
    LeastUsedStrategy,
    RandomStrategy,
    RoundRobinStrategy,
    SelectionStrategy,
)


def get_selection_strategy(name: str) -> SelectionStrategy:
    """Get a selection strategy by name."""
    strategies = {
        "random": RandomStrategy,
        "round_robin": RoundRobinStrategy,
        "least_used": LeastUsedStrategy,
    }
    cls = strategies.get(name, RandomStrategy)
    return cls()
