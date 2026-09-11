"""Account selection strategies."""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.dataplane.account.directory import AccountDirectory
    from app.dataplane.account.model import LiangzidpAccount


class SelectionStrategy(ABC):
    """Abstract account selection strategy."""

    @abstractmethod
    def select(self, directory: "AccountDirectory") -> "LiangzidpAccount | None":
        """Select an account from the directory."""
        ...


class RandomStrategy(SelectionStrategy):
    """Randomly select an available account."""

    def select(self, directory: "AccountDirectory"):
        accounts = directory.list_available()
        if not accounts:
            return None
        return random.choice(accounts)


class RoundRobinStrategy(SelectionStrategy):
    """Round-robin selection across available accounts."""

    def __init__(self):
        self._index = 0

    def select(self, directory: "AccountDirectory"):
        accounts = directory.list_available()
        if not accounts:
            return None
        account = accounts[self._index % len(accounts)]
        self._index += 1
        return account


class LeastUsedStrategy(SelectionStrategy):
    """Select the account with the fewest uses."""

    def select(self, directory: "AccountDirectory"):
        accounts = directory.list_available()
        if not accounts:
            return None
        return min(accounts, key=lambda a: a.use_count)
