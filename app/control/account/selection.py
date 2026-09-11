"""Account selection logic — picks an account for request routing."""

from __future__ import annotations

import random

from app.control.account.backends.base import AccountRepository
from app.dataplane.account.model import LiangzidpAccount


async def select_account(
    repo: AccountRepository,
    strategy: str = "random",
) -> LiangzidpAccount | None:
    """Select an available account using the configured strategy.

    This function MUST be called from an async context.
    """
    accounts_raw = await repo.list_all()

    accounts = [
        LiangzidpAccount.from_dict(a)
        for a in accounts_raw
        if LiangzidpAccount.from_dict(a).is_available()
    ]

    if not accounts:
        return None

    if strategy == "random":
        return random.choice(accounts)
    elif strategy == "least_used":
        return min(accounts, key=lambda a: a.use_count)
    else:
        return random.choice(accounts)
