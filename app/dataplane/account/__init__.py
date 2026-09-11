"""Account directory singleton."""

from __future__ import annotations

from app.dataplane.account.directory import AccountDirectory

_directory: AccountDirectory | None = None


async def get_account_directory(repo) -> AccountDirectory:
    """Get or create the global account directory."""
    global _directory
    if _directory is None:
        _directory = AccountDirectory()
        await _directory.load_from_repo(repo)
    return _directory
