"""Startup migrations."""

from __future__ import annotations


async def run_startup_migrations(config_backend, account_repo) -> None:
    """Run first-boot migrations: seed config, migrate accounts, etc."""
    # No-op for now — placeholder for future migration logic
    pass
