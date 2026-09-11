"""Logging setup using loguru."""

from __future__ import annotations

import sys

from loguru import logger

from app.platform.paths import LOG_DIR


def setup_logging(level: str = "INFO", file_logging: bool = True) -> None:
    """Configure loguru logging."""
    logger.remove()  # Remove default handler

    # Console handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=level,
    )

    if file_logging:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(LOG_DIR / "grok2api.log"),
            rotation="10 MB",
            retention="7 days",
            encoding="utf-8",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=level,
        )


def reload_logging(level: str = "INFO", file_level: str | None = None, max_files: int = 7) -> None:
    """Reload logging with new settings."""
    setup_logging(level=level, file_logging=True)
