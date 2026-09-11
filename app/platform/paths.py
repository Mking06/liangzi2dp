"""Path helpers."""

from __future__ import annotations

import os
from pathlib import Path

# Project root: go up from app/platform/ -> ../../../
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = Path(os.getenv("DATA_DIR", str(_PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(_PROJECT_ROOT / "logs")))


def data_path(*parts: str) -> Path:
    """Return a path under the data directory."""
    p = DATA_DIR
    for part in parts:
        p = p / part
    return p


def log_path(*parts: str) -> Path:
    """Return a path under the log directory."""
    p = LOG_DIR
    for part in parts:
        p = p / part
    return p


# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
