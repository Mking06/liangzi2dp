"""Project metadata."""

from __future__ import annotations

import tomllib
from pathlib import Path


def get_project_version() -> str:
    """Read version from pyproject.toml."""
    pyproject = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        return data["project"]["version"]
    except Exception:
        return "0.1.0"
