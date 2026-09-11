"""Configuration management using TOML files."""

from __future__ import annotations

import os
import tempfile
import tomllib
from pathlib import Path
from threading import Lock
from typing import Any

import tomli_w

from app.platform.paths import data_path

_config_lock = Lock()
_config_backend = None


class ConfigSnapshot:
    """Thread-safe TOML configuration with disk persistence."""

    def __init__(self):
        self._data: dict[str, Any] = {}
        self._loaded = False

    async def load(self) -> None:
        """Load config from disk."""
        config_file = data_path("config.toml")
        defaults = Path(__file__).resolve().parents[3] / "config.defaults.toml"
        default_data: dict[str, Any] = {}
        if defaults.exists():
            with open(defaults, "rb") as f:
                default_data = tomllib.load(f)

        if not config_file.exists():
            # Seed from defaults
            self._data = default_data
            self._save(config_file)
        else:
            with open(config_file, "rb") as f:
                user_data = tomllib.load(f)
            self._data = self._merge(default_data, user_data)
            if not user_data and default_data:
                self._save(config_file)
        self._loaded = True

    def _merge(self, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Deep-merge config dictionaries."""
        result = dict(base)
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._merge(result[key], value)
            else:
                result[key] = value
        return result

    def _save(self, path: Path) -> None:
        """Persist config to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = tomli_w.dumps(self._data).encode("utf-8")
        temporary = ""
        with _config_lock:
            try:
                descriptor, temporary = tempfile.mkstemp(
                    prefix=f".{path.name}.",
                    suffix=".tmp",
                    dir=path.parent,
                )
                with os.fdopen(descriptor, "wb") as file:
                    file.write(payload)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, path)
                temporary = ""
            finally:
                if temporary:
                    try:
                        os.unlink(temporary)
                    except FileNotFoundError:
                        pass

    async def save(self) -> None:
        """Persist current config to disk."""
        self._save(data_path("config.toml"))

    def get(self, key: str, default=None):
        """Get a config value by dot-separated key."""
        parts = key.split(".")
        val = self._data
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return default
            if val is None:
                return default
        return val if val is not None else default

    def get_str(self, key: str, default: str = "") -> str:
        val = self.get(key, default)
        return str(val) if val is not None else default

    def get_int(self, key: str, default: int = 0) -> int:
        val = self.get(key, default)
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        val = self.get(key, default)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in {"true", "1", "yes", "on"}
        return bool(val)

    def set(self, key: str, value: Any) -> None:
        """Set a config value by dot-separated key."""
        parts = key.split(".")
        d = self._data
        for part in parts[:-1]:
            if part not in d or not isinstance(d[part], dict):
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value

    async def reload(self) -> None:
        """Reload config from disk."""
        config_file = data_path("config.toml")
        if config_file.exists():
            with open(config_file, "rb") as f:
                self._data = tomllib.load(f)

    def _get_backend(self):
        """Return self for compatibility."""
        return self


# Global config instance
config = ConfigSnapshot()
