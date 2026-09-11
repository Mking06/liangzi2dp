"""Token validation utilities."""

from __future__ import annotations



def validate_api_key(key: str) -> bool:
    """Validate API key format. Accepts any non-empty string >= 8 chars."""
    if not key:
        return False
    return len(key.strip()) >= 8


def mask_token(token: str) -> str:
    """Mask a token for logging."""
    if not token:
        return ""
    if len(token) <= 12:
        return "***"
    return token[:8] + "***" + token[-4:]
