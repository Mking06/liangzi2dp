"""Application error types."""

from __future__ import annotations


class AppError(Exception):
    """Base application error.

    Constructor order is intentionally message-first because the codebase calls
    errors as ``AppError("message", 500, "code")``.
    """

    default_status = 500
    default_code = "server_error"
    default_message = "Internal server error"

    def __init__(
        self,
        message: str | None = None,
        status: int | None = None,
        code: str | None = None,
        details: dict | None = None,
    ):
        self.message = message or self.default_message
        self.status = int(status or self.default_status)
        self.code = code or self.default_code
        self.details = details
        super().__init__(self.message)

    def to_dict(self) -> dict:
        payload = {
            "error": {
                "message": self.message,
                "type": self.code,
                "code": self.code,
            }
        }
        if self.details:
            payload["error"]["details"] = self.details
        return payload


class AccountError(AppError):
    """Account-related errors."""

    default_status = 400
    default_code = "account_error"


class ProxyError(AppError):
    """Proxy-related errors."""

    default_status = 502
    default_code = "proxy_error"


class RateLimitError(AppError):
    """Rate limit error."""

    default_status = 429
    default_code = "rate_limit_exceeded"
