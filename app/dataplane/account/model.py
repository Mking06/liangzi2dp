"""Account data model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class AccountStatus(str, Enum):
    """Account health status."""
    ACTIVE = "active"
    EXPIRED = "expired"
    RATE_LIMITED = "rate_limited"
    BANNED = "banned"
    UNKNOWN = "unknown"


class AccountTier(str, Enum):
    """Account tier based on Liangzidp features."""
    BASIC = "basic"       # Free tier, limited access
    PLUS = "plus"         # Liangzidp Plus subscriber
    PRO = "pro"           # Liangzidp Pro subscriber


@dataclass
class LiangzidpAccount:
    """Liangzidp account credentials and state."""
    id: str
    email: str = ""
    password: str = ""
    session_token: str = ""      # Liangzidp session cookie
    csrf_token: str = ""
    status: AccountStatus = AccountStatus.UNKNOWN
    tier: AccountTier = AccountTier.BASIC
    is_temporary: bool = False
    expires_at: datetime | None = None
    last_refresh: datetime | None = None
    last_used: datetime | None = None
    use_count: int = 0
    error_count: int = 0
    metadata: dict = field(default_factory=dict)

    def is_available(self) -> bool:
        """Check if account is usable for requests."""
        if self.status in (AccountStatus.EXPIRED, AccountStatus.RATE_LIMITED, AccountStatus.BANNED):
            return False
        if self.error_count >= 3:
            return False
        return bool(self.session_token or self.email)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "email": self.email,
            "password": self.password,
            "session_token": self.session_token,
            "csrf_token": self.csrf_token,
            "status": self.status.value,
            "tier": self.tier.value,
            "is_temporary": self.is_temporary,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "use_count": self.use_count,
            "error_count": self.error_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> LiangzidpAccount:
        return cls(
            id=data["id"],
            email=data.get("email", ""),
            password=data.get("password", ""),
            session_token=data.get("session_token", ""),
            csrf_token=data.get("csrf_token", ""),
            status=AccountStatus(data.get("status", "unknown")),
            tier=AccountTier(data.get("tier", "basic")),
            is_temporary=data.get("is_temporary", False),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data.get("expires_at") else None,
            last_refresh=datetime.fromisoformat(data["last_refresh"]) if data.get("last_refresh") else None,
            last_used=datetime.fromisoformat(data["last_used"]) if data.get("last_used") else None,
            use_count=data.get("use_count", 0),
            error_count=data.get("error_count", 0),
            metadata=data.get("metadata", {}),
        )
