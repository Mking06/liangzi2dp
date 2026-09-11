"""Liangzidp authentication — session token refresh via Web API."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import curl_cffi.requests


class LiangzidpAuthRefresher:
    """Handles Liangzidp Web authentication and session refresh."""

    BASE_URL = "https://chat.deepseek.com"
    AUTH_URL = f"{BASE_URL}/api/v0/auth_session"
    USER_URL = f"{BASE_URL}/api/v0/users/me"

    # Browser fingerprint for curl_cffi. Keep this in curl_cffi's supported list.
    IMPERSONATE = "chrome136"

    def __init__(self, account_data: dict[str, Any]):
        self._data = dict(account_data)
        self._updated = False

    async def refresh(self) -> bool:
        """Refresh session token using stored credentials."""
        email = self._data.get("email", "")
        password = self._data.get("password", "")

        if not email or not password:
            return False

        try:
            await self._do_refresh(email, password)
            self._data["status"] = "active"
            self._data["last_refresh"] = datetime.now(UTC).isoformat()
            self._updated = True
            return True
        except Exception:
            self._data["status"] = "expired"
            self._data["last_refresh"] = datetime.now(UTC).isoformat()
            self._updated = True
            return False

    async def _do_refresh(self, email: str, password: str) -> None:
        """Perform the actual authentication request."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Content-Type": "application/json",
            "Origin": "https://chat.deepseek.com",
            "Referer": "https://chat.deepseek.com/",
        }

        payload = {
            "email": email,
            "password": password,
        }

        async with curl_cffi.requests.AsyncSession(
            impersonate=self.IMPERSONATE
        ) as session:
            resp = await session.post(
                self.AUTH_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp_data = resp.json()

            if resp.status_code == 200 and resp_data.get("success"):
                # Extract session token from response
                session_data = resp_data.get("data", {})
                session_token = session_data.get("session_token", "")
                csrf_token = session_data.get("csrf_token", "")

                # Also check cookies
                if not session_token and resp.cookies:
                    for cookie in resp.cookies:
                        if cookie.name in ("session", "session_token", "token"):
                            session_token = cookie.value
                            break

                self._data["session_token"] = session_token
                self._data["csrf_token"] = csrf_token

                # Verify account status
                await self._fetch_user_info(session, headers)
            else:
                error_msg = resp_data.get("message", resp_data.get("error", "unknown"))
                raise RuntimeError(f"Auth failed: {error_msg}")

    async def _fetch_user_info(self, session, headers: dict) -> None:
        """Fetch user info to determine account tier."""
        try:
            resp = await session.get(
                self.USER_URL,
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 200:
                user_data = resp.json().get("data", {})
                metadata = self._data.get("metadata")
                metadata = dict(metadata) if isinstance(metadata, dict) else {}
                metadata.update(
                    {
                        "username": user_data.get("name", ""),
                        "is_plus": user_data.get("is_plus", False),
                        "is_pro": user_data.get("is_pro", False),
                        "plan_type": user_data.get("plan_type", "free"),
                    }
                )
                self._data["metadata"] = metadata
                if user_data.get("is_pro"):
                    self._data["tier"] = "pro"
                elif user_data.get("is_plus"):
                    self._data["tier"] = "plus"
                else:
                    self._data["tier"] = "basic"
        except Exception:
            pass

    def get_updated_account(self) -> dict[str, Any]:
        """Return the updated account data."""
        return dict(self._data)
