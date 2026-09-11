"""Persistent state for the private administration console."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from app.platform.paths import data_path


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utcnow().isoformat()


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class AdminStore:
    """SQLite-backed admin identities, sessions, keys, and operational data."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or data_path("admin.db"))
        self.conn: aiosqlite.Connection | None = None
        self.password_hasher = PasswordHasher()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA busy_timeout=5000")
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS admin_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                token_version INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admin_sessions (
                token_hash TEXT PRIMARY KEY,
                admin_id INTEGER NOT NULL,
                csrf_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                ip_hash TEXT,
                user_agent TEXT,
                FOREIGN KEY(admin_id) REFERENCES admin_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS login_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_hash TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_time
                ON login_attempts(ip_hash, attempted_at);
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                prefix TEXT NOT NULL,
                suffix TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                request_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                api_key_id TEXT,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                model TEXT,
                stream INTEGER NOT NULL DEFAULT 0,
                status_code INTEGER NOT NULL,
                latency_ms INTEGER NOT NULL,
                request_bytes INTEGER NOT NULL DEFAULT 0,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_request_logs_created
                ON request_logs(created_at DESC);
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                action TEXT NOT NULL,
                target_type TEXT,
                target_id TEXT,
                ip TEXT,
                success INTEGER NOT NULL DEFAULT 1,
                detail TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_audit_logs_created
                ON audit_logs(created_at DESC);
            CREATE TABLE IF NOT EXISTS admin_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        request_columns = {
            str(row[1])
            for row in await (await self.conn.execute("PRAGMA table_info(request_logs)")).fetchall()
        }
        if "request_bytes" not in request_columns:
            await self.conn.execute(
                "ALTER TABLE request_logs ADD COLUMN request_bytes INTEGER NOT NULL DEFAULT 0"
            )
        await self.conn.execute(
            "INSERT OR IGNORE INTO admin_settings(key, value, updated_at) VALUES(?, ?, ?)",
            ("api_key_enforced", "false", iso_now()),
        )
        await self.conn.execute(
            "INSERT OR IGNORE INTO admin_settings(key, value, updated_at) VALUES(?, ?, ?)",
            ("request_log_retention_days", "7", iso_now()),
        )
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    def _db(self) -> aiosqlite.Connection:
        if not self.conn:
            raise RuntimeError("AdminStore is not initialized")
        return self.conn

    async def has_admin(self) -> bool:
        row = await (await self._db().execute("SELECT id FROM admin_users LIMIT 1")).fetchone()
        return bool(row)

    async def bootstrap_admin(self, username: str, password: str) -> bool:
        db = self._db()
        row = await (await db.execute("SELECT id FROM admin_users LIMIT 1")).fetchone()
        if row:
            return False
        now = iso_now()
        await db.execute(
            "INSERT INTO admin_users(username, password_hash, created_at, updated_at) VALUES(?, ?, ?, ?)",
            (username, self.password_hasher.hash(password), now, now),
        )
        await db.commit()
        return True

    async def verify_admin(self, username: str, password: str) -> dict[str, Any] | None:
        db = self._db()
        row = await (await db.execute(
            "SELECT id, username, password_hash FROM admin_users WHERE username = ?",
            (username,),
        )).fetchone()
        if not row:
            return None
        try:
            if not self.password_hasher.verify(row["password_hash"], password):
                return None
        except (VerifyMismatchError, InvalidHashError):
            return None
        if self.password_hasher.check_needs_rehash(row["password_hash"]):
            await db.execute(
                "UPDATE admin_users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (self.password_hasher.hash(password), iso_now(), row["id"]),
            )
            await db.commit()
        return {"id": row["id"], "username": row["username"]}

    async def change_password(self, admin_id: int, current: str, new: str) -> bool:
        db = self._db()
        row = await (await db.execute(
            "SELECT password_hash FROM admin_users WHERE id = ?", (admin_id,)
        )).fetchone()
        if not row:
            return False
        try:
            self.password_hasher.verify(row["password_hash"], current)
        except (VerifyMismatchError, InvalidHashError):
            return False
        await db.execute(
            "UPDATE admin_users SET password_hash = ?, token_version = token_version + 1, updated_at = ? WHERE id = ?",
            (self.password_hasher.hash(new), iso_now(), admin_id),
        )
        await db.execute("DELETE FROM admin_sessions WHERE admin_id = ?", (admin_id,))
        await db.commit()
        return True

    async def login_allowed(self, ip: str) -> bool:
        cutoff = (utcnow() - timedelta(minutes=15)).isoformat()
        row = await (await self._db().execute(
            "SELECT COUNT(*) AS failures FROM login_attempts WHERE ip_hash = ? AND attempted_at >= ? AND success = 0",
            (digest(ip), cutoff),
        )).fetchone()
        return int(row["failures"] or 0) < 5

    async def record_login(self, ip: str, success: bool) -> None:
        db = self._db()
        await db.execute(
            "INSERT INTO login_attempts(ip_hash, attempted_at, success) VALUES(?, ?, ?)",
            (digest(ip), iso_now(), int(success)),
        )
        if success:
            await db.execute("DELETE FROM login_attempts WHERE ip_hash = ?", (digest(ip),))
        await db.commit()

    async def create_session(
        self, admin_id: int, ip: str, user_agent: str, ttl_hours: int
    ) -> tuple[str, str, str]:
        token = secrets.token_urlsafe(48)
        csrf = secrets.token_urlsafe(32)
        expires = (utcnow() + timedelta(hours=ttl_hours)).isoformat()
        now = iso_now()
        db = self._db()
        await db.execute(
            """INSERT INTO admin_sessions(
                token_hash, admin_id, csrf_hash, expires_at, created_at, last_seen_at, ip_hash, user_agent
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (digest(token), admin_id, digest(csrf), expires, now, now, digest(ip), user_agent[:300]),
        )
        await db.commit()
        return token, csrf, expires

    async def get_session(self, token: str) -> dict[str, Any] | None:
        if not token:
            return None
        db = self._db()
        row = await (await db.execute(
            """SELECT s.token_hash, s.admin_id, s.csrf_hash, s.expires_at, u.username
               FROM admin_sessions s JOIN admin_users u ON u.id = s.admin_id
               WHERE s.token_hash = ?""",
            (digest(token),),
        )).fetchone()
        if not row:
            return None
        if parse_datetime(row["expires_at"]) <= utcnow():
            await db.execute("DELETE FROM admin_sessions WHERE token_hash = ?", (row["token_hash"],))
            await db.commit()
            return None
        await db.execute(
            "UPDATE admin_sessions SET last_seen_at = ? WHERE token_hash = ?",
            (iso_now(), row["token_hash"]),
        )
        await db.commit()
        return dict(row)

    async def delete_session(self, token: str) -> None:
        if token:
            await self._db().execute("DELETE FROM admin_sessions WHERE token_hash = ?", (digest(token),))
            await self._db().commit()

    async def create_api_key(self, name: str, expires_at: str | None) -> tuple[dict[str, Any], str]:
        raw = "sk-ds-" + secrets.token_urlsafe(32)
        item = {
            "id": secrets.token_hex(8),
            "name": name,
            "prefix": raw[:12],
            "suffix": raw[-4:],
            "enabled": True,
            "expires_at": expires_at,
            "created_at": iso_now(),
            "last_used_at": None,
            "request_count": 0,
            "error_count": 0,
        }
        db = self._db()
        await db.execute(
            """INSERT INTO api_keys(
                id, name, key_hash, prefix, suffix, enabled, expires_at, created_at
            ) VALUES(?, ?, ?, ?, ?, 1, ?, ?)""",
            (item["id"], name, digest(raw), item["prefix"], item["suffix"], expires_at, item["created_at"]),
        )
        await db.commit()
        return item, raw

    async def list_api_keys(self) -> list[dict[str, Any]]:
        rows = await (await self._db().execute(
            """SELECT id, name, prefix, suffix, enabled, expires_at, created_at,
                      last_used_at, request_count, error_count
               FROM api_keys ORDER BY created_at DESC"""
        )).fetchall()
        return [{**dict(row), "enabled": bool(row["enabled"])} for row in rows]

    async def verify_api_key(self, raw: str) -> dict[str, Any] | None:
        if not raw:
            return None
        row = await (await self._db().execute(
            "SELECT id, name, enabled, expires_at FROM api_keys WHERE key_hash = ?",
            (digest(raw),),
        )).fetchone()
        if not row or not row["enabled"]:
            return None
        if row["expires_at"] and parse_datetime(row["expires_at"]) <= utcnow():
            return None
        return dict(row)

    async def update_api_key(self, key_id: str, enabled: bool) -> bool:
        cur = await self._db().execute(
            "UPDATE api_keys SET enabled = ? WHERE id = ?", (int(enabled), key_id)
        )
        await self._db().commit()
        return cur.rowcount > 0

    async def delete_api_key(self, key_id: str) -> bool:
        cur = await self._db().execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        await self._db().commit()
        return cur.rowcount > 0

    async def log_request(self, data: dict[str, Any]) -> None:
        db = self._db()
        await db.execute(
            """INSERT INTO request_logs(
                created_at, api_key_id, method, path, model, stream, status_code,
                latency_ms, request_bytes, response_bytes, error
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                iso_now(), data.get("api_key_id"), data["method"], data["path"],
                data.get("model"), int(bool(data.get("stream"))), data["status_code"],
                data["latency_ms"], data.get("request_bytes", 0),
                data.get("response_bytes", 0), data.get("error"),
            ),
        )
        if data.get("api_key_id"):
            await db.execute(
                """UPDATE api_keys SET last_used_at = ?, request_count = request_count + 1,
                   error_count = error_count + ? WHERE id = ?""",
                (iso_now(), int(data["status_code"] >= 400), data["api_key_id"]),
            )
        await db.commit()

    async def list_requests(self, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        limit = max(1, min(limit, 500))
        db = self._db()
        rows = await (await db.execute(
            "SELECT * FROM request_logs ORDER BY id DESC LIMIT ? OFFSET ?", (limit, max(0, offset))
        )).fetchall()
        total = await (await db.execute("SELECT COUNT(*) AS n FROM request_logs")).fetchone()
        return {"items": [dict(row) for row in rows], "total": int(total["n"]), "limit": limit, "offset": offset}

    async def audit(
        self, action: str, target_type: str = "", target_id: str = "",
        ip: str = "", success: bool = True, detail: str = "",
    ) -> None:
        await self._db().execute(
            """INSERT INTO audit_logs(created_at, action, target_type, target_id, ip, success, detail)
               VALUES(?, ?, ?, ?, ?, ?, ?)""",
            (iso_now(), action, target_type, target_id, ip, int(success), detail[:500]),
        )
        await self._db().commit()

    async def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = await (await self._db().execute(
            "SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?", (max(1, min(limit, 500)),)
        )).fetchall()
        return [{**dict(row), "success": bool(row["success"])} for row in rows]

    async def get_setting(self, key: str, default: Any = None) -> Any:
        row = await (await self._db().execute(
            "SELECT value FROM admin_settings WHERE key = ?", (key,)
        )).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return row["value"]

    async def set_setting(self, key: str, value: Any) -> None:
        await self._db().execute(
            """INSERT INTO admin_settings(key, value, updated_at) VALUES(?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
            (key, json.dumps(value, ensure_ascii=True), iso_now()),
        )
        await self._db().commit()

    async def dashboard(self) -> dict[str, Any]:
        db = self._db()
        since = (utcnow() - timedelta(hours=24)).isoformat()
        row = await (await db.execute(
            """SELECT COUNT(*) AS requests,
                      SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS errors,
                      COALESCE(AVG(latency_ms), 0) AS avg_latency,
                      COALESCE(SUM(response_bytes), 0) AS response_bytes
               FROM request_logs WHERE created_at >= ?""",
            (since,),
        )).fetchone()
        models = await (await db.execute(
            """SELECT COALESCE(model, 'unknown') AS model, COUNT(*) AS count
               FROM request_logs WHERE created_at >= ? GROUP BY model ORDER BY count DESC LIMIT 8""",
            (since,),
        )).fetchall()
        return {
            "requests_24h": int(row["requests"] or 0),
            "errors_24h": int(row["errors"] or 0),
            "avg_latency_ms_24h": round(float(row["avg_latency"] or 0)),
            "response_bytes_24h": int(row["response_bytes"] or 0),
            "models_24h": [dict(item) for item in models],
        }

    async def cleanup(self) -> dict[str, int]:
        retention = int(await self.get_setting("request_log_retention_days", 7))
        request_cutoff = (utcnow() - timedelta(days=max(1, min(retention, 90)))).isoformat()
        login_cutoff = (utcnow() - timedelta(days=1)).isoformat()
        now = iso_now()
        db = self._db()
        req = await db.execute("DELETE FROM request_logs WHERE created_at < ?", (request_cutoff,))
        sessions = await db.execute("DELETE FROM admin_sessions WHERE expires_at < ?", (now,))
        attempts = await db.execute("DELETE FROM login_attempts WHERE attempted_at < ?", (login_cutoff,))
        await db.commit()
        return {"requests": req.rowcount, "sessions": sessions.rowcount, "login_attempts": attempts.rowcount}
