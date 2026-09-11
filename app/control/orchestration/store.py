from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

import aiosqlite

from app.platform.paths import data_path


_UNCHANGED = object()
QUOTA_WINDOW_SECONDS = 24 * 60 * 60


class OrchestrationStore:
    """SQLite-backed sticky sessions and shared account runtime counters."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else data_path("orchestration.db")
        self._conn: aiosqlite.Connection | None = None
        self._transaction_lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                session_key TEXT PRIMARY KEY,
                client_identity TEXT NOT NULL DEFAULT '',
                identity_source TEXT NOT NULL DEFAULT 'legacy',
                account_id TEXT NOT NULL,
                model TEXT NOT NULL,
                egress_proxy TEXT NOT NULL DEFAULT '',
                chat_session_id TEXT NOT NULL DEFAULT '',
                parent_message_id INTEGER,
                generation INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'active',
                checkpoint_json TEXT NOT NULL DEFAULT '{}',
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_activity_at REAL NOT NULL DEFAULT 0,
                expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_conversations_account
                ON conversations(account_id, expires_at);

            CREATE TABLE IF NOT EXISTS account_runtime (
                account_id TEXT PRIMARY KEY,
                inflight INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL,
                cooldown_reason TEXT NOT NULL DEFAULT '',
                last_selected_at REAL,
                last_success_at REAL,
                last_failure_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                pool_added_at REAL,
                quota_window_started_at REAL,
                quota_reset_at REAL,
                quota_request_limit INTEGER NOT NULL DEFAULT 0,
                quota_requests_used INTEGER NOT NULL DEFAULT 0,
                quota_token_limit INTEGER NOT NULL DEFAULT 0,
                quota_tokens_used INTEGER NOT NULL DEFAULT 0,
                quota_budget_microusd INTEGER NOT NULL DEFAULT 0,
                quota_cost_microusd INTEGER NOT NULL DEFAULT 0,
                quota_rate_microusd_per_million INTEGER NOT NULL DEFAULT 0,
                quota_exhausted INTEGER NOT NULL DEFAULT 0,
                last_health_check_at REAL,
                last_health_ok_at REAL,
                health_status TEXT NOT NULL DEFAULT 'unknown',
                health_latency_ms INTEGER,
                health_error TEXT NOT NULL DEFAULT '',
                health_consecutive_failures INTEGER NOT NULL DEFAULT 0,
                latency_ewma_ms REAL NOT NULL DEFAULT 0,
                error_ewma REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS tool_continuations (
                tool_call_id TEXT PRIMARY KEY,
                session_key TEXT NOT NULL,
                account_id TEXT NOT NULL,
                chat_session_id TEXT NOT NULL DEFAULT '',
                parent_message_id INTEGER,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tool_continuations_expiry
                ON tool_continuations(expires_at);

            CREATE TABLE IF NOT EXISTS task_aliases (
                client_identity TEXT NOT NULL,
                alias_hash TEXT NOT NULL,
                session_key TEXT NOT NULL,
                alias_type TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY(client_identity, alias_hash, session_key)
            );
            CREATE INDEX IF NOT EXISTS idx_task_alias_lookup
                ON task_aliases(client_identity, alias_hash, expires_at);
            CREATE INDEX IF NOT EXISTS idx_task_alias_expiry
                ON task_aliases(expires_at);

            CREATE TABLE IF NOT EXISTS account_leases (
                lease_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                session_key TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                acquired_at REAL NOT NULL,
                heartbeat_at REAL NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_account_leases_account
                ON account_leases(account_id, expires_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_account_leases_session
                ON account_leases(session_key);
            CREATE INDEX IF NOT EXISTS idx_account_leases_expiry
                ON account_leases(expires_at);

            CREATE TABLE IF NOT EXISTS orchestration_metrics (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        columns = {
            str(row[1])
            for row in await (await self._conn.execute("PRAGMA table_info(conversations)")).fetchall()
        }
        if "egress_proxy" not in columns:
            await self._conn.execute(
                "ALTER TABLE conversations ADD COLUMN egress_proxy TEXT NOT NULL DEFAULT ''"
            )
        conversation_migrations = {
            "client_identity": "TEXT NOT NULL DEFAULT ''",
            "identity_source": "TEXT NOT NULL DEFAULT 'legacy'",
            "last_activity_at": "REAL NOT NULL DEFAULT 0",
        }
        for name, definition in conversation_migrations.items():
            if name not in columns:
                await self._conn.execute(
                    f"ALTER TABLE conversations ADD COLUMN {name} {definition}"  # noqa: S608
                )
        await self._conn.execute(
            "UPDATE conversations SET last_activity_at=updated_at WHERE last_activity_at=0"
        )
        runtime_columns = {
            str(row[1])
            for row in await (await self._conn.execute("PRAGMA table_info(account_runtime)")).fetchall()
        }
        runtime_migrations = {
            "pool_added_at": "REAL",
            "quota_window_started_at": "REAL",
            "quota_reset_at": "REAL",
            "quota_request_limit": "INTEGER NOT NULL DEFAULT 0",
            "quota_requests_used": "INTEGER NOT NULL DEFAULT 0",
            "quota_token_limit": "INTEGER NOT NULL DEFAULT 0",
            "quota_tokens_used": "INTEGER NOT NULL DEFAULT 0",
            "quota_budget_microusd": "INTEGER NOT NULL DEFAULT 0",
            "quota_cost_microusd": "INTEGER NOT NULL DEFAULT 0",
            "quota_rate_microusd_per_million": "INTEGER NOT NULL DEFAULT 0",
            "quota_exhausted": "INTEGER NOT NULL DEFAULT 0",
            "last_health_check_at": "REAL",
            "last_health_ok_at": "REAL",
            "health_status": "TEXT NOT NULL DEFAULT 'unknown'",
            "health_latency_ms": "INTEGER",
            "health_error": "TEXT NOT NULL DEFAULT ''",
            "health_consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
            "latency_ewma_ms": "REAL NOT NULL DEFAULT 0",
            "error_ewma": "REAL NOT NULL DEFAULT 0",
        }
        for name, definition in runtime_migrations.items():
            if name not in runtime_columns:
                await self._conn.execute(
                    f"ALTER TABLE account_runtime ADD COLUMN {name} {definition}"  # noqa: S608
                )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("orchestration store is not initialized")
        return self._conn

    @staticmethod
    def _row(row: aiosqlite.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        if "checkpoint_json" in item:
            try:
                item["checkpoint"] = json.loads(item.pop("checkpoint_json") or "{}")
            except json.JSONDecodeError:
                item["checkpoint"] = {}
        return item

    async def get_conversation(self, session_key: str) -> dict[str, Any] | None:
        now = time.time()
        cursor = await self._db().execute(
            "SELECT * FROM conversations WHERE session_key = ? AND expires_at > ?",
            (session_key, now),
        )
        return self._row(await cursor.fetchone())

    async def bind_conversation(
        self,
        session_key: str,
        account_id: str,
        model: str,
        ttl_seconds: int,
        *,
        state: str = "active",
        egress_proxy: str = "",
        replace_account_id: str | None = None,
        client_identity: str = "",
        identity_source: str = "legacy",
    ) -> dict[str, Any]:
        now = time.time()
        expires_at = now + max(60, ttl_seconds)
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "DELETE FROM conversations WHERE session_key=? AND expires_at<=?",
                    (session_key, now),
                )
                await db.execute(
                    """
                    INSERT INTO conversations(
                        session_key, client_identity, identity_source,
                        account_id, model, egress_proxy, state,
                        created_at, updated_at, last_activity_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_key) DO NOTHING
                    """,
                    (
                        session_key,
                        client_identity,
                        identity_source,
                        account_id,
                        model,
                        egress_proxy,
                        state,
                        now,
                        now,
                        now,
                        expires_at,
                    ),
                )
                if replace_account_id:
                    await db.execute(
                        """
                        UPDATE conversations SET
                            account_id=?, model=?, egress_proxy=?, state=?,
                            client_identity=?, identity_source=?,
                            updated_at=?, last_activity_at=?, expires_at=?
                        WHERE session_key=? AND account_id=?
                        """,
                        (
                            account_id,
                            model,
                            egress_proxy,
                            state,
                            client_identity,
                            identity_source,
                            now,
                            now,
                            expires_at,
                            session_key,
                            replace_account_id,
                        ),
                    )
                else:
                    await db.execute(
                        "UPDATE conversations SET updated_at=?, last_activity_at=?, expires_at=? "
                        "WHERE session_key=?",
                        (now, now, expires_at, session_key),
                    )
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        item = await self.get_conversation(session_key)
        if item is None:
            raise RuntimeError("failed to bind conversation")
        return item

    async def update_conversation(
        self,
        session_key: str,
        *,
        chat_session_id: str | None | object = _UNCHANGED,
        parent_message_id: int | None | object = _UNCHANGED,
        egress_proxy: str | object = _UNCHANGED,
        ttl_seconds: int = 3600,
        state: str | None = None,
        checkpoint: dict[str, Any] | None = None,
        last_error: str | None = None,
        increment_generation: bool = False,
    ) -> None:
        async with self._transaction_lock:
            current = await self.get_conversation(session_key)
            if not current:
                return
            now = time.time()
            await self._db().execute(
                """
                UPDATE conversations SET
                    chat_session_id=?, parent_message_id=?, egress_proxy=?, state=?, checkpoint_json=?,
                    last_error=?, generation=?, updated_at=?, last_activity_at=?, expires_at=?
                WHERE session_key=?
                """,
                (
                    current["chat_session_id"] if chat_session_id is _UNCHANGED else chat_session_id,
                    current["parent_message_id"] if parent_message_id is _UNCHANGED else parent_message_id,
                    current["egress_proxy"] if egress_proxy is _UNCHANGED else egress_proxy,
                    state or current["state"],
                    json.dumps(
                        checkpoint if checkpoint is not None else current.get("checkpoint", {}),
                        ensure_ascii=False,
                    ),
                    current["last_error"] if last_error is None else last_error,
                    int(current["generation"]) + (1 if increment_generation else 0),
                    now,
                    now,
                    now + max(60, ttl_seconds),
                    session_key,
                ),
            )
            await self._db().commit()

    async def delete_conversation(self, session_key: str) -> None:
        async with self._transaction_lock:
            await self._db().execute("DELETE FROM conversations WHERE session_key = ?", (session_key,))
            await self._db().execute(
                "DELETE FROM tool_continuations WHERE session_key = ?",
                (session_key,),
            )
            await self._db().execute(
                "DELETE FROM task_aliases WHERE session_key = ?",
                (session_key,),
            )
            await self._db().commit()

    async def list_conversations(self, limit: int = 200) -> list[dict[str, Any]]:
        cursor = await self._db().execute(
            """
            SELECT * FROM conversations WHERE expires_at > ?
            ORDER BY updated_at DESC LIMIT ?
            """,
            (time.time(), max(1, min(limit, 1000))),
        )
        return [self._row(row) or {} for row in await cursor.fetchall()]

    async def resolve_task_alias(
        self,
        client_identity: str,
        alias_hash: str,
    ) -> dict[str, Any] | None:
        """Resolve an alias only when it identifies one live conversation."""
        now = time.time()
        cursor = await self._db().execute(
            """
            SELECT c.* FROM task_aliases a
            JOIN conversations c ON c.session_key=a.session_key
            WHERE a.client_identity=? AND a.alias_hash=?
              AND a.expires_at>? AND c.expires_at>?
            ORDER BY a.created_at DESC LIMIT 2
            """,
            (client_identity, alias_hash, now, now),
        )
        rows = await cursor.fetchall()
        if len(rows) != 1:
            return None
        return self._row(rows[0])

    async def register_task_alias(
        self,
        client_identity: str,
        alias_hash: str,
        session_key: str,
        alias_type: str,
        ttl_seconds: int,
    ) -> None:
        now = time.time()
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO task_aliases(
                    client_identity, alias_hash, session_key, alias_type,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(client_identity, alias_hash, session_key) DO UPDATE SET
                    alias_type=excluded.alias_type,
                    created_at=excluded.created_at,
                    expires_at=excluded.expires_at
                """,
                (
                    client_identity,
                    alias_hash,
                    session_key,
                    alias_type,
                    now,
                    now + max(30, ttl_seconds),
                ),
            )
            await self._db().commit()

    async def touch_conversation(self, session_key: str, ttl_seconds: int) -> None:
        now = time.time()
        async with self._transaction_lock:
            await self._db().execute(
                """
                UPDATE conversations SET
                    updated_at=?, last_activity_at=?, expires_at=?
                WHERE session_key=?
                """,
                (now, now, now + max(60, ttl_seconds), session_key),
            )
            await self._db().commit()

    @staticmethod
    def _quota_window(pool_added_at: float, now: float) -> tuple[float, float]:
        anchor = pool_added_at if 0 < pool_added_at <= now else now
        elapsed_windows = int((now - anchor) // QUOTA_WINDOW_SECONDS)
        started_at = anchor + elapsed_windows * QUOTA_WINDOW_SECONDS
        return started_at, started_at + QUOTA_WINDOW_SECONDS

    async def configure_account(
        self,
        account_id: str,
        *,
        pool_added_at: float | None = None,
        request_limit: int = 0,
        token_limit: int = 0,
        budget_microusd: int = 0,
        rate_microusd_per_million: int = 0,
    ) -> None:
        """Persist one account's local quota policy without moving its window anchor."""
        now = time.time()
        anchor = float(pool_added_at or now)
        started_at, reset_at = self._quota_window(anchor, now)
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    """
                    INSERT INTO account_runtime(
                        account_id, pool_added_at, quota_window_started_at, quota_reset_at,
                        quota_request_limit, quota_token_limit, quota_budget_microusd,
                        quota_rate_microusd_per_million
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(account_id) DO UPDATE SET
                        pool_added_at=COALESCE(account_runtime.pool_added_at, excluded.pool_added_at),
                        quota_window_started_at=COALESCE(
                            account_runtime.quota_window_started_at,
                            excluded.quota_window_started_at
                        ),
                        quota_reset_at=COALESCE(account_runtime.quota_reset_at, excluded.quota_reset_at),
                        quota_request_limit=excluded.quota_request_limit,
                        quota_token_limit=excluded.quota_token_limit,
                        quota_budget_microusd=excluded.quota_budget_microusd,
                        quota_rate_microusd_per_million=excluded.quota_rate_microusd_per_million
                    """,
                    (
                        account_id,
                        anchor,
                        started_at,
                        reset_at,
                        max(0, int(request_limit)),
                        max(0, int(token_limit)),
                        max(0, int(budget_microusd)),
                        max(0, int(rate_microusd_per_million)),
                    ),
                )
                await self._normalize_quota_window_locked(account_id, now)
                await db.execute(
                    """
                    UPDATE account_runtime SET quota_exhausted=CASE
                        WHEN quota_request_limit>0 AND quota_requests_used>=quota_request_limit THEN 1
                        WHEN quota_token_limit>0 AND quota_tokens_used>=quota_token_limit THEN 1
                        WHEN quota_budget_microusd>0 AND quota_cost_microusd>=quota_budget_microusd THEN 1
                        ELSE 0 END
                    WHERE account_id=?
                    """,
                    (account_id,),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def _normalize_quota_window_locked(self, account_id: str, now: float) -> None:
        cursor = await self._db().execute(
            """
            SELECT pool_added_at, quota_reset_at
            FROM account_runtime WHERE account_id=?
            """,
            (account_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return
        anchor = float(row["pool_added_at"] or now)
        reset_at = float(row["quota_reset_at"] or 0)
        if reset_at > now:
            return
        started_at, next_reset_at = self._quota_window(anchor, now)
        await self._db().execute(
            """
            UPDATE account_runtime SET
                quota_window_started_at=?, quota_reset_at=?,
                quota_requests_used=0, quota_tokens_used=0,
                quota_cost_microusd=0, quota_exhausted=0
            WHERE account_id=?
            """,
            (started_at, next_reset_at, account_id),
        )

    async def normalize_quota_windows(self, account_ids: list[str] | None = None) -> None:
        now = time.time()
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                if account_ids:
                    placeholders = ",".join("?" for _ in account_ids)
                    cursor = await db.execute(
                        f"SELECT account_id FROM account_runtime "  # noqa: S608
                        f"WHERE account_id IN ({placeholders}) AND "
                        "(quota_reset_at IS NULL OR quota_reset_at<=?)",
                        [*account_ids, now],
                    )
                else:
                    cursor = await db.execute(
                        "SELECT account_id FROM account_runtime "
                        "WHERE quota_reset_at IS NULL OR quota_reset_at<=?",
                        (now,),
                    )
                for row in await cursor.fetchall():
                    await self._normalize_quota_window_locked(str(row["account_id"]), now)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def reset_quota(self, account_id: str) -> dict[str, Any] | None:
        now = time.time()
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    "SELECT pool_added_at FROM account_runtime WHERE account_id=?",
                    (account_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    await db.rollback()
                    return None
                started_at, reset_at = self._quota_window(float(row["pool_added_at"] or now), now)
                await db.execute(
                    """
                    UPDATE account_runtime SET
                        quota_window_started_at=?, quota_reset_at=?,
                        quota_requests_used=0, quota_tokens_used=0,
                        quota_cost_microusd=0, quota_exhausted=0
                    WHERE account_id=?
                    """,
                    (started_at, reset_at, account_id),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return (await self.runtime_snapshot([account_id])).get(account_id)

    async def record_quota_usage(self, account_id: str, tokens: int) -> None:
        tokens = max(0, int(tokens))
        if not tokens:
            return
        await self.normalize_quota_windows([account_id])
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    """
                    UPDATE account_runtime SET
                        quota_tokens_used=quota_tokens_used+?,
                        quota_cost_microusd=quota_cost_microusd+
                            CAST(ROUND(? * quota_rate_microusd_per_million / 1000000.0) AS INTEGER)
                    WHERE account_id=?
                    """,
                    (tokens, tokens, account_id),
                )
                await db.execute(
                    """
                    UPDATE account_runtime SET quota_exhausted=CASE
                        WHEN quota_request_limit>0 AND quota_requests_used>=quota_request_limit THEN 1
                        WHEN quota_token_limit>0 AND quota_tokens_used>=quota_token_limit THEN 1
                        WHEN quota_budget_microusd>0 AND quota_cost_microusd>=quota_budget_microusd THEN 1
                        ELSE 0 END
                    WHERE account_id=?
                    """,
                    (account_id,),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def record_health(
        self,
        account_id: str,
        *,
        status: str,
        ok: bool,
        latency_ms: int,
        error: str = "",
    ) -> None:
        now = time.time()
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO account_runtime(
                    account_id, last_health_check_at, last_health_ok_at,
                    health_status, health_latency_ms, health_error,
                    health_consecutive_failures
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    last_health_check_at=excluded.last_health_check_at,
                    last_health_ok_at=CASE
                        WHEN excluded.health_status='healthy' THEN excluded.last_health_check_at
                        ELSE account_runtime.last_health_ok_at END,
                    health_status=excluded.health_status,
                    health_latency_ms=excluded.health_latency_ms,
                    health_error=excluded.health_error,
                    health_consecutive_failures=CASE
                        WHEN excluded.health_status='healthy' THEN 0
                        ELSE account_runtime.health_consecutive_failures+1 END
                """,
                (
                    account_id,
                    now,
                    now if ok else None,
                    status[:40],
                    max(0, int(latency_ms)),
                    error[:500],
                    0 if ok else 1,
                ),
            )
            await self._db().commit()

    async def delete_account_runtime(self, account_id: str) -> None:
        async with self._transaction_lock:
            await self._db().execute("DELETE FROM account_runtime WHERE account_id=?", (account_id,))
            await self._db().commit()

    async def runtime_snapshot(self, account_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
        await self.normalize_quota_windows(account_ids)
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            cursor = await self._db().execute(
                f"SELECT * FROM account_runtime WHERE account_id IN ({placeholders})",  # noqa: S608
                account_ids,
            )
        else:
            cursor = await self._db().execute("SELECT * FROM account_runtime")
        rows = {str(row["account_id"]): dict(row) for row in await cursor.fetchall()}
        now = time.time()
        active = await self._db().execute(
            """
            SELECT account_id, COUNT(*) AS active_sessions
            FROM conversations
            WHERE expires_at > ? AND last_activity_at > ?
              AND state IN ('active', 'recovering', 'resuming', 'waiting_tool')
            GROUP BY account_id
            """,
            (now, now - 120),
        )
        for row in await active.fetchall():
            rows.setdefault(str(row["account_id"]), {"account_id": str(row["account_id"]), "inflight": 0})
            rows[str(row["account_id"])]["active_sessions"] = int(row["active_sessions"])
        for item in rows.values():
            item.setdefault("active_sessions", 0)
            item["cooling_down"] = bool(item.get("cooldown_until") and item["cooldown_until"] > now)
            item["quota_exhausted"] = bool(item.get("quota_exhausted"))
            item["quota_budget_usd"] = round(
                int(item.get("quota_budget_microusd") or 0) / 1_000_000,
                6,
            )
            item["quota_cost_usd"] = round(
                int(item.get("quota_cost_microusd") or 0) / 1_000_000,
                6,
            )
            item["quota_usd_per_million_tokens"] = round(
                int(item.get("quota_rate_microusd_per_million") or 0) / 1_000_000,
                6,
            )
        return rows

    async def try_acquire_slot(self, account_id: str, max_inflight: int) -> bool:
        now = time.time()
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "INSERT INTO account_runtime(account_id) VALUES (?) ON CONFLICT(account_id) DO NOTHING",
                    (account_id,),
                )
                await self._normalize_quota_window_locked(account_id, now)
                cursor = await db.execute(
                    """
                    SELECT inflight, cooldown_until, quota_exhausted,
                           quota_request_limit, quota_requests_used,
                           quota_token_limit, quota_tokens_used,
                           quota_budget_microusd, quota_cost_microusd
                    FROM account_runtime WHERE account_id = ?
                    """,
                    (account_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    await db.rollback()
                    return False
                quota_blocked = bool(row["quota_exhausted"])
                quota_blocked = quota_blocked or bool(
                    row["quota_request_limit"]
                    and int(row["quota_requests_used"]) >= int(row["quota_request_limit"])
                )
                quota_blocked = quota_blocked or bool(
                    row["quota_token_limit"]
                    and int(row["quota_tokens_used"]) >= int(row["quota_token_limit"])
                )
                quota_blocked = quota_blocked or bool(
                    row["quota_budget_microusd"]
                    and int(row["quota_cost_microusd"]) >= int(row["quota_budget_microusd"])
                )
                if quota_blocked:
                    await db.execute(
                        "UPDATE account_runtime SET quota_exhausted=1 WHERE account_id=?",
                        (account_id,),
                    )
                    await db.commit()
                    return False
                if row["cooldown_until"] and float(row["cooldown_until"]) > now:
                    await db.rollback()
                    return False
                if int(row["inflight"]) >= max(1, max_inflight):
                    await db.rollback()
                    return False
                await db.execute(
                    """
                    UPDATE account_runtime SET
                        inflight=inflight+1,
                        quota_requests_used=quota_requests_used+1,
                        quota_exhausted=CASE
                            WHEN quota_request_limit>0
                             AND quota_requests_used+1>=quota_request_limit THEN 1
                            ELSE quota_exhausted END,
                        last_selected_at=?
                    WHERE account_id=?
                    """,
                    (now, account_id),
                )
                await db.commit()
                return True
            except Exception:
                await db.rollback()
                raise

    async def _reap_stale_leases_locked(self, now: float) -> int:
        cursor = await self._db().execute(
            """
            SELECT account_id, COUNT(*) AS n
            FROM account_leases WHERE expires_at<=?
            GROUP BY account_id
            """,
            (now,),
        )
        rows = await cursor.fetchall()
        if not rows:
            return 0
        await self._db().execute("DELETE FROM account_leases WHERE expires_at<=?", (now,))
        for row in rows:
            await self._db().execute(
                "UPDATE account_runtime SET inflight=MAX(0,inflight-?) WHERE account_id=?",
                (int(row["n"]), str(row["account_id"])),
            )
        return sum(int(row["n"]) for row in rows)

    async def try_acquire_lease(
        self,
        account_id: str,
        session_key: str,
        max_inflight: int,
        owner_id: str,
        lease_seconds: int = 90,
    ) -> str | None:
        now = time.time()
        lease_id = uuid.uuid4().hex
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await self._reap_stale_leases_locked(now)
                active_session = await db.execute(
                    "SELECT 1 FROM account_leases WHERE session_key=? AND expires_at>?",
                    (session_key, now),
                )
                if await active_session.fetchone():
                    await db.rollback()
                    return None
                await db.execute(
                    "INSERT INTO account_runtime(account_id) VALUES (?) "
                    "ON CONFLICT(account_id) DO NOTHING",
                    (account_id,),
                )
                await self._normalize_quota_window_locked(account_id, now)
                cursor = await db.execute(
                    """
                    SELECT inflight, cooldown_until, quota_exhausted,
                           quota_request_limit, quota_requests_used,
                           quota_token_limit, quota_tokens_used,
                           quota_budget_microusd, quota_cost_microusd
                    FROM account_runtime WHERE account_id=?
                    """,
                    (account_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    await db.rollback()
                    return None
                quota_blocked = bool(row["quota_exhausted"])
                quota_blocked = quota_blocked or bool(
                    row["quota_request_limit"]
                    and int(row["quota_requests_used"]) >= int(row["quota_request_limit"])
                )
                quota_blocked = quota_blocked or bool(
                    row["quota_token_limit"]
                    and int(row["quota_tokens_used"]) >= int(row["quota_token_limit"])
                )
                quota_blocked = quota_blocked or bool(
                    row["quota_budget_microusd"]
                    and int(row["quota_cost_microusd"]) >= int(row["quota_budget_microusd"])
                )
                if (
                    quota_blocked
                    or (row["cooldown_until"] and float(row["cooldown_until"]) > now)
                    or int(row["inflight"]) >= max(1, max_inflight)
                ):
                    await db.rollback()
                    return None
                expires_at = now + max(30, lease_seconds)
                await db.execute(
                    """
                    INSERT INTO account_leases(
                        lease_id, account_id, session_key, owner_id,
                        acquired_at, heartbeat_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (lease_id, account_id, session_key, owner_id, now, now, expires_at),
                )
                await db.execute(
                    """
                    UPDATE account_runtime SET
                        inflight=inflight+1,
                        quota_requests_used=quota_requests_used+1,
                        quota_exhausted=CASE
                            WHEN quota_request_limit>0
                             AND quota_requests_used+1>=quota_request_limit THEN 1
                            ELSE quota_exhausted END,
                        last_selected_at=?
                    WHERE account_id=?
                    """,
                    (now, account_id),
                )
                await db.commit()
                return lease_id
            except Exception:
                await db.rollback()
                raise

    async def reap_stale_leases(self) -> int:
        async with self._transaction_lock:
            db = self._db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                count = await self._reap_stale_leases_locked(time.time())
                await db.commit()
                return count
            except Exception:
                await db.rollback()
                raise

    async def heartbeat_lease(self, lease_id: str, lease_seconds: int = 90) -> bool:
        now = time.time()
        async with self._transaction_lock:
            cursor = await self._db().execute(
                """
                UPDATE account_leases SET heartbeat_at=?, expires_at=?
                WHERE lease_id=?
                """,
                (now, now + max(30, lease_seconds), lease_id),
            )
            await self._db().commit()
            return cursor.rowcount > 0

    async def release_slot(
        self,
        account_id: str,
        *,
        success: bool | None,
        error: str = "",
        cooldown_seconds: int = 0,
        failure_threshold: int = 1,
        lease_id: str = "",
        latency_ms: int = 0,
    ) -> None:
        now = time.time()
        async with self._transaction_lock:
            if lease_id:
                cursor = await self._db().execute(
                    "DELETE FROM account_leases WHERE lease_id=? AND account_id=?",
                    (lease_id, account_id),
                )
                if cursor.rowcount == 0:
                    await self._db().commit()
                    return
            if success:
                await self._db().execute(
                    """
                    UPDATE account_runtime SET
                        inflight=MAX(0,inflight-1), success_count=success_count+1,
                        consecutive_failures=0, last_success_at=?, last_error=''
                    WHERE account_id=?
                    """,
                    (now, account_id),
                )
            elif success is False:
                await self._db().execute(
                    """
                    UPDATE account_runtime SET
                        inflight=MAX(0,inflight-1), failure_count=failure_count+1,
                        consecutive_failures=consecutive_failures+1,
                        cooldown_until=CASE
                            WHEN consecutive_failures+1>=? AND ?>0 THEN ?
                            ELSE cooldown_until
                        END,
                        cooldown_reason=CASE
                            WHEN consecutive_failures+1>=? AND ?>0 THEN ?
                            ELSE cooldown_reason
                        END,
                        last_failure_at=?, last_error=?
                    WHERE account_id=?
                    """,
                    (
                        max(1, failure_threshold),
                        cooldown_seconds,
                        now + cooldown_seconds,
                        max(1, failure_threshold),
                        cooldown_seconds,
                        error[:500],
                        now,
                        error[:500],
                        account_id,
                    ),
                )
            else:
                await self._db().execute(
                    "UPDATE account_runtime SET "
                    "inflight=MAX(0,inflight-1), last_error=? WHERE account_id=?",
                    (error[:500], account_id),
                )
            error_sample = None if success is None else (0.0 if success else 1.0)
            await self._db().execute(
                """
                UPDATE account_runtime SET
                    latency_ewma_ms=CASE
                        WHEN ?>0 AND latency_ewma_ms<=0 THEN ?
                        WHEN ?>0 THEN latency_ewma_ms*0.8+?*0.2
                        ELSE latency_ewma_ms END,
                    error_ewma=CASE
                        WHEN ? IS NULL THEN error_ewma
                        ELSE error_ewma*0.8+?*0.2 END
                WHERE account_id=?
                """,
                (
                    latency_ms,
                    latency_ms,
                    latency_ms,
                    latency_ms,
                    error_sample,
                    error_sample,
                    account_id,
                ),
            )
            await self._db().commit()

    async def clear_cooldown(self, account_id: str) -> None:
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO account_runtime(account_id) VALUES (?)
                ON CONFLICT(account_id) DO UPDATE SET
                    cooldown_until=NULL, cooldown_reason='',
                    consecutive_failures=0, last_error=''
                """,
                (account_id,),
            )
            await self._db().commit()

    async def set_cooldown(self, account_id: str, seconds: int, reason: str) -> None:
        until = time.time() + max(1, seconds)
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO account_runtime(account_id, cooldown_until, cooldown_reason, last_error)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    cooldown_until=excluded.cooldown_until,
                    cooldown_reason=excluded.cooldown_reason,
                    last_error=excluded.last_error
                """,
                (account_id, until, reason[:500], reason[:500]),
            )
            await self._db().commit()

    async def register_tool_continuation(
        self,
        tool_call_id: str,
        session_key: str,
        account_id: str,
        chat_session_id: str,
        parent_message_id: int | None,
        ttl_seconds: int,
    ) -> None:
        now = time.time()
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO tool_continuations(
                    tool_call_id, session_key, account_id, chat_session_id,
                    parent_message_id, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tool_call_id) DO UPDATE SET
                    session_key=excluded.session_key,
                    account_id=excluded.account_id,
                    chat_session_id=excluded.chat_session_id,
                    parent_message_id=excluded.parent_message_id,
                    expires_at=excluded.expires_at
                """,
                (
                    tool_call_id,
                    session_key,
                    account_id,
                    chat_session_id,
                    parent_message_id,
                    now,
                    now + max(60, ttl_seconds),
                ),
            )
            await self._db().commit()

    async def get_tool_continuation(self, tool_call_id: str) -> dict[str, Any] | None:
        cursor = await self._db().execute(
            "SELECT * FROM tool_continuations WHERE tool_call_id=? AND expires_at>?",
            (tool_call_id, time.time()),
        )
        return self._row(await cursor.fetchone())

    async def increment_metric(self, name: str, amount: int = 1) -> None:
        async with self._transaction_lock:
            await self._db().execute(
                """
                INSERT INTO orchestration_metrics(name, value) VALUES(?, ?)
                ON CONFLICT(name) DO UPDATE SET value=value+excluded.value
                """,
                (name, amount),
            )
            await self._db().commit()

    async def metrics(self) -> dict[str, int]:
        rows = await (await self._db().execute(
            "SELECT name, value FROM orchestration_metrics"
        )).fetchall()
        return {str(row["name"]): int(row["value"]) for row in rows}

    async def cleanup(self) -> dict[str, int]:
        now = time.time()
        async with self._transaction_lock:
            conversations = await self._db().execute(
                "DELETE FROM conversations WHERE expires_at <= ?",
                (now,),
            )
            tools = await self._db().execute(
                "DELETE FROM tool_continuations WHERE expires_at <= ?",
                (now,),
            )
            aliases = await self._db().execute(
                "DELETE FROM task_aliases WHERE expires_at <= ?",
                (now,),
            )
            leases = await self._reap_stale_leases_locked(now)
            await self._db().commit()
        return {
            "conversations": conversations.rowcount,
            "tool_continuations": tools.rowcount,
            "task_aliases": aliases.rowcount,
            "leases": leases,
        }
