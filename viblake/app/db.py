"""SQLite 元数据库。

设计要点：
- 双连接：writer 走 BEGIN IMMEDIATE 短事务（由 asyncio.Lock 串行化），
  reader 走 WAL 自动提交读，两者互不阻塞。
- 任何 Parquet 文件读写都在本模块的写事务之外执行（见 service / compaction），
  数据库里只保存元数据，写锁持有时间仅限几次索引更新。
- 读快照(snapshots/snapshot_files)固定一次查询开始时的文件集合；
  压实产生的旧文件只要仍被某个快照引用就保持 superseded 不被回收。
"""
from __future__ import annotations

import contextlib
import asyncio
from typing import Any, AsyncIterator, Iterable, Sequence

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id          TEXT PRIMARY KEY,
    day         TEXT NOT NULL,              -- UTC 日分区 YYYY-MM-DD
    path        TEXT NOT NULL UNIQUE,       -- 相对 lake_dir 的路径
    row_count   INTEGER NOT NULL,
    state       TEXT NOT NULL DEFAULT 'active', -- active / superseded / deleted
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    deleted_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_day_state ON files(day, state);

CREATE TABLE IF NOT EXISTS file_turbines (
    file_id     TEXT NOT NULL REFERENCES files(id),
    turbine     TEXT NOT NULL,
    PRIMARY KEY (file_id, turbine)
);
CREATE INDEX IF NOT EXISTS idx_ft_turbine ON file_turbines(turbine);

CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    refcount    INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    released_at TEXT
);

CREATE TABLE IF NOT EXISTS snapshot_files (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    file_id     TEXT NOT NULL REFERENCES files(id),
    PRIMARY KEY (snapshot_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_sf_file ON snapshot_files(file_id);

CREATE TABLE IF NOT EXISTS compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL,             -- running / published / failed
    temp_path   TEXT,
    new_file_id TEXT REFERENCES files(id),
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_comp_status ON compactions(status);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._writer: aiosqlite.Connection | None = None
        self._reader: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    # ---------- 生命周期 ----------

    async def connect(self) -> None:
        self._writer = await aiosqlite.connect(self._path)
        self._reader = await aiosqlite.connect(self._path)
        for conn in (self._writer, self._reader):
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA foreign_keys=ON")
            await conn.execute("PRAGMA busy_timeout=5000")
            await conn.execute("PRAGMA synchronous=NORMAL")
        await self._writer.executescript(SCHEMA)
        await self._writer.commit()

    async def close(self) -> None:
        for conn in (self._writer, self._reader):
            if conn is not None:
                await conn.close()

    @contextlib.asynccontextmanager
    async def tx(self) -> AsyncIterator[aiosqlite.Connection]:
        """串行化的即时写事务；Parquet IO 不允许放在此上下文内。"""
        assert self._writer is not None
        async with self._write_lock:
            await self._writer.execute("BEGIN IMMEDIATE")
            try:
                yield self._writer
                await self._writer.commit()
            except BaseException:
                await self._writer.rollback()
                raise

    async def query_all(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        assert self._reader is not None
        async with self._reader.execute(sql, params) as cur:
            return list(await cur.fetchall())

    async def query_one(self, sql: str, params: Sequence[Any] = ()) -> aiosqlite.Row | None:
        assert self._reader is not None
        async with self._reader.execute(sql, params) as cur:
            return await cur.fetchone()

    # ---------- 文件清单 ----------

    async def insert_file(
        self,
        file_id: str,
        day: str,
        rel_path: str,
        row_count: int,
        turbines: Iterable[str],
    ) -> None:
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO files(id, day, path, row_count, state) VALUES (?,?,?,?, 'active')",
                (file_id, day, rel_path, row_count),
            )
            await conn.executemany(
                "INSERT OR IGNORE INTO file_turbines(file_id, turbine) VALUES (?,?)",
                [(file_id, t) for t in sorted(set(turbines))],
            )

    async def list_active_files(
        self,
        turbine: str | None = None,
        start_day: str | None = None,
        end_day: str | None = None,
        conn: aiosqlite.Connection | None = None,
    ) -> list[aiosqlite.Row]:
        """返回满足条件的 active 文件。传入 conn 时在该事务连接上执行（快照一致性）。"""
        sql = "SELECT f.* FROM files f WHERE f.state = 'active'"
        params: list[Any] = []
        if start_day is not None:
            sql += " AND f.day >= ?"
            params.append(start_day)
        if end_day is not None:
            sql += " AND f.day <= ?"
            params.append(end_day)
        if turbine is not None:
            sql += (
                " AND EXISTS (SELECT 1 FROM file_turbines ft "
                "WHERE ft.file_id = f.id AND ft.turbine = ?)"
            )
            params.append(turbine)
        sql += " ORDER BY f.day, f.id"
        target = conn if conn is not None else self._reader
        assert target is not None
        async with target.execute(sql, params) as cur:
            return list(await cur.fetchall())

    async def mark_files_deleted(self, ids: Sequence[str]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        async with self.tx() as conn:
            await conn.execute(
                f"UPDATE files SET state='deleted', "
                f"deleted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id IN ({placeholders})",
                list(ids),
            )

    async def list_reapable_files(self, limit: int = 32) -> list[aiosqlite.Row]:
        """已被替代且没有任何在读快照引用的文件，可安全回收。"""
        return await self.query_all(
            """
            SELECT f.* FROM files f
            WHERE f.state = 'superseded'
              AND NOT EXISTS (
                  SELECT 1 FROM snapshot_files sf WHERE sf.file_id = f.id
              )
            ORDER BY f.id LIMIT ?
            """,
            (limit,),
        )

    async def all_live_paths(self) -> set[str]:
        """元数据中仍存在（active/superseded）的磁盘相对路径，用于孤儿清理。"""
        rows = await self.query_all(
            "SELECT path FROM files WHERE state IN ('active','superseded')"
        )
        return {r["path"] for r in rows}

    # ---------- 读快照 ----------

    async def create_snapshot(
        self,
        turbine: str | None,
        start_day: str | None,
        end_day: str | None,
    ) -> tuple[int, list[aiosqlite.Row]]:
        """在同一写事务内固定查询开始时的 active 文件集合。"""
        async with self.tx() as conn:
            files = await self.list_active_files(turbine, start_day, end_day, conn=conn)
            cur = await conn.execute("INSERT INTO snapshots DEFAULT VALUES")
            snapshot_id = int(cur.lastrowid)
            await conn.executemany(
                "INSERT INTO snapshot_files(snapshot_id, file_id) VALUES (?,?)",
                [(snapshot_id, f["id"]) for f in files],
            )
        return snapshot_id, files

    async def release_snapshot(self, snapshot_id: int) -> None:
        """释放一次引用；归零后删除快照及其文件关联，旧文件随即可回收。"""
        async with self.tx() as conn:
            cur = await conn.execute(
                "UPDATE snapshots SET refcount=refcount-1 "
                "WHERE id=? AND released_at IS NULL",
                (snapshot_id,),
            )
            if cur.rowcount == 0:
                return
            row = await self._fetch_one(
                conn, "SELECT refcount FROM snapshots WHERE id=?", (snapshot_id,)
            )
            if row is not None and row["refcount"] <= 0:
                await conn.execute(
                    "UPDATE snapshots SET released_at="
                    "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    (snapshot_id,),
                )
                await conn.execute("DELETE FROM snapshot_files WHERE snapshot_id=?", (snapshot_id,))

    @staticmethod
    async def _fetch_one(conn: aiosqlite.Connection, sql: str, params: Sequence[Any]):
        async with conn.execute(sql, params) as cur:
            return await cur.fetchone()

    # ---------- 压实任务 ----------

    async def insert_compaction(self, day: str) -> int:
        async with self.tx() as conn:
            cur = await conn.execute(
                "INSERT INTO compactions(day, status) VALUES (?, 'running')", (day,)
            )
            return int(cur.lastrowid)

    async def set_compaction_temp(self, compaction_id: int, temp_path: str) -> None:
        async with self.tx() as conn:
            await conn.execute(
                "UPDATE compactions SET temp_path=? WHERE id=?", (temp_path, compaction_id)
            )

    async def publish_compaction(
        self,
        compaction_id: int,
        day: str,
        new_file_id: str,
        new_rel_path: str,
        row_count: int,
        turbines: Iterable[str],
        old_ids: Sequence[str],
    ) -> None:
        """压实发布：新文件登记为 active，旧文件转 superseded（不立即删除）。"""
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO files(id, day, path, row_count, state) VALUES (?,?,?,?,'active')",
                (new_file_id, day, new_rel_path, row_count),
            )
            await conn.executemany(
                "INSERT OR IGNORE INTO file_turbines(file_id, turbine) VALUES (?,?)",
                [(new_file_id, t) for t in sorted(set(turbines))],
            )
            if old_ids:
                placeholders = ",".join("?" for _ in old_ids)
                await conn.execute(
                    f"UPDATE files SET state='superseded' "
                    f"WHERE id IN ({placeholders}) AND state='active'",
                    list(old_ids),
                )
            await conn.execute(
                "UPDATE compactions SET status='published', new_file_id=?, "
                "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (new_file_id, compaction_id),
            )

    async def fail_compaction(self, compaction_id: int, error: str) -> None:
        async with self.tx() as conn:
            await conn.execute(
                "UPDATE compactions SET status='failed', error=?, "
                "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (error[:500], compaction_id),
            )

    async def get_compaction(self, compaction_id: int) -> aiosqlite.Row | None:
        return await self.query_one("SELECT * FROM compactions WHERE id=?", (compaction_id,))

    async def list_compactions(self, limit: int = 20) -> list[aiosqlite.Row]:
        return await self.query_all(
            "SELECT * FROM compactions ORDER BY id DESC LIMIT ?", (limit,)
        )

    async def has_running_compaction(self) -> aiosqlite.Row | None:
        return await self.query_one("SELECT * FROM compactions WHERE status='running' LIMIT 1")

    # ---------- 统计 ----------

    async def daily_stats(self) -> list[aiosqlite.Row]:
        return await self.query_all(
            """
            SELECT day,
                   SUM(CASE WHEN state='active' THEN 1 ELSE 0 END) AS active_files,
                   SUM(CASE WHEN state='superseded' THEN 1 ELSE 0 END) AS superseded_files,
                   SUM(CASE WHEN state='deleted' THEN 1 ELSE 0 END) AS deleted_files,
                   SUM(CASE WHEN state='active' THEN row_count ELSE 0 END) AS active_rows
            FROM files GROUP BY day ORDER BY day
            """
        )

    # ---------- 演示开关（持久化，重启后保持） ----------

    async def get_setting(self, key: str, default: str) -> str:
        row = await self.query_one("SELECT value FROM settings WHERE key=?", (key,))
        return default if row is None else row["value"]

    async def set_setting(self, key: str, value: str) -> None:
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO settings(key, value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
