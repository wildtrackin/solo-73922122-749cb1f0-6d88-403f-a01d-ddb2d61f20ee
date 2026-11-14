"""业务服务层：把 DB、Parquet、压实、开关串成完整用例。"""
from __future__ import annotations

import asyncio
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from . import parquet_store as ps
from .compaction import CompactionError, CompactionManager
from .config import Settings
from .csv_import import parse_readings
from .db import Database
from .gates import PauseGate


class Snapshot:
    """上下文管理器：查询期间固定文件集合，结束即释放引用。"""

    def __init__(self, db: Database, snapshot_id: int, paths: list[str]) -> None:
        self._db = db
        self.snapshot_id = snapshot_id
        self.paths = paths

    async def __aenter__(self) -> "Snapshot":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._db.release_snapshot(self.snapshot_id)


class DataLakeService:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        gate: PauseGate,
        compaction: CompactionManager,
    ) -> None:
        self._db = db
        self._settings = settings
        self._gate = gate
        self._compaction = compaction
        self._io_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="svc-io")
        self._bg: dict[int, asyncio.Task] = {}

    async def close(self) -> None:
        for task in self._bg.values():
            task.cancel()
        self._io_pool.shutdown(wait=False, cancel_futures=True)

    async def _io(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._io_pool, func, *args)

    # ---------------- 上传 ----------------

    async def upload_csv(self, data: bytes, filename: str) -> dict[str, object]:
        """整批校验通过后，写成一个 Parquet 并登记；保留全部重复行。"""
        readings = parse_readings(data, self._settings)  # 失败直接 422，不产生文件
        day = readings[0].ts_utc.date().isoformat()
        file_id = ps.new_file_id("batch")

        # Parquet 写在 DB 写事务之外
        rel_path = await self._io(ps.write_batch, self._settings.lake_dir, day, file_id, readings)
        try:
            await self._db.insert_file(
                file_id=file_id,
                day=day,
                rel_path=rel_path,
                row_count=len(readings),
                turbines=(r.turbine for r in readings),
            )
        except BaseException:
            # 元数据登记失败：移除磁盘文件，避免成为孤儿
            await self._io(ps.delete_file, self._settings.lake_dir, rel_path)
            raise
        return {
            "file_id": file_id,
            "day": day,
            "path": rel_path,
            "rows": len(readings),
            "turbines": sorted({r.turbine for r in readings}),
            "filename": filename,
        }

    # ---------------- 查询（读快照固定文件集合） ----------------

    async def query(
        self,
        turbine: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> dict[str, object]:
        # 暂停开关：在进入快照流程前等待，恢复后照常服务，不丢请求
        await self._gate.wait_query_open()

        limit = max(1, min(int(limit), self._settings.query_limit_max))
        start_day = start.date().isoformat() if start else None
        end_day = (end - timedelta(microseconds=1)).date().isoformat() if end else None

        snapshot_id, files = await self._db.create_snapshot(turbine, start_day, end_day)
        snapshot = Snapshot(self._db, snapshot_id, [f["path"] for f in files])
        try:
            rows, total_matched, scanned = await self._io(
                ps.read_filtered,
                self._settings.lake_dir,
                snapshot.paths,
                turbine,
                start,
                end,
                limit,
            )
        except BaseException:
            await snapshot.__aexit__(None, None, None)
            raise
        else:
            await snapshot.__aexit__(None, None, None)

        return {
            "snapshot_id": snapshot_id,
            "files_in_snapshot": len(files),
            "files_scanned": scanned,
            "matched_rows": total_matched,
            "returned_rows": len(rows),
            "truncated": total_matched > len(rows),
            "limit": limit,
            "rows": rows,
        }

    # ---------------- 统计 / 状态 ----------------

    async def stats(self) -> dict[str, object]:
        daily = [dict(r) for r in await self._db.daily_stats()]
        totals = {
            "active_files": sum(int(d["active_files"] or 0) for d in daily),
            "superseded_files": sum(int(d["superseded_files"] or 0) for d in daily),
            "deleted_files": sum(int(d["deleted_files"] or 0) for d in daily),
            "active_rows": sum(int(d["active_rows"] or 0) for d in daily),
            "partitions": len(daily),
        }
        running = await self._db.has_running_compaction()
        return {
            "totals": totals,
            "daily": daily,
            "running_compaction": dict(running) if running else None,
            "gates": self._gate.snapshot(),
        }

    # ---------------- 压实 ----------------

    async def start_compaction(self, day: str) -> tuple[int, dict[str, object]]:
        """启动后台压实任务，立即返回任务 id（HTTP 202）。"""
        if self._gate.merge_paused:
            raise CompactionError("演示开关已暂停合并，请先恢复后再启动压实")
        existing = await self._db.has_running_compaction()
        if existing is not None:
            raise CompactionError(
                f"一次只能压实一份日分区，当前任务 id={existing['id']} day={existing['day']} 仍在运行"
            )
        # 先占位任务行，保证即使尚未被事件循环调度也能在 DB 层面互斥
        compaction_id = await self._db.insert_compaction(day)

        async def runner() -> None:
            try:
                await self._compaction.compact_day(day, compaction_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # 失败已在编排层落库（fail_compaction）

        task = asyncio.create_task(runner(), name=f"compact-{day}")
        self._bg[compaction_id] = task
        task.add_done_callback(lambda t: self._bg.pop(compaction_id, None))
        return compaction_id, {"day": day, "compaction_id": compaction_id, "status": "running"}

    async def compaction_status(self, compaction_id: int) -> dict[str, object] | None:
        row = await self._db.get_compaction(compaction_id)
        return dict(row) if row else None

    async def compaction_history(self) -> list[dict[str, object]]:
        return [dict(r) for r in await self._db.list_compactions()]

    # ---------------- 演示开关 ----------------

    async def set_gate(self, kind: str, paused: bool) -> dict[str, object]:
        if kind == "merge":
            self._gate.set_merge_paused(paused)
            await self._db.set_setting("pause_merge", "true" if paused else "false")
        elif kind == "query":
            self._gate.set_query_paused(paused)
            await self._db.set_setting("pause_query", "true" if paused else "false")
        else:
            raise ValueError(f"未知开关: {kind}")
        return self._gate.snapshot()

    # ---------------- 虚构读数 ----------------

    async def generate_demo_csv(
        self,
        rows: int,
        day: str | None,
        turbines: list[str],
    ) -> bytes:
        if not 1 <= rows <= self._settings.max_batch_rows:
            raise ValueError(f"虚构行数需在 1..{self._settings.max_batch_rows} 之间")
        if day is None:
            day = datetime.now(timezone.utc).date().isoformat()
        # 校验日期格式
        datetime.strptime(day, "%Y-%m-%d")

        def _build() -> bytes:
            import csv
            import io
            import random

            rng = random.Random(20261113)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["turbine", "ts_utc", "vibration"])
            base = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            for i in range(rows):
                turbine = turbines[i % len(turbines)]
                ts = base + timedelta(
                    seconds=rng.randint(0, 24 * 3600 - 1),
                    microseconds=rng.randint(0, 999_999),
                )
                # 带机组差异的正弦 + 噪声，保证有限值
                phase = turbines.index(turbine)
                value = (
                    3.0 * math.sin(2 * math.pi * (i % 720) / 720.0 + phase)
                    + rng.gauss(0.0, 0.4)
                )
                writer.writerow(
                    [turbine, ts.isoformat().replace("+00:00", "Z"), f"{value:.4f}"]
                )
            return buf.getvalue().encode("utf-8")

        return await self._io(_build)
