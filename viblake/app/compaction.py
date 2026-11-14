"""压实（compaction）编排。

保证的性质：
1. 一次只压实一份日分区：进程内 asyncio.Lock + 数据库 running 任务双重互斥；
2. 启动时固定该日文件集合（事务内读取 active 清单），压实期间新上传的文件
   不会并入本次产物，发布后仍以 active 状态保留；
3. 合并件先写 _tmp，再原子改名并在单个 DB 事务内发布；旧件仅转 superseded，
   仍被查询快照引用期间不回收（“旧文件无人读取后才回收”）；
4. 合并 / 回收期间不持有数据库写锁：所有 PyArrow IO 都在线程池执行，
   DB 只在短事务里更新元数据；
5. 任意阶段崩溃：重启后 running 任务标记 failed、未发布 _tmp 清理、
   已发布的 active 文件全部保留，随后继续回收无人引用的旧件。
"""
from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from . import parquet_store as ps
from .db import Database
from .gates import PauseGate

logger = logging.getLogger("viblake.compaction")


class CompactionError(RuntimeError):
    pass


class CompactionManager:
    def __init__(self, db: Database, lake_dir: str, gate: PauseGate) -> None:
        self._db = db
        self._lake = lake_dir
        self._gate = gate
        self._single = asyncio.Lock()
        self._io_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="parquet-io")
        self._reaper_stop = asyncio.Event()
        self._reaper_task: asyncio.Task | None = None

    # ---------- 线程池帮助函数 ----------

    async def run_io(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._io_pool, func, *args)

    # ---------- 生命周期 ----------

    async def start_reaper(self) -> None:
        if self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reaper_loop(), name="reaper")

    async def close(self) -> None:
        self._reaper_stop.set()
        if self._reaper_task is not None:
            try:
                await asyncio.wait_for(self._reaper_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._reaper_task.cancel()
        self._io_pool.shutdown(wait=False, cancel_futures=True)

    # ---------- 重启恢复 ----------

    async def recover(self) -> dict[str, object]:
        """进程启动时调用：续完回收、清理未发布件、标记未完成任务。"""
        result: dict[str, object] = {
            "aborted_running": 0,
            "temp_removed": 0,
            "orphans_removed": 0,
            "reaped": 0,
        }

        running = await self._db.query_all(
            "SELECT * FROM compactions WHERE status='running' ORDER BY id"
        )
        for row in running:
            if row["temp_path"]:
                await self.run_io(ps.delete_file, self._lake, row["temp_path"])
                result["temp_removed"] += 1
            await self._db.fail_compaction(
                row["id"], "进程重启：任务未完成，已回滚（已提交文件不受影响）"
            )
            result["aborted_running"] += 1
            logger.warning(
                "恢复：压实任务 %s (day=%s) 标记失败并清理临时件", row["id"], row["day"]
            )

        # 清理所有未发布的 _tmp 件（含崩溃前未来得及登记任务的）
        for rel_tmp in await self.run_io(ps.list_temp_paths, self._lake):
            await self.run_io(ps.delete_file, self._lake, rel_tmp)
            result["temp_removed"] += 1

        # 孤儿清理：磁盘有、元数据无的 Parquet 一律移除（在 running 任务处理之后，
        # 因此已发布的合并件因元数据存在而保留）
        live_paths = await self._db.all_live_paths()
        for rel in await self.run_io(ps.list_disk_paths, self._lake):
            if rel not in live_paths:
                await self.run_io(ps.delete_file, self._lake, rel)
                result["orphans_removed"] += 1

        result["reaped"] = await self.reap_once()
        return result

    # ---------- 回收 ----------

    async def reap_once(self) -> int:
        """回收 superseded 且无快照引用的旧文件。返回回收数量。"""
        candidates = await self._db.list_reapable_files()
        if not candidates:
            return 0
        reaped = 0
        for row in candidates:
            # 先删磁盘，再在短事务里改元数据；崩溃后重启由孤儿清理兜底
            await self.run_io(ps.delete_file, self._lake, row["path"])
            await self._db.mark_files_deleted([row["id"]])
            reaped += 1
        logger.info("回收 %d 个无人读取的旧文件", reaped)
        return reaped

    async def _reaper_loop(self) -> None:
        while not self._reaper_stop.is_set():
            try:
                await self.reap_once()
            except Exception:  # 回收失败不影响主流程，下一轮重试
                logger.exception("回收循环异常")
            try:
                await asyncio.wait_for(self._reaper_stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    # ---------- 压实 ----------

    async def compact_day(self, day: str, compaction_id: int) -> dict[str, object]:
        """执行一个已登记 running 的日分区压实任务（由后台任务调用）。"""
        async with self._single:
            existing = await self._db.has_running_compaction()
            if existing is None or int(existing["id"]) != compaction_id:
                raise CompactionError("压实任务状态异常或已被其他任务占用，拒绝执行")

            self._gate.raise_if_merge_paused()
            try:
                # 1) 固定启动时该日的 active 文件集合（同事务读取，保证一致）
                async with self._db.tx() as conn:
                    files = await self._db.list_active_files(
                        turbine=None, start_day=day, end_day=day, conn=conn
                    )
                if len(files) < 2:
                    raise CompactionError(
                        f"日分区 {day} 只有 {len(files)} 个 active 文件，"
                        "无需压实（至少需要 2 个）"
                    )
                old_ids = [f["id"] for f in files]
                old_paths = [f["path"] for f in files]
                expected_rows = sum(int(f["row_count"]) for f in files)

                # 2) 读 + 合并完全在 DB 写事务之外
                merged = await self.run_io(ps.merge_files, self._lake, old_paths)
                if merged.num_rows != expected_rows:
                    raise CompactionError(
                        f"合并后行数 {merged.num_rows} 与元数据行数 {expected_rows} 不一致，中止发布"
                    )

                # 3) 写临时件（仍未发布，崩溃不影响已提交数据）
                new_id = ps.new_file_id("compact")
                rel_tmp = await self.run_io(ps.write_temp, self._lake, new_id, merged)
                await self._db.set_compaction_temp(compaction_id, rel_tmp)

                # 测试钩子：发布前延时，便于演练“临时件已写、未发布时崩溃”
                if delay_ms := int(os.environ.get("VIBLAKE_COMPACT_DELAY_MS", "0")):
                    await asyncio.sleep(delay_ms / 1000.0)

                # 4) 暂停开关安全检查点：发布前停下则不留新件
                self._gate.raise_if_merge_paused()

                # 5) 原子改名 + 单事务发布
                rel_final = await self.run_io(
                    ps.publish_temp, self._lake, day, new_id, rel_tmp
                )
                turbines = await self.run_io(ps.table_turbines, merged)
                await self._db.publish_compaction(
                    compaction_id,
                    day=day,
                    new_file_id=new_id,
                    new_rel_path=rel_final,
                    row_count=merged.num_rows,
                    turbines=turbines,
                    old_ids=old_ids,
                )
                logger.info(
                    "压实发布 day=%s: %d 个文件 -> %s（%d 行）",
                    day, len(old_ids), new_id, merged.num_rows,
                )

                # 6) 立即尝试回收（无快照引用的旧件）；仍被引用的留给后台 reaper
                reaped = await self.reap_once()
                return {
                    "compaction_id": compaction_id,
                    "day": day,
                    "merged_files": len(old_ids),
                    "new_file_id": new_id,
                    "rows": merged.num_rows,
                    "reaped_now": reaped,
                    "note": "压实期间新上传的文件未并入本次任务，仍以独立 active 文件保留",
                }
            except BaseException as exc:
                # 回滚：临时件删除、任务置 failed；已发布文件永远不会走到这里
                try:
                    row = await self._db.get_compaction(compaction_id)
                    if row is not None and row["temp_path"]:
                        await self.run_io(ps.delete_file, self._lake, row["temp_path"])
                except Exception:  # 回滚清理本身失败时依赖重启恢复
                    logger.exception("压实回滚清理异常")
                if not isinstance(exc, CompactionError):
                    logger.exception("压实失败 day=%s", day)
                await self._db.fail_compaction(compaction_id, str(exc))
                raise
