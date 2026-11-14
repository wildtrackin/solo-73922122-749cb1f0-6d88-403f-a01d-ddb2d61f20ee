"""压实（compaction）编排。

保证的性质：
1. 一次只压实一份日分区：进程内 asyncio.Lock + 数据库 running 任务双重互斥；
2. 启动时固定该日文件集合（事务内读取 active 清单），压实期间新上传的文件
   不会并入本次产物，发布后仍以 active 状态保留；
3. 合并件先写 _tmp，再原子改名并在单个 DB 事务内发布；旧件仅转 superseded，
   仍被查询快照引用期间不回收（“旧文件无人读取后才回收”）；
4. **发布即终态**：文件清单与 published 状态在同一事务提交后，任务结果不再改变。
   回收失败（如磁盘 I/O 错误）只记录为“回收未决”，绝不能改报压实失败或撤销新件；
   未回收的旧件留待后台回收器继续、或重启后的恢复流程清理；
5. 发布**之前**的合并、落盘、登记失败仍如实报告 failed，并回滚未发布件，
   active 读数保持可查，不暴露未发布的合并件；
6. 合并 / 回收期间不持有数据库写锁：所有 PyArrow IO 都在线程池执行，
   DB 只在短事务里更新元数据；
7. 任意阶段崩溃：重启后 running 任务标记 failed、未发布 _tmp 清理、
   已发布的 active 文件全部保留，随后继续回收无人引用的旧件。
"""
from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from . import parquet_store as ps
from .db import Database
from .faults import FaultInjector, ReapFault
from .gates import PauseGate

logger = logging.getLogger("viblake.compaction")


class CompactionError(RuntimeError):
    pass


class CompactionManager:
    def __init__(
        self,
        db: Database,
        lake_dir: str,
        gate: PauseGate,
        faults: FaultInjector | None = None,
    ) -> None:
        self._db = db
        self._lake = lake_dir
        self._gate = gate
        self._faults = faults or FaultInjector()
        self._single = asyncio.Lock()
        self._reap_lock = asyncio.Lock()
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

        result["reaped"] = await self._reap_after_recovery()
        await self._refresh_reap_outcomes()
        return result

    async def _reap_after_recovery(self) -> int:
        """重启后续完旧件回收；演示故障持续存在时不应阻断启动。"""
        try:
            return await self.reap_once()
        except Exception as exc:
            logger.warning("启动回收未完成（已提交数据不受影响，留待后续回收）: %s", exc)
            return 0

    # ---------- 回收 ----------

    async def reap_once(self) -> int:
        """回收 superseded 且无快照引用的旧文件。返回本次回收数量。

        回收属于发布后的独立阶段：
        - 单个旧件删除失败只记录错误、保留该旧件，不影响其他旧件回收；
        - 演示故障（磁盘 I/O 错误）在删除任何旧件前抛出，由调用方按“回收未决”处理。
        """
        async with self._reap_lock:
            return await self._reap_once_locked()

    async def _reap_once_locked(self) -> int:
        candidates = await self._db.list_reapable_files()
        if not candidates:
            await self._refresh_reap_outcomes()
            return 0

        # 演示故障：真实生产不会走到这里；故障发生在发布之后，绝不能影响任务结果
        self._faults.raise_if_reap_fault()

        reaped = 0
        failed: dict[int, str] = {}
        for row in candidates:
            # 先删磁盘，再在短事务里改元数据；崩溃后重启由孤儿清理兜底
            try:
                await self.run_io(ps.delete_file, self._lake, row["path"])
                await self._db.mark_files_deleted([row["id"]])
                reaped += 1
            except Exception as exc:  # 保留旧件，下一轮或重启后重试
                failed[row["id"]] = f"回收旧件 {row['path']} 失败: {exc}"
                logger.warning(failed[row["id"]])
        if reaped:
            logger.info("回收 %d 个无人读取的旧文件", reaped)

        # 把回收结果按来源任务落库：失败只记录“回收未决”，任务状态不变
        owners = await self._db.reap_owner_map(list(failed))
        for file_id, message in failed.items():
            owner = owners.get(file_id)
            if owner is not None:
                await self._db.update_reap_outcome(owner, message)
        # 其余未决任务本轮没有失败：刷新可能已清除的旧错误（仍被快照引用时保持 pending）
        await self._refresh_reap_outcomes(exclude=set(owners.values()))
        if failed:
            # 让调用方知悉“本轮有回收失败”，但不影响已提交任务
            fault = ReapFault("；".join(failed.values()))
            fault.reaped = reaped  # type: ignore[attr-defined]
            raise fault
        return reaped

    async def _refresh_reap_outcomes(self, exclude: set[int] | None = None) -> None:
        """把“已发布任务”的旧件回收结果同步到任务行；失败只记录，不改变任务状态。

        exclude 用于跳过本轮已按来源任务写入失败说明的行，避免刚写入的错误被覆盖。
        """
        try:
            for row in await self._db.pending_reap_compactions():
                if exclude and int(row["id"]) in exclude:
                    continue
                await self._db.update_reap_outcome(int(row["id"]))
        except Exception:
            logger.exception("刷新回收结果失败（任务状态仍为 published）")

    async def _reaper_loop(self) -> None:
        while not self._reaper_stop.is_set():
            try:
                await self.reap_once()
            except Exception:  # 回收失败不影响主流程，下一轮重试
                logger.debug("回收循环本轮未完成", exc_info=True)
            try:
                await asyncio.wait_for(self._reaper_stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    # ---------- 压实 ----------

    async def compact_day(self, day: str, compaction_id: int) -> dict[str, object]:
        """执行一个已登记 running 的日分区压实任务（由后台任务调用）。

        分为两段：
        - 发布段（可失败）：合并 → 写临时件 → 原子改名 → 登记并提交 published；
          任何失败都如实报 failed，并清理未发布件；
        - 回收段（发布后，不改变任务结果）：尝试立即回收旧件，失败只记录
          “回收未决”，新件与 published 状态保持不变。
        """
        async with self._single:
            existing = await self._db.has_running_compaction()
            if existing is None or int(existing["id"]) != compaction_id:
                raise CompactionError("压实任务状态异常或已被其他任务占用，拒绝执行")

            try:
                result = await self._publish(day, compaction_id)
            except BaseException as exc:
                # 只处理发布前 / 发布事务内的失败；此时 published 尚未提交
                try:
                    row = await self._db.get_compaction(compaction_id)
                    if row is not None and row["status"] == "published":
                        # 极端竞态：发布已提交，绝不改报失败，交给回收段处理
                        logger.warning(
                            "压实 %s 已发布，忽略后续异常: %s", compaction_id, exc
                        )
                        return await self._settle_after_publish(
                            compaction_id,
                            day,
                            {
                                "compaction_id": compaction_id,
                                "day": day,
                                "new_file_id": row["new_file_id"],
                            },
                        )
                    if row is not None and row["temp_path"]:
                        await self.run_io(ps.delete_file, self._lake, row["temp_path"])
                except Exception:  # 回滚清理本身失败时依赖重启恢复
                    logger.exception("压实回滚清理异常")
                if not isinstance(exc, CompactionError):
                    logger.exception("压实失败 day=%s", day)
                await self._db.fail_compaction(compaction_id, str(exc))
                raise

            # 发布事务已提交：从这里开始回收失败不得影响任务结果
            return await self._settle_after_publish(compaction_id, day, result)

    async def _publish(self, day: str, compaction_id: int) -> dict[str, object]:
        """发布段：成功后返回发布信息；任何异常向上抛出，由调用方报 failed。"""
        self._gate.raise_if_merge_paused()

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
        try:
            await self._db.set_compaction_temp(compaction_id, rel_tmp)
        except BaseException:
            # 登记临时件失败：立即清除，避免留下未登记的半成品
            await self.run_io(ps.delete_file, self._lake, rel_tmp)
            raise

        # 测试钩子：发布前延时，便于演练“临时件已写、未发布时崩溃”
        if delay_ms := int(os.environ.get("VIBLAKE_COMPACT_DELAY_MS", "0")):
            await asyncio.sleep(delay_ms / 1000.0)

        # 4) 暂停开关安全检查点：发布前停下则不留新件
        self._gate.raise_if_merge_paused()

        # 5) 原子改名 + 单事务发布（提交后任务即 published）
        #    先算好机组集合（纯内存操作），改名后只剩一个可能失败的登记步骤，
        #    便于失败时精确清理已改名的未发布件。
        turbines = await self.run_io(ps.table_turbines, merged)
        rel_final = await self.run_io(ps.publish_temp, self._lake, day, new_id, rel_tmp)
        try:
            await self._db.publish_compaction(
                compaction_id,
                day=day,
                new_file_id=new_id,
                new_rel_path=rel_final,
                row_count=merged.num_rows,
                turbines=turbines,
                old_ids=old_ids,
            )
        except BaseException:
            # 改名已发生但登记失败：文件不在元数据里，查询看不到它。
            # 立即移除，避免暴露未发布的合并件；即便删除失败，重启孤儿清理兜底。
            try:
                await self.run_io(ps.delete_file, self._lake, rel_final)
            except Exception:
                logger.exception("清理未登记的合并件失败，重启时将按孤儿清理")
            raise
        logger.info(
            "压实发布 day=%s: %d 个文件 -> %s（%d 行）",
            day, len(old_ids), new_id, merged.num_rows,
        )
        return {
            "compaction_id": compaction_id,
            "day": day,
            "merged_files": len(old_ids),
            "new_file_id": new_id,
            "rows": merged.num_rows,
            "note": "压实期间新上传的文件未并入本次任务，仍以独立 active 文件保留",
        }

    async def _settle_after_publish(
        self, compaction_id: int, day: str, result: dict[str, object]
    ) -> dict[str, object]:
        """回收段：尝试立即回收旧件。无论成败，任务结果都保持 published。"""
        reaped = 0
        reap_error: str | None = None
        try:
            reaped = await self.reap_once()
        except Exception as exc:
            # 典型场景：旧件回收遇到磁盘错误。已发布的合并件与任务状态不受影响，
            # 未回收旧件留待后台回收器 / 重启恢复继续处理。
            reaped = int(getattr(exc, "reaped", 0) or 0)
            reap_error = str(exc)
            logger.warning(
                "压实 %s 已发布，但发布后回收未完成（不影响任务结果）: %s",
                compaction_id, exc,
            )
            try:
                current = await self._db.get_compaction(compaction_id)
                # 回收循环已按来源任务记录过错误时不要覆盖，避免张冠李戴
                if current is not None and not current["reap_error"]:
                    await self._db.update_reap_outcome(compaction_id, reap_error)
            except Exception:
                logger.exception("记录回收未决状态失败（任务仍为 published）")

        try:
            row = await self._db.get_compaction(compaction_id)
        except Exception:
            row = None
        payload = dict(result)
        payload["status"] = "published"
        if row is not None:
            payload["reap_status"] = row["reap_status"] or "pending"
            payload["reap_pending"] = int(row["reap_pending"] or 0)
            payload["reap_error"] = row["reap_error"]
        payload["reaped_now"] = reaped
        if reap_error:
            payload["reap_error"] = reap_error
            payload["note"] = (
                "压实已发布；发布后的旧件回收遇到错误，旧件保留待后续回收或重启清理"
            )
        return payload
