"""端到端集成测试（不依赖 pytest，直接 asyncio 运行）。

覆盖题目要求的关键性质：
1. 批次校验：行数上限、同一 UTC 日、有限振动值、必需列；
2. 整批一个 Parquet、重复行保留；
3. 读快照固定文件集合：压实进行中 / 完成后查询不漏行、不重行、不中断；
4. 一次只压实一份日分区；压实期间上传的文件保留且不被并入；
5. 旧文件 superseded 期间被快照保护，释放后才回收；
6. 暂停合并 / 暂停查询开关；
7. 崩溃重启恢复：running 任务回滚、未发布 _tmp 清理、孤儿清理、已提交数据完整；
8. 多日分区隔离。
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import parquet_store as ps  # noqa: E402
from app.compaction import CompactionError, CompactionManager  # noqa: E402
from app.config import Settings  # noqa: E402
from app.csv_import import ValidationError, parse_readings  # noqa: E402
from app.db import Database  # noqa: E402
from app.gates import PauseGate  # noqa: E402
from app.service import DataLakeService  # noqa: E402

TMP_ROOT = Path(__file__).resolve().parent / "tmp"
DAY = "2026-11-13"
NEXT_DAY = "2026-11-14"


def make_settings(case: str) -> Settings:
    root = TMP_ROOT / case
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    return Settings(data_dir=str(root), max_batch_rows=10000)


def csv_bytes(rows: list[tuple[str, str, float]]) -> bytes:
    lines = ["turbine,ts_utc,vibration"]
    lines.extend(f"{t},{ts},{v}" for t, ts, v in rows)
    return ("\n".join(lines) + "\n").encode()


def ts(hour: int, minute: int = 0, day: str = DAY) -> str:
    return f"{day}T{hour:02d}:{minute:02d}:00Z"


class Harness:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.gate = PauseGate()
        self.cm = CompactionManager(self.db, settings.lake_dir, self.gate)
        self.svc = DataLakeService(self.db, settings, self.gate, self.cm)

    async def start(self) -> None:
        ps.ensure_dirs(self.settings.lake_dir)
        await self.db.connect()
        await self.gate.load(self.db)
        await self.cm.start_reaper()

    async def stop(self) -> None:
        await self.svc.close()
        await self.cm.close()
        await self.db.close()

    async def upload(self, rows: list[tuple[str, str, float]], name: str = "x.csv"):
        return await self.svc.upload_csv(csv_bytes(rows), name)

    async def query_all(self, turbine=None, start=None, end=None):
        return await self.svc.query(turbine, start, end, 50000)

    async def compact(self, day: str, allow_fail: bool = False) -> dict:
        cid, _ = await self.svc.start_compaction(day)
        for _ in range(200):
            row = await self.svc.compaction_status(cid)
            if row["status"] != "running":
                if row["status"] == "failed" and not allow_fail:
                    raise AssertionError(f"压实失败: {row['error']}")
                return dict(row)
            await asyncio.sleep(0.05)
        raise AssertionError("压实超时未完成")


PASSED = 0
FAILED: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASSED
    if cond:
        PASSED += 1
        print(f"  ✓ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ✗ {name} {detail}")


async def test_validation() -> None:
    print("[1] CSV 校验规则")
    s = make_settings("validation")
    h = Harness(s)
    await h.start()
    try:
        # 跨 UTC 日 -> 拒绝
        bad_day = [("WTG-01", ts(8), 1.0), ("WTG-01", ts(8, day=NEXT_DAY), 2.0)]
        try:
            parse_readings(csv_bytes(bad_day), s)
            check("跨 UTC 日批次被拒绝", False)
        except ValidationError:
            check("跨 UTC 日批次被拒绝", True)

        # NaN / Inf 非有限值 -> 拒绝
        for bad in ("NaN", "Infinity", "-Infinity"):
            try:
                parse_readings(csv_bytes([("W", ts(8), float(bad))]), s)
                check(f"非有限值 {bad} 被拒绝", False)
            except ValidationError:
                check(f"非有限值 {bad} 被拒绝", True)

        # 超行数 -> 拒绝
        big = Settings(data_dir=s.data_dir, max_batch_rows=10)
        rows = [("W", ts(8, i % 60), 1.0) for i in range(11)]
        try:
            parse_readings(csv_bytes(rows), big)
            check("超过每批 10000（测试覆盖为 10）行被拒绝", False)
        except ValidationError:
            check("超过每批 10000（测试覆盖为 10）行被拒绝", True)

        # 缺列 / 空机组 / 坏时间
        for label, payload in [
            ("缺列", b"turbine,ts_utc\nW,2026-11-13T08:00:00Z\n"),
            ("空机组", csv_bytes([("", ts(8), 1.0)])),
            ("坏时间", csv_bytes([("W", "not-a-time", 1.0)])),
            ("空文件", b""),
        ]:
            try:
                parse_readings(payload, s)
                check(f"{label} 被拒绝", False)
            except ValidationError:
                check(f"{label} 被拒绝", True)

        # 合法批次上传成功并落一个 Parquet
        ok_rows = [("WTG-01", ts(8, i), float(i)) for i in range(5)]
        r = await h.upload(ok_rows)
        check("合法批次上传返回 5 行", r["rows"] == 5 and r["day"] == DAY)
        on_disk = list(Path(s.lake_dir).rglob("*.parquet"))
        check("磁盘上恰好 1 个 Parquet", len(on_disk) == 1)
    finally:
        await h.stop()


async def test_duplicates_preserved() -> None:
    print("[2] 重复行原样保留")
    s = make_settings("dupes")
    h = Harness(s)
    await h.start()
    try:
        rows = [("WTG-01", ts(8, 0), 3.14)] * 4
        await h.upload(rows)
        r = await h.query_all()
        check("4 行完全重复数据查询仍返回 4 行", r["matched_rows"] == 4)
    finally:
        await h.stop()


async def test_snapshot_vs_compaction() -> None:
    print("[3] 读快照 vs 压实：不漏行 / 不重行 / 不中断")
    s = make_settings("snapshot")
    h = Harness(s)
    await h.start()
    try:
        # 三个批次件，共 30 行
        for i in range(3):
            rows = [
                ("WTG-01", ts(9, i * 10 + j), float(i * 100 + j))
                for j in range(10)
            ]
            await h.upload(rows)
        stats = await h.svc.stats()
        check("压实前 active 文件数为 3", stats["totals"]["active_files"] == 3)

        # 先创建一个长期持有的读快照（模拟慢查询横跨压实全过程）
        snap_id, files = await h.db.create_snapshot(None, DAY, DAY)
        check("快照固定 3 个文件", len(files) == 3)
        snap_paths = [f["path"] for f in files]

        # 压实
        job = await h.compact(DAY)
        check("压实发布成功", job["status"] == "published")
        stats = await h.svc.stats()
        check(
            "压实后 1 个 active + 3 个 superseded",
            stats["totals"]["active_files"] == 1
            and stats["totals"]["superseded_files"] == 3,
        )

        # 旧文件仍在磁盘（快照未释放）
        for rel in snap_paths:
            check(f"快照保护旧文件仍在磁盘: {rel[:24]}…",
                  os.path.exists(os.path.join(s.lake_dir, rel)))

        # 用旧快照路径直接读：仍是完整 30 行（不中断、不漏）
        rows_old, total_old, _ = await h.cm.run_io(
            ps.read_filtered, s.lake_dir, snap_paths, None, None, None, 50000
        )
        check("旧快照读取仍为 30 行（无漏行/中断）", total_old == 30)

        # 新查询走新 active 集合：也是 30 行（不重行）
        r = await h.query_all()
        check("压实后新查询返回 30 行（无重行）", r["matched_rows"] == 30,
              f"got {r['matched_rows']}")
        check("新查询快照只含 1 个文件", r["files_in_snapshot"] == 1)

        # 压实完成后再上传：新文件必须保留
        later = [("WTG-02", ts(10), 9.0)]
        await h.upload(later)
        r = await h.query_all()
        check("后来上传的文件保留，总行数 31", r["matched_rows"] == 31)
        stats = await h.svc.stats()
        check("active 文件变为 2（压实件 + 新批次件）",
              stats["totals"]["active_files"] == 2)

        # 释放旧快照后，后台回收器应清掉 3 个 superseded
        await h.db.release_snapshot(snap_id)
        for _ in range(30):
            stats = await h.svc.stats()
            if stats["totals"]["superseded_files"] == 0:
                break
            await asyncio.sleep(0.1)
        check("快照释放后旧文件被回收（superseded=0）",
              stats["totals"]["superseded_files"] == 0)
        check("回收状态登记为 deleted=3", stats["totals"]["deleted_files"] == 3)
        for rel in snap_paths:
            check(f"旧文件已从磁盘删除: {rel[:24]}…",
                  not os.path.exists(os.path.join(s.lake_dir, rel)))
        # 数据仍完整
        r = await h.query_all()
        check("回收后全部 31 行数据仍可查", r["matched_rows"] == 31)
    finally:
        await h.stop()


async def test_upload_during_compaction() -> None:
    print("[4] 压实期间上传：启动集合固定，新件保留且不并入")
    s = make_settings("during")
    h = Harness(s)
    await h.start()
    try:
        for i in range(2):
            await h.upload([("WTG-01", ts(8, i * 5 + j), float(j)) for j in range(5)])

        cid, _ = await h.svc.start_compaction(DAY)
        # 压实运行中立刻上传另一批
        await h.upload([("WTG-09", ts(20), 7.7) for _ in range(3)])
        for _ in range(200):
            row = await h.svc.compaction_status(cid)
            if row["status"] != "running":
                break
            await asyncio.sleep(0.05)
        row = await h.svc.compaction_status(cid)
        check("压实成功发布", row["status"] == "published")
        stats = await h.svc.stats()
        check("日分区 active 文件为 2（压实件 + 压实期间新件）",
              stats["totals"]["active_files"] == 2, str(stats["totals"]))
        r = await h.query_all()
        check("总行数 13 = 旧 10 + 新 3", r["matched_rows"] == 13)
        # 再来一次压实，应把两个 active 合成一个
        job2 = await h.compact(DAY)
        check("二次压实成功", job2["status"] == "published")
        r = await h.query_all()
        check("二次压实后仍为 13 行（无重无漏）", r["matched_rows"] == 13)
    finally:
        await h.stop()


async def test_single_compaction_lock() -> None:
    print("[5] 一次只压实一份日分区")
    s = make_settings("lock")
    h = Harness(s)
    await h.start()
    try:
        for i in range(2):
            await h.upload([("W", ts(8, j), float(j)) for j in range(3)])
        await h.svc.start_compaction(DAY)
        try:
            await h.svc.start_compaction(DAY)
            check("并发压实请求被拒绝", False)
        except CompactionError:
            check("并发压实请求被拒绝", True)
        # 等任务结束
        for _ in range(200):
            if not await h.db.has_running_compaction():
                break
            await asyncio.sleep(0.05)
        check("任务结束后锁释放", await h.db.has_running_compaction() is None)
        # 单文件分区无需压实：在新的一天只放一个批次件
        await h.upload([("W", ts(8, 0, NEXT_DAY), 1.0)])
        job = await h.compact(NEXT_DAY, allow_fail=True)
        check("单文件分区压实被拒绝",
              job["status"] == "failed" and "无需压实" in job["error"], str(job))
    finally:
        await h.stop()


async def test_gates() -> None:
    print("[6] 暂停合并 / 暂停查询开关")
    s = make_settings("gates")
    h = Harness(s)
    await h.start()
    try:
        for i in range(2):
            await h.upload([("W", ts(8, j), float(j)) for j in range(3)])

        # 暂停合并：启动请求 409
        await h.svc.set_gate("merge", True)
        try:
            await h.svc.start_compaction(DAY)
            check("合并暂停时启动压实被拒", False)
        except CompactionError:
            check("合并暂停时启动压实被拒", True)

        # 暂停查询：查询挂起等待，恢复后自动返回
        await h.svc.set_gate("query", True)
        qtask = asyncio.create_task(h.query_all())
        await asyncio.sleep(0.3)
        check("查询暂停期间请求挂起", not qtask.done())
        await h.svc.set_gate("query", False)
        result = await asyncio.wait_for(qtask, timeout=5)
        check("恢复查询后挂起请求自动完成并返回 6 行", result["matched_rows"] == 6)

        # 恢复合并后压实正常
        await h.svc.set_gate("merge", False)
        job = await h.compact(DAY)
        check("恢复合并后压实成功", job["status"] == "published")
    finally:
        await h.stop()


async def test_multi_day() -> None:
    print("[7] 多日分区隔离与按机组/时段过滤")
    s = make_settings("multiday")
    h = Harness(s)
    await h.start()
    try:
        await h.upload([("WTG-01", ts(8, 0, DAY), 1.0), ("WTG-02", ts(9, 0, DAY), 2.0)])
        # 当日第二个批次件，使 DAY 满足可压实条件（≥2 个文件）
        await h.upload([("WTG-02", ts(10, 0, DAY), 2.5)])
        await h.upload([("WTG-01", ts(8, 0, NEXT_DAY), 3.0)])
        r = await h.query_all(turbine="WTG-01")
        check("按机组过滤返回 2 行", r["matched_rows"] == 2)
        start = datetime.fromisoformat(DAY).replace(tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        r = await h.query_all(start=start, end=end)
        check("按时段过滤只返回当日 3 行", r["matched_rows"] == 3)
        r = await h.query_all()
        check("全量 4 行", r["matched_rows"] == 4)
        job = await h.compact(DAY)
        check("只压实指定日分区", job["status"] == "published" and job["day"] == DAY)
        stats = await h.svc.stats()
        check("两个日分区均在统计中", stats["totals"]["partitions"] == 2)
        r = await h.query_all()
        check("跨日数据完整 4 行", r["matched_rows"] == 4)
    finally:
        await h.stop()


async def test_crash_recovery() -> None:
    print("[8] 崩溃重启恢复")
    s = make_settings("crash")
    h = Harness(s)
    await h.start()
    try:
        # 已提交的两批数据
        await h.upload([("W", ts(8, j), float(j)) for j in range(4)])
        await h.upload([("W", ts(9, j), float(j)) for j in range(4)])

        # 模拟崩溃场景 A：一个 running 压实，临时件已写但未发布
        cid = await h.db.insert_compaction(DAY)
        ghost_id = ps.new_file_id("compact")
        import pyarrow as pa
        import pyarrow.parquet as pq
        merged = ps.merge_files(
            s.lake_dir,
            [f["path"] for f in (await h.db.list_active_files(None, DAY, DAY))],
        )
        rel_tmp = ps.write_temp(s.lake_dir, ghost_id, merged)
        await h.db.set_compaction_temp(cid, rel_tmp)

        # 模拟崩溃场景 B：完全未登记的孤儿 _tmp
        empty = pa.table({f.name: [] for f in ps.SCHEMA}, schema=ps.SCHEMA)
        orphan_id = ps.new_file_id("batch")
        ps.write_temp(s.lake_dir, orphan_id, empty)

        # 模拟崩溃场景 C：磁盘上有元数据不知道的孤儿 Parquet（在日分区目录）
        orphan_final = ps.final_path(DAY, ps.new_file_id("orphan"))
        full = os.path.join(s.lake_dir, orphan_final)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        pq.write_table(empty, full)
    finally:
        await h.stop()

    # 全新进程视角：重新实例化并执行 recover()
    h2 = Harness(s)
    ps.ensure_dirs(s.lake_dir)
    await h2.db.connect()
    await h2.gate.load(h2.db)
    report = await h2.cm.recover()
    await h2.cm.start_reaper()
    try:
        check("running 任务被标记失败", report["aborted_running"] == 1, str(report))
        check("未发布临时件被清理", report["temp_removed"] >= 2, str(report))
        check("磁盘孤儿 Parquet 被清理", report["orphans_removed"] == 1, str(report))
        # 已提交数据完整
        r = await h2.query_all()
        check("重启后已提交 8 行数据完整", r["matched_rows"] == 8)
        disk = ps.list_disk_paths(s.lake_dir)
        tmp_left = ps.list_temp_paths(s.lake_dir)
        check("重启后 _tmp 目录无残留", len(tmp_left) == 0, str(tmp_left))
        check("重启后磁盘只剩 2 个已发布批次件", len(disk) == 2, str(disk))
        failed = await h2.db.get_compaction(1)
        check("失败任务保留错误说明", failed["status"] == "failed" and bool(failed["error"]))

        # 恢复后压实可以正常续做
        job = await h2.compact(DAY)
        check("恢复后压实成功", job["status"] == "published")
        r = await h2.query_all()
        check("恢复压实后仍为 8 行", r["matched_rows"] == 8)
    finally:
        await h2.stop()


async def test_gate_persistence() -> None:
    print("[9] 开关状态跨重启持久化")
    s = make_settings("persist")
    h = Harness(s)
    await h.start()
    await h.svc.set_gate("merge", True)
    await h.svc.set_gate("query", True)
    await h.stop()

    h2 = Harness(s)
    await h2.db.connect()
    await h2.gate.load(h2.db)
    try:
        check("合并暂停状态重启保持", h2.gate.merge_paused is True)
        check("查询暂停状态重启保持", h2.gate.query_paused is True)
    finally:
        await h2.stop()


async def main() -> int:
    if TMP_ROOT.exists():
        shutil.rmtree(TMP_ROOT)
    await test_validation()
    await test_duplicates_preserved()
    await test_snapshot_vs_compaction()
    await test_upload_during_compaction()
    await test_single_compaction_lock()
    await test_gates()
    await test_multi_day()
    await test_crash_recovery()
    await test_gate_persistence()
    print("\n" + "=" * 56)
    print(f"通过 {PASSED} 项，失败 {len(FAILED)} 项")
    if FAILED:
        for f in FAILED:
            print("  FAIL:", f)
        return 1
    print("全部断言通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
