"""Parquet 文件层。

关键约束：本模块所有函数只做磁盘 / PyArrow 操作，不打开数据库事务，
因此“读写 Parquet 不持数据库写锁”。调用方先在 DB 短事务之外完成文件 IO，
再用 DB 短事务登记元数据。

文件布局：
  lake/day=YYYY-MM-DD/<id>.parquet        已发布的日分区文件（批次件 / 压实件）
  lake/_tmp/<id>.parquet                  尚未发布的临时件（崩溃后重启清理）
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# 与 CSV 字段对应：机组号、UTC 时间、振动值（均为非空有限值）
SCHEMA = pa.schema(
    [
        pa.field("turbine", pa.string(), nullable=False),
        pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("vibration", pa.float64(), nullable=False),
    ]
)


@dataclass(frozen=True)
class Reading:
    turbine: str
    ts_utc: datetime
    vibration: float


def new_file_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _day_dir(lake_dir: str, day: str) -> str:
    return os.path.join(lake_dir, f"day={day}")


def final_path(day: str, file_id: str) -> str:
    return os.path.join(f"day={day}", f"{file_id}.parquet")


def temp_path(file_id: str) -> str:
    return os.path.join("_tmp", f"{file_id}.parquet")


def _abs(lake_dir: str, rel_path: str) -> str:
    return os.path.join(lake_dir, rel_path)


def ensure_dirs(lake_dir: str) -> None:
    os.makedirs(os.path.join(lake_dir, "_tmp"), exist_ok=True)


def write_batch(lake_dir: str, day: str, file_id: str, readings: Sequence[Reading]) -> str:
    """把一整批读数写成一个 Parquet 文件，返回相对路径（先落 _tmp，再原子改名）。"""
    ensure_dirs(lake_dir)
    table = pa.table(
        {
            "turbine": pa.array([r.turbine for r in readings], type=pa.string()),
            "ts_utc": pa.array(
                [r.ts_utc.astimezone(timezone.utc) for r in readings],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "vibration": pa.array([r.vibration for r in readings], type=pa.float64()),
        },
        schema=SCHEMA,
    )
    rel_tmp = temp_path(file_id)
    rel_final = final_path(day, file_id)
    os.makedirs(_day_dir(lake_dir, day), exist_ok=True)
    pq.write_table(table, _abs(lake_dir, rel_tmp), compression="zstd")
    os.replace(_abs(lake_dir, rel_tmp), _abs(lake_dir, rel_final))
    return rel_final


def write_temp(lake_dir: str, file_id: str, table: pa.Table) -> str:
    """压实合并结果先写 _tmp（发布前崩溃不影响已提交数据）。"""
    ensure_dirs(lake_dir)
    rel_tmp = temp_path(file_id)
    pq.write_table(table, _abs(lake_dir, rel_tmp), compression="zstd")
    return rel_tmp


def publish_temp(lake_dir: str, day: str, file_id: str, rel_tmp: str) -> str:
    """临时件原子改名进入日分区。"""
    rel_final = final_path(day, file_id)
    os.makedirs(_day_dir(lake_dir, day), exist_ok=True)
    os.replace(_abs(lake_dir, rel_tmp), _abs(lake_dir, rel_final))
    return rel_final


def read_filtered(
    lake_dir: str,
    rel_paths: Iterable[str],
    turbine: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> tuple[list[dict], int, int]:
    """读取给定文件集合（由读快照固定），按机组 / 时段下推过滤。

    重复行原样保留（pyarrow 不做去重）。返回 (行, 命中总数, 实际扫描文件数)。
    """
    frames: list[pa.Table] = []
    scanned = 0
    for rel in rel_paths:
        table = pq.read_table(_abs(lake_dir, rel), schema=SCHEMA)
        scanned += 1
        mask = None
        if turbine is not None:
            mask = pc.equal(table["turbine"], turbine)
        if start is not None:
            m = pc.greater_equal(table["ts_utc"], pa.scalar(start, type=table["ts_utc"].type))
            mask = m if mask is None else pc.and_(mask, m)
        if end is not None:
            m = pc.less(table["ts_utc"], pa.scalar(end, type=table["ts_utc"].type))
            mask = m if mask is None else pc.and_(mask, m)
        if mask is not None:
            table = table.filter(mask)
        if table.num_rows:
            frames.append(table)

    if not frames:
        return [], 0, scanned
    combined = pa.concat_tables(frames, promote_options="default")
    total = combined.num_rows
    if total > limit:
        combined = combined.slice(0, limit)
    rows = [
        {
            "turbine": t,
            "ts_utc": ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "vibration": v,
        }
        for t, ts, v in zip(
            combined.column("turbine").to_pylist(),
            combined.column("ts_utc").to_pylist(),
            combined.column("vibration").to_pylist(),
        )
    ]
    return rows, total, scanned


def merge_files(lake_dir: str, rel_paths: Sequence[str]) -> pa.Table:
    """按文件名顺序拼接多个 Parquet，保留全部重复行。"""
    tables = [pq.read_table(_abs(lake_dir, rel), schema=SCHEMA) for rel in rel_paths]
    if not tables:
        return pa.table({f.name: [] for f in SCHEMA}, schema=SCHEMA)
    return pa.concat_tables(tables, promote_options="default")


def delete_file(lake_dir: str, rel_path: str) -> None:
    try:
        os.remove(_abs(lake_dir, rel_path))
    except FileNotFoundError:
        # 恢复场景下可能已被回收，幂等处理
        pass


def list_disk_paths(lake_dir: str) -> set[str]:
    found: set[str] = set()
    for root, _dirs, files in os.walk(lake_dir):
        for name in files:
            if not name.endswith(".parquet"):
                continue
            full = os.path.join(root, name)
            found.add(os.path.relpath(full, lake_dir))
    return found


def list_temp_paths(lake_dir: str) -> list[str]:
    tmp_dir = os.path.join(lake_dir, "_tmp")
    if not os.path.isdir(tmp_dir):
        return []
    return [
        os.path.join("_tmp", name)
        for name in os.listdir(tmp_dir)
        if name.endswith(".parquet")
    ]


def table_turbines(table: pa.Table) -> list[str]:
    return sorted(set(table.column("turbine").to_pylist()))
