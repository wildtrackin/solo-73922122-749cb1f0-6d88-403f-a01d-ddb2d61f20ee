"""CSV 校验：整批要么全通过、要么整体拒绝（不产生任何文件）。"""
from __future__ import annotations

import csv
import io
import math
from datetime import datetime, timezone

from .config import Settings
from .parquet_store import Reading

REQUIRED_COLUMNS = ("turbine", "ts_utc", "vibration")


class ValidationError(ValueError):
    """带中文提示的输入校验错误，路由层映射为 HTTP 422。"""


def _parse_ts(raw: str, row_no: int) -> datetime:
    raw = raw.strip()
    # 同时支持 2026-11-13T08:30:00Z / 带偏移量 / 无时区（按 UTC 解释）
    try:
        iso = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise ValidationError(f"第 {row_no} 行时间格式非法: {raw!r}（需 ISO 8601 / UTC）") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_vibration(raw: str, row_no: int, settings: Settings) -> float:
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ValidationError(f"第 {row_no} 行振动值不是数字: {raw!r}") from exc
    if not math.isfinite(value):
        raise ValidationError(f"第 {row_no} 行振动值必须为有限值，收到 {raw!r}")
    if not (settings.vibration_min <= value <= settings.vibration_max):
        raise ValidationError(
            f"第 {row_no} 行振动值 {value} 超出允许范围 "
            f"[{settings.vibration_min}, {settings.vibration_max}]"
        )
    return value


def parse_readings(data: bytes, settings: Settings) -> list[Reading]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须为 UTF-8 编码") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValidationError("CSV 为空，至少需要表头和一行数据") from exc
    header = [h.strip() for h in header]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValidationError(f"缺少必需列: {', '.join(missing)}；实际表头: {header}")
    idx = {name: header.index(name) for name in REQUIRED_COLUMNS}

    readings: list[Reading] = []
    day: str | None = None
    for row_no, row in enumerate(reader, start=2):
        if not row or all((cell.strip() == "" for cell in row)):
            continue  # 容忍文件末尾空行
        if len(row) < len(header):
            raise ValidationError(f"第 {row_no} 行列数不足（表头 {len(header)} 列，实际 {len(row)} 列）")
        turbine = row[idx["turbine"]].strip()
        if not turbine:
            raise ValidationError(f"第 {row_no} 行机组号不能为空")
        ts = _parse_ts(row[idx["ts_utc"]], row_no)
        vibration = _parse_vibration(row[idx["vibration"]], row_no, settings)

        row_day = ts.date().isoformat()
        if day is None:
            day = row_day
        elif row_day != day:
            raise ValidationError(
                f"第 {row_no} 行 UTC 日期 {row_day} 与首批日期 {day} 不一致；"
                "每批必须限定在同一个 UTC 日内，请按日拆分文件"
            )
        readings.append(Reading(turbine=turbine, ts_utc=ts, vibration=vibration))

    if not readings:
        raise ValidationError("CSV 没有任何数据行")
    if len(readings) > settings.max_batch_rows:
        raise ValidationError(
            f"批次共 {len(readings)} 行，超过每批最多 {settings.max_batch_rows} 行的限制"
        )
    return readings
