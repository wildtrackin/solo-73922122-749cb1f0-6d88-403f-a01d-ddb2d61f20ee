"""运行配置：全部可通过环境变量覆盖，便于容器部署。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    # 数据根目录：data/ 下放 lake/（Parquet 分区）与 meta.db（SQLite）
    data_dir: str = os.environ.get("VIBLAKE_DATA_DIR", "/data")
    # 每个 CSV 批次最多 10000 行
    max_batch_rows: int = _env_int("VIBLAKE_MAX_BATCH_ROWS", 10_000)
    # 振动幅值合法范围（题目要求“有限振动值”）
    vibration_min: float = _env_float("VIBLAKE_VIBRATION_MIN", -1_000.0)
    vibration_max: float = _env_float("VIBLAKE_VIBRATION_MAX", 1_000.0)
    # 查询最多返回的行数（超过后截断，但仍给出命中总数）
    query_limit_max: int = _env_int("VIBLAKE_QUERY_LIMIT_MAX", 50_000)
    demo_rows_default: int = _env_int("VIBLAKE_DEMO_ROWS", 200)
    # 测试钩子：压实发布前额外延时（毫秒），默认 0，生产不开启
    compact_delay_ms: int = _env_int("VIBLAKE_COMPACT_DELAY_MS", 0)

    @property
    def lake_dir(self) -> str:
        return os.path.join(self.data_dir, "lake")

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "meta.db")


settings = Settings()
