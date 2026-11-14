"""FastAPI 入口：路由、生命周期与静态页面。"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from . import parquet_store as ps
from .compaction import CompactionError, CompactionManager
from .config import settings
from .csv_import import ValidationError
from .db import Database
from .gates import PauseGate
from .service import DataLakeService

logger = logging.getLogger("viblake")

WEB_DIR = Path(__file__).parent / "web"

# 单进程内共享的组件（题目明确要求单进程网页）
db = Database(settings.db_path)
gate = PauseGate()
compaction = CompactionManager(db, settings.lake_dir, gate)
service = DataLakeService(db, settings, gate, compaction)


def _parse_dt(raw: str | None, field: str) -> datetime | None:
    if raw is None or raw.strip() == "":
        return None
    try:
        iso = raw.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field} 时间格式非法: {raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    os.makedirs(settings.data_dir, exist_ok=True)
    ps.ensure_dirs(settings.lake_dir)
    await db.connect()
    await gate.load(db)
    recovery = await compaction.recover()
    if any(recovery.values()):
        logger.info("启动恢复结果: %s", recovery)
    await compaction.start_reaper()
    app.state.recovery = recovery
    try:
        yield
    finally:
        await service.close()
        await compaction.close()
        await db.close()


app = FastAPI(
    title="风电场振动读数数据湖",
    description="小文件合并 / 日分区 Parquet / 读快照 / 安全压实演示",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "recovery": app.state.recovery}


# ---------------- 上传 ----------------

@app.post("/api/readings")
async def upload_readings(file: UploadFile = File(..., description="含 turbine,ts_utc,vibration 三列的 CSV")):
    data = await file.read()
    if not data:
        raise HTTPException(status_code=422, detail="上传内容为空")
    try:
        result = await service.upload_csv(data, file.filename or "unnamed.csv")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return result


# ---------------- 查询 ----------------

@app.get("/api/readings/query")
async def query_readings(
    turbine: str | None = Query(None, description="机组号，可空"),
    start: str | None = Query(None, description="UTC 起时间（含），ISO 8601"),
    end: str | None = Query(None, description="UTC 止时间（不含），ISO 8601"),
    limit: int = Query(1000, ge=1, le=settings.query_limit_max),
):
    start_dt = _parse_dt(start, "start")
    end_dt = _parse_dt(end, "end")
    if start_dt and end_dt and end_dt <= start_dt:
        raise HTTPException(status_code=422, detail="end 必须晚于 start")
    return await service.query(turbine, start_dt, end_dt, limit)


# ---------------- 统计与压实 ----------------

@app.get("/api/stats")
async def stats():
    return await service.stats()


@app.post("/api/compactions", status_code=202)
async def start_compaction(day: str = Query(..., description="要压实的 UTC 日，格式 YYYY-MM-DD")):
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"日期格式非法: {day!r}") from exc
    try:
        _id, payload = await service.start_compaction(day)
    except CompactionError as exc:
        # 暂停 / 已有任务属于当前状态不允许，返回 409
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(status_code=202, content=payload)


@app.get("/api/compactions/{compaction_id}")
async def compaction_status(compaction_id: int):
    row = await service.compaction_status(compaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="压实任务不存在")
    return row


@app.get("/api/compactions")
async def compaction_history():
    return await service.compaction_history()


# ---------------- 演示开关 ----------------

@app.post("/api/demo/gates/{kind}")
async def set_gate(
    kind: str,
    paused: bool = Query(..., description="true=暂停，false=恢复"),
):
    if kind not in ("merge", "query"):
        raise HTTPException(status_code=422, detail="kind 仅支持 merge 或 query")
    return await service.set_gate(kind, paused)


# ---------------- 虚构读数 ----------------

@app.post("/api/demo/generate")
async def generate_demo(
    rows: int = Query(settings.demo_rows_default, ge=1, le=settings.max_batch_rows),
    day: str | None = Query(None, description="UTC 日 YYYY-MM-DD，默认今天"),
    turbines: str = Query("WTG-01,WTG-02,WTG-03", description="逗号分隔的机组号"),
):
    turbine_list = [t.strip() for t in turbines.split(",") if t.strip()]
    if not turbine_list:
        raise HTTPException(status_code=422, detail="至少需要一个机组号")
    try:
        csv_bytes = await service.generate_demo_csv(rows, day, turbine_list)
        result = await service.upload_csv(csv_bytes, "demo-generated.csv")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    result["source"] = "demo-generated"
    return result
