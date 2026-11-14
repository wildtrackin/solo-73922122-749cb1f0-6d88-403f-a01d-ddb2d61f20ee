"""硬崩溃恢复演练：子进程在压实发布前调用 os._exit(0) 模拟硬退出。

os._exit 不执行 finally / atexit / 缓冲区刷新，等价于进程被 SIGKILL 的
磁盘持久化效果（SQLite WAL + 已落盘 Parquet 即为全部可恢复状态）。
沙箱禁止外部信号，故用该方式演练；每个服务实例使用独立空闲端口。
"""
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import httpx

DATA = Path("/tmp/viblake-crash-drill")
if DATA.exists():
    shutil.rmtree(DATA)

PY = "/workspace/.venv/bin/python"
ROOT = "/workspace/viblake"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(base: str, timeout: float = 15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/health", timeout=1)
            if r.status_code == 200:
                return r.json()
        except httpx.HTTPError:
            time.sleep(0.2)
    raise RuntimeError("server not healthy in time")


def stop(proc: subprocess.Popen) -> None:
    """沙箱可能禁止发信号；尽力终止，忽略权限错误，不无限等待。"""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except (PermissionError, subprocess.TimeoutExpired):
        pass


CRASHER = r"""
import os, time
from app.config import settings
# 轮询 _tmp，一旦压实临时件落盘，立即硬退出（发布前崩溃窗口）
tmp = os.path.join(settings.lake_dir, "_tmp")
deadline = time.time() + 12
while time.time() < deadline:
    if os.path.isdir(tmp) and [f for f in os.listdir(tmp) if f.endswith(".parquet")]:
        os._exit(0)
    time.sleep(0.02)
raise SystemExit("crash window missed")
"""


def run_server(env: dict, port: int, crasher: bool) -> subprocess.Popen:
    if crasher:
        code = (
            "import threading, uvicorn\n"
            f"threading.Thread(target=lambda: exec({CRASHER!r}), daemon=True).start()\n"
            f"uvicorn.run('app.main:app', host='127.0.0.1', port={port})\n"
        )
        return subprocess.Popen([PY, "-u", "-c", code], env=env, cwd=ROOT)
    return subprocess.Popen(
        [PY, "-u", "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        env=env, cwd=ROOT,
    )


# ---------- 实例 1：制造崩溃 ----------
port1 = free_port()
base1 = f"http://127.0.0.1:{port1}"
env1 = dict(os.environ, VIBLAKE_DATA_DIR=str(DATA), VIBLAKE_COMPACT_DELAY_MS="1500")
proc = run_server(env1, port1, crasher=True)
try:
    wait_health(base1)
    for _ in range(2):
        httpx.post(f"{base1}/api/demo/generate?rows=6&day=2026-11-13", timeout=10).raise_for_status()
    r = httpx.post(f"{base1}/api/compactions?day=2026-11-13", timeout=10)
    assert r.status_code == 202, r.text
    rc = proc.wait(timeout=20)
    assert rc == 0, f"crasher exit={rc}"
    print(f"[1] 子进程 {proc.pid} 在发布前 os._exit 硬退出", flush=True)
finally:
    stop(proc)

tmps = list((DATA / "lake" / "_tmp").glob("*.parquet"))
batches = list((DATA / "lake" / "day=2026-11-13").glob("batch-*.parquet"))
print(f"[2] 崩溃现场：_tmp 残留 {len(tmps)} 个，已发布批次件 {len(batches)} 个（应为 2）", flush=True)
assert len(tmps) == 1 and len(batches) == 2

# ---------- 实例 2：重启恢复 ----------
port2 = free_port()
base2 = f"http://127.0.0.1:{port2}"
env2 = dict(os.environ, VIBLAKE_DATA_DIR=str(DATA))
env2.pop("VIBLAKE_COMPACT_DELAY_MS", None)
proc2 = run_server(env2, port2, crasher=False)
try:
    health = wait_health(base2)
    print("[3] 重启恢复报告:", health["recovery"], flush=True)
    assert health["recovery"]["aborted_running"] == 1
    assert health["recovery"]["temp_removed"] >= 1

    r = httpx.get(f"{base2}/api/readings/query?limit=100", timeout=10).json()
    assert r["matched_rows"] == 12, r
    print(f"[4] 已提交数据完整：{r['matched_rows']} 行，快照内 {r['files_in_snapshot']} 个文件", flush=True)

    jobs = httpx.get(f"{base2}/api/compactions", timeout=10).json()
    assert jobs[0]["status"] == "failed" and jobs[0]["error"]
    print("[5] 未完成任务已标记 failed:", jobs[0]["error"], flush=True)

    assert not list((DATA / "lake" / "_tmp").glob("*.parquet"))
    print("[6] _tmp 目录已清空，未发布新件已删除", flush=True)

    r = httpx.post(f"{base2}/api/compactions?day=2026-11-13", timeout=10)
    cid2 = r.json()["compaction_id"]
    for _ in range(100):
        job = httpx.get(f"{base2}/api/compactions/{cid2}", timeout=10).json()
        if job["status"] != "running":
            break
        time.sleep(0.1)
    assert job["status"] == "published", job
    r = httpx.get(f"{base2}/api/readings/query?limit=100", timeout=10).json()
    assert r["matched_rows"] == 12 and r["files_in_snapshot"] == 1
    print("[7] 恢复后压实成功，仍为 12 行，合并为 1 个文件", flush=True)
finally:
    stop(proc2)

# ---------- 实例 3：再重启，published 状态稳定 ----------
port3 = free_port()
base3 = f"http://127.0.0.1:{port3}"
proc3 = run_server(env2, port3, crasher=False)
try:
    health = wait_health(base3)
    r = httpx.get(f"{base3}/api/readings/query?limit=100", timeout=10).json()
    assert r["matched_rows"] == 12 and r["files_in_snapshot"] == 1
    assert all(v == 0 for v in health["recovery"].values())
    print("[8] 二次重启：无遗留问题，12 行数据与单一压实件保持稳定", flush=True)
finally:
    stop(proc3)

print("\n硬崩溃恢复演练全部通过 ✅", flush=True)
