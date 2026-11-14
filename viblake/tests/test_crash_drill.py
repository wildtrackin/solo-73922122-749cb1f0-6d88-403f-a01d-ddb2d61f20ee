"""硬崩溃恢复演练：子进程调用 os._exit(0) 模拟硬退出。

覆盖两个窗口：
1. 发布前崩溃：临时件已写、发布未提交 → 重启后任务 failed、_tmp 清理、数据完整；
2. 发布后回收前崩溃：published 已提交、旧件尚未回收 → 重启后任务仍 published，
   不得改报 failed，新件保留并被查询快照引用，旧件留待回收。

os._exit 不执行 finally / atexit / 缓冲区刷新，等价于进程被 SIGKILL 的
磁盘持久化效果（SQLite WAL + 已落盘 Parquet 即为全部可恢复状态）。
沙箱禁止向子进程发信号，故用退出标记文件 + 看门狗线程结束服务实例；
每次运行使用独立数据目录与空闲端口。
"""
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

# 每次运行使用独立数据目录：避免历史上未能终止的沙箱进程访问同一分区
DATA = Path(f"/tmp/viblake-crash-drill-{os.getpid()}")
if DATA.exists():
    shutil.rmtree(DATA)

PY = os.environ.get("VIBLAKE_PYTHON") or sys.executable
ROOT = str(Path(__file__).resolve().parent.parent)
EXIT_MARKER = DATA / "_exit_marker"

# 沙箱内可能无法向子进程发信号，因此每个服务实例内置一个“看门狗”线程：
# 检测到退出标记文件后自行 os._exit(0)，便于测试确定地结束旧进程。
WATCHDOG = r"""
import os, time
marker = os.environ.get("VIBLAKE_EXIT_MARKER")
while True:
    if marker and os.path.exists(marker):
        os._exit(0)
    time.sleep(0.05)
"""


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
    """通过退出标记让子进程自行退出；沙箱可能禁止发信号。"""
    if proc.poll() is not None:
        return
    try:
        EXIT_MARKER.write_text(str(proc.pid))
        proc.wait(timeout=5)
    except (PermissionError, subprocess.TimeoutExpired):
        pass
    finally:
        try:
            EXIT_MARKER.unlink()
        except FileNotFoundError:
            pass


CRASHER = r"""
import os, time
from app.config import settings
# 轮询 _tmp，等“压实临时件”落盘后立即硬退出（发布前崩溃窗口）。
# 只认 compact-* 前缀，避免与上传批次短暂的 _tmp 文件混淆。
tmp = os.path.join(settings.lake_dir, "_tmp")
deadline = time.time() + 12
while time.time() < deadline:
    if os.path.isdir(tmp):
        compact = [f for f in os.listdir(tmp)
                   if f.startswith("compact-") and f.endswith(".parquet")]
        if compact:
            os._exit(0)
    time.sleep(0.02)
raise SystemExit("crash window missed")
"""

# 发布后、回收前崩溃：轮询到 published 任务立即硬退出
POST_PUBLISH_CRASHER = r"""
import os, sqlite3, time
from app.config import settings
deadline = time.time() + 12
while time.time() < deadline:
    try:
        conn = sqlite3.connect(settings.db_path)
        row = conn.execute(
            "SELECT status, reap_status, reap_pending FROM compactions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row and row[0] == "published" and row[1] == "pending" and int(row[2] or 0) > 0:
            os._exit(0)
    except Exception:
        pass
    time.sleep(0.01)
raise SystemExit("post-publish crash window missed")
"""


def run_server(env: dict, port: int, crasher: str | None = None) -> subprocess.Popen:
    base = (
        "import threading, uvicorn\n"
        f"threading.Thread(target=lambda: exec({WATCHDOG!r}), daemon=True).start()\n"
    )
    if crasher:
        base += f"threading.Thread(target=lambda: exec({crasher!r}), daemon=True).start()\n"
    code = base + f"uvicorn.run('app.main:app', host='127.0.0.1', port={port})\n"
    return subprocess.Popen([PY, "-u", "-c", code], env=env, cwd=ROOT)


# ---------- 实例 1：制造崩溃 ----------
port1 = free_port()
base1 = f"http://127.0.0.1:{port1}"
env1 = dict(
    os.environ,
    VIBLAKE_DATA_DIR=str(DATA),
    VIBLAKE_COMPACT_DELAY_MS="1500",
    VIBLAKE_EXIT_MARKER=str(EXIT_MARKER),
)
proc = run_server(env1, port1, crasher=CRASHER)
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
env2 = dict(
    os.environ, VIBLAKE_DATA_DIR=str(DATA), VIBLAKE_EXIT_MARKER=str(EXIT_MARKER)
)
env2.pop("VIBLAKE_COMPACT_DELAY_MS", None)
proc2 = run_server(env2, port2, crasher=None)
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
proc3 = run_server(env2, port3, crasher=None)
try:
    health = wait_health(base3)
    r = httpx.get(f"{base3}/api/readings/query?limit=100", timeout=10).json()
    assert r["matched_rows"] == 12 and r["files_in_snapshot"] == 1
    assert all(v == 0 for v in health["recovery"].values())
    print("[8] 二次重启：无遗留问题，12 行数据与单一压实件保持稳定", flush=True)
finally:
    stop(proc3)

# ---------- 实例 4：发布已提交、回收前硬崩溃 ----------
# 关键性质：重启后任务仍是 published，不能被恢复流程改报 failed；
# 已发布合并件保留且被查询快照引用；未回收旧件留待回收，关闭故障后清掉。
port4 = free_port()
base4 = f"http://127.0.0.1:{port4}"
proc4 = run_server(env2, port4, crasher=POST_PUBLISH_CRASHER)
try:
    wait_health(base4)
    # 让当日重新具备 2 个 active 文件
    httpx.post(f"{base4}/api/demo/generate?rows=5&day=2026-11-13", timeout=10).raise_for_status()
    assert httpx.post(f"{base4}/api/demo/faults/reap?enabled=true", timeout=10).json()["reap_error"]
    r = httpx.post(f"{base4}/api/compactions?day=2026-11-13", timeout=10)
    assert r.status_code == 202, r.text
    rc = proc4.wait(timeout=20)
    assert rc == 0, f"post-publish crasher exit={rc}"
    print("[9] 子进程在发布提交后、旧件回收前 os._exit 硬退出", flush=True)
finally:
    stop(proc4)

# ---------- 实例 5：重启，已发布任务不得改报失败 ----------
port5 = free_port()
base5 = f"http://127.0.0.1:{port5}"
proc5 = run_server(env2, port5, crasher=None)
try:
    health = wait_health(base5)
    print("[10] 重启恢复报告:", health["recovery"], flush=True)
    assert health["recovery"]["aborted_running"] == 0, health

    jobs = httpx.get(f"{base5}/api/compactions", timeout=10).json()
    assert jobs[0]["status"] == "published", jobs[0]
    assert not jobs[0]["error"], jobs[0]
    assert jobs[0]["reap_status"] == "pending" and jobs[0]["reap_pending"] > 0, jobs[0]
    print("[11] 崩溃前已发布的任务重启后仍为 published（回收未决）", flush=True)

    r = httpx.get(f"{base5}/api/readings/query?limit=100", timeout=10).json()
    assert r["matched_rows"] == 17 and r["files_in_snapshot"] == 1, r
    print("[12] 已发布合并件被快照引用，17 行数据完整", flush=True)

    # 关闭故障：后台回收器在无需重启的情况下续完回收
    assert httpx.post(f"{base5}/api/demo/faults/reap?enabled=false", timeout=10).status_code == 200
    for _ in range(60):
        job = httpx.get(
            f"{base5}/api/compactions/{jobs[0]['id']}", timeout=10
        ).json()
        if job["reap_status"] == "clean":
            break
        time.sleep(0.1)
    assert job["status"] == "published" and job["reap_status"] == "clean", job
    print("[13] 关闭故障后旧件回收完成，任务仍 published/clean", flush=True)
finally:
    stop(proc5)

print("\n硬崩溃恢复演练全部通过 ✅", flush=True)