"""HTTP 层冒烟测试：通过 ASGI TestClient 走完整 API 流程。"""
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TMP = ROOT / "tests" / "tmp_http"
if TMP.exists():
    shutil.rmtree(TMP)
TMP.mkdir(parents=True)
os.environ["VIBLAKE_DATA_DIR"] = str(TMP)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

with TestClient(app) as client:
    r = client.get("/health")
    assert r.status_code == 200, r.text
    print("health:", r.json())

    # 页面
    r = client.get("/")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    print("index html: ok,", len(r.content), "bytes")

    # 虚构读数 ×3 个批次（制造可压实的小文件）
    for day in ("2026-11-12", "2026-11-13"):
        for _ in range(2):
            r = client.post(f"/api/demo/generate?rows=50&day={day}")
            assert r.status_code == 200, r.text
            print("demo upload:", r.json()["file_id"], r.json()["day"], r.json()["rows"], "rows")

    # 手动上传含重复行的 CSV
    csv = (
        "turbine,ts_utc,vibration\n"
        "WTG-07,2026-11-13T08:00:00Z,1.5\n"
        "WTG-07,2026-11-13T08:00:00Z,1.5\n"
        "WTG-07,2026-11-13T08:01:00Z,NaN\n"
    ).encode()
    r = client.post("/api/readings", files={"file": ("dup.csv", csv, "text/csv")})
    assert r.status_code == 422, r.text
    print("rejected NaN batch:", r.json()["detail"][:40], "…")

    csv_ok = csv.replace(b"NaN", b"2.5")
    r = client.post("/api/readings", files={"file": ("dup.csv", csv_ok, "text/csv")})
    assert r.status_code == 200, r.text
    print("dup batch uploaded:", r.json()["rows"], "rows")

    # 跨日批次被拒
    bad = b"turbine,ts_utc,vibration\nW,2026-11-13T01:00:00Z,1\nW,2026-11-14T01:00:00Z,1\n"
    r = client.post("/api/readings", files={"file": ("bad.csv", bad, "text/csv")})
    assert r.status_code == 422
    print("rejected cross-day batch: ok")

    # 查询
    r = client.get("/api/readings/query?turbine=WTG-07")
    body = r.json()
    assert body["matched_rows"] == 3, body
    print("query WTG-07:", body["matched_rows"], "rows, snapshot", body["snapshot_id"])

    # 暂停查询 -> 请求挂起；用线程验证等待行为
    r = client.post("/api/demo/gates/query?paused=true")
    assert r.json()["query_paused"] is True
    import threading
    import time

    holder: dict = {}

    def slow_query():
        holder["start"] = time.monotonic()
        holder["resp"] = client.get("/api/readings/query").json()
        holder["elapsed"] = time.monotonic() - holder["start"]

    t = threading.Thread(target=slow_query)
    t.start()
    time.sleep(0.5)
    assert t.is_alive(), "查询暂停时请求应当挂起"
    client.post("/api/demo/gates/query?paused=false")
    t.join(timeout=10)
    assert not t.is_alive() and holder["resp"]["matched_rows"] >= 103
    print(f"paused query resumed after {holder['elapsed']:.2f}s, rows={holder['resp']['matched_rows']}")

    # 统计
    stats = client.get("/api/stats").json()
    print("stats totals:", stats["totals"])

    # 暂停合并 -> 409
    client.post("/api/demo/gates/merge?paused=true")
    r = client.post("/api/compactions?day=2026-11-13")
    assert r.status_code == 409, r.text
    print("compaction while paused -> 409:", r.json()["detail"][:32], "…")
    client.post("/api/demo/gates/merge?paused=false")

    # 压实 2026-11-13
    r = client.post("/api/compactions?day=2026-11-13")
    assert r.status_code == 202, r.text
    cid = r.json()["compaction_id"]
    print("compaction started:", r.json())
    for _ in range(100):
        job = client.get(f"/api/compactions/{cid}").json()
        if job["status"] != "running":
            break
        time.sleep(0.1)
    print("compaction finished:", job["status"], "| reaped info in stats below")
    assert job["status"] == "published", job

    # 同时只能有一个压实
    r1 = client.post("/api/compactions?day=2026-11-12")
    assert r1.status_code == 202
    r2 = client.post("/api/compactions?day=2026-11-12")
    assert r2.status_code == 409, r2.text
    print("second concurrent compaction -> 409: ok")
    cid2 = r1.json()["compaction_id"]
    for _ in range(100):
        job2 = client.get(f"/api/compactions/{cid2}").json()
        if job2["status"] != "running":
            break
        time.sleep(0.1)
    assert job2["status"] == "published", job2

    # 数据完整性
    all_rows = client.get("/api/readings/query?limit=50000").json()
    assert all_rows["matched_rows"] == 4 * 50 + 3, all_rows["matched_rows"]
    print("total rows after both compactions:", all_rows["matched_rows"], "(expected 203)")

    stats = client.get("/api/stats").json()
    print("final stats:", stats["totals"])
    assert stats["totals"]["active_files"] == 2  # 每日 1 个压实件
    assert stats["totals"]["partitions"] == 2

print("\nHTTP 冒烟测试全部通过 ✅")
