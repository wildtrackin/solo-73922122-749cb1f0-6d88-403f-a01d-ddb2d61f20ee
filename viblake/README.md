# 🌬️ 风电场振动读数数据湖（单进程演示）

把大量振动读数 CSV 小文件合并为按 **UTC 日分区的 Parquet**，减少故障回溯时的文件扫描。
单进程 FastAPI 应用，PyArrow 负责 Parquet 读写，SQLite（WAL 模式）只存元数据。

## 功能与约束对照

| 需求 | 实现方式 |
| --- | --- |
| 上传含机组号、UTC 时间、有限振动值的 CSV | `POST /api/readings`，整批校验：必需三列、ISO 8601 时间、有限数值、非空机组 |
| 每批 ≤ 10000 行且限同一 UTC 日 | `app/csv_import.py` 整体校验，任一规则不满足整批拒绝（不产生任何文件） |
| 整批追加为一个 Parquet，保留重复行 | 一批 = `lake/day=YYYY-MM-DD/<id>.parquet` 一个文件；PyArrow 拼接不去重 |
| 浏览器按机组和时段查读数、查看文件数 | 单页面 `GET /`；`GET /api/readings/query`、`GET /api/stats` |
| 启动压实，一次只压实一份日分区 | `POST /api/compactions?day=...`，进程内锁 + DB running 任务双重互斥，并发请求返回 409 |
| 合并启动时该日文件为一个 | 任务启动在同一事务内固定当时的 active 文件集合 |
| 压实期间仍能上传/查询，后来上传的文件须保留 | Parquet IO 在线程池、DB 仅短事务；新上传文件不并入本次任务，发布后仍为独立 active 文件 |
| 查询固定开始时的文件集合，切换不漏行、不重行、不中断 | 查询开始即创建 `snapshots/snapshot_files` 读快照，全程读快照内文件；压实只把旧件置 `superseded` |
| 旧文件无人读取后才回收 | 旧件被快照引用期间保留；引用归零后由 2 秒周期的后台回收器删除并登记 `deleted` |
| **发布后回收失败不得改报压实失败、不得撤销新件** | 发布（新件 active + 旧件 superseded + 任务 published）在单个事务内提交；回收是提交后的独立阶段，失败只记录 `reap_status=pending` 与 `reap_error`，任务结果不变（DB 层 `fail_compaction` 带 `status='running'` 守卫） |
| **未回收旧件留待后续回收或重启清理** | 回收循环每 2 秒重试；重启 `recover()` 续完回收；仍被查询快照引用的旧件继续保留 |
| **发布前失败仍报告 failed、原读数可查** | 合并 / 落盘 / 登记失败 → `failed` + 清理临时件与未登记合并件；active 读数保持可查，未发布件不暴露 |
| 读写 Parquet 不持数据库写锁 | 所有 PyArrow 调用均在 DB 事务之外的线程池中执行，DB 写事务只更新几行元数据 |
| 失败/切换中退出，重启保留完整已提交数据、清掉未发布新件、续完旧件回收 | 启动 `recover()`：running 任务置 failed、`_tmp/` 全清、磁盘孤儿清理、立即执行一轮回收 |
| 暂停合并及查询的演示开关 | `POST /api/demo/gates/merge\|query?paused=true\|false`，状态持久化于 SQLite，重启保持 |
| **可控的发布后回收报错演示** | `POST /api/demo/faults/reap?enabled=true\|false`（页面按钮），只在删除旧件时注入磁盘错误 |
| 虚构读数 | `POST /api/demo/generate`（页面按钮），确定性随机种子，可指定行数/日期/机组 |
| 可实际启动的 Docker Compose | `docker compose up --build`，数据存于命名卷 `viblake-data` |

## 目录结构

```
viblake/
├── app/
│   ├── main.py            # FastAPI 路由、生命周期（启动恢复）
│   ├── service.py         # 上传/查询/统计/后台压实调度/虚构数据
│   ├── compaction.py      # 压实编排、发布 / 回收分段、回收器、崩溃恢复
│   ├── db.py              # SQLite 元数据（双连接 + WAL + 串行写事务 + 读快照）
│   ├── parquet_store.py   # PyArrow 读写（纯文件 IO，不碰 DB）
│   ├── csv_import.py      # CSV 整批校验
│   ├── gates.py           # 暂停合并 / 暂停查询开关
│   ├── faults.py          # 演示故障注入（仅回收阶段）
│   ├── config.py          # 环境变量配置
│   └── web/index.html     # 单页面（无外部前端依赖）
├── tests/                 # 82 项断言的集成测试 + HTTP 冒烟测试 + 硬崩溃演练
├── examples/              # 示例 CSV（含重复行示例）
├── Dockerfile
└── docker-compose.yml
```

## 快速开始（Docker Compose）

```bash
cd viblake
docker compose up --build
# 浏览器打开 http://localhost:8000
```

页面操作顺序建议：② 生成并上传虚构读数（同一点 2~3 次制造小文件）→ ① 查看文件数 →
⑤ 对该 UTC 日启动压实 → 观察 active 文件降为 1、旧件经 superseded 后被回收。

### 演示“发布已提交，但回收遇到磁盘错误”

1. 页面 ③b 点击「启用回收报错」（或 `curl -X POST "localhost:8000/api/demo/faults/reap?enabled=true"`）；
2. 对一个有 ≥2 个 active 文件的日分区启动压实；
3. 观察任务表：
   - **任务结果** = `published`（新合并件已发布、查询立即可见，数据行数不变）；
   - **发布后回收** = `未决：N 个旧件`，错误信息里是模拟的磁盘 I/O 错误；
   - 任务**不会**变成 `failed`，新件**不会**被撤销；
4. 点击「关闭回收报错」，后台回收器约 2 秒内清掉旧件，回收状态变为「旧件已回收」；
   若在故障仍开启时重启容器，重启恢复同样不会把已发布任务改报失败，恢复后继续等待回收。

对照实验（发布前失败）：把「暂停合并」打开后启动压实，任务在发布安全检查点失败，
状态为 `failed`、原 active 读数仍可查、不产生任何未发布合并件。

## 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 数据默认在 ./data（可用 VIBLAKE_DATA_DIR 覆盖）
```

上传示例：

```bash
curl -F "file=@examples/readings_sample.csv" http://localhost:8000/api/readings
curl "http://localhost:8000/api/readings/query?turbine=WTG-01&limit=100"
curl -X POST "http://localhost:8000/api/compactions?day=2026-11-13"
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/readings` | 上传 CSV（multipart 字段 `file`），422 = 整批校验失败 |
| GET | `/api/readings/query` | 参数 `turbine,start,end,limit`；返回快照 id、快照文件数、命中/返回行数 |
| GET | `/api/stats` | 各日分区 active/superseded/deleted 文件数与行数、开关状态 |
| POST | `/api/compactions?day=YYYY-MM-DD` | 后台压实，202 立即返回；重复请求 409 |
| GET | `/api/compactions[/{id}]` | 任务历史 / 单任务状态（running/published/failed） |
| POST | `/api/demo/gates/merge?paused=true\|false` | 暂停/恢复合并 |
| POST | `/api/demo/gates/query?paused=true\|false` | 暂停/恢复查询（查询请求服务端等待，不报错不丢失） |
| POST | `/api/demo/faults/reap?enabled=true\|false` | 发布后回收报错演示（只影响旧件删除，任务状态保持 published） |
| POST | `/api/demo/generate?rows=&day=&turbines=` | 生成并直接上传虚构读数 |
| GET | `/health` | 健康检查，含本次启动恢复报告 |

## 正确性设计要点

### 读快照（不漏行 / 不重行 / 不中断）

1. 查询开始时在一个 `BEGIN IMMEDIATE` 事务内：读取当前全部符合条件的 active 文件，
   并写入 `snapshots` + `snapshot_files`，文件集合就此固定；
2. 查询全程只读取快照内的 Parquet 文件。压实发布是把新合并件登记为 active、
   旧批次件改为 **superseded（不删除）**，因此进行中的查询读到的文件内容不变；
3. 查询结束释放快照引用；引用归零后旧件才进入可回收集合，后台回收器删除磁盘文件。

### 压实与并发

- 压实任务先在 DB 占位 `running` 行（双重互斥：asyncio.Lock + DB 状态），
  再由后台任务执行；HTTP 请求立即返回 202，不阻塞上传与查询；
- 合并结果先写 `lake/_tmp/`，`os.replace` 原子改名进入日分区，
  然后在**单个 DB 事务**内完成「新件 active + 旧件 superseded + 任务 published」；
  事务提交后任务即为 `published`，旧件 id 记入 `compaction_files` 便于追踪回收；
- 合并行数与元数据总行数校验不一致则拒绝发布；
- 暂停合并开关在「任务启动前」和「临时件发布前」两个安全检查点生效，
  暂停时任务回滚（删临时件、置 failed），不留半成品。

### 发布后回收失败（本仓库修复的核心场景）

`compact_day` 明确分成两段：

| 阶段 | 失败时 |
| --- | --- |
| 发布段（合并 / 写 `_tmp` / 改名 / 登记并提交） | 任务 `failed`，清理未发布件；原 active 文件与查询快照不受影响 |
| 回收段（删除 superseded 旧件） | 任务保持 `published`；记录 `reap_status=pending` / `reap_pending` / `reap_error`；旧件保留 |

约束由数据库层强制：`fail_compaction` 的 UPDATE 带 `WHERE status='running'`，
因此即使回收路径异常、重启恢复或人工误调用，也无法把已发布任务改报失败。
回收段的错误按“旧件 → 来源任务”（`compaction_files`）归属，不会把 A 任务的
回收错误写到 B 任务上。未回收旧件由每 2 秒一次的回收循环与重启 `recover()` 续做；
只要仍被查询快照引用，就继续保留。

### 崩溃恢复（`CompactionManager.recover()`）

启动时依次执行：

1. 所有 `running` 压实任务 → 删除其登记的临时件、置 `failed`（已提交数据从不触碰）；
   已 `published` 的任务不在此列，绝不被改报失败；
2. 清空 `_tmp/` 中所有未发布文件；
3. 扫描磁盘，删除元数据中不存在的孤儿 Parquet；
4. 立即执行一轮旧件回收（演示故障仍开启时记录为“回收未决”，不阻断启动），
   并启动周期回收器继续「续完旧件回收」。

### 为什么 Parquet IO 不持 DB 写锁

`parquet_store.py` 不导入数据库模块；`service.py` / `compaction.py` 通过
`ThreadPoolExecutor` 执行全部 PyArrow 调用，DB 写事务（`db.tx()`）只包裹
几条元数据语句，持锁时间为毫秒级，上传与压实互不阻塞。

## 测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
python tests/test_integration.py   # 82 项断言：校验/重复行/快照/并发/开关/崩溃恢复/回收报错/发布失败
python tests/test_http_smoke.py    # 完整 HTTP 流程（含回收报错演示、暂停查询挂起/恢复）
python tests/test_crash_drill.py   # 真实多进程硬退出（os._exit）：发布前崩溃 + 发布后回收前崩溃
```

`test_crash_drill.py` 默认用当前解释器（可用 `VIBLAKE_PYTHON` 指定路径），
每次运行使用独立数据目录，子进程通过退出标记文件可控结束。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `VIBLAKE_DATA_DIR` | `/data` | 数据根目录（`lake/` + `meta.db`） |
| `VIBLAKE_MAX_BATCH_ROWS` | `10000` | 单批最大行数 |
| `VIBLAKE_VIBRATION_MIN` / `_MAX` | `-1000` / `1000` | 振动值合法范围 |
| `VIBLAKE_QUERY_LIMIT_MAX` | `50000` | 单次查询返回上限 |
| `VIBLAKE_COMPACT_DELAY_MS` | `0` | 测试钩子：压实发布前额外延时（毫秒），仅崩溃演练使用，生产保持 0 |
