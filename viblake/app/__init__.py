"""风电场振动读数数据湖（单进程演示版）。

模块划分：
- config:   运行配置（数据目录、批次上限等）
- db:       SQLite 元数据库（文件清单、读快照、压实任务、演示开关）
- parquet_store: Parquet 读写（全部在 DB 写事务之外执行）
- compaction:    压实编排、回收与重启恢复
- service:  上传 / 查询 / 统计业务逻辑与 CSV 校验
- main:     FastAPI 路由与页面
"""
