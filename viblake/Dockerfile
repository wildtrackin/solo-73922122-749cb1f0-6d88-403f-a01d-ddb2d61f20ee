FROM python:3.11-slim

# 容器内无外部网络依赖；pyarrow 使用 manylinux 轮子
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 数据目录由 compose 挂载命名卷；SQLite 与 Parquet 均在其中
ENV VIBLAKE_DATA_DIR=/data \
    PYTHONUNBUFFERED=1
EXPOSE 8000

# 单进程 uvicorn（题目要求单进程；后台压实 / 回收在同一事件循环内运行）
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
