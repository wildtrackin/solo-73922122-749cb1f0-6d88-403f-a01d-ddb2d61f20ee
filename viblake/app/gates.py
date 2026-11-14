"""演示开关：暂停合并 / 暂停查询。

- 状态持久化在 SQLite settings 表，重启后保持；
- 查询暂停时，查询接口在进入快照流程前等待，恢复后继续，不报错也不丢请求；
- 合并暂停时，进行中的压实任务走到安全检查点（写完合并件之前）主动失败回滚，
  已发布的文件不受影响；重新开始即可再次压实。
"""
from __future__ import annotations

import asyncio


class PauseGate:
    def __init__(self) -> None:
        # Event 置位 = 放行；clear = 暂停
        self._merge_open = asyncio.Event()
        self._query_open = asyncio.Event()
        self._merge_open.set()
        self._query_open.set()

    async def load(self, db) -> None:  # noqa: ANN001 - 避免与 db 模块循环导入
        merge_paused = (await db.get_setting("pause_merge", "false")) == "true"
        query_paused = (await db.get_setting("pause_query", "false")) == "true"
        if merge_paused:
            self._merge_open.clear()
        if query_paused:
            self._query_open.clear()

    # ---- 合并开关 ----
    @property
    def merge_paused(self) -> bool:
        return not self._merge_open.is_set()

    def set_merge_paused(self, paused: bool) -> None:
        if paused:
            self._merge_open.clear()
        else:
            self._merge_open.set()

    def raise_if_merge_paused(self) -> None:
        """压实流程在安全检查点调用；暂停则抛错，由编排层按失败回滚。"""
        if self.merge_paused:
            raise RuntimeError("演示开关已暂停合并，任务在安全检查点中止（未发布任何新文件）")

    # ---- 查询开关 ----
    @property
    def query_paused(self) -> bool:
        return not self._query_open.is_set()

    def set_query_paused(self, paused: bool) -> None:
        if paused:
            self._query_open.clear()
        else:
            self._query_open.set()

    async def wait_query_open(self) -> None:
        await self._query_open.wait()

    def snapshot(self) -> dict[str, bool]:
        return {"merge_paused": self.merge_paused, "query_paused": self.query_paused}
