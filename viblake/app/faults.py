"""可控故障注入（仅用于演示，不影响正常路径）。

当前只提供“回收旧文件时磁盘 I/O 错误”一种故障：
- 故障只作用于**回收**阶段，绝不作用于合并、落盘、发布登记；
- 发布事务一旦提交，回收故障不会改变任务状态，也不会撤销新件；
- 故障状态持久化在 settings 表，重启后保持，便于演示“重启后继续回收”。
"""
from __future__ import annotations


class ReapFault(RuntimeError):
    """模拟删除旧文件时的磁盘错误。"""


class FaultInjector:
    def __init__(self) -> None:
        self._reap_error = False

    async def load(self, db) -> None:  # noqa: ANN001 - 避免与 db 模块循环导入
        self._reap_error = (await db.get_setting("fault_reap_error", "false")) == "true"

    @property
    def reap_error_enabled(self) -> bool:
        return self._reap_error

    async def set_reap_error(self, db, enabled: bool) -> None:  # noqa: ANN001
        self._reap_error = bool(enabled)
        await db.set_setting("fault_reap_error", "true" if self._reap_error else "false")

    def raise_if_reap_fault(self) -> None:
        """在真正删除旧件之前调用；启用演示故障时抛错。"""
        if self._reap_error:
            raise ReapFault(
                "演示故障：删除已替代的旧文件时磁盘 I/O 错误（模拟）；"
                "已提交的发布不受影响，旧件留待后续回收"
            )

    def snapshot(self) -> dict[str, bool]:
        return {"reap_error": self._reap_error}
