"""Honor 系统的"已通知"状态管理（防重复通知，**通用机制**）。

**说明**（2026-08-31 用户拍板迁移）：
- 原本叫 `cup_honor_module_notification_state_data_manager.py`，放在 `cup_honor/` 目录下——
  当时只服务于杯赛 honor 的过期通知防重复。
- 现在 `HonorExpirationCog`（独立 cog）**合并处理** 普通 honor + cup_honor 的过期通知，
  也用这个 manager 防重复通知。
- **功能是 honor 系统通用机制**，但 `DATA_NAME = "cup_honor_notified"` 保留（兼容历史 json 文件名）。
- Python 模块路径 / 类名改为中性（`honor_notification_state_data_manager`）。

**后续**：若部署环境无历史 json 数据，可考虑改 `DATA_NAME` 为 `"honor_notified"` 避免目录命名谎言——
**但**这会导致历史已通知 UUID 状态丢失（重发一次性提醒，无副作用）。
"""
from __future__ import annotations

import json

from pydantic import RootModel, Field

from shared.data.json_manager import AsyncJsonDataManager

# 保留 "cup_honor_notified" json 文件名以兼容历史部署数据。
DATA_NAME = "cup_honor_notified"


class NotifiedUUIDStore(RootModel):
    """已通知 UUID 集合——防重复通知的真相源。"""

    root: set[str] = Field(default_factory=set)

    # set ↔ list 序列化/反序列化
    def model_dump_json(self, **kwargs):
        return json.dumps(list(self.root), **kwargs)

    @classmethod
    def model_validate_json(cls, json_data: str, **kwargs):
        data = json.loads(json_data)
        return cls(root=set(data))

    def __contains__(self, item):
        return item in self.root

    def add(self, item):
        self.root.add(item)

    def remove(self, item):
        self.root.remove(item)


class NotificationStateManager(AsyncJsonDataManager[NotifiedUUIDStore]):
    """Honor 系统"已通知"状态管理器——**通用**，不限于 cup_honor。

    单例（继承 AsyncJsonDataManager 的 `_instances` 全局字典）。
    所有 honor（普通 + cup_honor）的"已发过过期通知"标记都进同一个 store。
    """

    DATA_FILENAME = DATA_NAME
    DATA_MODEL = NotifiedUUIDStore

    @property
    def _notified_uuids(self) -> set[str]:
        return self.data.root

    async def add_notified(self, honor_uuid: str) -> None:
        """将一个 honor UUID 标记为已通知，并立即保存。"""
        if honor_uuid not in self._notified_uuids:
            self._notified_uuids.add(honor_uuid)
            await self.save_data()
            self.logger.info(f"已将 honor {honor_uuid} 标记为已通知并持久化。")

    async def remove_notified(self, honor_uuid: str) -> bool:
        """从已通知列表移除一个 honor UUID，并立即保存。成功移除返回 True。"""
        if honor_uuid in self._notified_uuids:
            self._notified_uuids.remove(honor_uuid)
            await self.save_data()
            self.logger.info(f"已从已通知列表移除 honor {honor_uuid}。")
            return True
        return False

    def has_been_notified(self, honor_uuid: str) -> bool:
        return honor_uuid in self._notified_uuids
