from __future__ import annotations

import json

from pydantic import RootModel, Field, ConfigDict

from utility.base_data_manager import AsyncJsonDataManager

DATA_NAME = "cup_honor_notified"


class NotifiedUUIDStore(RootModel):
    root: set[str] = Field(default_factory=set)

    # 重写序列化方法：将set转为list
    def model_dump_json(self, **kwargs):
        return json.dumps(list(self.root), **kwargs)

    # 重写反序列化方法：将list转为set
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
    """
    一个单例类，用于管理已发送通知的杯赛荣誉状态，并将其持久化到JSON文件中。
    """
    DATA_FILENAME = DATA_NAME
    DATA_MODEL = NotifiedUUIDStore

    @property
    def _notified_uuids(self):
        """获取已通知的UUID列表。"""
        return self.data.root

    async def add_notified(self, honor_uuid: str):
        """将一个UUID标记为已通知，并立即保存。"""
        if honor_uuid not in self._notified_uuids:
            self._notified_uuids.add(honor_uuid)
            await self.save_data()
            self.logger.info(f"已将荣誉 {honor_uuid} 标记为已通知并持久化。")

    async def remove_notified(self, honor_uuid: str) -> bool:
        """
        从已通知列表中移除一个UUID，并立即保存。
        如果UUID存在并被成功移除，返回True。否则返回False。
        """
        if honor_uuid in self._notified_uuids:
            self._notified_uuids.remove(honor_uuid)
            await self.save_data()
            self.logger.info(f"已从已通知列表中移除荣誉 {honor_uuid}。")
            return True
        return False

    def has_been_notified(self, honor_uuid: str) -> bool:
        """检查一个UUID是否已被通知。"""
        return honor_uuid in self._notified_uuids
