# honor_system/cup_honor_json_manager.py
from __future__ import annotations

import uuid
from typing import Dict, List, Optional

from pydantic import RootModel, Field

from utility.base_data_manager import AsyncJsonDataManager
from .cup_honor_models import CupHonorDefinition

DATA_NAME = "cup_honors"


class CupHonorStore(RootModel):
    root: Dict[str, CupHonorDefinition] = Field(default_factory=dict)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    def get(self, key, default=None):
        return self.root.get(key, default)

    def values(self):
        return self.root.values()

    def items(self):
        return self.root.items()

    def __contains__(self, item):
        return item in self.root

    def __delitem__(self, key):
        del self.root[key]

    def __setitem__(self, key, value):
        self.root[key] = value


class CupHonorJsonManager(AsyncJsonDataManager[CupHonorStore]):
    """
    一个单例类，用于管理存储在JSON文件中的杯赛荣誉定义。
    使用Pydantic模型进行数据验证。
    """
    DATA_FILENAME = DATA_NAME
    DATA_MODEL = CupHonorStore

    @property
    def _cup_honors(self) -> Dict[str, CupHonorDefinition]:
        """直接访问内部字典"""
        return self.data.root

    def get_all_cup_honors(self) -> List[CupHonorDefinition]:
        """获取所有杯赛荣誉定义的列表。"""
        return list(self._cup_honors.values())

    def get_cup_honor_by_uuid(self, honor_uuid: str | uuid.UUID) -> Optional[CupHonorDefinition]:
        """通过UUID获取单个杯赛荣誉定义。"""
        return self._cup_honors.get(str(honor_uuid))

    async def add_or_update_cup_honor(self, honor_def: CupHonorDefinition):
        """添加或更新一个杯赛荣誉定义，并立即保存。"""
        self._cup_honors[str(honor_def.uuid)] = honor_def
        await self.save_data()
        self.logger.info(f"已添加/更新杯赛荣誉 '{honor_def.name}' (UUID: {honor_def.uuid})。")

    async def delete_cup_honor(self, honor_uuid: str | uuid.UUID) -> bool:
        """通过UUID删除一个杯赛荣誉定义。"""
        uuid_str = str(honor_uuid)
        if uuid_str in self._cup_honors:
            del self._cup_honors[uuid_str]
            await self.save_data()
            self.logger.info(f"已删除杯赛荣誉 (UUID: {uuid_str})。")
            return True
        return False
