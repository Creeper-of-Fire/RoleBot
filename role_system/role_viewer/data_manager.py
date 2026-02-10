# role_viewer/data_manager.py

from typing import Dict, List

from pydantic import BaseModel, Field

from utility.base_data_manager import AsyncJsonDataManager

DATA_NAME = "role_viewer_separator_roles"


# --- Models ---

class SeparatorData(BaseModel):
    # Key是公会ID，Value是RoleID列表
    # 就像一个普通的 dict
    guilds: Dict[str, List[int]] = Field(default_factory=dict)

    def get_role_ids(self, guild_id: int) -> List[int]:
        """像字典一样获取，如果没有就返回空列表。"""
        return self.guilds.get(str(guild_id), [])

    def add_role(self, guild_id: int, role_id: int) -> bool:
        """数据内部处理初始化和去重逻辑。"""
        key = str(guild_id)

        # 像字典一样自然地处理初始化
        if key not in self.guilds:
            self.guilds[key] = []

        current_list = self.guilds[key]
        if role_id in current_list:
            return False

        current_list.append(role_id)
        return True

    def remove_role(self, guild_id: int, role_id: int) -> bool:
        """数据内部处理删除逻辑。"""
        key = str(guild_id)

        # 像字典一样，没有这个key就直接跳过
        if key not in self.guilds:
            return False

        current_list = self.guilds[key]
        if role_id not in current_list:
            return False

        current_list.remove(role_id)
        # 可以在这里决定是否删除空key，也可以不删，随你心情，保持简单就不删
        return True

    def clear_guild(self, guild_id: int):
        """清除指定公会数据。"""
        self.guilds.pop(str(guild_id), None)


class SeparatorDataManager(AsyncJsonDataManager[SeparatorData]):
    """管理分隔符身份组的数据存储。"""
    DATA_FILENAME = DATA_NAME
    DATA_MODEL = SeparatorData

    def get_separators(self, guild_id: int) -> List[int]:
        return self.data.get_role_ids(guild_id)

    async def add_separator(self, guild_id: int, role_id: int) -> bool:
        # 逻辑都在 Model 里，Manager 只要负责存盘
        if self.data.add_role(guild_id, role_id):
            await self.save_data()
            return True
        return False

    async def remove_separator(self, guild_id: int, role_id: int) -> bool:
        if self.data.remove_role(guild_id, role_id):
            await self.save_data()
            return True
        return False

    async def clear_separators(self, guild_id: int):
        self.data.clear_guild(guild_id)
        await self.save_data()
