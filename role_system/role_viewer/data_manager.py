# role_viewer/data_manager.py

import asyncio
import json
import os
from typing import Dict, List

DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "role_viewer_separator_roles.json")


class SeparatorDataManager:
    """管理分隔符身份组的数据存储。"""

    def __init__(self):
        # 数据结构: { "guild_id": [role_id1, role_id2, ...] }
        self._data: Dict[str, List[int]] = {}
        self._lock = asyncio.Lock()

        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        """从 JSON 文件加载数据。"""
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {}

    async def save_data(self):
        """保存数据到 JSON 文件。"""
        async with self._lock:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=4)

    def get_separators(self, guild_id: int) -> List[int]:
        """获取指定服务器的分隔符 ID 列表。"""
        return self._data.get(str(guild_id), [])

    async def add_separator(self, guild_id: int, role_id: int) -> bool:
        """添加一个分隔符。如果已存在返回 False，成功返回 True。"""
        guild_id_str = str(guild_id)
        current_list = self._data.get(guild_id_str, [])

        if role_id in current_list:
            return False

        current_list.append(role_id)
        self._data[guild_id_str] = current_list
        await self.save_data()
        return True

    async def remove_separator(self, guild_id: int, role_id: int) -> bool:
        """移除一个分隔符。如果不存在返回 False，成功返回 True。"""
        guild_id_str = str(guild_id)
        current_list = self._data.get(guild_id_str, [])

        if role_id not in current_list:
            return False

        current_list.remove(role_id)
        self._data[guild_id_str] = current_list
        await self.save_data()
        return True

    async def clear_separators(self, guild_id: int):
        """清空指定服务器的所有分隔符。"""
        guild_id_str = str(guild_id)
        if guild_id_str in self._data:
            self._data[guild_id_str] = []
            await self.save_data()