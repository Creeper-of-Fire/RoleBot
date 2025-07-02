# role_sync/role_sync_data_manager.py

import asyncio
import json
import os
from typing import Dict, List

DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "role_sync_log.json")


def create_rule_key(source_id: int, target_id: int) -> str:
    """为同步规则创建一个唯一的字符串键。"""
    return f"{source_id}-{target_id}"


class RoleSyncDataManager:
    """管理角色同步日志，记录哪些用户已经针对特定规则被同步过。"""

    def __init__(self):
        # 数据结构: { "guild_id": { "source_id-target_id": [user_id1, user_id2] } }
        self._data: Dict[str, Dict[str, List[int]]] = {}
        self._lock = asyncio.Lock()
        self._dirty = False
        self._save_task: asyncio.Task | None = None

        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        """从 JSON 文件加载数据。"""
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {}

    async def _delayed_save(self):
        """延迟保存，实现防抖。"""
        try:
            await asyncio.sleep(3)
            async with self._lock:
                if self._dirty:
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump(self._data, f, indent=4)
                    self._dirty = False
                    print("[RoleSyncDataManager] 同步日志已保存到文件。")
        except asyncio.CancelledError:
            pass
        finally:
            self._save_task = None

    async def save_data(self):
        """触发一个带防抖的保存操作。"""
        self._dirty = True
        if self._save_task:
            self._save_task.cancel()
        self._save_task = asyncio.create_task(self._delayed_save())

    async def mark_as_synced(self, guild_id: int, source_id: int, target_id: int, user_id: int):
        """将一个用户标记为已针对某个规则同步过。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        async with self._lock:
            guild_logs = self._data.setdefault(guild_id_str, {})
            synced_users = guild_logs.setdefault(rule_key, [])
            if user_id not in synced_users:
                synced_users.append(user_id)
                await self.save_data()

    def is_synced(self, guild_id: int, source_id: int, target_id: int, user_id: int) -> bool:
        """检查用户是否已经被标记为同步过。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        return user_id in self._data.get(guild_id_str, {}).get(rule_key, [])

    async def clear_rule_log(self, guild_id: int, source_id: int, target_id: int) -> bool:
        """清除指定规则的同步日志。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        async with self._lock:
            if guild_id_str in self._data and rule_key in self._data[guild_id_str]:
                del self._data[guild_id_str][rule_key]
                await self.save_data()
                return True
        return False

    async def clear_all_logs(self):
        """清除所有同步日志（删除文件）。"""
        async with self._lock:
            self._data.clear()
            if os.path.exists(DATA_FILE):
                os.remove(DATA_FILE)
                return True
        return False