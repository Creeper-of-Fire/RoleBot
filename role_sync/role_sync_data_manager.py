# role_sync/role_sync_data_manager.py

import asyncio
import json
import os
from typing import Dict, List

DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "role_sync_log.json")


class RoleSyncDataManager:
    """管理角色同步日志，记录哪些用户已经针对特定规则被同步过。"""

    def __init__(self):
        self._data: Dict[str, Dict[str, List[int]]] = {}
        self._lock = asyncio.Lock()
        self._dirty = False  # 标记数据是否被修改
        self._save_task: asyncio.Task | None = None  # 后台保存任务

        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        """从 JSON 文件加载数据。"""
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {}  # 格式: { "guild_id": { "source_role_id": [user_id1, user_id2] } }

    async def _delayed_save(self):
        """延迟保存，实现防抖（Debouncing）。"""
        try:
            # 在最后一次调用 save_data 后等待 3 秒再执行保存
            await asyncio.sleep(3)
            async with self._lock:
                if self._dirty:
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump(self._data, f, indent=4)
                    self._dirty = False
                    print("[RoleSyncDataManager] 同步日志已保存到文件。")  # 可以在日志中看到
        except asyncio.CancelledError:
            # 如果在等待期间有新的保存请求，旧任务会被取消
            pass
        finally:
            self._save_task = None

    async def save_data(self):
        """
        触发一个带防抖的保存操作。
        连续的调用会在最后一次调用后延迟执行一次实际的保存。
        """
        self._dirty = True
        # 如果已有保存任务，先取消它
        if self._save_task:
            self._save_task.cancel()

        # 创建新的延迟保存任务
        self._save_task = asyncio.create_task(self._delayed_save())

    async def mark_as_synced(self, guild_id: int, source_role_id: int, user_id: int):
        """将一个用户标记为已针对某个规则同步过。"""
        guild_id_str = str(guild_id)
        source_role_id_str = str(source_role_id)

        async with self._lock:
            guild_logs = self._data.setdefault(guild_id_str, {})
            synced_users = guild_logs.setdefault(source_role_id_str, [])
            if user_id not in synced_users:
                synced_users.append(user_id)
                await self.save_data()

    def is_synced(self, guild_id: int, source_role_id: int, user_id: int) -> bool:
        """检查用户是否已经被标记为同步过。"""
        guild_id_str = str(guild_id)
        source_role_id_str = str(source_role_id)

        return user_id in self._data.get(guild_id_str, {}).get(source_role_id_str, [])