# jukebox/role_jukebox_manager.py
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import config

# --- 常量定义 ---
DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "jukebox_data.json")
UTC8 = timezone(timedelta(hours=8))


class RoleJukeboxManager:
    """
    管理“身份组点歌机”系统的所有数据和核心逻辑。
    - 数据按服务器(guild)隔离。
    - 负责状态的读取、更新和持久化。
    - 不直接执行Discord API调用，而是返回需要执行的操作给上层Cog。
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._dirty = False
        self._save_task: Optional[asyncio.Task] = None
        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        """
        从JSON文件加载数据。如果文件不存在或为空，则初始化一个默认结构。
        "queues": {
            "role_id": {
              "current_preset": Optional[Dict],
              "unlock_timestamp": Optional[str], // ISO 8601, null表示可变更
              "pending_requests": [{"user_id": int, "preset": Dict}]
            }
        }
        """
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {"guilds": {}, "users": {}}

    async def save_data(self, force: bool = False):
        """
        异步保存数据到JSON文件，支持防抖以减少磁盘I/O。
        :param force: 如果为True，则立即安排保存，取消任何正在等待的保存任务。
        """
        self._dirty = True
        if force or self._save_task is None:
            if self._save_task:
                self._save_task.cancel()
            self._save_task = asyncio.create_task(self._delayed_save())

    async def _delayed_save(self):
        """延迟1秒保存，以合并短时间内的多次写入请求。"""
        try:
            await asyncio.sleep(1)
            async with self._lock:
                if self._dirty:
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump(self._data, f, indent=4, ensure_ascii=False)
                    self._dirty = False
        except asyncio.CancelledError:
            pass
        finally:
            self._save_task = None

    # --- 内部数据访问辅助方法 ---

    def _get_or_create_guild_data(self, guild_id: int) -> Dict[str, Any]:
        """获取指定服务器的数据，如果不存在则创建并返回默认结构。"""
        guild_id_str = str(guild_id)
        if guild_id_str not in self._data["guilds"]:
            self._data["guilds"][guild_id_str] = {
                "general_presets": [],
                "queues": {}
            }
        return self._data["guilds"][guild_id_str]

    def _get_or_create_user_data(self, user_id: int) -> Dict[str, Any]:
        """获取指定用户的数据，如果不存在则创建并返回默认结构。"""
        user_id_str = str(user_id)
        if user_id_str not in self._data["users"]:
            self._data["users"][user_id_str] = {
                "custom_presets": []
            }
        return self._data["users"][user_id_str]

    def _get_or_create_queue_state(self, guild_id: int, role_id: int) -> Dict[str, Any]:
        """获取指定队列身份组的状态，如果不存在则创建并返回默认的空闲状态。"""
        guild_data = self._get_or_create_guild_data(guild_id)
        role_id_str = str(role_id)
        if role_id_str not in guild_data["queues"]:
            guild_data["queues"][role_id_str] = {
                "current_preset": None,
                "unlock_timestamp": None,
                "pending_requests": []
            }
        return guild_data["queues"][role_id_str]

    # --- 公共API方法 ---

    def get_guild_state(self, guild_id: int) -> Dict[str, Any]:
        """获取指定服务器的完整点歌机状态。"""
        return self._get_or_create_guild_data(guild_id)

    def get_user_presets(self, user_id: int) -> List[Dict[str, Any]]:
        """获取VIP用户的个人预设列表。"""
        user_data = self._get_or_create_user_data(user_id)
        return user_data["custom_presets"]

    async def add_general_preset(self, guild_id: int, name: str, color: str, icon_url: Optional[str]) -> Tuple[bool, str]:
        """管理员为服务器添加一个通用预设。"""
        async with self._lock:
            guild_data = self._get_or_create_guild_data(guild_id)
            # 检查预设数量限制
            max_presets = config.JUKEBOX_GUILD_CONFIGS[guild_id].get("max_general_presets", 10)
            if len(guild_data["general_presets"]) >= max_presets:
                return False, f"通用预设数量已达上限 ({max_presets}个)。"
            # 检查名称是否重复
            if any(p["name"] == name for p in guild_data["general_presets"]):
                return False, f"名为 '{name}' 的预设已存在。"

            guild_data["general_presets"].append({"name": name, "color": color, "icon": icon_url})
            await self.save_data()
        return True, f"成功添加通用预设 '{name}'。"

    async def remove_general_preset(self, guild_id: int, name: str) -> bool:
        """管理员从服务器移除一个通用预设。"""
        async with self._lock:
            guild_data = self._get_or_create_guild_data(guild_id)
            preset_found = False
            initial_len = len(guild_data["general_presets"])
            guild_data["general_presets"] = [p for p in guild_data["general_presets"] if p["name"] != name]

            if len(guild_data["general_presets"]) < initial_len:
                preset_found = True
                await self.save_data()
        return preset_found

    async def add_user_preset(self, user_id: int, guild_id: int, name: str, color: str, icon_url: Optional[str]) -> Tuple[bool, str]:
        """VIP用户为自己添加一个专属预设。"""
        async with self._lock:
            user_data = self._get_or_create_user_data(user_id)
            guild_config = config.JUKEBOX_GUILD_CONFIGS.get(guild_id, {})
            if not guild_config:
                # 理论上不应该发生，因为Cog会做检查，但作为安全措施
                return False, "服务器未配置点歌机功能。"

            max_presets = guild_config.get("max_vip_presets_per_user", 3)

            if len(user_data["custom_presets"]) >= max_presets:
                return False, f"您的专属预设数量已达上限 ({max_presets}个)。"
            if any(p["name"] == name for p in user_data["custom_presets"]):
                return False, f"您已有一个名为 '{name}' 的预设。"

            user_data["custom_presets"].append({"name": name, "color": color, "icon": icon_url})
            await self.save_data()
        return True, f"成功添加您的专属预设 '{name}'。"

    async def remove_user_preset(self, user_id: int, name: str) -> bool:
        """VIP用户移除自己的一个专属预设。"""
        async with self._lock:
            user_data = self._get_or_create_user_data(user_id)
            preset_found = False
            initial_len = len(user_data["custom_presets"])
            user_data["custom_presets"] = [p for p in user_data["custom_presets"] if p["name"] != name]

            if len(user_data["custom_presets"]) < initial_len:
                preset_found = True
                await self.save_data()
        return preset_found

    async def change_or_claim_queue(self, guild_id: int, user_id: int, role_id: int, preset: Dict[str, Any]) -> Tuple[bool, str]:
        """
        用户尝试变更一个可用的队列，或点播一个尚未初始化的队列。
        """
        async with self._lock:
            queue_state = self._get_or_create_queue_state(guild_id, role_id)
            now = datetime.now(UTC8)

            # 检查是否锁定
            if queue_state["unlock_timestamp"]:
                unlock_time = datetime.fromisoformat(queue_state["unlock_timestamp"])
                if now < unlock_time:
                    return False, "这个队列的变更权仍在锁定中，您可以选择排队。"

            guild_config = config.JUKEBOX_GUILD_CONFIGS.get(guild_id, {})
            lock_hours = guild_config.get("lock_duration_hours", 2)
            unlock_time = now + timedelta(hours=lock_hours)

            queue_state["current_preset"] = preset
            queue_state["unlock_timestamp"] = unlock_time.isoformat()

            await self.save_data()
        return True, "变更成功！身份组外观已更新。"

    async def queue_request(self, guild_id: int, user_id: int, role_id: int, preset: Dict[str, Any]) -> Tuple[bool, str]:
        """
        用户在一个变更权被锁定的队列中“排队”下一个身份组。
        """
        async with self._lock:
            queue_state = self._get_or_create_queue_state(guild_id, role_id)

            # 检查是否已在排队
            if any(req["user_id"] == user_id for req in queue_state["pending_requests"]):
                return False, "您已经在这个队列中排队了。"

            queue_state["pending_requests"].append({"user_id": user_id, "preset": preset})
            await self.save_data()

            position = len(queue_state["pending_requests"])
        return True, f"排队成功！您是第 {position} 位。"

    async def process_expirations(self) -> List[Dict[str, Any]]:
        """
        检查所有到期的队列，并返回轮换操作。
        如果队列为空，仅将unlock_timestamp设为null。
        """
        actions_to_take = []
        now = datetime.now(UTC8)
        data_changed = False

        async with self._lock:
            for guild_id_str, guild_data in self._data["guilds"].items():
                for role_id_str, queue_state in guild_data["queues"].items():
                    if queue_state.get("unlock_timestamp"):
                        unlock_time = datetime.fromisoformat(queue_state["unlock_timestamp"])
                        if now >= unlock_time:
                            data_changed = True

                            if queue_state["pending_requests"]:
                                # 轮换到下一个用户
                                next_request = queue_state["pending_requests"].pop(0)
                                guild_config = config.JUKEBOX_GUILD_CONFIGS.get(int(guild_id_str), {})
                                lock_hours = guild_config.get("lock_duration_hours", 2)
                                new_unlock_time = now + timedelta(hours=lock_hours)

                                queue_state["current_preset"] = next_request["preset"]
                                queue_state["unlock_timestamp"] = new_unlock_time.isoformat()

                                actions_to_take.append({
                                    "type": "ROTATE",
                                    "guild_id": int(guild_id_str),
                                    "role_id": int(role_id_str),
                                    "new_preset": next_request["preset"],
                                    "requester_id": next_request["user_id"],
                                })
                            else:
                                # 仅解锁，不重置
                                queue_state["unlock_timestamp"] = None

            if data_changed:
                await self.save_data(force=True)

        return actions_to_take