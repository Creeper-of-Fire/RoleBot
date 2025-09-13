# jukebox/role_jukebox_manager.py
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import config
from role_jukebox.models import JukeboxData, QueueState, UserData, GuildData, Preset, PendingRequest, RotationAction

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
        self._data: JukeboxData = JukeboxData()
        self._lock = asyncio.Lock()
        self._dirty = False
        self._save_task: Optional[asyncio.Task] = None
        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        """从JSON文件加载数据，并将其反序列化为dataclass对象。"""
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                self._data = JukeboxData.from_dict(raw_data)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = JukeboxData()  # 修改点: 初始化为空的 dataclass

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
                        json.dump(asdict(self._data), f, indent=4, ensure_ascii=False)
                    self._dirty = False
        except asyncio.CancelledError:
            pass
        finally:
            self._save_task = None

    # --- 内部数据访问辅助方法 ---

    def _get_or_create_guild_data(self, guild_id: int) -> GuildData:
        return self._data.guilds.setdefault(str(guild_id), GuildData())

    def _get_or_create_user_data(self, user_id: int) -> UserData:
        return self._data.users.setdefault(str(user_id), UserData())

    def get_queue_state(self, guild_id: int, role_id: int) -> QueueState:
        """公开方法：获取队列状态，如果不存在则创建。"""
        guild_data = self._get_or_create_guild_data(guild_id)
        return guild_data.queues.setdefault(str(role_id), QueueState())

    # --- 公开数据访问方法 ---
    def get_all_presets_for_admin_view(self) -> List[Preset]:
        """提供所有预设，只读地替代直接访问_data。"""
        all_presets = []
        for guild_data in self._data.guilds.values():
            all_presets.extend(guild_data.general_presets)
        for user_data in self._data.users.values():
            all_presets.extend(user_data.custom_presets)
        return all_presets

    def get_all_queues_using_preset(self, preset_uuid: str) -> List[Tuple[int, int]]:
        """查找所有使用特定预设的活跃队列，返回 (guild_id, role_id) 列表。"""
        results = []
        for guild_id_str, guild_data in self._data.guilds.items():
            for role_id_str, queue_state in guild_data.queues.items():
                if queue_state.current_preset_uuid == preset_uuid:
                    results.append((int(guild_id_str), int(role_id_str)))
        return results

    # --- 全局预设查找 ---
    def get_preset_by_uuid(self, preset_uuid: str) -> Optional[Preset]:
        # 1. 查找通用预设
        for guild_data in self._data.guilds.values():
            for preset in guild_data.general_presets:
                if preset.uuid == preset_uuid:
                    return preset
        # 2. 查找用户预设
        for user_data in self._data.users.values():
            for preset in user_data.custom_presets:
                if preset.uuid == preset_uuid:
                    return preset
        return None

    # --- 获取预设列表 ---
    def get_general_presets(self, guild_id: int) -> List[Preset]:
        """获取服务器的所有通用预设，返回Preset对象列表。"""
        guild_data = self._get_or_create_guild_data(guild_id)
        return guild_data.general_presets

    def get_user_presets(self, user_id: int) -> List[Preset]:
        """获取用户的所有专属预设，返回Preset对象列表。"""
        user_data = self._get_or_create_user_data(user_id)
        return user_data.custom_presets

    # --- PUT操作 ---
    async def upsert_preset(self, preset: Preset, guild_id: Optional[int] = None) -> Tuple[bool, str]:
        """
        幂等操作：创建或更新一个预设。
        - 如果preset.uuid已存在，则更新。
        - 如果preset.uuid不存在（或为新生成），则创建。
        - 通过 preset.owner_id 是否存在来判断是用户预设还是通用预设。
        """
        async with self._lock:
            is_user_preset = preset.owner_id is not None

            if is_user_preset:
                target_list = self._get_or_create_user_data(preset.owner_id).custom_presets
                if guild_id is None: return False, "为用户预设提供guild_id以检查限制。"
                guild_config = config.JUKEBOX_GUILD_CONFIGS.get(guild_id, {})
                max_presets = guild_config.get("max_vip_presets_per_user", 3)
                limit_msg = f"您的专属预设数量已达上限 ({max_presets}个)。"
            else:
                if guild_id is None: return False, "必须为通用预设提供guild_id。"
                target_list = self._get_or_create_guild_data(guild_id).general_presets
                max_presets = config.JUKEBOX_GUILD_CONFIGS[guild_id].get("max_general_presets", 10)
                limit_msg = f"通用预设数量已达上限 ({max_presets}个)。"

            existing_preset_index = -1
            for i, p in enumerate(target_list):
                if p.uuid == preset.uuid:
                    existing_preset_index = i
                    break

            if existing_preset_index != -1:
                target_list[existing_preset_index] = preset
                await self.save_data()
                return True, f"成功更新预设 '{preset.name}'。"
            else:
                if len(target_list) >= max_presets:
                    return False, limit_msg
                if any(p.name == preset.name for p in target_list):
                    return False, f"在当前范围内，名为 '{preset.name}' 的预设已存在。"
                target_list.append(preset)
                await self.save_data()
                return True, f"成功添加预设 '{preset.name}'。"

    async def delete_preset_by_uuid(self, preset_uuid: str) -> bool:
        """通过UUID在全局删除一个预设。"""
        async with self._lock:

            for guild_data in self._data.guilds.values():
                initial_len = len(guild_data.general_presets)
                guild_data.general_presets = [p for p in guild_data.general_presets if p.uuid != preset_uuid]
                if len(guild_data.general_presets) < initial_len:
                    await self.save_data()
                    return True

            for user_data in self._data.users.values():
                initial_len = len(user_data.custom_presets)
                user_data.custom_presets = [p for p in user_data.custom_presets if p.uuid != preset_uuid]
                if len(user_data.custom_presets) < initial_len:
                    await self.save_data()
                    return True
        return False

    async def change_or_claim_queue(self, guild_id: int, user_id: int, role_id: int, preset: Preset) -> Tuple[bool, str]:
        async with self._lock:
            queue_state = self.get_queue_state(guild_id, role_id)
            now = datetime.now(UTC8)

            if queue_state.unlock_timestamp:
                unlock_time = datetime.fromisoformat(queue_state.unlock_timestamp)
                if now < unlock_time:
                    return False, "这个队列的变更权仍在锁定中，您可以选择排队。"

            guild_config = config.JUKEBOX_GUILD_CONFIGS.get(guild_id, {})
            lock_hours = guild_config.get("lock_duration_hours", 2)
            unlock_time = now + timedelta(hours=lock_hours)

            queue_state.current_preset_uuid = preset.uuid
            queue_state.unlock_timestamp = unlock_time.isoformat()
            await self.save_data()
        return True, "变更成功！身份组外观已更新。"

    async def queue_request(self, guild_id: int, user_id: int, role_id: int, preset: Preset) -> Tuple[bool, str]:
        async with self._lock:
            queue_state = self.get_queue_state(guild_id, role_id)

            if any(req.user_id == user_id for req in queue_state.pending_requests):
                return False, "您已经在这个队列中排队了。"

            new_request = PendingRequest(user_id=user_id, preset=preset)
            queue_state.pending_requests.append(new_request)
            await self.save_data()

            position = len(queue_state.pending_requests)
        return True, f"排队成功！您是第 {position} 位。"

    async def force_unlock_all_queues(self, guild_id: int) -> int:
        """
        强制解锁指定服务器的所有队列。
        :param guild_id: 服务器ID。
        :return: 被解锁的队列数量。
        """
        unlocked_count = 0
        data_changed = False
        async with self._lock:
            guild_data = self._get_or_create_guild_data(guild_id)
            for queue_state in guild_data.queues.values():
                if queue_state.unlock_timestamp:
                    queue_state.unlock_timestamp = None
                    unlocked_count += 1
                    data_changed = True
            if data_changed:
                await self.save_data(force=True)
        return unlocked_count

    async def process_expirations(self) -> List[RotationAction]:
        """
        检查所有到期的队列，并返回轮换操作。
        如果队列为空，仅将unlock_timestamp设为null。
        """
        actions_to_take = []
        now = datetime.now(UTC8)
        data_changed = False

        async with self._lock:
            for guild_id_str, guild_data in self._data.guilds.items():
                for role_id_str, queue_state in guild_data.queues.items():
                    if queue_state.unlock_timestamp:
                        unlock_time = datetime.fromisoformat(queue_state.unlock_timestamp)
                        if now >= unlock_time:
                            data_changed = True
                            if queue_state.pending_requests:
                                next_request = queue_state.pending_requests.pop(0)
                                guild_config = config.JUKEBOX_GUILD_CONFIGS.get(int(guild_id_str), {})
                                lock_hours = guild_config.get("lock_duration_hours", 2)
                                new_unlock_time = now + timedelta(hours=lock_hours)

                                queue_state.current_preset_uuid = next_request.preset.uuid
                                queue_state.unlock_timestamp = new_unlock_time.isoformat()

                                actions_to_take.append(RotationAction(
                                    guild_id=int(guild_id_str),
                                    role_id=int(role_id_str),
                                    new_preset=next_request.preset,
                                    requester_id=next_request.user_id,
                                ))
                            else:
                                queue_state.unlock_timestamp = None

            if data_changed:
                await self.save_data(force=True)
        return actions_to_take
