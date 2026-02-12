# role_jukebox/manager.py
from __future__ import annotations

import os
import random
import time
from typing import List, Optional, Tuple

import aiofiles

from role_system.role_jukebox.models import JukeboxData, GuildData, Track, Preset, TrackMode, PlayerAction
from utility.base_data_manager import AsyncJsonDataManager, DATA_DIR

# 使用新文件名以避免旧数据冲突，实现“不需要兼容”
DATA_NAME = "jukebox_data"
ICON_DIR = f"{DATA_DIR}/jukebox_icons"


class RoleJukeboxManager(AsyncJsonDataManager[JukeboxData]):
    """
    身份组点唱机数据管理器。
    管理 JSON 配置以及本地图标文件的存取。
    """
    DATA_FILENAME = DATA_NAME
    DATA_MODEL = JukeboxData

    def __init__(self,*args,**kwargs):
        # 初始化基类
        super().__init__(*args,**kwargs)
        # 确保图片目录存在
        os.makedirs(ICON_DIR, exist_ok=True)

    # --- 图片文件管理 ---

    def _get_icon_path(self, filename: str) -> str:
        return os.path.join(ICON_DIR, filename)

    async def save_icon(self, image_bytes: bytes, extension: str = "png") -> str:
        """
        保存图片字节流到本地，返回文件名。
        """
        import uuid
        filename = f"{uuid.uuid4()}.{extension}"
        filepath = self._get_icon_path(filename)

        async with aiofiles.open(filepath, 'wb') as f:
            await f.write(image_bytes)

        return filename

    async def get_icon_bytes(self, filename: str) -> Optional[bytes]:
        """
        读取本地图片。
        """
        filepath = self._get_icon_path(filename)
        if not os.path.exists(filepath):
            return None

        async with aiofiles.open(filepath, 'rb') as f:
            return await f.read()

    async def delete_icon(self, filename: str):
        """删除本地图片文件"""
        if not filename: return
        try:
            filepath = self._get_icon_path(filename)
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            print(f"Error deleting icon {filename}: {e}")

    # --- 轨道管理 --
    def _get_gd(self, guild_id: int) -> GuildData:
        """
        获取或创建服务器数据对象。
        """
        gid_str = str(guild_id)
        if gid_str not in self.data.guilds:
            self.data.guilds[gid_str] = GuildData()
        return self.data.guilds[gid_str]

    def get_track(self, guild_id: int, role_id: int) -> Optional[Track]:
        return self._get_gd(guild_id).tracks.get(str(role_id))

    def get_all_tracks(self, guild_id: int) -> List[Track]:
        return list(self._get_gd(guild_id).tracks.values())

    async def create_track(self, guild_id: int, role_id: int):
        gd = self._get_gd(guild_id)
        if str(role_id) not in gd.tracks:
            gd.tracks[str(role_id)] = Track(role_id=role_id)
            await self.save_data()

    async def delete_track(self, guild_id: int, role_id: int):
        gd = self._get_gd(guild_id)
        role_key = str(role_id)
        if role_key in gd.tracks:
            # 清理图片
            for p in gd.tracks[role_key].presets:
                await self.delete_icon(p.icon_filename)
            del gd.tracks[role_key]
            await self.save_data()

    async def update_track(self, guild_id: int, role_id: int, **kwargs):
        t = self.get_track(guild_id, role_id)
        if t:
            for k, v in kwargs.items():
                if hasattr(t, k): setattr(t, k, v)
            await self.save_data()

    # --- 预设管理 ---

    async def add_preset(self, guild_id: int, role_id: int, preset: Preset):
        t = self.get_track(guild_id, role_id)
        if t:
            t.presets.append(preset)
            await self.save_data()

    async def remove_preset(self, guild_id: int, role_id: int, uuid: str):
        t = self.get_track(guild_id, role_id)
        if t:
            # 找到要删除的预设以清理图片
            to_remove = next((p for p in t.presets if p.uuid == uuid), None)
            if to_remove:
                await self.delete_icon(to_remove.icon_filename)
                t.presets = [p for p in t.presets if p.uuid != uuid]
                await self.save_data()

    async def update_preset(
            self,
            guild_id: int,
            role_id: int,
            preset_uuid: str,
            new_name: str,
            new_color: str,
            new_secondary_color: Optional[str],
            new_tertiary_color: Optional[str]
    ):
        """
        根据 UUID 找到并更新一个预设的名称和颜色。
        """
        t = self.get_track(guild_id, role_id)
        if t:
            preset_to_update = next((p for p in t.presets if p.uuid == preset_uuid), None)
            if preset_to_update:
                preset_to_update.name = new_name
                preset_to_update.color = new_color
                preset_to_update.secondary_color = new_secondary_color
                preset_to_update.tertiary_color = new_tertiary_color
                await self.save_data()
                return True  # 表示成功
        return False  # 表示失败

    # --- 循环逻辑 ---

    def get_due_rotations(self) -> List[Tuple[int, Track, Preset]]:
        """
        检查所有轨道，找出此时此刻需要进行轮播的轨道。
        返回: (guild_id, track, next_preset) 的列表
        注意：此方法会更新内存中的 last_run_timestamp，调用者需确保执行了轮播。
        """
        actions = []
        now = time.time()

        for guild_id_str, guild_data in self.data.guilds.items():
            for track in guild_data.tracks.values():
                if not track.enabled or not track.presets:
                    continue

                # 检查时间间隔 (秒)
                # 或者如果是首次运行 (last_run_timestamp == 0)
                if (now - track.last_run_timestamp) >= track.interval_seconds:
                    next_preset = track.get_next_preset()
                    if next_preset:
                        track.last_run_timestamp = now
                        actions.append((int(guild_id_str), track, next_preset))

        return actions

    async def manual_control(self, guild_id: int, role_id: int, action: PlayerAction) -> Optional[Preset]:
        """
        手动控制轨道播放。
        返回: 需要应用到 Discord 的 Preset 对象
        """
        t = self.get_track(guild_id, role_id)
        if not t or not t.presets:
            return None

        count = len(t.presets)

        # --- 如果索引越界，直接重置为 0 ---
        if t.current_index >= count:
            t.current_index = 0

        # 根据动作计算新索引
        if action == PlayerAction.NEXT:
            if t.mode == TrackMode.RANDOM and count > 1:
                # 随机模式下，找一个和当前不一样的
                candidates = [i for i in range(count) if i != t.current_index]
                t.current_index = random.choice(candidates)
            else:
                # 顺序模式
                t.current_index = (t.current_index + 1) % count
        elif action == PlayerAction.PREV:
            # 上一首，永远按顺序来
            t.current_index = (t.current_index - 1 + count) % count
        elif action == PlayerAction.SYNC:
            # 同步操作，索引不变，直接使用当前的
            pass
        else:
            return None  # 无效操作

        # 重置计时器，避免刚手动切歌，又被自动任务切了
        t.last_run_timestamp = time.time()

        await self.save_data()
        return t.presets[t.current_index]

    # --- 狂暴模式控制 ---

    async def set_hyper_mode(self, guild_id: int, role_id: int, active: bool, hyper_interval: int = 1, original_interval: int = 60):
        """
        切换狂暴模式状态。
        active=True: 将间隔设为 1秒 (或者你敢设的最低值)
        active=False: 恢复原来的间隔
        """
        t = self.get_track(guild_id, role_id)
        if t:
            if active:
                # 开启狂暴：直接修改间隔为 1 秒
                # 注意：这里我们不修改 name_prefix，但你可以加上 "[狂暴]" 前缀如果想的话
                t.interval_seconds = hyper_interval
                # 强制重置时间戳，确保立刻触发下一次轮换
                t.last_run_timestamp = 0
            else:
                # 关闭狂暴：恢复原状
                t.interval_seconds = original_interval

            await self.save_data()
