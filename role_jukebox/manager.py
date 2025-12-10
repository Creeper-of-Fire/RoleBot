# role_jukebox/manager.py
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict
from typing import List, Optional, Tuple

import aiofiles
import aiohttp

from role_jukebox.models import JukeboxData, GuildData, Track, Preset

# 使用新文件名以避免旧数据冲突，实现“不需要兼容”
DATA_FILE = "data/jukebox_data.json"
ICON_DIR = "data/jukebox_icons"


class RoleJukeboxManager:
    def __init__(self):
        self._data: JukeboxData = JukeboxData()
        self._lock = asyncio.Lock()

        # 确保存储目录存在
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        os.makedirs(ICON_DIR, exist_ok=True)

        self.load_data()

    def load_data(self):
        if not os.path.exists(DATA_FILE):
            self._data = JukeboxData()
            return
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = JukeboxData.from_dict(json.load(f))
        except Exception as e:
            print(f"[Jukebox] Error loading data: {e}, initializing empty.")
            self._data = JukeboxData()

    async def save_data(self):
        """保存数据到磁盘。"""
        async with self._lock:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(asdict(self._data), f, indent=4, ensure_ascii=False)

    def _get_guild_data(self, guild_id: int) -> GuildData:
        return self._data.guilds.setdefault(str(guild_id), GuildData())

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

    def _get_gd(self, guild_id: int):
        return self._data.guilds.setdefault(str(guild_id), type(self._data.guilds[str(guild_id)])())

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

    # --- 核心逻辑 ---
    def get_due_rotations(self) -> List[Tuple[int, Track, Preset]]:
        """
        检查所有轨道，找出此时此刻需要进行轮播的轨道。
        返回: (guild_id, track, next_preset) 的列表
        注意：此方法会更新内存中的 last_run_timestamp，调用者需确保执行了轮播。
        """
        actions = []
        now = time.time()

        for guild_id_str, guild_data in self._data.guilds.items():
            for track in guild_data.tracks.values():
                if not track.enabled or not track.presets:
                    continue

                # 检查时间间隔 (分钟 -> 秒)
                # 或者如果是首次运行 (last_run_timestamp == 0)
                if (now - track.last_run_timestamp) >= (track.interval_minutes * 60):
                    next_preset = track.get_next_preset()
                    if next_preset:
                        track.last_run_timestamp = now
                        actions.append((int(guild_id_str), track, next_preset))

        return actions