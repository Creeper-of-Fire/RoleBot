# role_manager/timed_role_data_manager.py
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone, time
from typing import TYPE_CHECKING

import discord

import config
from timed_role import timer
from utility.role_service import batch_update_member_roles

if TYPE_CHECKING:
    from utility.feature_cog import FeatureCog

DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "user_data.json")
UTC8 = timezone(timedelta(hours=8))

RESET_HOUR = config.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
RESET_TIME = time(RESET_HOUR, 0, 0, tzinfo=UTC8)


class TimedRoleDataManager:
    def __init__(self):
        self._data = {}
        self._lock = asyncio.Lock()
        self._dirty = False  # 标记数据是否被修改
        self._save_task = None  # 后台保存任务
        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {"users": {}, "last_reset": datetime.min.isoformat()}

    async def save_data(self, force=False):
        """保存数据，支持防抖和增量更新"""
        self._dirty = True

        # 如果强制保存或没有正在进行的保存任务
        if force or self._save_task is None:
            if self._save_task:
                self._save_task.cancel()

            # 延迟1秒保存，实现防抖
            self._save_task = asyncio.create_task(self._delayed_save())

    async def _delayed_save(self):
        """延迟保存实现防抖"""
        try:
            await asyncio.sleep(1)
            async with self._lock:
                if self._dirty:
                    with open(DATA_FILE, 'w', encoding='utf-8') as f:
                        json.dump(self._data, f, indent=4)
                    self._dirty = False
        except asyncio.CancelledError:
            pass
        finally:
            self._save_task = None

    # 【核心改动】数据结构变更为 per-guild
    def _get_guild_user_data(self, user_id: int, guild_id: int):
        """获取指定服务器中用户的数据，如果不存在则创建默认结构。"""
        user_id_str, guild_id_str = str(user_id), str(guild_id)

        if user_id_str not in self._data["users"]:
            self._data["users"][user_id_str] = {}

        if guild_id_str not in self._data["users"][user_id_str]:
            self._data["users"][user_id_str][guild_id_str] = {
                "used_seconds": 0,
                "current_timed_roles": [],  # 【改动】从单个 role 变为 roles 列表
                "last_claim_timestamp": None,
            }
        return self._data["users"][user_id_str][guild_id_str]

    # 【核心改动】方法现在需要 guild_id
    def get_remaining_seconds(self, user_id: int, guild_id: int) -> int:
        """获取用户在指定服务器今天剩余的可用时长。"""
        user_data = self._get_guild_user_data(user_id, guild_id)
        return timer.get_remaining_seconds(user_data, guild_id)

    # 【核心改动】方法现在需要 guild_id，并且接受 role_ids 列表
    async def claim_timed_roles(self, user_id: int, role_ids: list[int], guild_id: int):
        """用户在指定服务器领取一个或多个限时身份组。"""
        user_data = self._get_guild_user_data(user_id, guild_id)
        now = datetime.now(UTC8)

        # 只要新领取的列表不为空，且之前是空的，就开始计时
        if role_ids and not user_data["current_timed_roles"]:
            user_data["last_claim_timestamp"] = now.isoformat()

        user_data["current_timed_roles"] = role_ids

        await self.save_data(force=False)

    # 【核心改动】方法现在需要 guild_id
    async def return_timed_roles(self, user_id: int, guild_id: int) -> float:
        """用户归还指定服务器的所有限时身份组，并计算本次使用了多长时间。"""
        user_data = self._get_guild_user_data(user_id, guild_id)
        if not user_data["current_timed_roles"] or not user_data["last_claim_timestamp"]:
            return 0

        now = datetime.now(UTC8)
        last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
        used_this_session = (now - last_claim_time).total_seconds()

        user_data["used_seconds"] += used_this_session
        user_data["current_timed_roles"] = []
        user_data["last_claim_timestamp"] = None

        await self.save_data(force=False)
        return used_this_session

    async def force_return_timed_roles(self, user_id: int, guild_id: int):
        """由机器人强制归还限时身份组（不计算使用时长，只重置状态）。"""
        user_data = self._get_guild_user_data(user_id, guild_id)
        if user_data["current_timed_roles"]:
            user_data["current_timed_roles"] = []
            user_data["last_claim_timestamp"] = None
            await self.save_data(force=False)

    async def get_last_reset_time(self) -> datetime:
        """获取上次重置的时间。"""
        async with self._lock:
            last_reset_iso_str = self._data.get("last_reset", datetime.min.isoformat())
            last_reset_time = datetime.fromisoformat(last_reset_iso_str)
            if last_reset_time.tzinfo is None:
                last_reset_time = last_reset_time.replace(tzinfo=UTC8)
            return last_reset_time

    async def update_last_reset_time(self):
        """仅更新重置时间戳。"""
        async with self._lock:
            self._data["last_reset"] = datetime.now(UTC8).isoformat()
            await self.save_data(force=True)

    async def daily_reset(self, cog: 'FeatureCog', guilds_to_reset: list[discord.Guild]):
        """
        重置指定服务器列表中所有用户的每日计时。
        此方法现在由Cog驱动，只处理传入的、非永久的服务器。
        """
        now = datetime.now(UTC8)
        bot = cog.bot

        async with self._lock:
            # 1. 识别需要保留身份组的用户（豁免名单），但只在需要重置的服务器中
            guilds_to_reset_ids = {g.id for g in guilds_to_reset}
            exclusion_map = {}

            # 遍历数据库，处理正在计时的用户
            for user_id_str, guilds_data in list(self._data["users"].items()):
                user_id = int(user_id_str)
                for guild_id_str, user_data in list(guilds_data.items()):
                    guild_id = int(guild_id_str)

                    # 只处理传入的、需要重置的服务器
                    if guild_id not in guilds_to_reset_ids:
                        continue

                    if user_data.get("current_timed_roles") and user_data.get("last_claim_timestamp"):
                        if guild_id not in exclusion_map:
                            exclusion_map[guild_id] = {}
                        # 记录需要重新同步的身份组
                        exclusion_map[guild_id][user_id] = {"add": user_data["current_timed_roles"], "remove": []}

                        # 新的一天开始，重置使用时长
                        user_data["used_seconds"] = 0
                        # 重新开始计时
                        user_data["last_claim_timestamp"] = now.isoformat()
                    else:
                        # 对于没有在计时的用户，直接重置其使用时间
                        user_data["used_seconds"] = 0
                        # # 如果用户数据变得空洞，可以考虑清理
                        # if not user_data.get("current_timed_roles"):
                        #     # 为了简化，这里暂时不删除，但可以优化
                        #     pass

            # 2. 构建一个拥有身份组的成员列表，仅针对需要重置的服务器
            all_members_with_timed_roles = {}
            for guild in guilds_to_reset:
                all_members_with_timed_roles[guild.id] = {}
                guild_timed_roles = set(config.GUILD_CONFIGS[guild.id].get('timed_roles', []))

                for role_id in guild_timed_roles:
                    role = guild.get_role(role_id)
                    if role:
                        for member in role.members:
                            if member.id not in all_members_with_timed_roles[guild.id]:
                                all_members_with_timed_roles[guild.id][member.id] = set()
                            all_members_with_timed_roles[guild.id][member.id].add(role.id)


            # 3. 同步和清理身份组
            cog.logger.info(f"每日重置：开始处理 {len(guilds_to_reset)} 个服务器的身份组同步...")
            for guild in guilds_to_reset:
                guild_id = guild.id
                members_map = all_members_with_timed_roles.get(guild_id, {})
                guild_exclusion_user_ids = set(exclusion_map.get(guild_id, {}).keys())

                members_to_update = {}

                for member_id, role_ids_on_server in members_map.items():
                    if member_id not in guild_exclusion_user_ids:
                        # 不在豁免名单内，移除所有限时身份组
                        members_to_update[member_id] = {"add": [], "remove": list(role_ids_on_server)}
                        # 清理数据库中这些用户在该服务器的数据
                        if str(member_id) in self._data["users"] and str(guild_id) in self._data["users"][str(member_id)]:
                            del self._data["users"][str(member_id)][str(guild_id)]
                            if not self._data["users"][str(member_id)]:
                                del self._data["users"][str(member_id)]

                # 3.1. 对于豁免名单内的用户，重新上号，确保他们的身份组是最新的
                guild_exclusion_map = exclusion_map.get(guild_id, {})
                if guild_exclusion_map:
                    cog.logger.info(f"服务器 {guild.name}：开始为 {len(guild_exclusion_map)} 个豁免用户重新同步身份组...")
                    await batch_update_member_roles(cog, guild, guild_exclusion_map, reason="每日重置豁免用户身份组同步")

                # 3.2. 移除需要清理的成员的身份组
                if members_to_update:
                    cog.logger.info(f"服务器 {guild.name}：开始为 {len(members_to_update)} 个非豁免用户移除身份组...")
                    await batch_update_member_roles(cog, guild, members_to_update, reason="每日重置自动移除")

            # 4. 清理数据库
            # 仅移除那些不在任何豁免名单中的用户的数据
            all_exclusion_user_ids = set()
            for guild_exclusion in exclusion_map.values():
                all_exclusion_user_ids.update(guild_exclusion.keys())

            users_to_clear = [uid for uid in self._data["users"].keys() if int(uid) not in all_exclusion_user_ids]
            for user_id_str in users_to_clear:
                if user_id_str in self._data["users"]:
                    del self._data["users"][user_id_str]

            # 更新重置时间戳
            self._data["last_reset"] = now.isoformat()
            await self.save_data(force=True)
            cog.logger.info("指定服务器的每日重置任务已成功完成。")

    async def reset_user_used_seconds(self, user_id: int, guild_id: int):
        """
        对永久服务器，检查并修复用户的异常使用时长。
        仅当用户的 'used_seconds' > 服务器配置的 'daily_limit_seconds' 时，
        才将其重置为0。这主要用于处理数据异常或配置变更后的自愈。
        """
        user_data = self._get_guild_user_data(user_id, guild_id)
        current_used = user_data.get("used_seconds", 0)

        # 获取当前服务器的理论总时长
        daily_limit_seconds = timer.get_daily_limit_seconds(guild_id)

        # 仅当已用时间异常地超过了总上限时，才触发修复
        if current_used > daily_limit_seconds:
            user_data["used_seconds"] = 0
            await self.save_data(force=False)
            return True  # 表示已修复

        return False  # 表示数据正常，无需修复

    # 【核心改动】返回的结构也变了
    def get_users_with_active_timed_role(self) -> list[tuple[int, int, list[int]]]:
        """
        获取当前持有计时身份组的用户列表。
        返回列表中的每个元素是 (user_id, guild_id, role_ids)。
        """
        active_users = []
        for user_id_str, guilds_data in self._data["users"].items():
            for guild_id_str, user_data in guilds_data.items():
                if user_data["current_timed_roles"]:
                    active_users.append((
                        int(user_id_str),
                        int(guild_id_str),
                        user_data["current_timed_roles"]
                    ))
        return active_users