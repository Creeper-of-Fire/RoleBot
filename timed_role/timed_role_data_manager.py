# role_manager/timed_role_data_manager.py
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone, time

import config

from typing import TYPE_CHECKING

from timed_role import timer
from utility.role_service import update_member_roles

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
        return timer.get_remaining_seconds(user_data)

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

    async def daily_reset(self, cog: 'FeatureCog'):
        """重置所有服务器中所有用户的每日计时。"""
        async with self._lock:  # 添加锁机制防止并发问题
            now = datetime.now(UTC8)
            last_reset_iso_str = self._data.get("last_reset", datetime.min.isoformat())
            last_reset_time = datetime.fromisoformat(last_reset_iso_str)
            if last_reset_time.tzinfo is None:
                last_reset_time = last_reset_time.replace(tzinfo=UTC8)

            today_reset_time = now.replace(hour=RESET_TIME.hour, minute=RESET_TIME.minute, second=RESET_TIME.second, microsecond=0)

            if now >= today_reset_time > last_reset_time:
                # 1. 识别所有由本功能管理的限时身份组ID
                all_timed_role_ids = set()
                for guild_id, guild_config in config.GUILD_CONFIGS.items():
                    for role_id in guild_config.get('timed_roles', []):
                        all_timed_role_ids.add(role_id)

                # 2. 识别出不应移除身份组的用户（正在正常计时的）
                excluded_user_ids = set()
                for user_id_str, guilds_data in self._data["users"].items():
                    for guild_id_str, user_data in guilds_data.items():
                        if user_data.get("current_timed_roles"):
                            excluded_user_ids.add(int(user_id_str))

                # 3. 遍历所有服务器，统一移除所有过期的身份组
                for guild in cog.bot.guilds:
                    members_to_check = set()
                    # 从拥有身份组的成员中收集
                    for role_id in all_timed_role_ids:
                        role = guild.get_role(role_id)
                        if role:
                            members_to_check.update(role.members)

                    # 移除被豁免的用户
                    members_to_process = {m for m in members_to_check if m.id not in excluded_user_ids}

                    # 批量移除
                    for member in members_to_process:
                        await update_member_roles(
                            cog=cog,
                            member=member,
                            to_add_ids=set(),
                            to_remove_ids=all_timed_role_ids,
                            reason="每日限时身份组重置"
                        )

                # 4. 重置数据（原逻辑）
                for user_id_str, guilds_data in self._data["users"].items():
                    for guild_id_str, user_data in guilds_data.items():
                        # 结算仍在计时的人
                        if user_data["current_timed_roles"] and user_data["last_claim_timestamp"]:
                            last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
                            used_this_session = (now - last_claim_time).total_seconds()
                            user_data["used_seconds"] += used_this_session
                            user_data["current_timed_roles"] = []
                            user_data["last_claim_timestamp"] = None
                        # 重置时长
                        user_data["used_seconds"] = 0

                # 清理不活跃的用户数据
                users_to_delete = []
                for user_id_str, guilds_data in self._data["users"].items():
                    all_guilds_inactive = True
                    for guild_id_str, user_data in guilds_data.items():
                        if user_data["used_seconds"] != 0 or user_data["current_timed_roles"] or user_data["last_claim_timestamp"] is not None:
                            all_guilds_inactive = False
                            break
                    if all_guilds_inactive:
                        users_to_delete.append(user_id_str)

                for user_id_str in users_to_delete:
                    del self._data["users"][user_id_str]

                self._data["last_reset"] = now.isoformat()
                await self.save_data(force=True)
                cog.logger.info("每日重置完成，所有非豁免用户的限时身份组已被移除。")
                return True
            return False

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