# src/role_manager/data_manager.py
import asyncio
import json
import os
from datetime import datetime, time, timedelta, timezone

import config_data  # 引入全局配置

DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "user_data.json")
# 北京时间 UTC+8
UTC8 = timezone(timedelta(hours=8))

# 从配置文件读取配置，并提供默认值
RESET_HOUR = config_data.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
DAILY_LIMIT_HOURS = config_data.ROLE_MANAGER_CONFIG.get("daily_limit_hours", 1)

# 根据配置计算最终值
RESET_TIME = time(RESET_HOUR, 0, 0, tzinfo=UTC8)
DAILY_LIMIT_SECONDS = int(DAILY_LIMIT_HOURS * 3600)


class DataManager:
    """处理所有用户数据的加载、保存和逻辑计算"""

    def __init__(self):
        self._data = {}
        self._lock = asyncio.Lock()
        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    def load_data(self):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {"users": {}, "last_reset": datetime.min.isoformat()}

    async def save_data(self):
        async with self._lock:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=4)

    def _get_user(self, user_id: int):
        """获取用户数据，如果不存在则创建默认结构"""
        user_id_str = str(user_id)
        if user_id_str not in self._data["users"]:
            self._data["users"][user_id_str] = {
                "used_seconds": 0,
                "current_timed_role": None,
                "current_timed_role_guild_id": None,
                "last_claim_timestamp": None,
            }
        # 确保旧数据有这个字段
        if "current_timed_role_guild_id" not in self._data["users"][user_id_str]:
            self._data["users"][user_id_str]["current_timed_role_guild_id"] = None
        return self._data["users"][user_id_str]

    def get_remaining_seconds(self, user_id: int) -> int:
        """获取用户今天剩余的可用时长"""
        user_data = self._get_user(user_id)
        # 如果当前正在使用，需要先计算刚刚用掉的时间
        if user_data["current_timed_role"] and user_data["last_claim_timestamp"]:
            last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
            used_this_session = (datetime.now(UTC8) - last_claim_time).total_seconds()
            total_used = user_data["used_seconds"] + used_this_session
            return max(0, int(DAILY_LIMIT_SECONDS - total_used))

        return max(0, int(DAILY_LIMIT_SECONDS - user_data["used_seconds"]))

    async def claim_timed_role(self, user_id: int, role_id: int, guild_id: int):
        """用户领取一个限时身份组"""
        user_data = self._get_user(user_id)
        now = datetime.now(UTC8)

        # 如果用户之前有未归还的身份组，先强制结算其使用时长
        # 这种场景通常发生在机器人重启，或用户在归还前退出了服务器等情况
        # 此时只结算时间，不改变current_timed_role和guild_id，因为这可能是跨服务器的情况
        # 实际移除角色在 cog 中进行
        if user_data["current_timed_role"] and user_data["last_claim_timestamp"]:
            last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
            used_this_session = (now - last_claim_time).total_seconds()
            user_data["used_seconds"] += used_this_session

        user_data["current_timed_role"] = role_id
        user_data["current_timed_role_guild_id"] = guild_id
        user_data["last_claim_timestamp"] = now.isoformat()
        await self.save_data()

    async def return_timed_role(self, user_id: int) -> float:
        """
        用户归还限时身份组，并计算本次使用了多长时间
        返回本次会话使用的秒数
        """
        user_data = self._get_user(user_id)
        if not user_data["current_timed_role"] or not user_data["last_claim_timestamp"]:
            return 0

        now = datetime.now(UTC8)
        last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])

        used_this_session = (now - last_claim_time).total_seconds()

        user_data["used_seconds"] += used_this_session
        user_data["current_timed_role"] = None
        user_data["current_timed_role_guild_id"] = None
        user_data["last_claim_timestamp"] = None

        await self.save_data()
        return used_this_session

    async def force_return_timed_role(self, user_id: int):
        """由机器人强制归还限时身份组（不计算使用时长，只重置状态）。"""
        user_data = self._get_user(user_id)
        if user_data["current_timed_role"]:
            user_data["current_timed_role"] = None
            user_data["current_timed_role_guild_id"] = None
            user_data["last_claim_timestamp"] = None
            await self.save_data()

    async def daily_reset(self):
        """重置所有用户的每日计时"""
        now = datetime.now(UTC8)
        # --- 核心改动开始 ---
        # 步骤 1: 获取 last_reset_time，如果不存在，则使用带时区的最小值
        aware_min_datetime = datetime.min.replace(tzinfo=UTC8)
        last_reset_iso_str = self._data.get("last_reset", aware_min_datetime.isoformat())
        last_reset_time = datetime.fromisoformat(last_reset_iso_str)

        # 步骤 2: 强制检查和附加时区，确保万无一失
        # 如果解析出来的时间对象没有时区信息(naive)，就给它安上 UTC8
        if last_reset_time.tzinfo is None:
            last_reset_time = last_reset_time.replace(tzinfo=UTC8)
        # --- 核心改动结束 ---

        today_reset_time = now.replace(hour=RESET_TIME.hour, minute=RESET_TIME.minute, second=RESET_TIME.second, microsecond=0)

        if now >= today_reset_time > last_reset_time:
            # 确保在重置前，所有正在计时的用户都先“归还”，将已用时间计入
            for user_id_str, user_data in self._data["users"].items():
                if user_data["current_timed_role"] and user_data["last_claim_timestamp"]:
                    last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
                    used_this_session = (now - last_claim_time).total_seconds()
                    user_data["used_seconds"] += used_this_session
                    # 重置状态，但不清除 used_seconds，因为要重置了
                    user_data["current_timed_role"] = None
                    user_data["current_timed_role_guild_id"] = None
                    user_data["last_claim_timestamp"] = None

            for user_id in self._data["users"]:
                self._data["users"][user_id]["used_seconds"] = 0
            self._data["last_reset"] = now.isoformat()
            await self.save_data()
            return True
        return False

    def get_users_with_active_timed_role(self) -> list[tuple[int, int, int]]:
        """
        获取当前持有计时身份组的用户列表。
        返回列表中的每个元素是 (user_id, role_id, guild_id)。
        """
        active_users = []
        for user_id_str, user_data in self._data["users"].items():
            if user_data["current_timed_role"] and user_data["current_timed_role_guild_id"]:
                active_users.append((
                    int(user_id_str),
                    user_data["current_timed_role"],
                    user_data["current_timed_role_guild_id"]
                ))
        return active_users
