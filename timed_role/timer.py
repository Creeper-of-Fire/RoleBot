# src/role_manager/utility/timer.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import config

UTC8 = timezone(timedelta(hours=8))

def get_daily_limit_seconds(guild_id: int) -> int:
    """
    获取指定服务器的每日限时时长（秒）。
    优先从 GUILD_CONFIGS 获取，如果未配置则使用全局默认值。
    """
    guild_specific_config = config.GUILD_CONFIGS.get(guild_id, {})
    limit_hours = guild_specific_config.get(
        "daily_limit_hours",
        config.ROLE_MANAGER_CONFIG.get("daily_limit_hours", 1)  # 双重保险，默认1小时
    )
    return int(limit_hours * 3600)


def get_remaining_seconds(user_data: dict, guild_id: int) -> int:
    """
    【核心改动】根据用户数据和服务器ID计算今天剩余的可用时长。
    """
    daily_limit_seconds = get_daily_limit_seconds(guild_id)

    if user_data.get("current_timed_roles") and user_data.get("last_claim_timestamp"):
        last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
        used_this_session = (datetime.now(UTC8) - last_claim_time).total_seconds()
        total_used = user_data.get("used_seconds", 0) + used_this_session
        return max(0, int(daily_limit_seconds - total_used))

    return max(0, int(daily_limit_seconds - user_data.get("used_seconds", 0)))