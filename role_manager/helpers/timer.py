# src/role_manager/helpers/timer.py
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import config

UTC8 = timezone(timedelta(hours=8))
DAILY_LIMIT_HOURS = config.ROLE_MANAGER_CONFIG.get("daily_limit_hours", 1)
DAILY_LIMIT_SECONDS = int(DAILY_LIMIT_HOURS * 3600)

def get_remaining_seconds(user_data) -> int:
    """根据用户数据计算今天剩余的可用时长。"""
    if user_data.get("current_timed_roles") and user_data.get("last_claim_timestamp"):
        last_claim_time = datetime.fromisoformat(user_data["last_claim_timestamp"])
        used_this_session = (datetime.now(UTC8) - last_claim_time).total_seconds()
        total_used = user_data.get("used_seconds", 0) + used_this_session
        return max(0, int(DAILY_LIMIT_SECONDS - total_used))

    return max(0, int(DAILY_LIMIT_SECONDS - user_data.get("used_seconds", 0)))