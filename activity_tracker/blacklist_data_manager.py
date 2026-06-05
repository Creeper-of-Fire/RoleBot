import time
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
from utility.base_data_manager import AsyncUserGuildDataManager

BEIJING_TZ = timezone(timedelta(hours=8))


class BlacklistEntry(BaseModel):
    expiry: float   # 过期时间戳 (Unix)
    added_at: float  # 添加时间戳
    reason: str = ""  # 处罚原因（业务层兜底为"刷屏"）


class BlacklistDataManager(AsyncUserGuildDataManager[BlacklistEntry]):
    DATA_FILENAME = "activity_blacklist"
    USER_MODEL = BlacklistEntry

    def is_blacklisted(self, guild_id: int, user_id: int) -> tuple[bool, float]:
        """检查用户是否在黑名单中。返回 (是否黑名单, 过期时间戳)。"""
        entry = self.get_user_data(guild_id, user_id)
        if not entry:
            return False, 0.0
        if entry.expiry < time.time():
            self.remove_user_data(guild_id, user_id)
            self.save_data()
            return False, 0.0
        return True, entry.expiry

    def add_to_blacklist(self, guild_id: int, user_id: int, duration_days: int = 30, reason: str = ""):
        now = time.time()
        entry = BlacklistEntry(expiry=now + duration_days * 86400, added_at=now, reason=reason)
        self.set_user_data(guild_id, user_id, entry)
        self.save_data()

    def remove_from_blacklist(self, guild_id: int, user_id: int) -> bool:
        result = self.remove_user_data(guild_id, user_id)
        if result:
            self.save_data()
        return result

    def get_all_blacklisted(self, guild_id: int) -> list[tuple[int, float]]:
        """获取服务器内所有黑名单用户，自动清理过期条目。返回 [(user_id, expiry), ...]。"""
        guild_data = self.data.get(str(guild_id), {})
        now = time.time()
        active = []
        expired_keys = []
        for user_id_str, entry in guild_data.items():
            if entry.expiry < now:
                expired_keys.append(int(user_id_str))
            else:
                active.append((int(user_id_str), entry.expiry))
        for uid in expired_keys:
            self.remove_user_data(guild_id, uid)
        if expired_keys:
            self.save_data()
        return active
