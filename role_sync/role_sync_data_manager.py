# role_sync/role_sync_data_manager.py

from typing import Dict, List

from utility.base_data_manager import AsyncJsonDataManager

DATA_NAME = "role_sync_log"


def create_rule_key(source_id: int, target_id: int) -> str:
    """为同步规则创建一个唯一的字符串键。"""
    return f"{source_id}-{target_id}"


class RoleSyncDataManager(AsyncJsonDataManager[Dict[str, Dict[str, List[int]]]]):
    """管理角色同步日志，记录哪些用户已经针对特定规则被同步过。"""
    DATA_FILENAME = DATA_NAME
    # 不传 DATA_MODEL，默认 data 为 dict
    # 数据结构: { "guild_id": { "source_id-target_id": [user_id1, user_id2] } }
    DATA_MODEL = None

    async def mark_as_synced(self, guild_id: int, source_id: int, target_id: int, user_id: int):
        """将一个用户标记为已针对某个规则同步过。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        guild_logs = self.data.setdefault(guild_id_str, {})
        synced_users = guild_logs.setdefault(rule_key, [])
        if user_id not in synced_users:
            synced_users.append(user_id)
            await self.save_data()

    def is_synced(self, guild_id: int, source_id: int, target_id: int, user_id: int) -> bool:
        """检查用户是否已经被标记为同步过。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        return user_id in self.data.get(guild_id_str, {}).get(rule_key, [])

    async def clear_rule_log(self, guild_id: int, source_id: int, target_id: int) -> bool:
        """清除指定规则的同步日志。"""
        guild_id_str = str(guild_id)
        rule_key = create_rule_key(source_id, target_id)

        if guild_id_str in self.data and rule_key in self.data[guild_id_str]:
            del self.data[guild_id_str][rule_key]
            await self.save_data()
            return True
        return False
