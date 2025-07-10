# cogs/information/data_manager.py

import asyncio
import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, Optional, List

CONFIG_FILE_PATH = "./data/heartbeat_info.json"


# 使用 dataclass 来清晰地定义数据结构
@dataclass
class HeartbeatInfo:
    """存储单个心跳资讯的所有信息。"""
    source_guild_id: int
    source_channel_id: int
    source_message_id: int
    target_guild_id: int
    target_channel_id: int
    target_message_id: int
    update_interval_seconds: int
    embed_mode: bool
    last_update: datetime
    created_by: int
    key: str = field(init=False)  # key是target_message_id的字符串形式

    def __post_init__(self):
        """在初始化后自动生成key。"""
        self.key = str(self.target_message_id)

    @property
    def source_url(self) -> str:
        """生成源消息的URL。"""
        return f"https://discord.com/channels/{self.source_guild_id}/{self.source_channel_id}/{self.source_message_id}"

    @property
    def target_url(self) -> str:
        """生成目标消息的URL。"""
        return f"https://discord.com/channels/{self.target_guild_id}/{self.target_channel_id}/{self.target_message_id}"


class HeartbeatDataManager:
    """管理所有心跳资讯的加载、保存和操作。"""

    def __init__(self):
        self.logger = logging.getLogger("HeartbeatDataManager")
        # 将心跳资讯存储在字典中，以目标消息ID作为键，方便快速查找
        self._heartbeats: Dict[str, HeartbeatInfo] = {}
        self._lock = asyncio.Lock()  # 用于文件I/O的异步锁

    async def load_data(self):
        """从JSON文件加载数据到内存。"""
        async with self._lock:
            try:
                with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 将字典转换回 HeartbeatInfo 对象
                    # 临时创建一个新的字典来存储加载的对象
                    loaded_heartbeats = {}
                    for key, value in data.items():
                        # 在传递给构造函数前，从字典中移除 'key'
                        value.pop('key', None)  # 使用.pop(key, None)是安全的，即使key不存在也不会报错

                        # 现在调用构造函数就是安全的了
                        loaded_heartbeats[key] = HeartbeatInfo(**value)

                    self._heartbeats = loaded_heartbeats
                self.logger.info(f"成功加载了 {len(self._heartbeats)} 条心跳资讯记录。")
            except FileNotFoundError:
                self.logger.info(f"心跳资讯配置文件 {CONFIG_FILE_PATH} 未找到，将自动创建。")
                self._heartbeats = {}
            except json.JSONDecodeError:
                self.logger.error(f"解析心跳资讯配置文件失败，将使用空数据。")
                self._heartbeats = {}

    async def _save_data(self):
        """将内存中的数据保存到JSON文件。"""
        async with self._lock:
            try:
                # 将 HeartbeatInfo 对象转换为字典以便序列化
                data_to_save = {key: asdict(info) for key, info in self._heartbeats.items()}
                with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, indent=4)
            except Exception as e:
                self.logger.error(f"保存心跳资讯数据时发生错误: {e}")

    async def add_heartbeat(self, info: HeartbeatInfo):
        """添加一条新的心跳资讯记录并保存。"""
        self._heartbeats[info.key] = info
        await self._save_data()
        self.logger.info(f"已添加新的心跳资讯: {info.key}")

    async def remove_heartbeat(self, target_message_id: int) -> Optional[HeartbeatInfo]:
        """移除一条心跳资讯记录并保存。"""
        key = str(target_message_id)
        info = self._heartbeats.pop(key, None)
        if info:
            await self._save_data()
            self.logger.info(f"已移除心跳资讯: {key}")
        return info

    def get_heartbeat(self, target_message_id: int) -> Optional[HeartbeatInfo]:
        """根据目标消息ID获取一条心跳资讯记录。"""
        return self._heartbeats.get(str(target_message_id))

    def get_all_heartbeats(self) -> List[HeartbeatInfo]:
        """获取所有心跳资讯记录的列表。"""
        return list(self._heartbeats.values())
