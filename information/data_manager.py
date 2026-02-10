# cogs/information/data_manager.py

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List

from pydantic import BaseModel, RootModel, Field

from utility.base_data_manager import AsyncJsonDataManager

DATA_NAME = "heartbeat_info"


class HeartbeatInfo(BaseModel):
    """存储单个心跳资讯的所有信息。"""
    source_guild_id: int
    source_channel_id: int
    source_message_id: Optional[int] = None  # 频道订阅模式下可以为None
    is_channel_feed: bool = False  # 新增字段：是否为频道最新消息订阅
    target_guild_id: int
    target_channel_id: int
    target_message_id: int
    update_interval_seconds: int
    embed_mode: bool = True
    last_update: datetime
    created_by: int
    title: Optional[str] = None  # 新增字段：资讯标题

    @property
    def key(self):
        """用于字典存储的唯一键。使用 target_message_id，因为它是唯一的。"""
        if self.target_message_id:
            return str(self.target_message_id)
        # 如果没有 target_message_id (例如一次性发送，但目前结构中所有存储的都应该有)
        # 可以考虑生成一个 UUID，但这会复杂化删除。
        # 暂时保持 target_message_id 为主键，因为所有心跳任务都依赖它。
        # 对于非心跳的存储，可能需要不同的存储方式或键。
        return f"{self.source_channel_id}-{self.source_message_id or 'latest'}-{self.title}"  # 临时 fallback key

    @property
    def source_url(self) -> str:
        """生成源消息的URL。"""
        if self.source_message_id:
            return f"https://discord.com/channels/{self.source_guild_id}/{self.source_channel_id}/{self.source_message_id}"
        # 对于频道订阅，返回频道URL
        return f"https://discord.com/channels/{self.source_guild_id}/{self.source_channel_id}"

    @property
    def target_url(self) -> str:
        """生成目标消息的URL。"""
        if self.target_message_id:
            return f"https://discord.com/channels/{self.target_guild_id}/{self.target_channel_id}/{self.target_message_id}"
        return "N/A"  # 如果没有目标消息ID

class HeartbeatStore(RootModel):
    root: Dict[str, HeartbeatInfo] = Field(default_factory=dict)

class HeartbeatDataManager(AsyncJsonDataManager[HeartbeatStore]):
    """管理所有心跳资讯的加载、保存和操作。"""
    DATA_FILENAME = DATA_NAME
    DATA_MODEL = HeartbeatStore

    @property
    def _heartbeats(self) -> Dict[str, HeartbeatInfo]:
        return self.data.root

    async def add_heartbeat(self, info: HeartbeatInfo):
        """添加一条新的心跳资讯记录并保存。"""
        if not info.target_message_id:
            self.logger.error(f"尝试添加无 target_message_id 的 HeartbeatInfo: {info.title}")
            return
        self._heartbeats[info.key] = info
        await self.save_data()
        self.logger.info(f"已添加新的心跳资讯: {info.title} (ID: {info.key})")

    async def update_heartbeat(self, info: HeartbeatInfo):
        """更新一条已存在的心跳资讯记录并保存。"""
        if not info.target_message_id:
            self.logger.error(f"尝试更新无 target_message_id 的 HeartbeatInfo: {info.title}")
            return
        if info.key not in self._heartbeats:
            self.logger.warning(f"尝试更新不存在的心跳资讯: {info.title} (ID: {info.key})")
            return
        self._heartbeats[info.key] = info
        await self.save_data()
        self.logger.debug(f"已更新心跳资讯: {info.title} (ID: {info.key})")

    async def remove_heartbeat(self, target_message_id: int) -> Optional[HeartbeatInfo]:
        """移除一条心跳资讯记录并保存。"""
        key = str(target_message_id)
        info = self._heartbeats.pop(key, None)
        if info:
            await self.save_data()
            self.logger.info(f"已移除心跳资讯: {info.title} (ID: {key})")
        return info

    def get_heartbeat(self, target_message_id: int) -> Optional[HeartbeatInfo]:
        """根据目标消息ID获取一条心跳资讯记录。"""
        return self._heartbeats.get(str(target_message_id))

    def get_heartbeat_by_title(self, title: str, guild_id: int) -> Optional[HeartbeatInfo]:
        """根据标题和服务器ID获取一条心跳资讯记录。"""
        for info in self._heartbeats.values():
            if info.target_guild_id == guild_id and info.title == title:
                return info
        return None

    def get_all_heartbeats(self) -> List[HeartbeatInfo]:
        """获取所有心跳资讯记录的列表。"""
        return list(self._heartbeats.values())