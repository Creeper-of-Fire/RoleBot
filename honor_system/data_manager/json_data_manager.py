# honor_system/json_data_manager.py
import json
import logging
import os
import threading
from typing import Dict, Any, Optional, List

from utility.base_data_manager import AsyncJsonDataManager

DATA_NAME = "claimable_honor_panels"


class HonorPanelDataManager(AsyncJsonDataManager[Dict[str, Dict[str, Any]]]):
    DATA_FILENAME = DATA_NAME
    # 不传 DATA_MODEL，默认 data 为 dict
    DATA_MODEL = None

    async def add_panel(self, message_id: int, channel_id: int, guild_id: int, honor_uuid: str):
        """添加一个新的面板记录。"""
        message_id_str = str(message_id)
        self.data[message_id_str] = {
            "channel_id": channel_id,
            "guild_id": guild_id,
            "honor_uuid": honor_uuid
        }
        await self.save_data()

    def get_panel(self, message_id: int) -> Optional[Dict[str, Any]]:
        """通过消息ID获取一个面板。"""
        return self.data.get(str(message_id))

    def get_all_panels(self) -> List[Dict[str, Any]]:
        """获取所有面板的列表，并包含其message_id。"""
        return [{**panel_data, 'message_id': int(msg_id)} for msg_id, panel_data in self.data.items()]

    async def remove_panel(self, message_id: int):
        """移除一个面板记录。"""
        message_id_str = str(message_id)
        if message_id_str in self.data:
            del self.data[message_id_str]
            await self.save_data()
