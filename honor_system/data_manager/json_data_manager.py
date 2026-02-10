# honor_system/json_data_manager.py
import json
import logging
import os
import threading
from typing import Dict, Any, Optional, List

DATA_FILE_PATH = os.path.join('data', 'claimable_honor_panels.json')


class JsonDataManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.panels: Dict[str, Dict[str, Any]] = {}
        self._ensure_data_file()
        self.load_data()

    @classmethod
    def get_instance(cls, logger: logging.Logger) -> 'JsonDataManager':
        if cls._instance is None:
            cls._instance = cls(logger)
        return cls._instance

    @staticmethod
    def _ensure_data_file():
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
        if not os.path.exists(DATA_FILE_PATH):
            with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump({}, f)

    def load_data(self):
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
                    self.panels = json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                self.logger.error(f"无法加载可领取荣誉面板数据: {e}")
                self.panels = {}

    def save_data(self):
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(self.panels, f, indent=4)
            except IOError as e:
                self.logger.error(f"无法保存可领取荣誉面板数据: {e}")

    def add_panel(self, message_id: int, channel_id: int, guild_id: int, honor_uuid: str):
        """添加一个新的面板记录。"""
        message_id_str = str(message_id)
        self.panels[message_id_str] = {
            "channel_id": channel_id,
            "guild_id": guild_id,
            "honor_uuid": honor_uuid
        }
        self.save_data()

    def get_panel(self, message_id: int) -> Optional[Dict[str, Any]]:
        """通过消息ID获取一个面板。"""
        return self.panels.get(str(message_id))

    def get_all_panels(self) -> List[Dict[str, Any]]:
        """获取所有面板的列表，并包含其message_id。"""
        return [{**panel_data, 'message_id': int(msg_id)} for msg_id, panel_data in self.panels.items()]

    def remove_panel(self, message_id: int):
        """移除一个面板记录。"""
        message_id_str = str(message_id)
        if message_id_str in self.panels:
            del self.panels[message_id_str]
            self.save_data()
