# honor_system/cup_honor_json_manager.py
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from typing import Dict, List, Optional, Self

from pydantic import ValidationError

from .cup_honor_models import CupHonorDefinition

DATA_FILE_PATH = os.path.join('data', 'cup_honors.json')


class CupHonorJsonManager:
    """
    一个单例类，用于管理存储在JSON文件中的杯赛荣誉定义。
    使用Pydantic模型进行数据验证。
    """
    _instance: Optional[Self] = None
    _lock = threading.Lock()

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.cup_honors: Dict[str, CupHonorDefinition] = {}
        self._ensure_data_file()
        self.load_data()

    @classmethod
    def get_instance(cls, logger: logging.Logger) -> Self:
        """获取本类的单例实例。"""
        if cls._instance is None:
            cls._instance = cls(logger)
        return cls._instance

    def _ensure_data_file(self):
        """确保数据文件和目录存在。"""
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
        if not os.path.exists(DATA_FILE_PATH):
            with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump({}, f)  # 初始为空字典

    def load_data(self):
        """从JSON文件加载杯赛荣誉定义。"""
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                self.cup_honors = {}
                for uuid_str, honor_data in data.items():
                    try:
                        # 使用 Pydantic 模型进行验证和加载
                        honor_def = CupHonorDefinition.model_validate(honor_data)
                        self.cup_honors[str(honor_def.uuid)] = honor_def
                    except ValidationError as e:
                        self.logger.error(f"加载杯赛荣誉 '{uuid_str}' 时数据校验失败，已跳过: {e}")
                self.logger.info(f"成功从 {DATA_FILE_PATH} 加载了 {len(self.cup_honors)} 条杯赛荣誉定义。")
            except (IOError, json.JSONDecodeError) as e:
                self.logger.error(f"无法加载杯赛荣誉定义: {e}", exc_info=True)
                self.cup_honors = {}

    def save_data(self):
        """将当前杯赛荣誉定义保存到JSON文件。"""
        with self._lock:
            try:
                # 将 Pydantic 模型转换为可序列化的字典
                data_to_save = {
                    uuid_str: honor_def.model_dump(mode='json')
                    for uuid_str, honor_def in self.cup_honors.items()
                }
                with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, indent=4, ensure_ascii=False)
            except IOError as e:
                self.logger.error(f"无法保存杯赛荣誉定义: {e}", exc_info=True)

    def get_all_cup_honors(self) -> List[CupHonorDefinition]:
        """获取所有杯赛荣誉定义的列表。"""
        return list(self.cup_honors.values())

    def get_cup_honor_by_uuid(self, honor_uuid: str | uuid.UUID) -> Optional[CupHonorDefinition]:
        """通过UUID获取单个杯赛荣誉定义。"""
        return self.cup_honors.get(str(honor_uuid))

    def add_or_update_cup_honor(self, honor_def: CupHonorDefinition):
        """添加或更新一个杯赛荣誉定义，并立即保存。"""
        self.cup_honors[str(honor_def.uuid)] = honor_def
        self.save_data()
        self.logger.info(f"已添加/更新杯赛荣誉 '{honor_def.name}' (UUID: {honor_def.uuid})。")

    def delete_cup_honor(self, honor_uuid: str | uuid.UUID) -> bool:
        """通过UUID删除一个杯赛荣誉定义。"""
        uuid_str = str(honor_uuid)
        if uuid_str in self.cup_honors:
            del self.cup_honors[uuid_str]
            self.save_data()
            self.logger.info(f"已删除杯赛荣誉 (UUID: {uuid_str})。")
            return True
        return False