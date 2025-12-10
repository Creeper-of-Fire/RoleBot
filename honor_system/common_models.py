# honor_system/common_models.py
from __future__ import annotations

import uuid as uuid_lib
from typing import Optional

from pydantic import BaseModel, Field


class BaseHonorDefinition(BaseModel):
    """
    一个基础的、统一的荣誉定义模型，用于规范来自不同来源（配置文件、JSON等）的荣誉数据。
    """
    uuid: str | uuid_lib.UUID
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(max_length=255)
    role_id: Optional[int] = None
    hidden_until_earned: bool = True
    role_sync_honor: bool = False  # 关键字段，默认False
    icon_url: Optional[str] = None # 从config.py中同步过来的字段

    class Config:
        # 允许从非Pydantic对象创建模型
        from_attributes = True