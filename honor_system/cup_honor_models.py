# honor_system/cup_honor_models.py
from __future__ import annotations

import datetime
import uuid as uuid_lib
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, BaseModel


class CupHonorDetails(BaseModel):
    """定义杯赛荣誉的特定属性"""
    expiration_date: datetime.datetime

    @field_validator('expiration_date', mode='before')
    @classmethod
    def parse_expiration_date(cls, v: str | datetime.datetime) -> datetime.datetime:
        """
        验证并解析过期日期。
        允许 'YYYY-MM-DD HH:MM:SS' 或 ISO 格式的字符串，并自动附加上海时区。
        """
        if isinstance(v, datetime.datetime):
            # 如果已经有时区，则保留；如果没有，则附加上海时区
            return v if v.tzinfo else v.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        if isinstance(v, str):
            try:
                dt_naive = datetime.datetime.fromisoformat(v)
                return dt_naive.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            except ValueError:
                raise ValueError("expiration_date 必须是 'YYYY-MM-DDTHH:MM:SS' 或 'YYYY-MM-DD HH:MM:SS' 格式")
        raise TypeError("expiration_date 必须是字符串或 datetime 对象")


class CupHonorDefinition(BaseModel):
    """
    杯赛荣誉的完整Pydantic模型，用于验证和序列化。
    """
    cup_honor: CupHonorDetails
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(max_length=255)
    role_id: int
    uuid: uuid_lib.UUID = Field(default_factory=uuid_lib.uuid4)
    hidden_until_earned: bool = True
    role_sync_honor: bool = True
