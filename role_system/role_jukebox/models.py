# role_jukebox/models.py
from __future__ import annotations

import random
import uuid
from enum import Enum
from typing import List, Optional, Dict, TypeVar

from pydantic import BaseModel, Field, ConfigDict

T = TypeVar('T')

DEFAULT_NAME_PREFIX = "[轮播]"


class TrackMode(str, Enum):
    SEQUENCE = 'sequence'
    RANDOM = 'random'


class PlayerAction(str, Enum):
    NEXT = 'next'
    PREV = 'prev'
    SYNC = 'sync'


class DashboardMode(str, Enum):
    ADMIN = 'admin'
    USER = 'user'


class Preset(BaseModel):
    """单个外观预设（名字、颜色、图标）。"""
    # Pydantic V2 会自动处理多余字段的过滤（默认忽略不匹配字段）
    model_config = ConfigDict(from_attributes=True)

    name: str
    color: str  # Hex 字符串 (#RRGGBB) - 主色
    secondary_color: Optional[str] = None
    tertiary_color: Optional[str] = None
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()))
    icon_filename: Optional[str] = None


class Track(BaseModel):
    """
    轨道：对应一个身份组的轮播配置。
    包含一组预设池，以及轮播的规则。
    """
    model_config = ConfigDict(from_attributes=True)

    role_id: int
    presets: List[Preset] = Field(default_factory=list)
    name: Optional[str] = None
    name_prefix: Optional[str] = DEFAULT_NAME_PREFIX

    # 配置参数
    mode: TrackMode = TrackMode.RANDOM  # 轮播模式
    interval_seconds: int = 3600  # 轮播间隔（秒）
    enabled: bool = True  # 是否开启轮播

    # 运行时状态 (不一定需要严格序列化，但为了重启后保持状态，建议存下来)
    last_run_timestamp: float = 0.0  # 上次轮播的时间戳
    current_index: int = 0  # 顺序播放时的当前索引

    def get_next_preset(self) -> Optional[Preset]:
        """根据模式计算下一个预设，并更新内部状态。"""
        if not self.presets:
            return None

        if self.mode == TrackMode.RANDOM:
            # 随机模式：随机选一个
            if len(self.presets) > 1:
                # 尝试避免连续两次重复（简单的随机优化）
                candidates = [p for p in self.presets if self.presets.index(p) != self.current_index]
                choice = random.choice(candidates)
                self.current_index = self.presets.index(choice)
                return choice
            else:
                return self.presets[0]

        else:  # sequence
            # 顺序模式：索引 + 1
            self.current_index = (self.current_index + 1) % len(self.presets)
            return self.presets[self.current_index]


class GuildData(BaseModel):
    """服务器数据：存储该服务器下所有的轨道。"""
    tracks: Dict[str, Track] = Field(default_factory=dict)  # key: role_id (str)


class JukeboxData(BaseModel):
    """根数据结构。"""
    guilds: Dict[str, GuildData] = Field(default_factory=dict)  # key: guild_id (str)