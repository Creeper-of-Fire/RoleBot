# role_jukebox/models.py
from __future__ import annotations

import uuid
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Type, TypeVar

T = TypeVar('T')

@dataclass
class Preset:
    """单个外观预设（名字、颜色、图标）。"""
    name: str
    color: str  # Hex 字符串 (#RRGGBB)
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    icon_filename: Optional[str] = None

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        # 过滤掉不需要的旧字段
        clean_data = {k: v for k, v in data.items() if k in cls.__annotations__}
        return cls(**clean_data)


@dataclass
class Track:
    """
    轨道：对应一个身份组的轮播配置。
    包含一组预设池，以及轮播的规则。
    """
    role_id: int
    presets: List[Preset] = field(default_factory=list)
    name: Optional[str] = None
    name_prefix: Optional[str] = None

    # 配置参数
    mode: str = 'sequence'  # 'sequence' (顺序) 或 'random' (随机)
    interval_minutes: int = 60  # 轮播间隔（分钟）
    enabled: bool = True  # 是否开启轮播

    # 运行时状态 (不一定需要严格序列化，但为了重启后保持状态，建议存下来)
    last_run_timestamp: float = 0.0  # 上次轮播的时间戳
    current_index: int = 0  # 顺序播放时的当前索引

    def get_next_preset(self) -> Optional[Preset]:
        """根据模式计算下一个预设，并更新内部状态。"""
        if not self.presets:
            return None

        if self.mode == 'random':
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

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        presets_data = data.get('presets', [])
        data['presets'] = [Preset.from_dict(p) for p in presets_data]
        # 自动填充新增字段的默认值
        clean_data = {k: v for k, v in data.items() if k in cls.__annotations__}
        return cls(**clean_data)


@dataclass
class GuildData:
    """服务器数据：存储该服务器下所有的轨道。"""
    tracks: Dict[str, Track] = field(default_factory=dict)  # key: role_id (str)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        tracks_data = data.get('tracks', {})
        valid_tracks = {}
        for k, v in tracks_data.items():
            try:
                valid_tracks[k] = Track.from_dict(v)
            except Exception:
                continue
        return cls(tracks=valid_tracks)


@dataclass
class JukeboxData:
    """根数据结构。"""
    guilds: Dict[str, GuildData] = field(default_factory=dict)  # key: guild_id (str)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        guilds_data = data.get('guilds', {})
        parsed_guilds = {k: GuildData.from_dict(v) for k, v in guilds_data.items()}
        return cls(guilds=parsed_guilds)