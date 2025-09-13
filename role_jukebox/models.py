# jukebox/models.py
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any, TypeVar, Type

from timed_role.timer import UTC8

# 用于 from_dict 方法的泛型
T = TypeVar('T')


@dataclass
class Preset:
    """身份组外观预设。"""
    name: str
    color: str
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    icon_url: Optional[str] = None
    owner_id: Optional[int] = None

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        if 'icon' in data:
            data['icon_url'] = data.pop('icon')
        return cls(**data)


@dataclass
class PendingRequest:
    """一个排队请求。"""
    user_id: int
    preset: Preset

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        # 特殊处理嵌套的 Preset 对象
        preset_data = data.get('preset')
        if preset_data:
            data['preset'] = Preset.from_dict(preset_data)
        return cls(**data)


@dataclass
class QueueState:
    """单个点歌队列的状态。"""
    current_preset_uuid: Optional[str] = None
    unlock_timestamp: Optional[str] = None  # 存储ISO格式的字符串
    pending_requests: List[PendingRequest] = field(default_factory=list)

    @property
    def is_locked(self) -> bool:
        return self.unlock_timestamp is not None and datetime.fromisoformat(self.unlock_timestamp) > datetime.now(UTC8)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        if 'status' in data:
            del data['status']

        if 'current_preset' in data:
            del data['current_preset']

        if 'controller_id' in data:
            del data['controller_id']

        # 特殊处理嵌套的 PendingRequest 列表
        requests_data = data.get('pending_requests', [])
        data['pending_requests'] = [PendingRequest.from_dict(req) for req in requests_data]
        return cls(**data)


@dataclass
class GuildData:
    """单个服务器的所有点歌机数据。"""
    general_presets: List[Preset] = field(default_factory=list)
    queues: Dict[str, QueueState] = field(default_factory=dict)  # key: role_id_str

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        presets_data = data.get('general_presets', [])
        queues_data = data.get('queues', {})

        data['general_presets'] = [Preset.from_dict(p) for p in presets_data]
        data['queues'] = {k: QueueState.from_dict(v) for k, v in queues_data.items()}
        return cls(**data)


@dataclass
class UserData:
    """单个用户的专属数据。"""
    custom_presets: List[Preset] = field(default_factory=list)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        presets_data = data.get('custom_presets', [])
        data['custom_presets'] = [Preset.from_dict(p) for p in presets_data]
        return cls(**data)


@dataclass
class JukeboxData:
    """点歌机功能的顶层数据容器。"""
    guilds: Dict[str, GuildData] = field(default_factory=dict)  # key: guild_id_str
    users: Dict[str, UserData] = field(default_factory=dict)  # key: user_id_str

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        guilds_data = data.get('guilds', {})
        users_data = data.get('users', {})

        data['guilds'] = {k: GuildData.from_dict(v) for k, v in guilds_data.items()}
        data['users'] = {k: UserData.from_dict(v) for k, v in users_data.items()}
        return cls(**data)


@dataclass
class RotationAction:
    """用于后台任务的轮换操作数据类。"""
    guild_id: int
    role_id: int
    new_preset: Preset
    requester_id: int
