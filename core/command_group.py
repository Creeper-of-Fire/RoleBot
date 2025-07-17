from typing import Self, Optional

import discord
from discord import app_commands

import config


class RoleBotMainGroup(app_commands.Group):
    _group: Optional[Self] = None

    def __init__(self, *args, **kwargs):
        super().__init__(
            name=f"{config.COMMAND_GROUP_NAME}",
            description=f"{config.COMMAND_GROUP_NAME}的管理员指令，需要`管理身份组`的权限。",
            guild_ids=[gid for gid in config.GUILD_IDS],
            default_permissions=discord.Permissions(manage_roles=True),
            *args,
            **kwargs
        )

    @classmethod
    def getGroup(cls) -> Self:
        if not cls._group:
            cls._group = cls()
        return cls._group

class RoleBotGroup(app_commands.Group):
    _group: Optional[Self] = None

    def __init__(self, *args, **kwargs):
        super().__init__(
            name=f"{config.COMMAND_GROUP_NAME}-通用",
            description=f"{config.COMMAND_GROUP_NAME}的通用机器人指令，只需要可以查看消息就能使用",
            guild_ids=[gid for gid in config.GUILD_IDS],
            default_permissions=discord.Permissions(read_messages=True),
            *args,
            **kwargs
        )

    @classmethod
    def getGroup(cls) -> Self:
        if not cls._group:
            cls._group = cls()
        return cls._group
