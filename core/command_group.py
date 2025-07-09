from typing import Self, Optional

import discord
from discord import app_commands

import config


class RoleBotGroup(app_commands.Group):
    _role_group: Optional[Self] = None

    def __init__(self, *args, **kwargs):
        super().__init__(
            name=f"{config.COMMAND_GROUP_NAME}_通用",
            description=f"{config.COMMAND_GROUP_NAME}的通用机器人指令，只需要可以查看消息就能使用",
            guild_ids=[gid for gid in config.GUILD_IDS],
            default_permissions=discord.Permissions(read_messages=True),
            *args,
            **kwargs
        )

    @classmethod
    def getRoleGroup(cls) -> Self:
        if not cls._role_group:
            cls._role_group = RoleBotGroup()
        return cls._role_group
