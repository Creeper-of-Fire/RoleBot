# utility/permissions.py

from __future__ import annotations
import typing
from enum import StrEnum
from typing import Union

from discord import app_commands
import discord

from config import (
    ADMIN_USER_IDS,
    SUPER_ADMIN_USER_IDS,
    ADMIN_ROLE_IDS,
    CAPABILITIES,
    MAINTAINER_USER_IDS,
)


# --- Capability 枚举（这里 re-export，源头是 config.py） ---
# config.py 已经定义了一份 Capability；这边再导一份主要是为了写装饰器时方便：
#   from utility.permison import requires_capability, Capability
# config 那边是「配置数据」，util 这里是「装饰器入口」，两边通过同一个 enum 实例绑定。
from config import Capability


# --- 权限检查函数 ---

def is_super_admin_check(interaction: discord.Interaction) -> bool:
    """检查用户是否为超级管理员。"""
    # 直接检查用户ID是否在超级管理员集合中
    return interaction.user.id in SUPER_ADMIN_USER_IDS


def is_admin_check(interaction: discord.Interaction) -> bool:
    """检查用户是否为管理员或超级管理员。"""
    # 1. 超级管理员自动拥有所有管理员权限
    if is_super_admin_check(interaction):
        return True

    # 2. 检查用户是否在指定的管理员用户ID列表中
    if interaction.user.id in ADMIN_USER_IDS:
        return True

    # 3. 检查用户是否拥有任何一个指定的管理员角色
    # interaction.user 是一个 Member 对象，有 roles 属性
    if isinstance(interaction.user, discord.Member):
        user_role_ids = {role.id for role in interaction.user.roles}
        # 检查两个集合是否有交集
        if not user_role_ids.isdisjoint(ADMIN_ROLE_IDS):
            return True

    return False


def _has_capability(user: typing.Union[discord.User, discord.Member], capability: Capability) -> bool:
    """检查用户是否拥有指定 capability。

    优先级：
        1. 用户在 MAINTAINER_USER_IDS → 直接放行（覆盖一切）
        2. 用户拥有 capability 对应身份组集合中的任一个 → 放行
    """
    # 1. 维护者通道
    if user.id in MAINTAINER_USER_IDS:
        return True

    # 2. 身份组通道
    if isinstance(user, discord.Member):
        role_ids = {role.id for role in user.roles}
        allowed_roles = CAPABILITIES.get(capability, set())
        if not role_ids.isdisjoint(allowed_roles):
            return True

    return False


# --- App Command 装饰器 ---
# 这些是我们将用在命令上的实际装饰器。

def is_super_admin():
    """
    一个 app_commands.check 装饰器，用于验证命令使用者是否为超级管理员。
    如果检查失败，会自动向用户发送一条预设的错误消息。
    """

    async def predicate(interaction: discord.Interaction) -> bool:
        if is_super_admin_check(interaction):
            return True
        else:
            # 发送一个私密的错误消息
            await interaction.response.send_message(
                "❌ **权限不足**\n你没有权限执行此操作。此操作仅限**超级管理员**。",
                ephemeral=True
            )
            return False

    return app_commands.check(predicate)


def is_admin():
    """
    一个 app_commands.check 装饰器，用于验证命令使用者是否为管理员或更高级别。
    如果检查失败，会自动向用户发送一条预设的错误消息。
    """

    async def predicate(interaction: discord.Interaction) -> bool:
        if is_admin_check(interaction):
            return True
        else:
            await interaction.response.send_message(
                "❌ **权限不足**\n你没有权限执行此操作。此操作需要**管理员**权限。",
                ephemeral=True
            )
            return False

    return app_commands.check(predicate)


# --- 示例：如果你想创建一个只允许特定角色使用的检查器 ---
def has_role(role_id: int):
    """
    一个更通用的检查器，用于验证用户是否拥有特定角色。
    """

    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            # 如果在私聊中使用，则 interaction.user 不是 Member 对象
            return False

        user_role_ids = {role.id for role in interaction.user.roles}
        if role_id in user_role_ids:
            return True

        # 可以在这里添加错误消息，或者让默认的 CheckFailure 处理
        return False

    return app_commands.check(predicate)


# --- 核心新增：requires_capability（双作用装饰器） ---
def requires_capability(capability: Capability):
    """装饰器：一个装饰器，两个作用

    作用 1（执行拦截）：命令被调用时检查用户身份组 / 维护者身份
    作用 2（API 注入）：在 Command 对象上挂 ``_required_capability`` 标记，
                      bot 启动后由 ``sync_all_command_permissions`` 扫描并
                      把身份组覆盖写入 Discord API

    必须挂在 ``@xxx_group.command(...)`` 之外（装饰 Command 对象），
    这样能同时挂上 check 和标记。例如：

        @requires_capability(Capability.MANAGE_BLACKLIST)
        @activity_group.command(name="刷屏黑名单-添加", ...)
        async def blacklist_add(...): ...
    """
    async def predicate(interaction: discord.Interaction) -> bool:
        if _has_capability(interaction.user, capability):
            return True
        await interaction.response.send_message(
            "❌ **权限不足**\n你没有权限执行此操作。",
            ephemeral=True,
        )
        return False

    def decorator(
        func_or_cmd: Union[app_commands.Command, app_commands.ContextMenu, typing.Callable],
    ):
        # 作用 1：app_commands.check 会自动识别 Command/ContextMenu/函数
        wrapped = app_commands.check(predicate)(func_or_cmd)
        # 作用 2：挂 capability 标记，供 sync 协程扫描
        wrapped._required_capability = capability
        return wrapped

    return decorator