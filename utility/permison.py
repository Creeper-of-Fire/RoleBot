# utility/permissions.py

from __future__ import annotations
import typing
from discord import app_commands
import discord

from config import ADMIN_USER_IDS, SUPER_ADMIN_USER_IDS, ADMIN_ROLE_IDS


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