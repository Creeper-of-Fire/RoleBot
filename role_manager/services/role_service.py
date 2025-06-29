from __future__ import annotations

from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from role_manager.cog import RoleManagerCog


async def update_member_roles(
    cog: RoleManagerCog,
    member: discord.Member,
    to_add_ids: set[int],
    to_remove_ids: set[int],
    reason: str
) -> None:
    """批量更新成员的身份组，处理实际的添加和移除操作。"""
    guild = member.guild
    if to_add_ids:
        roles_to_add = [r for r in guild.roles if r.id in to_add_ids]
        if roles_to_add:
            await member.add_roles(*roles_to_add, reason=reason)
            cog.logger.debug(f"为 {member.display_name} 添加了 {len(roles_to_add)} 个身份组。")

    if to_remove_ids:
        roles_to_remove = [r for r in guild.roles if r.id in to_remove_ids]
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason=reason)
            cog.logger.debug(f"为 {member.display_name} 移除了 {len(roles_to_remove)} 个身份组。")