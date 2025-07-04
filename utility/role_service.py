from __future__ import annotations

from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from utility.feature_cog import FeatureCog


async def batch_update_member_roles(
    cog: 'FeatureCog',
    guild: discord.Guild,
    members_to_update: dict[int, dict[str, list[int]]],
    reason: str
) -> None:
    """批量更新多个成员的身份组。"""
    for member_id, roles in members_to_update.items():
        member = guild.get_member(member_id)
        if not member:
            continue
        await update_member_roles(
            cog,
            member,
            to_add_ids=set(roles.get("add", [])),
            to_remove_ids=set(roles.get("remove", [])),
            reason=reason
        )

async def update_member_roles(
    cog: 'FeatureCog',
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