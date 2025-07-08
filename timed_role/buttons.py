from __future__ import annotations

import typing

import discord
from discord import ui

from timed_role.timed_role_view import TimedRoleManageView
from utility.helpers import safe_defer, try_get_member

if typing.TYPE_CHECKING:
    from timed_role.cog import TimedRolesCog


class TimedRolePanelButton(ui.Button):
    """打开限时身份组管理面板的按钮。"""

    def __init__(self, cog: TimedRolesCog):
        super().__init__(label="限时身份组", style=discord.ButtonStyle.primary, custom_id="open_timed_role_panel", emoji="⏳")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户创建并发送一个限时身份组管理面板。"""
        await safe_defer(interaction, thinking=True)
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        view = TimedRoleManageView(self.cog, member,interaction.guild)
        await view._rebuild_view()
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=True)
