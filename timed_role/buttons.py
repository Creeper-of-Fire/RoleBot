from __future__ import annotations

import typing

import discord
from discord import ui

import config
from utility.helpers import safe_defer, try_get_member, format_duration_hms
from timed_role.timer import DAILY_LIMIT_SECONDS
from timed_role.timed_role_view import TimedRoleManageView

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
        view = TimedRoleManageView(self.cog, member)
        await view._rebuild_view()
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=True)


class QueryTimeButton(ui.Button):
    """查询用户限时身份组剩余时间的按钮。"""

    def __init__(self, cog: TimedRolesCog):
        super().__init__(label="查询我的时间", style=discord.ButtonStyle.secondary, custom_id="query_time_button", emoji="⏱️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，查询并显示用户的限时身份组使用情况。"""
        await safe_defer(interaction, thinking=True)
        member, guild = interaction.user, interaction.guild
        remaining_seconds = self.cog.timed_role_data_manager.get_remaining_seconds(member.id, guild.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_guild_data = self.cog.timed_role_data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        embed = discord.Embed(title=f"⏱️ 你在「{guild.name}」的时间使用情况", color=discord.Color.blue())
        embed.add_field(name="今日已用时长", value=format_duration_hms(used_seconds), inline=False)
        embed.add_field(name="今日剩余时长", value=format_duration_hms(remaining_seconds), inline=False)
        if current_role_ids:
            roles_text = ", ".join([f"**{guild.get_role(rid).name}**" for rid in current_role_ids if guild.get_role(rid)])
            embed.add_field(name="当前持有", value=f"你当前正在使用 {roles_text}，计时进行中。", inline=False)
        else:
            embed.add_field(name="当前持有", value="你当前未持有任何限时身份组。", inline=False)
        reset_hour = config.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
        embed.set_footer(text=f"每日UTC+8 {reset_hour}点重置时长。")
        await interaction.followup.send(embed=embed, ephemeral=True)


class ReturnTimedRoleButton(ui.Button):
    """一键归还所有限时身份组的按钮。"""

    def __init__(self, cog: TimedRolesCog):
        super().__init__(label="一键归还限时组", style=discord.ButtonStyle.red, custom_id="return_timed_role_button", emoji="↩️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户移除所有限时身份组并结算使用时间。"""
        await safe_defer(interaction, thinking=True)
        member, guild = interaction.user, interaction.guild
        user_guild_data = self.cog.timed_role_data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        if not current_role_ids:
            await interaction.followup.send(f"你在 **{guild.name}** 当前没有可归还的限时身份组。", ephemeral=True)
            return
        roles_to_remove = [role for role in member.roles if role.id in current_role_ids]
        if roles_to_remove: await member.remove_roles(*roles_to_remove, reason="用户一键归还限时身份组")
        used_seconds = await self.cog.timed_role_data_manager.return_timed_roles(member.id, guild.id)
        remaining_seconds = self.cog.timed_role_data_manager.get_remaining_seconds(member.id, guild.id)
        roles_text = ", ".join([f"**{r.name}**" for r in roles_to_remove]) if roles_to_remove else "已归还的身份组"
        await interaction.followup.send(
            f"✅ 你已归还服务器 **{guild.name}** 的限时组: {roles_text}。\n本次使用 {format_duration_hms(int(used_seconds))}。\n今天在本服剩余可用时间：{format_duration_hms(remaining_seconds)}。",
            ephemeral=True)
