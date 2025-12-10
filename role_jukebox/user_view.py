# role_jukebox/user_view.py
from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import ui, ButtonStyle, Embed, Color

from role_jukebox.admin_view import PreviewBtn  # 复用预览按钮逻辑
from utility.helpers import safe_defer

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class UserJukeboxView(ui.View):
    """
    用户大厅：使用按钮网格展示可加入的轨道
    """

    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild

    async def show(self, interaction: discord.Interaction):
        """
        构建 Embed 和 View，并作为一个全新的消息发送出去。
        """
        self.clear_items()
        tracks = self.cog.manager.get_all_tracks(self.guild.id)

        # 过滤并计数
        valid_tracks = []
        for t in tracks:
            role = self.guild.get_role(t.role_id)
            if role and t.enabled:  # 只展示开启的
                valid_tracks.append((t, role))

        embed = Embed(
            title="🎶 身份组轮播大厅",
            description="点击下方的身份组按钮，即可加入或退出对应的外观轮播轨道！",
            color=Color.from_rgb(255, 105, 180)
        )

        if not valid_tracks:
            embed.description = "⚠️ 暂时没有开放的轮播活动，请稍后再来。"
        else:
            # 动态生成按钮
            for track, role in valid_tracks:
                # 检查用户是否已有该身份组，改变按钮样式
                has_role = role in interaction.user.roles if isinstance(interaction.user, discord.Member) else False
                style = ButtonStyle.success if has_role else ButtonStyle.secondary
                # 优先显示自定义名称
                display_name = track.name or role.name
                label = display_name[:80]

                self.add_item(UserTrackBtn(track, role, style, label))

        # 确保总是发送一个新消息
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=self, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)


class UserTrackBtn(ui.Button):
    def __init__(self, track, role, style, label: str):
        super().__init__(label=label, style=style, emoji="💿")
        self.track = track
        self.role = role

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)

        # 重新检查用户状态（防止缓存滞后）
        member = interaction.guild.get_member(interaction.user.id)
        has_role = self.role in member.roles if member else False

        # 优先显示自定义名称
        display_name = self.track.name or self.role.name

        embed = Embed(
            title=f"💿 {display_name}",
            color=self.role.color
        )

        mode_text = "随机切换" if self.track.mode == 'random' else "顺序切换"
        status_text = "✅ **已加入**" if has_role else "⬜ **未加入**"

        embed.description = (
            f"{status_text}\n\n"
            f"**频率**: 每 {self.track.interval_minutes} 分钟\n"
            f"**模式**: {mode_text}\n"
            f"**包含外观**: {len(self.track.presets)} 种"
        )

        # 使用一个新的 View 来显示操作选项，而不是以前的 Select
        view = JoinLeaveView(self.role, has_role, self.track, self.view.cog.manager)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class JoinLeaveView(ui.View):
    def __init__(self, role: discord.Role, has_role: bool, track, manager):
        super().__init__(timeout=60)
        self.role = role
        self.track = track
        self.manager = manager

        # 1. 核心动作按钮
        if has_role:
            self.add_item(ActionBtn("退出轨道", ButtonStyle.red, "📤", False))
        else:
            self.add_item(ActionBtn("加入轨道", ButtonStyle.green, "📥", True))

        # 2. 预览按钮 (复用 admin_view 中的逻辑)
        self.add_item(PreviewBtn(self.track, self.manager))


class ActionBtn(ui.Button):
    def __init__(self, label, style, emoji, is_join):
        super().__init__(label=label, style=style, emoji=emoji)
        self.is_join = is_join

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        view: JoinLeaveView = self.view
        try:
            if self.is_join:
                await interaction.user.add_roles(view.role, reason="Jukebox User Join")
                await interaction.followup.send(f"✅ 成功加入 **{view.role.name}**！", ephemeral=True)
            else:
                await interaction.user.remove_roles(view.role, reason="Jukebox User Leave")
                await interaction.followup.send(f"👋 成功退出 **{view.role.name}**。", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ 机器人权限不足，无法分配此身份组，请联系管理员。", ephemeral=True)
