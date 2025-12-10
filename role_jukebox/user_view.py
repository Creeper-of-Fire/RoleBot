# role_jukebox/user_view.py
from __future__ import annotations

import discord
from discord import ui, ButtonStyle, Embed, Color
from typing import TYPE_CHECKING
from utility.helpers import safe_defer

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class UserJukeboxView(ui.View):
    """
    用户大厅：展示所有可用的轮播轨道。
    """

    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild

    async def refresh(self, interaction: discord.Interaction):
        self.clear_items()
        tracks = self.cog.manager.get_all_tracks(self.guild.id)

        # 过滤掉已失效（身份组不存在）的轨道
        valid_tracks = []
        for t in tracks:
            if self.guild.get_role(t.role_id):
                valid_tracks.append(t)

        embed = Embed(
            title="🎶 身份组轮播大厅",
            description="加入一个轨道，机器人会自动定期为你更换炫酷的身份组外观！",
            color=Color.from_rgb(255, 105, 180)
        )

        if not valid_tracks:
            embed.description = "⚠️ 暂无开放的轮播轨道。"
        else:
            options = []
            for track in valid_tracks:
                role = self.guild.get_role(track.role_id)

                # 预览前3个预设名
                preview = [p.name for p in track.presets[:3]]
                if len(track.presets) > 3: preview.append("...")
                preview_str = ", ".join(preview) if preview else "暂无预设"

                field_name = f"💿 {role.name}"
                field_val = (f"⏱️ 每{track.interval_minutes}分钟 | 🎨 包含: {preview_str}\n"
                             f"🔁 {'随机' if track.mode == 'random' else '顺序'}")

                embed.add_field(name=field_name, value=field_val, inline=False)

                options.append(discord.SelectOption(
                    label=role.name, value=str(role.id), description="点击查看详情或加入", emoji="💿"
                ))

            self.add_item(TrackSelect(options))

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)


class TrackSelect(ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="选择一个轨道...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        role_id = int(self.values[0])
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.followup.send("❌ 身份组已失效。", ephemeral=True)

        has_role = role in interaction.user.roles

        embed = Embed(title=f"💿 {role.name}", description=f"您当前{'**已加入**' if has_role else '**未加入**'}此轨道。", color=role.color)
        view = JoinLeaveView(role, has_role)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class JoinLeaveView(ui.View):
    def __init__(self, role: discord.Role, has_role: bool):
        super().__init__(timeout=60)
        self.role = role
        if has_role:
            self.add_item(ActionBtn("退出轨道", ButtonStyle.red, "📤", False))
        else:
            self.add_item(ActionBtn("加入轨道", ButtonStyle.green, "📥", True))


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
            await interaction.followup.send("❌ 机器人权限不足，无法分配此身份组。", ephemeral=True)