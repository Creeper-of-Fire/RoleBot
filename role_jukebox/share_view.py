# role_jukebox/share_view.py
from __future__ import annotations

from typing import List

import discord
from discord import Embed, Color, ui, ButtonStyle

from role_jukebox.manager import RoleJukeboxManager

from role_jukebox.models import Track, TrackMode, DashboardMode


def create_dashboard_embed(guild: discord.Guild, all_tracks: List[Track], mode: DashboardMode) -> discord.Embed:
    """
    一个用于生成管理员和用户仪表盘 Embed 的共享函数。

    Args:
        guild (discord.Guild): 当前服务器对象。
        all_tracks (List[Track]): 所有的轨道数据。
        mode (DashboardMode): 决定了 Embed 的样式和内容。

    Returns:
        discord.Embed: 构建好的 Embed 对象。
    """
    if mode == DashboardMode.ADMIN:
        embed = Embed(title="🛠️ 轮播管理面板", color=Color.blurple())
        embed.description = (
            "使用 `/身份组轮播 添加预设` 指令来上传图片和添加预设。\n"
            "点击下方按钮管理对应轨道的详细配置。"
        )
    else:
        embed = Embed(
            title="🎶 身份组轮播大厅",
            description="点击下方的身份组按钮，即可加入或退出对应的外观轮播轨道！\n\n",
            color=Color.from_rgb(255, 105, 180)
        )

    valid_count = 0
    for t in all_tracks:
        role = guild.get_role(t.role_id)

        # 用户模式下，只展示有效且开启的轨道
        if mode == DashboardMode.USER and (not role or not t.enabled):
            continue

        # 管理员模式下，即使身份组失效也展示，但字段内容会不同（由调用方决定按钮状态）
        if not role:
            continue

        # 优先使用自定义名称，否则回退到身份组名称
        display_name = t.name or role.name
        mode_str = "随机" if t.mode == TrackMode.RANDOM else "顺序"
        summary_line = f"⏱️ {t.interval_minutes}m | 🔁 {mode_str} | 🎨 {len(t.presets)}个预设"

        if not t.presets:
            field_value = summary_line + "\n*暂无预设*"
        else:
            preset_lines = []
            presets_to_show = t.presets[:10]
            for i, p in enumerate(presets_to_show):
                # 截断过长的名称以保持排版整洁
                truncated_name = p.name if len(p.name) <= 25 else p.name[:24] + '…'
                preset_lines.append(f"`{i + 1}.` {truncated_name}")

            if len(t.presets) > 10:
                preset_lines.append(f"...等共 {len(t.presets)} 个")

            preset_list_str = "\n".join(preset_lines)
            field_value = f"{summary_line}\n{preset_list_str}"

        if mode == DashboardMode.ADMIN:
            status_emoji = "🟢" if t.enabled else "🔴"
            field_name = f"{status_emoji} {display_name}"
        else:
            field_name = f"💿 {display_name}"

        embed.add_field(name=field_name, value=field_value, inline=True)
        valid_count += 1

    # 根据是否有有效轨道更新描述
    if valid_count == 0:
        if mode == DashboardMode.ADMIN:
            embed.description += "\n\n⚠️ **当前没有活跃的轨道**"
        else:
            embed.description = "⚠️ 暂时没有开放的轮播活动，请稍后再来。"

    return embed


class PreviewBtn(ui.Button):
    def __init__(self, track: Track, manager: RoleJukeboxManager, **kwargs):
        super().__init__(label="预览效果", style=ButtonStyle.secondary, **kwargs, emoji="👀")
        self.track = track
        self.manager = manager

    async def callback(self, interaction: discord.Interaction):
        if not self.track.presets:
            return await interaction.response.send_message("❌ 暂无预设可预览", ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        presets_to_show = self.track.presets[:10]
        files, embeds = [], []

        try:
            for p in presets_to_show:
                try:
                    c = Color.from_str(p.color)
                except:
                    c = Color.default()

                emb = Embed(title=p.name, description=f"Color: `{p.color}`", color=c)

                if p.icon_filename:
                    data = await self.manager.get_icon_bytes(p.icon_filename)
                    if data:
                        import io
                        f = discord.File(io.BytesIO(data), filename=p.icon_filename)
                        emb.set_thumbnail(url=f"attachment://{p.icon_filename}")
                        files.append(f)
                embeds.append(emb)

            content = f"👀 **外观预览 (前{len(embeds)}个)**"
            if len(self.track.presets) > 10:
                content += f" (共 {len(self.track.presets)} 个)"

            await interaction.followup.send(content=content, embeds=embeds, files=files, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"❌ 预览生成失败: {str(e)}", ephemeral=True)
