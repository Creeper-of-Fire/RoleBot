# role_jukebox/admin_view.py
from __future__ import annotations

import discord
from discord import ui, ButtonStyle, Embed, Color, SelectOption
from typing import TYPE_CHECKING, Optional

from role_jukebox.models import Track, Preset
from utility.paginated_view import PaginatedView

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class AdminDashboardView(ui.View):
    """一级面板：列表"""

    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild

    async def refresh(self, interaction: Optional[discord.Interaction] = None):
        self.clear_items()
        tracks = self.cog.manager.get_all_tracks(self.guild.id)

        embed = Embed(title="🛠️ 轮播管理面板", color=Color.blurple())
        embed.description = "使用 `/jukebox 添加预设` 指令来上传图片和添加预设。\n以下是当前活跃的轨道："

        opts = []
        for t in tracks:
            r = self.guild.get_role(t.role_id)
            name = r.name if r else f"失效ID {t.role_id}"

            status = "🟢" if t.enabled else "🔴"
            embed.add_field(
                name=f"{status} {name}",
                value=f"{t.interval_minutes}分钟 | {len(t.presets)}个预设 | {t.mode}",
                inline=False
            )

            if r:
                opts.append(SelectOption(label=name, value=str(t.role_id), emoji="⚙️"))

        self.add_item(CreateButton())
        if opts:
            self.add_item(TrackSelect(opts))

        if interaction:
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=self)
            else:
                await interaction.response.send_message(embed=embed, view=self, ephemeral=True)


class CreateButton(ui.Button):
    def __init__(self):
        super().__init__(label="新建轨道", style=ButtonStyle.green, row=1)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateTrackModal(self.view))


class CreateTrackModal(ui.Modal, title="输入身份组ID"):
    rid = ui.TextInput(label="身份组ID", required=True)

    def __init__(self, parent):
        super().__init__()
        self.parent = parent

    async def on_submit(self, interaction: discord.Interaction):
        try:
            val = int(self.rid.value)
            role = interaction.guild.get_role(val)
            if not role: return await interaction.response.send_message("❌ 找不到身份组", ephemeral=True)
            await self.parent.cog.manager.create_track(interaction.guild_id, val)
            await interaction.response.send_message(f"✅ 轨道 {role.name} 已创建", ephemeral=True)
            await self.parent.refresh(interaction)
        except ValueError:
            await interaction.response.send_message("❌ ID格式错误", ephemeral=True)


class TrackSelect(ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="选择轨道进行管理...", options=options)

    async def callback(self, interaction: discord.Interaction):
        role_id = int(self.values[0])
        view = TrackDetailView(self.view.cog, self.view.guild, role_id, self.view)
        await view.refresh(interaction)


# --- 二级面板：详情 ---

class TrackDetailView(PaginatedView):
    def __init__(self, cog, guild, role_id, parent):
        self.cog = cog
        self.guild = guild
        self.role_id = role_id
        self.parent = parent
        self.track = None
        super().__init__(all_items_provider=self._get_data, items_per_page=5)

    async def _get_data(self):
        self.track = self.cog.manager.get_track(self.guild.id, self.role_id)
        return self.track.presets if self.track else []

    async def _rebuild_view(self):
        self.clear_items()
        self.track = self.cog.manager.get_track(self.guild.id, self.role_id)
        if not self.track:
            self.embed = Embed(title="❌ 轨道已删除")
            self.add_item(BackButton(self.parent))
            return

        r = self.guild.get_role(self.role_id)
        self.embed = Embed(title=f"⚙️ {r.name if r else 'Unknown'}", color=r.color if r else Color.default())
        self.embed.description = (
            f"**添加预设**: 请使用 `/jukebox 添加预设` 指令\n"
            f"**状态**: {'✅' if self.track.enabled else '⏸️'} | **模式**: {self.track.mode}\n"
            f"**间隔**: {self.track.interval_minutes} min"
        )

        items = self.get_page_items()
        if items:
            txt = ""
            for i, p in enumerate(items):
                idx = (self.page * self.items_per_page) + i + 1
                icon = "🖼️" if p.icon_filename else "⚪"
                txt += f"`#{idx}` **{p.name}** {icon} ({p.color})\n"
            self.embed.add_field(name="预设列表", value=txt)
            self.add_item(DeleteSelect(items))
        else:
            self.embed.add_field(name="空池子", value="请使用指令添加预设")

        # 控制按钮
        self.add_item(ToggleBtn(self.track.enabled))
        self.add_item(ModeBtn(self.track.mode))
        self.add_item(IntervalBtn())
        self.add_item(DelTrackBtn())
        self.add_item(BackButton(self.parent))

        self._add_pagination_buttons(row=4)

    async def refresh(self, interaction):
        await self.update_view(interaction)


# --- 简单按钮组件 ---

class BackButton(ui.Button):
    def __init__(self, parent):
        super().__init__(label="返回", style=ButtonStyle.secondary, row=3)
        self.p = parent

    async def callback(self, itx): await self.p.refresh(itx)


class ToggleBtn(ui.Button):
    def __init__(self, on):
        super().__init__(label="暂停" if on else "开启", style=ButtonStyle.danger if on else ButtonStyle.success, row=1)

    async def callback(self, itx):
        view = self.view
        await view.cog.manager.update_track(view.guild.id, view.role_id, enabled=not view.track.enabled)
        await view.refresh(itx)


class ModeBtn(ui.Button):
    def __init__(self, mode):
        super().__init__(label="切为随机" if mode == 'sequence' else "切为顺序", style=ButtonStyle.primary, row=1)

    async def callback(self, itx):
        view = self.view
        new = 'random' if view.track.mode == 'sequence' else 'sequence'
        await view.cog.manager.update_track(view.guild.id, view.role_id, mode=new)
        await view.refresh(itx)


class IntervalBtn(ui.Button):
    def __init__(self):
        super().__init__(label="间隔", style=ButtonStyle.secondary, row=1)

    async def callback(self, itx):
        await itx.response.send_modal(IntervalModal(self.view))


class DelTrackBtn(ui.Button):
    def __init__(self):
        super().__init__(label="删除轨道", style=ButtonStyle.danger, row=3)

    async def callback(self, itx):
        view = self.view
        await view.cog.manager.delete_track(view.guild.id, view.role_id)
        await itx.response.send_message("🗑️ 已删除", ephemeral=True)
        await view.parent.refresh(itx)


class DeleteSelect(ui.Select):
    def __init__(self, items):
        opts = [SelectOption(label=p.name, value=p.uuid, emoji="🗑️") for p in items]
        super().__init__(placeholder="删除预设...", options=opts, row=0)

    async def callback(self, itx):
        view = self.view
        await view.cog.manager.remove_preset(view.guild.id, view.role_id, self.values[0])
        await itx.response.defer()
        await view.refresh(itx)


class IntervalModal(ui.Modal, title="设置间隔"):
    val = ui.TextInput(label="分钟")

    def __init__(self, p):
        super().__init__()
        self.p = p

    async def on_submit(self, itx):
        try:
            v = int(self.val.value)
            if v < 1: raise ValueError
            await self.p.cog.manager.update_track(self.p.guild.id, self.p.role_id, interval=v)
            await itx.response.send_message("✅", ephemeral=True)
            await self.p.refresh(itx)
        except:
            await itx.response.send_message("❌", ephemeral=True)