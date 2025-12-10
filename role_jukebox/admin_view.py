# role_jukebox/admin_view.py
from __future__ import annotations

import discord
from discord import ui, ButtonStyle, Embed, Color, SelectOption
from typing import TYPE_CHECKING, Optional

from role_jukebox.models import Track
from utility.paginated_view import PaginatedView
from utility.views import ConfirmationView

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog
    from role_jukebox.manager import RoleJukeboxManager


# =============================================================================
# 一级面板：主仪表盘
# =============================================================================

class AdminDashboardView(ui.View):
    """一级面板：使用按钮展示轨道列表"""

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

        embed = Embed(title="🛠️ 轮播管理面板", color=Color.blurple())
        embed.description = (
            "使用 `/身份组轮播 添加预设` 指令来上传图片和添加预设。\n"
            "点击下方按钮管理对应轨道的详细配置。"
        )

        valid_count = 0
        for t in tracks:
            r = self.guild.get_role(t.role_id)
            # 优先使用自定义名称，否则回退到身份组名称
            display_name = t.name or (r.name if r else f"失效ID {t.role_id}")
            if not r:
                self.add_item(TrackBtn(t.role_id, display_name, ButtonStyle.secondary, disabled=True))
                continue

            valid_count += 1
            status_emoji = "🟢" if t.enabled else "🔴"
            btn_style = ButtonStyle.success if t.enabled else ButtonStyle.secondary
            label = f"{display_name[:10]}"

            self.add_item(TrackBtn(t.role_id, label, btn_style, emoji=status_emoji))

            mode_str = "随机" if t.mode == 'random' else "顺序"
            embed.add_field(
                name=f"{status_emoji} {display_name}",
                value=f"⏱️ {t.interval_minutes}m | 🎨 {len(t.presets)}个 | 🔁 {mode_str}",
                inline=True
            )

        if valid_count == 0:
            embed.description += "\n\n⚠️ **当前没有活跃的轨道**"

        self.add_item(CreateButton())

        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=self, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)


class TrackBtn(ui.Button):
    def __init__(self, role_id: int, label: str, style: ButtonStyle, emoji=None, disabled=False):
        super().__init__(label=label, style=style, emoji=emoji, disabled=disabled)
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        detail_view = TrackDetailView(self.view.cog, self.view.guild, self.role_id, self.view)
        await detail_view.start(interaction, ephemeral=True)


class CreateButton(ui.Button):
    def __init__(self):
        super().__init__(label="新建轨道", style=ButtonStyle.primary, emoji="➕", row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateTrackModal(self.view))


class CreateTrackModal(ui.Modal, title="输入身份组ID"):
    rid = ui.TextInput(label="身份组ID", placeholder="开启开发者模式右键复制ID", required=True)

    def __init__(self, parent_view: AdminDashboardView):
        super().__init__()
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            val = int(self.rid.value)
            role = interaction.guild.get_role(val)
            if not role:
                return await interaction.followup.send("❌ 找不到身份组，请检查ID", ephemeral=True)

            await self.parent_view.cog.manager.create_track(interaction.guild_id, val)
            await interaction.followup.send(f"✅ 轨道 **{role.name}** 已创建", ephemeral=True)

            new_dashboard = AdminDashboardView(self.parent_view.cog, interaction.guild)
            await new_dashboard.show(interaction)
        except ValueError:
            await interaction.followup.send("❌ ID格式错误，必须是数字", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ 操作失败: {e}", ephemeral=True)


# =============================================================================
# 二级面板：轨道详情与编辑
# =============================================================================

class TrackDetailView(PaginatedView):
    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild, role_id: int, parent_view: AdminDashboardView):
        self.cog = cog
        self.guild = guild
        self.role_id = role_id
        self.parent_view = parent_view
        self.track: Optional[Track] = None
        super().__init__(all_items_provider=self._get_data, items_per_page=10)

    async def _get_data(self):
        self.track = self.cog.manager.get_track(self.guild.id, self.role_id)
        return self.track.presets if self.track else []

    async def _rebuild_view(self):
        self.clear_items()
        # 数据在 _update_data 中已经获取，这里直接用

        if not self.track:
            self.embed = Embed(title="❌ 轨道已不存在")
            self.add_item(BackButton(self.parent_view))
            return

        role = self.guild.get_role(self.role_id)
        role_name = role.name if role else "未知身份组"
        role_color = role.color if role else Color.default()

        # 优先使用自定义名称
        display_name = self.track.name or role_name

        self.embed = Embed(title=f"⚙️ 配置轨道: {display_name}", color=role_color)
        status = "✅ 运行中" if self.track.enabled else "⏸️ 已暂停"
        mode = "🔀 随机播放" if self.track.mode == 'random' else "🔁 顺序播放"

        self.embed.description = (
            f"**状态**: {status}\n"
            f"**模式**: {mode}\n"
            f"**间隔**: {self.track.interval_minutes} 分钟\n"
            f"----------------"
        )
        self.embed.set_footer(text="提示: 使用 /身份组轮播 添加预设 来增加更多外观")

        items = self.get_page_items()
        if items:
            desc_lines = []
            for i, p in enumerate(items):
                idx = (self.page * self.items_per_page) + i + 1
                icon_mark = "🖼️" if p.icon_filename else "⚪"
                desc_lines.append(f"`{idx}.` **{p.name}** {icon_mark} `Hex:{p.color}`")
            self.embed.add_field(name=f"预设列表 (第 {self.page + 1} 页)", value="\n".join(desc_lines), inline=False)
            self.add_item(DeleteSelect(items))
        else:
            self.embed.add_field(name="预设列表", value="*暂无预设，请添加*", inline=False)

        # Row 1: 核心控制
        self.add_item(ToggleBtn(self.track.enabled))
        self.add_item(ModeBtn(self.track.mode))
        self.add_item(IntervalBtn(self.track.interval_minutes))

        # Row 2: 功能按钮
        self.add_item(RenameBtn())  # <-- 新增重命名按钮
        self.add_item(PreviewBtn(self.track, self.cog.manager))

        # Row 3: 危险/导航操作
        self.add_item(DelTrackBtn())
        self.add_item(BackButton(self.parent_view))

        # Row 4: 翻页
        self._add_pagination_buttons(row=4)

    async def refresh_and_edit(self, interaction: discord.Interaction):
        """在当前视图内更新（编辑）消息"""
        await self.update_view(interaction)


# =============================================================================
# 详情面板的组件
# =============================================================================

class BackButton(ui.Button):
    def __init__(self, parent_view: AdminDashboardView):
        super().__init__(label="返回列表", style=ButtonStyle.secondary, row=2)
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        new_dashboard = AdminDashboardView(self.parent_view.cog, interaction.guild)
        await new_dashboard.show(interaction)


class DelTrackBtn(ui.Button):
    def __init__(self):
        super().__init__(label="删除轨道", style=ButtonStyle.danger, row=3, emoji="🗑️")

    async def callback(self, interaction: discord.Interaction):
        view: TrackDetailView = self.view

        # 1. 创建确认视图并发起确认请求
        confirmation_view = ConfirmationView(author=interaction.user)
        confirm_msg_content = "⚠️ **你确定要删除这个轨道吗？**\n此操作会一并删除所有关联的预设和图标，且无法恢复。"

        await interaction.response.send_message(confirm_msg_content, view=confirmation_view, ephemeral=True)
        confirmation_view.message = await interaction.original_response()

        # 2. 等待用户响应
        await confirmation_view.wait()

        # 3. 根据用户响应执行操作
        if confirmation_view.value is True:
            # 用户点击了“确认”
            await view.cog.manager.delete_track(view.guild.id, view.role_id)

            # 更新确认消息，告知用户操作已完成
            await confirmation_view.message.edit(content="✅ 轨道已成功删除。", view=None)

            # 显示一个新的主面板
            new_dashboard = AdminDashboardView(view.cog, interaction.guild)
            await new_dashboard.show(interaction)

        elif confirmation_view.value is False:
            # 用户点击了“取消”
            await confirmation_view.message.edit(content="👍 操作已取消。", view=None)

        # 如果是超时(value is None)，on_timeout 已经处理了消息编辑


class ToggleBtn(ui.Button):
    def __init__(self, on: bool):
        super().__init__(label="暂停轮播" if on else "开启轮播", style=ButtonStyle.danger if on else ButtonStyle.success, row=1, emoji="⏯️")

    async def callback(self, itx: discord.Interaction):
        view: TrackDetailView = self.view
        await view.cog.manager.update_track(view.guild.id, view.role_id, enabled=not view.track.enabled)
        await view.refresh_and_edit(itx)


class ModeBtn(ui.Button):
    def __init__(self, mode: str):
        super().__init__(label="切换为随机" if mode == 'sequence' else "切换为顺序", style=ButtonStyle.primary, row=1, emoji="🔀" if mode == 'sequence' else "🔁")

    async def callback(self, itx: discord.Interaction):
        view: TrackDetailView = self.view
        new_mode = 'random' if view.track.mode == 'sequence' else 'sequence'
        await view.cog.manager.update_track(view.guild.id, view.role_id, mode=new_mode)
        await view.refresh_and_edit(itx)


class IntervalBtn(ui.Button):
    def __init__(self, current_interval: int):
        super().__init__(label=f"间隔 ({current_interval}m)", style=ButtonStyle.secondary, row=1, emoji="⏱️")

    async def callback(self, itx: discord.Interaction):
        await itx.response.send_modal(IntervalModal(self.view))


class RenameBtn(ui.Button):
    def __init__(self):
        super().__init__(label="重命名", style=ButtonStyle.secondary, row=2, emoji="✏️")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RenameTrackModal(self.view))


class RenameTrackModal(ui.Modal, title="重命名轨道"):
    name_input = ui.TextInput(label="轨道新名称", placeholder="留空则恢复为身份组默认名称", required=False, max_length=100)

    def __init__(self, parent_view: TrackDetailView):
        super().__init__()
        self.parent_view = parent_view
        # 将当前自定义名称填入输入框作为默认值
        if self.parent_view.track and self.parent_view.track.name:
            self.name_input.default = self.parent_view.track.name

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self.name_input.value.strip()
        # 如果用户输入为空，则将名称设为 None，以使用身份组默认名
        await self.parent_view.cog.manager.update_track(
            self.parent_view.guild.id,
            self.parent_view.role_id,
            name=new_name if new_name else None
        )
        await self.parent_view.refresh_and_edit(interaction)


class PreviewBtn(ui.Button):
    def __init__(self, track: Track, manager: RoleJukeboxManager):
        super().__init__(label="预览效果", style=ButtonStyle.secondary, row=2, emoji="👀")
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


class DeleteSelect(ui.Select):
    def __init__(self, items):
        opts = [SelectOption(label=p.name[:25], value=p.uuid, emoji="🗑️", description=p.color) for p in items]
        super().__init__(placeholder="选择要删除的预设...", options=opts, row=0)

    async def callback(self, itx: discord.Interaction):
        view: TrackDetailView = self.view
        uuid_to_delete = self.values[0]

        # 查找要删除的预设以获取其名称
        preset_to_delete = next((p for p in view.track.presets if p.uuid == uuid_to_delete), None)
        if not preset_to_delete:
            await itx.response.send_message("❌ 错误：找不到要删除的预设。", ephemeral=True)
            return

        # --- 加上二次确认 ---
        confirmation_view = ConfirmationView(author=itx.user)
        confirm_msg_content = f"⚠️ **你确定要删除预设【{preset_to_delete.name}】吗？**\n此操作无法恢复。"

        await itx.response.send_message(confirm_msg_content, view=confirmation_view, ephemeral=True)
        confirmation_view.message = await itx.original_response()

        await confirmation_view.wait()

        if confirmation_view.value is True:
            await view.cog.manager.remove_preset(view.guild.id, view.role_id, uuid_to_delete)
            await confirmation_view.message.edit(content=f"✅ 预设 **{preset_to_delete.name}** 已删除。", view=None)
            # 刷新主详情视图
            await view.refresh_and_edit(itx)
        elif confirmation_view.value is False:
            await confirmation_view.message.edit(content="👍 操作已取消。", view=None)

class IntervalModal(ui.Modal, title="设置轮播间隔"):
    val = ui.TextInput(label="间隔 (分钟)", placeholder="例如: 60", min_length=1, max_length=4)

    def __init__(self, parent_view: TrackDetailView):
        super().__init__()
        self.parent_view = parent_view

    async def on_submit(self, itx: discord.Interaction):
        try:
            v = int(self.val.value)
            if v < 1:
                return await itx.response.send_message("❌ 间隔至少为1分钟", ephemeral=True)

            await self.parent_view.cog.manager.update_track(
                self.parent_view.guild.id,
                self.parent_view.role_id,
                interval_minutes=v  # 使用正确的字段名
            )

            # 因为是在详情页内部修改参数，所以我们编辑当前消息，而不是发新的
            await self.parent_view.refresh_and_edit(itx)

        except ValueError:
            await itx.response.send_message("❌ 请输入有效的数字", ephemeral=True)