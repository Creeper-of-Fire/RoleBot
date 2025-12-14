# role_jukebox/admin_view.py
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import discord
from discord import ui, ButtonStyle, Embed, Color, SelectOption

from role_jukebox.models import Track, Preset, TrackMode, PlayerAction, DashboardMode
from role_jukebox.share_view import create_dashboard_embed, PreviewBtn
from utility.paginated_view import PaginatedView
from utility.views import ConfirmationView

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


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

        # --- 使用共享函数创建 Embed ---
        embed = create_dashboard_embed(self.guild, tracks, DashboardMode.ADMIN)

        # --- 添加特定于管理视图的按钮 ---
        for t in tracks:
            r = self.guild.get_role(t.role_id)
            # 优先使用自定义名称，否则回退到身份组名称
            display_name = t.name or (r.name if r else f"失效ID {t.role_id}")
            if not r:
                self.add_item(TrackBtn(t.role_id, display_name, ButtonStyle.secondary, disabled=True))
                continue

            status_emoji = "🟢" if t.enabled else "🔴"
            btn_style = ButtonStyle.success if t.enabled else ButtonStyle.secondary
            label = f"{display_name[:10]}"

            self.add_item(TrackBtn(t.role_id, label, btn_style, emoji=status_emoji))

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

        # --- 在重建视图时，检查并修正无效的 current_index ---
        if self.track.presets and self.track.current_index >= len(self.track.presets):
            self.track.current_index = 0

        role = self.guild.get_role(self.role_id)
        role_name = role.name if role else "未知身份组"
        role_color = role.color if role else Color.default()

        # 优先使用自定义名称
        display_name = self.track.name or role_name

        self.embed = Embed(title=f"⚙️ 配置轨道: {display_name}", color=role_color)
        status = "✅ 运行中" if self.track.enabled else "⏸️ 已暂停"
        mode = "🔀 随机播放" if self.track.mode == TrackMode.RANDOM else "🔁 顺序播放"

        prefix_display = f"`{self.track.name_prefix}`" if self.track.name_prefix else "*未设置*"

        role_mention_str = f"{role.mention} (`{self.role_id}`)" if role else f"失效ID `{self.role_id}`"
        self.embed.description = (
            f"**目标身份组**: {role_mention_str}\n"
            f"----------------\n"
            f"**状态**: {status}\n"
            f"**模式**: {mode}\n"
            f"**间隔**: {self.track.interval_minutes} 分钟\n"
            f"**名称前缀**: {prefix_display}\n"
            f"----------------"
        )
        self.embed.set_footer(text="提示: 使用 /身份组轮播 添加预设 来增加更多外观")

        items = self.get_page_items()
        if items:
            desc_lines = []
            for i, p in enumerate(items):
                absolute_idx = (self.page * self.items_per_page) + i
                # 高亮当前播放的预设
                current_marker = "▶️ " if absolute_idx == self.track.current_index else ""

                icon_mark = "🖼️" if p.icon_filename else "⚪"
                # 默认显示主色
                color_display = f"`{p.color}`"

                # 如果是渐变，显示两种颜色
                if p.secondary_color:
                    color_display = f"`{p.color}` -> `{p.secondary_color}`"

                # 如果是全息模式，明确标注
                if p.tertiary_color:
                    color_display = "✨ `全息模式`"

                # 最终组合成一行
                desc_lines.append(
                    f"`{absolute_idx + 1}.` {current_marker}**{p.name}** {icon_mark} {color_display}"
                )
            self.embed.add_field(name=f"预设列表 (第 {self.page + 1} 页)", value="\n".join(desc_lines), inline=False)

            # Row 0: 管理预设下拉菜单
            self.add_item(ManagePresetSelect(items, row=0))
        else:
            self.embed.add_field(name="预设列表", value="*暂无预设，请添加*", inline=False)

        # Row 1: 核心控制
        self.add_item(ToggleBtn(self.track.enabled, row=1))
        self.add_item(ModeBtn(self.track.mode, row=1))
        self.add_item(IntervalBtn(self.track.interval_minutes, row=1))

        # Row 2: 播放控制
        self.add_item(PrevBtn(disabled=not self.track.presets, row=2))
        self.add_item(SyncBtn(disabled=not self.track.presets, row=2))
        self.add_item(NextBtn(disabled=not self.track.presets, row=2))

        # Row 3: 功能按钮
        self.add_item(RenameBtn(row=3))
        self.add_item(PreviewBtn(self.track, self.cog.manager, row=3))
        self.add_item(SetPrefixBtn(row=3))

        # Row 3: 危险/导航操作
        self.add_item(DelTrackBtn(row=3))
        self.add_item(BackButton(self.parent_view, row=3))

        # Row 4: 翻页
        self._add_pagination_buttons(row=4)

    async def refresh_and_edit(self, interaction: discord.Interaction):
        """在当前视图内更新（编辑）消息"""
        await self.update_view(interaction)


# =============================================================================
# 详情面板的组件
# =============================================================================

class BackButton(ui.Button):
    def __init__(self, parent_view: AdminDashboardView, **kwargs):
        super().__init__(label="返回列表", style=ButtonStyle.secondary, **kwargs)
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        new_dashboard = AdminDashboardView(self.parent_view.cog, interaction.guild)
        await new_dashboard.show(interaction)


class DelTrackBtn(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(label="删除轨道", style=ButtonStyle.danger, **kwargs, emoji="🗑️")

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


# =============================================================================
# 播放控制按钮
# =============================================================================

class PlayerControlBtn(ui.Button):
    """播放控制按钮的基类，处理通用逻辑"""

    def __init__(self, *, style: ButtonStyle = ButtonStyle.secondary, label: str | None = None, emoji: str | None = None, row: int | None = None,
                 disabled: bool = False, action: str):
        super().__init__(style=style, label=label, emoji=emoji, row=row, disabled=disabled)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        view: TrackDetailView = self.view
        await interaction.response.defer()

        new_preset = None

        # 1. 调用 manager 获取下一个状态
        try:
            new_preset = await view.cog.manager.manual_control(
                view.guild.id, view.role_id, self.action
            )
        except Exception as e:
            await interaction.followup.send(f"❌ 操作失败: {e}", ephemeral=True)

        if new_preset:
            # 2. 调用 cog 的方法应用到 Discord
            try:
                await view.cog._apply_preset(view.guild.id, view.role_id, new_preset)

                action_text = {"next": "切换到", "prev": "切换到", "sync": "同步为"}
                await interaction.followup.send(f"✅ 操作成功！已**{action_text[self.action]}**: **{new_preset.name}**", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send("❌ **权限不足**，无法修改该身份组。", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ 应用身份组时发生未知错误: {e}", ephemeral=True)
        else:
            await interaction.followup.send("❌ 操作失败，轨道可能没有可用的预设。", ephemeral=True)

        # 3. 刷新视图，显示新的高亮位置
        await view.refresh_and_edit(interaction)


class PrevBtn(PlayerControlBtn):
    def __init__(self, disabled: bool = False, **kwargs):
        super().__init__(emoji="⏮️", style=ButtonStyle.primary, **kwargs, disabled=disabled, action=PlayerAction.PREV)


class SyncBtn(PlayerControlBtn):
    def __init__(self, disabled: bool = False, **kwargs):
        super().__init__(label="同步", emoji="🔄", style=ButtonStyle.success, **kwargs, disabled=disabled, action=PlayerAction.SYNC)


class NextBtn(PlayerControlBtn):
    def __init__(self, disabled: bool = False, **kwargs):
        super().__init__(emoji="⏭️", style=ButtonStyle.primary, **kwargs, disabled=disabled, action=PlayerAction.NEXT)


class ToggleBtn(ui.Button):
    def __init__(self, on: bool, **kwargs):
        super().__init__(label="暂停轮播" if on else "开启轮播", style=ButtonStyle.danger if on else ButtonStyle.success, **kwargs, emoji="⏯️")

    async def callback(self, itx: discord.Interaction):
        view: TrackDetailView = self.view
        await view.cog.manager.update_track(view.guild.id, view.role_id, enabled=not view.track.enabled)
        await view.refresh_and_edit(itx)


class SetPrefixBtn(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(label="设置前缀", style=ButtonStyle.secondary, **kwargs, emoji="🏷️")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(SetPrefixModal(self.view))


class SetPrefixModal(ui.Modal, title="设置轮播名称前缀"):
    prefix_input = ui.TextInput(
        label="身份组名称前缀",
        placeholder="例如: [轮播] (留空则不使用前缀)",
        required=False,
        max_length=20  # 设置一个合理的前缀长度限制
    )

    def __init__(self, parent_view: TrackDetailView):
        super().__init__()
        self.parent_view = parent_view
        # 将当前前缀填入输入框作为默认值
        if self.parent_view.track and self.parent_view.track.name_prefix:
            self.prefix_input.default = self.parent_view.track.name_prefix

    async def on_submit(self, interaction: discord.Interaction):
        new_prefix = self.prefix_input.value.strip()
        # 如果用户输入为空，则将前缀设为 None
        await self.parent_view.cog.manager.update_track(
            self.parent_view.guild.id,
            self.parent_view.role_id,
            name_prefix=new_prefix if new_prefix else None
        )
        await self.parent_view.refresh_and_edit(interaction)


class ModeBtn(ui.Button):
    def __init__(self, mode: str, **kwargs):
        super().__init__(label="切换为随机" if mode == 'sequence' else "切换为顺序", style=ButtonStyle.primary, **kwargs,
                         emoji="🔀" if mode == 'sequence' else "🔁")

    async def callback(self, itx: discord.Interaction):
        view: TrackDetailView = self.view
        new_mode = TrackMode.RANDOM if view.track.mode == TrackMode.SEQUENCE else TrackMode.SEQUENCE
        await view.cog.manager.update_track(view.guild.id, view.role_id, mode=new_mode)
        await view.refresh_and_edit(itx)


class IntervalBtn(ui.Button):
    def __init__(self, current_interval: int, **kwargs):
        super().__init__(label=f"间隔 ({current_interval}m)", style=ButtonStyle.secondary, **kwargs, emoji="⏱️")

    async def callback(self, itx: discord.Interaction):
        await itx.response.send_modal(IntervalModal(self.view))


class RenameBtn(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(label="重命名", style=ButtonStyle.secondary, **kwargs, emoji="✏️")

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


# =============================================================================
# 三级面板：预设子详情 (管理单个预设)
# =============================================================================

class PresetManageView(ui.View):
    """子页面：用于查看、编辑和删除单个预设"""

    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild, role_id: int, preset: Preset, parent_view: TrackDetailView):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild
        self.role_id = role_id
        self.preset = preset
        self.parent_view = parent_view  # 用于返回上一级

    async def get_embed_and_files(self):
        # 构建详情 Embed
        try:
            c = Color.from_str(self.preset.color)
        except:
            c = Color.default()
        embed = Embed(title=f"🎨 管理预设: {self.preset.name}", color=c)
        desc = (
            f"**主色**: `{self.preset.color}`\n"
        )
        if self.preset.secondary_color:
            desc += f"**副色**: `{self.preset.secondary_color}`\n"
        if self.preset.tertiary_color:
            desc += f"**三色**: `{self.preset.tertiary_color}` (触发全息模式)\n"

        desc += f"**UUID**: `{self.preset.uuid}`"
        embed.description = desc

        files = []
        if self.preset.icon_filename:
            # 读取并展示图标
            data = await self.cog.manager.get_icon_bytes(self.preset.icon_filename)
            if data:
                import io
                f = discord.File(io.BytesIO(data), filename=self.preset.icon_filename)
                embed.set_thumbnail(url=f"attachment://{self.preset.icon_filename}")
                files.append(f)
            else:
                embed.set_footer(text="⚠️ 图标文件丢失")
        else:
            embed.set_footer(text="此预设没有图标")

        return embed, files

    async def refresh(self, interaction: discord.Interaction):
        embed, files = await self.get_embed_and_files()
        await interaction.response.edit_message(embed=embed, view=self, attachments=files)

    async def show(self, interaction: discord.Interaction):
        embed, files = await self.get_embed_and_files()

        # 添加按钮
        self.add_item(EditPresetBtn())
        self.add_item(DeletePresetBtn())
        self.add_item(BackToTrackBtn())

        if interaction.response.is_done():
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=self, attachments=files)
        else:
            # 这里的 edit_message 需要注意，如果是 select 触发的，通常是 response.edit_message
            await interaction.response.edit_message(embed=embed, view=self, attachments=files)


class EditPresetBtn(ui.Button):
    def __init__(self):
        super().__init__(label="编辑属性", style=ButtonStyle.primary, emoji="✏️")

    async def callback(self, interaction: discord.Interaction):
        view: PresetManageView = self.view
        await interaction.response.send_modal(EditPresetModal(view))


class DeletePresetBtn(ui.Button):
    def __init__(self):
        super().__init__(label="删除预设", style=ButtonStyle.danger, emoji="🗑️")

    async def callback(self, interaction: discord.Interaction):
        view: PresetManageView = self.view

        # 二次确认
        confirm_view = ConfirmationView(author=interaction.user)
        await interaction.response.send_message(
            f"⚠️ **确定要删除预设【{view.preset.name}】吗？**\n此操作不可恢复。",
            view=confirm_view,
            ephemeral=True
        )
        confirm_view.message = await interaction.original_response()

        await confirm_view.wait()

        if confirm_view.value:
            # 执行删除
            await view.cog.manager.remove_preset(view.guild.id, view.role_id, view.preset.uuid)
            await confirm_view.message.edit(content="✅ 预设已删除。", view=None)

            # 删除后无法停留在子页面，必须返回上一级
            # 我们重新发送一个 TrackDetailView
            new_track_view = TrackDetailView(view.cog, view.guild, view.role_id, view.parent_view.parent_view)

            await new_track_view.start(interaction, ephemeral=True)

        else:
            await confirm_view.message.edit(content="👍 操作已取消。", view=None)


class BackToTrackBtn(ui.Button):
    def __init__(self):
        super().__init__(label="返回列表", style=ButtonStyle.secondary, emoji="↩️")

    async def callback(self, interaction: discord.Interaction):
        view: PresetManageView = self.view
        # 返回上一级，直接刷新父视图即可
        await view.parent_view.update_view(interaction)


# =============================================================================
# 组件：下拉菜单与模态框
# =============================================================================

class ManagePresetSelect(ui.Select):
    def __init__(self, items: list[Preset], **kwargs):
        # 限制长度，防止名称过长报错
        opts = [
            SelectOption(
                label=p.name[:25],
                value=p.uuid,
                emoji="⚙️",
                description=f"管理: {p.color}"
            ) for p in items
        ]
        super().__init__(placeholder="选择一个预设进行管理 (编辑/删除)...", options=opts, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        view: TrackDetailView = self.view
        uuid_selected = self.values[0]

        # 查找对象
        preset = next((p for p in view.track.presets if p.uuid == uuid_selected), None)
        if not preset:
            return await interaction.response.send_message("❌ 预设不存在，可能已被删除", ephemeral=True)

        # 进入子页面
        sub_view = PresetManageView(view.cog, view.guild, view.role_id, preset, parent_view=view)
        await sub_view.show(interaction)


class EditPresetModal(ui.Modal, title="编辑预设属性"):
    name_input = ui.TextInput(label="预设名称", required=True, max_length=100)
    color_input = ui.TextInput(label="颜色 (HEX)", placeholder="#FF0000", required=True, min_length=6, max_length=7)
    secondary_color_input = ui.TextInput(label="[可选] 副颜色 (HEX)", placeholder="留空则不使用渐变", required=False, max_length=7)
    tertiary_color_input = ui.TextInput(
        label="[可选] 第三种颜色 (HEX)",
        placeholder="设置为任意合法值即可，用于触发全息模式",
        required=False,
        max_length=7
    )

    def __init__(self, parent_view: PresetManageView):
        super().__init__()
        self.parent_view = parent_view
        self.name_input.default = self.parent_view.preset.name
        self.color_input.default = self.parent_view.preset.color
        self.secondary_color_input.default = self.parent_view.preset.secondary_color
        self.tertiary_color_input.default = self.parent_view.preset.tertiary_color

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self.name_input.value.strip()
        new_color = self.color_input.value.strip()
        new_secondary = self.secondary_color_input.value.strip() or None
        new_tertiary = self.tertiary_color_input.value.strip() or None

        try:
            Color.from_str(new_color)
            if new_secondary:
                Color.from_str(new_secondary)
            if new_tertiary:
                Color.from_str(new_tertiary)
        except ValueError:
            return await interaction.response.send_message("❌ 颜色格式错误 (例如 #FF0000)", ephemeral=True)

        # 更新数据库
        success = await self.parent_view.cog.manager.update_preset(
            self.parent_view.guild.id,
            self.parent_view.role_id,
            self.parent_view.preset.uuid,
            new_name,
            new_color,
            new_secondary,
            new_tertiary
        )

        if success:
            # 更新内存对象，以便立即显示
            self.parent_view.preset.name = new_name
            self.parent_view.preset.color = new_color
            self.parent_view.preset.secondary_color = new_secondary
            self.parent_view.preset.tertiary_color = new_tertiary

            # 刷新子页面
            await self.parent_view.show(interaction)
            # 给一个隐式的反馈
            # await interaction.followup.send("✅ 更新成功", ephemeral=True)
        else:
            await interaction.response.send_message("❌ 更新失败，轨道可能已变更", ephemeral=True)
