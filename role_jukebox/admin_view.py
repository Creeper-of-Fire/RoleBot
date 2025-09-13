# jukebox/admin_view.py
from __future__ import annotations
from typing import TYPE_CHECKING, List, Dict, Any, Optional

import discord
from discord import ui, Interaction, SelectOption, ButtonStyle, Embed, Color

from role_jukebox.role_jukebox_manager import Preset
from utility.helpers import safe_defer, try_get_member
from utility.paginated_view import PaginatedView
from role_jukebox.view import PresetEditModal

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class PresetAdminView(PaginatedView):
    """一个分页视图，用于管理员管理服务器的所有身份组预设。"""

    def __init__(self, cog: RoleJukeboxCog, guild: discord.Guild):
        self.cog = cog
        self.guild = guild
        # provider 是一个函数，每次更新数据时都会调用它
        super().__init__(all_items_provider=self._fetch_all_presets, items_per_page=5, timeout=600)

    async def _fetch_all_presets(self) -> List[Preset]:
        """从Manager获取并格式化所有预设数据为Preset对象列表。"""
        # 1. 获取通用预设
        all_presets = self.cog.jukebox_manager.get_all_presets_for_admin_view()

        # 附加临时属性 _display_owner 用于视图显示
        for preset in all_presets:
            if preset.owner_id:
                member = await try_get_member(self.guild, preset.owner_id)
                preset._display_owner = member.display_name if member else f"用户ID: {preset.owner_id}"

        # 筛选出属于本服务器的通用预设和所有用户预设
        guild_id = self.guild.id
        filtered_presets = [
            p for p in all_presets
            if p.owner_id is not None or self._is_general_preset_for_guild(p, guild_id)
        ]
        return filtered_presets

    def _is_general_preset_for_guild(self, preset: Preset, guild_id: int) -> bool:
        """检查一个通用预设是否属于当前服务器"""
        # 这是一个简化的检查。更稳妥的方式是让 manager 方法直接返回过滤后的结果。
        # 但为了保持 manager 的通用性，暂时在视图层处理。
        guild_general_presets = self.cog.jukebox_manager.get_general_presets(guild_id)
        return preset.uuid in {p.uuid for p in guild_general_presets}

    async def _rebuild_view(self):
        """核心方法：重建Embed和组件。"""
        self.clear_items()
        self.embed = Embed(
            title="🛠️ 身份组预设管理",
            description=f"管理服务器的所有通用预设和用户专属预设。\n当前页码: {self.page + 1}/{self.total_pages}",
            color=Color.orange()
        )
        page_items = self.get_page_items()

        if not page_items:
            self.embed.description += "\n\n*这里空空如也...*"
        else:
            for i, preset in enumerate(page_items):
                if preset.owner_id is None:  # 通用预设
                    field_name = f"🎨 **{preset.name}** (通用预设)"
                    field_value = f"颜色: `{preset.color}`\n图标: {preset.icon_url or '无'}"
                else:  # 用户预设
                    field_name = f"👤 **{preset.name}** (用户: {getattr(preset, '_display_owner', preset.owner_id)})"
                    field_value = f"颜色: `{preset.color}`\n图标: {preset.icon_url or '无'}"
                self.embed.add_field(name=field_name, value=field_value, inline=False)

        # 添加操作组件
        if page_items:
            self.add_item(EditPresetSelect(page_items))  # 编辑选择器
            self.add_item(DeletePresetSelect(page_items))  # 删除选择器

        self.add_item(AddPresetButton(row=2))
        self.add_item(CloneRoleButton(row=2))
        self._add_pagination_buttons(row=4)


# --- Components for Admin View ---

class EditPresetSelect(ui.Select):
    def __init__(self, page_items: List[Preset]):
        options = []
        for preset in page_items:
            label_prefix = "编辑通用预设:" if preset.owner_id is None else "编辑用户预设:"
            options.append(SelectOption(label=f"{label_prefix} {preset.name}", value=preset.uuid, emoji="✏️"))
        super().__init__(placeholder="选择一个预设进行编辑...", options=options, row=0)

    async def callback(self, interaction: Interaction):
        preset_uuid = self.values[0]
        preset_to_edit = self.view.cog.jukebox_manager.get_preset_by_uuid(preset_uuid)
        if not preset_to_edit:
            await interaction.response.send_message("❌ 错误：找不到该预设，可能已被删除。", ephemeral=True)
            await self.view.update_view(interaction)
            return

        # 弹出模态框，并传入现有预设对象进行填充
        modal = PresetEditModal(self.view.cog, existing_preset=preset_to_edit, is_admin=True)
        await interaction.response.send_modal(modal)
        await modal.wait()
        await self.view.update_view(interaction) # 模态框结束后刷新


class DeletePresetSelect(ui.Select):
    def __init__(self, page_items: List[Preset]):
        options = []
        for preset in page_items:
            label_prefix = "删除通用预设:" if preset.owner_id is None else "删除用户预设:"
            options.append(SelectOption(label=f"{label_prefix} {preset.name}", value=preset.uuid, emoji="🗑️"))
        super().__init__(placeholder="选择一个预设将其删除...", options=options, row=1)

    async def callback(self, interaction: Interaction):
        await safe_defer(interaction)
        preset_uuid = self.values[0]
        preset_to_delete = self.view.cog.jukebox_manager.get_preset_by_uuid(preset_uuid)  # 获取信息用于反馈

        success = await self.view.cog.jukebox_manager.delete_preset_by_uuid(preset_uuid)

        if success and preset_to_delete:
            msg = f"已删除预设 '{preset_to_delete.name}'。"
        elif success:
            msg = "预设已删除。"
        else:
            msg = "删除失败，可能预设已被移除。"

        await interaction.followup.send(f"✅ {msg}" if success else f"❌ {msg}", ephemeral=True)
        await self.view.update_view(interaction)


class AddPresetButton(ui.Button):
    def __init__(self, row: int):
        super().__init__(label="添加通用预设", style=ButtonStyle.green, emoji="➕", row=row)

    async def callback(self, interaction: Interaction):
        # is_admin=True, existing_preset=None 表示创建新的通用预设
        modal = PresetEditModal(self.view.cog, is_admin=True, existing_preset=None)
        await interaction.response.send_modal(modal)
        await modal.wait()
        await self.view.update_view(interaction)


class CloneRoleModal(ui.Modal, title="从身份组克隆预设"):
    def __init__(self, cog: 'RoleJukeboxCog', guild: discord.Guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild

        self.role_id_input = ui.TextInput(
            label="身份组ID",
            placeholder="请粘贴要克隆的身份组ID",
            required=True,
            # min_length=17,  # Discord ID 最小长度
            # max_length=20,
        )
        self.add_item(self.role_id_input)

    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        role_id_str = self.role_id_input.value
        try:
            role_id = int(role_id_str)
        except ValueError:
            await interaction.followup.send("❌ 无效的ID格式，请输入纯数字的身份组ID。", ephemeral=True)
            return

        role = self.guild.get_role(role_id)
        if not role:
            await interaction.followup.send(f"❌ 在本服务器中未找到ID为 `{role_id}` 的身份组。", ephemeral=True)
            return

        # 提取身份组信息
        name = role.name
        color_hex = str(role.color)  # discord.Color.__str__ 返回 #RRGGBB 格式

        icon_url = None  # 默认为 None
        if role.icon:
            try:
                # 1. 从临时URL下载图片数据
                image_bytes = await role.icon.read()

                # 2. 上传到存储库并获取永久URL
                permanent_url = await self.cog._upload_icon_and_get_url(
                    self.guild.id, image_bytes, f"{role.id}_icon.png"  # 创建一个文件名
                )

                if permanent_url:
                    icon_url = permanent_url
                else:
                    await interaction.followup.send("⚠️ 身份组信息已提取，但图标上传失败，将创建不带图标的预设。", ephemeral=True)

            except Exception as e:
                self.cog.logger.error(f"Failed to read icon from role {role.id}: {e}")
                await interaction.followup.send("⚠️ 无法读取身份组图标，将创建不带图标的预设。", ephemeral=True)

        new_preset = Preset(name=name, color=color_hex, icon_url=icon_url)
        success, msg = await self.cog.jukebox_manager.upsert_preset(new_preset, guild_id=self.guild.id)

        if not success and "已存在" in msg:
            msg += f"\n您可能需要先删除同名旧预设，或修改被克隆身份组的名称。"

        await interaction.followup.send(msg, ephemeral=True)


class CloneRoleButton(ui.Button):
    def __init__(self, row: int):
        super().__init__(label="从身份组克隆", style=ButtonStyle.secondary, emoji="🧬", row=row)

    async def callback(self, interaction: Interaction):
        modal = CloneRoleModal(self.view.cog, self.view.guild)
        await interaction.response.send_modal(modal)

        await modal.wait()
        # 刷新视图以显示可能新增的预设
        await self.view.update_view(interaction)