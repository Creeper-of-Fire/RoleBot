from typing import List, TYPE_CHECKING

import discord
from discord import ui, Embed, Color, SelectOption, Interaction, ButtonStyle

from role_jukebox.models import Preset
from role_jukebox.share_view import PresetEditModal, CloneRoleButton
from utility.helpers import safe_defer
from utility.paginated_view import PaginatedView

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class UserPresetView(PaginatedView):
    """一个分页视图，用于VIP用户管理自己的专属预设。"""

    def __init__(self, cog: 'RoleJukeboxCog', user: discord.Member):
        self.cog = cog
        self.user = user
        self.guild = user.guild  # 克隆功能需要 guild 对象
        super().__init__(all_items_provider=self._fetch_user_presets, items_per_page=5, timeout=600)

    async def _fetch_user_presets(self) -> List[Preset]:
        """从Manager获取当前用户的所有专属预设。"""
        return self.cog.jukebox_manager.get_user_presets(self.user.id)

    async def _rebuild_view(self):
        """核心方法：重建Embed和组件。"""
        self.clear_items()
        self.embed = Embed(
            title="✨ 我的专属预设",
            description=f"管理您的个性化身份组预设。\n当前页码: {self.page + 1}/{self.total_pages}",
            color=Color.gold()
        )
        page_items = self.get_page_items()

        if not page_items:
            self.embed.description += "\n\n*您还没有创建任何专属预设...*"
        else:
            for i, preset in enumerate(page_items):
                field_name = f"🎨 **{preset.name}**"
                field_value = f"颜色: `{preset.color}`\n图标: {preset.icon_url or '无'}"
                self.embed.add_field(name=field_name, value=field_value, inline=False)

        # 添加操作组件
        if page_items:
            self.add_item(EditUserPresetSelect(page_items))
            self.add_item(DeleteUserPresetSelect(page_items))

        self.add_item(AddUserPresetButton(row=2))
        # 明确告知 CloneRoleButton 这是为用户使用
        self.add_item(CloneRoleButton(row=2, is_for_user=True))
        self._add_pagination_buttons(row=4)


# --- Components for User View ---

class EditUserPresetSelect(ui.Select):
    def __init__(self, page_items: List[Preset]):
        options = [SelectOption(label=f"编辑预设: {p.name}", value=p.uuid, emoji="✏️") for p in page_items]
        super().__init__(placeholder="选择一个预设进行编辑...", options=options, row=0)

    async def callback(self, interaction: Interaction):
        preset_uuid = self.values[0]
        preset_to_edit = self.view.cog.jukebox_manager.get_preset_by_uuid(preset_uuid)
        if not preset_to_edit or preset_to_edit.owner_id != interaction.user.id:
            await interaction.response.send_message("❌ 错误：找不到该预设或您无权操作。", ephemeral=True)
            await self.view.update_view(interaction)
            return

        # 弹出模态框，is_admin=False 表示这是用户在操作
        modal = PresetEditModal(self.view.cog, existing_preset=preset_to_edit, is_admin=False)
        await interaction.response.send_modal(modal)
        await modal.wait()
        await self.view.update_view(interaction)


class DeleteUserPresetSelect(ui.Select):
    def __init__(self, page_items: List[Preset]):
        options = [SelectOption(label=f"删除预设: {p.name}", value=p.uuid, emoji="🗑️") for p in page_items]
        super().__init__(placeholder="选择一个预设将其删除...", options=options, row=1)

    async def callback(self, interaction: Interaction):
        await safe_defer(interaction)
        preset_uuid = self.values[0]
        preset_to_delete = self.view.cog.jukebox_manager.get_preset_by_uuid(preset_uuid)

        # 权限检查
        if not preset_to_delete or preset_to_delete.owner_id != interaction.user.id:
            await interaction.followup.send("❌ 错误：找不到该预设或您无权操作。", ephemeral=True)
            return

        success = await self.view.cog.jukebox_manager.delete_preset_by_uuid(preset_uuid)

        if success and preset_to_delete:
            msg = f"已删除您的专属预设 '{preset_to_delete.name}'。"
        elif success:
            msg = "您的专属预设已删除。"
        else:
            msg = "删除失败，可能预设已被移除。"

        await interaction.followup.send(f"✅ {msg}" if success else f"❌ {msg}", ephemeral=True)
        await self.view.update_view(interaction)


class AddUserPresetButton(ui.Button):
    def __init__(self, row: int):
        super().__init__(label="添加专属预设", style=ButtonStyle.green, emoji="➕", row=row)

    async def callback(self, interaction: Interaction):
        # is_admin=False, existing_preset=None 表示为当前用户创建新的专属预设
        modal = PresetEditModal(self.view.cog, is_admin=False, existing_preset=None)
        await interaction.response.send_modal(modal)
        await modal.wait()
        await self.view.update_view(interaction)
