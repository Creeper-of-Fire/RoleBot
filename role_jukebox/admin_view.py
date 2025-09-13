# jukebox/admin_view.py
from __future__ import annotations
from typing import TYPE_CHECKING, List, Dict, Any, Optional

import discord
from discord import ui, Interaction, SelectOption, ButtonStyle, Embed, Color

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

    async def _fetch_all_presets(self) -> List[Dict[str, Any]]:
        """从Manager获取并格式化所有预设数据。"""
        all_presets = []

        # 1. 获取通用预设
        general_presets = self.cog.jukebox_manager.get_guild_state(self.guild.id).get("general_presets", [])
        for preset in general_presets:
            all_presets.append({"type": "general", "data": preset})

        # 2. 获取所有用户的预设
        user_presets_map = self.cog.jukebox_manager.get_all_user_presets()
        for user_id_str, presets in user_presets_map.items():
            for preset in presets:
                all_presets.append({"type": "user", "user_id": int(user_id_str), "data": preset})

        return all_presets

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
            for i, item in enumerate(page_items):
                preset_data = item['data']
                name = preset_data['name']
                color = preset_data['color']
                icon = preset_data.get('icon', '无')

                if item['type'] == 'general':
                    field_name = f"🎨 **{name}** (通用预设)"
                    field_value = f"颜色: `{color}`\n图标: {icon}"
                else:  # user
                    user = self.guild.get_member(item['user_id']) or f"用户ID: {item['user_id']}"
                    display_name = user.display_name if isinstance(user, discord.Member) else user
                    field_name = f"👤 **{name}** (用户: {display_name})"
                    field_value = f"颜色: `{color}`\n图标: {icon}"

                self.embed.add_field(name=field_name, value=field_value, inline=False)

        # 添加操作组件
        if page_items:
            self.add_item(DeletePresetSelect(page_items))

        self.add_item(AddPresetButton(row=1))
        self.add_item(CloneRoleButton(row=1))

        # 添加分页按钮
        self._add_pagination_buttons(row=4)


# --- Components for Admin View ---

class DeletePresetSelect(ui.Select):
    def __init__(self, page_items: List[Dict[str, Any]]):
        options = []
        for i, item in enumerate(page_items):
            preset_data = item['data']
            name = preset_data['name']

            # 编码所有需要的信息到 value 中
            if item['type'] == 'general':
                label = f"删除通用预设: {name}"
                value = f"g_{name}"
            else:
                user_id = item['user_id']
                label = f"删除用户 {user_id} 的预设: {name}"
                value = f"u_{user_id}_{name}"

            options.append(SelectOption(label=label, value=value, emoji="🗑️"))

        super().__init__(placeholder="选择一个预设将其删除...", options=options, row=0)

    async def callback(self, interaction: Interaction):
        await safe_defer(interaction)

        value = self.values[0]
        parts = value.split('_', 2)
        preset_type = parts[0]

        success = False
        if preset_type == 'g':
            preset_name = parts[1]
            success = await self.view.cog.jukebox_manager.remove_general_preset(self.view.guild.id, preset_name)
            msg = f"已删除通用预设 '{preset_name}'。" if success else "删除失败。"
        elif preset_type == 'u':
            user_id = int(parts[1])
            preset_name = parts[2]
            success = await self.view.cog.jukebox_manager.remove_user_preset(user_id, preset_name)
            msg = f"已删除用户 {user_id} 的预设 '{preset_name}'。" if success else "删除失败。"
        else:
            msg = "无效的选择。"

        await interaction.followup.send(f"✅ {msg}" if success else f"❌ {msg}", ephemeral=True)
        # 刷新视图以反映更改
        await self.view.update_view(interaction)


class AddPresetButton(ui.Button):
    def __init__(self, row: int):
        super().__init__(label="添加通用预设", style=ButtonStyle.green, emoji="➕", row=row)

    async def callback(self, interaction: Interaction):
        # 管理员面板只添加通用预设
        modal = PresetEditModal(self.view.cog, is_admin=True)
        await interaction.response.send_modal(modal)

        # 等待模态框结束，然后刷新视图
        await modal.wait()
        # Modal 已经响应了 interaction，所以我们不能再用它
        # 我们需要从 self.view.message 获取一个新的 interaction，但这很复杂
        # 一个更简单的方法是直接编辑消息
        # 但因为 update_view 需要一个 interaction，我们还是用原来的，它只是用来编辑消息
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
            min_length=17,  # Discord ID 最小长度
            max_length=20,
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

        # 调用 manager 添加为通用预设
        success, msg = await self.cog.jukebox_manager.add_general_preset(
            self.guild.id, name, color_hex, icon_url
        )

        # 如果是因为名称重复而失败，提供更清晰的提示
        if not success and "已存在" in msg:
            msg += f"\n您可能需要先删除名为 **{name}** 的旧预设，或手动修改被克隆身份组的名称。"

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