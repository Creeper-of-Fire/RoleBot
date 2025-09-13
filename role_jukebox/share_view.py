import uuid
from typing import Optional, TYPE_CHECKING

import discord
from discord import ui, Interaction, ButtonStyle, Color

from role_jukebox.models import Preset
if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class CloneRoleModal(ui.Modal, title="从身份组克隆预设"):
    def __init__(self, cog: 'RoleJukeboxCog', guild: discord.Guild, is_for_user: bool = False):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild
        self.is_for_user = is_for_user

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

        owner_id = interaction.user.id if self.is_for_user else None

        new_preset = Preset(name=name, color=color_hex, icon_url=icon_url, owner_id=owner_id)

        success, msg = await self.cog.jukebox_manager.upsert_preset(new_preset, guild_id=self.guild.id)

        if not success and "已存在" in msg:
            msg += f"\n您可能需要先删除同名旧预设，或修改被克隆身份组的名称。"

        await interaction.followup.send(msg, ephemeral=True)


class CloneRoleButton(ui.Button):
    def __init__(self, row: int, is_for_user: bool = False):
        super().__init__(label="从身份组克隆", style=ButtonStyle.secondary, emoji="🧬", row=row)
        self.is_for_user = is_for_user

    async def callback(self, interaction: Interaction):
        modal = CloneRoleModal(self.view.cog, self.view.guild, is_for_user=self.is_for_user)
        await interaction.response.send_modal(modal)

        await modal.wait()
        # 刷新视图以显示可能新增的预设
        await self.view.update_view(interaction)

class PresetEditModal(ui.Modal, title="创建/编辑身份组预设", ):
    def __init__(self, cog: 'RoleJukeboxCog', is_admin: bool, existing_preset: Optional[Preset] = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.is_admin = is_admin
        self.existing_preset = existing_preset

        self.preset_name = ui.TextInput(label="预设名称", placeholder="例如：深海之心", required=True, max_length=50,
                                        default=existing_preset.name if existing_preset else None)
        self.add_item(self.preset_name)

        self.preset_color = ui.TextInput(label="颜色 (HEX格式)", placeholder="例如：#4A90E2", required=True, min_length=7, max_length=7,
                                         default=existing_preset.color if existing_preset else None)
        self.add_item(self.preset_color)

        self.preset_icon = ui.TextInput(label="图标URL (可选)", placeholder="留空或输入 '无' 以移除图标", required=False,
                                        default=existing_preset.icon_url if existing_preset else None)
        self.add_item(self.preset_icon)

    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        # 验证颜色
        try:
            color_str = self.preset_color.value
            if not color_str.startswith("#"): color_str = f"#{color_str}"
            Color.from_str(color_str)
        except ValueError:
            await interaction.followup.send("❌ 颜色格式无效。", ephemeral=True)
            return

        icon_url_input = self.preset_icon.value
        icon_url = icon_url_input if icon_url_input and icon_url_input.lower() not in ['无', 'none'] else None

        if self.existing_preset:  # 更新模式
            # 创建一个新对象来更新，而不是修改旧的
            updated_preset = Preset(
                uuid=self.existing_preset.uuid,
                name=self.preset_name.value,
                color=color_str,
                icon_url=icon_url,
                owner_id=self.existing_preset.owner_id
            )
        else:  # 创建模式
            owner_id = None if self.is_admin else interaction.user.id
            updated_preset = Preset(
                name=self.preset_name.value,
                color=color_str,
                icon_url=icon_url,
                owner_id=owner_id
            )

        # PUT 操作
        success, result_msg = await self.cog.jukebox_manager.upsert_preset(
            updated_preset, guild_id=interaction.guild_id
        )

        await interaction.followup.send(result_msg, ephemeral=True)

        if success:
            # 触发实时更新
            await self.cog.live_update_role_by_preset_uuid(updated_preset.uuid)