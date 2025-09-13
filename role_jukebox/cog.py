# jukebox/cog.py
from __future__ import annotations

import asyncio
import io
import typing
from typing import Optional, Dict, List

import aiohttp
import discord
from discord import app_commands, Color, Interaction, ui
from discord.ext import tasks

import config
from role_jukebox.admin_view import PresetAdminView
from role_jukebox.role_jukebox_manager import RoleJukeboxManager
from role_jukebox.view import RoleJukeboxView
from utility.feature_cog import FeatureCog
from utility.helpers import try_get_member

if typing.TYPE_CHECKING:
    from main import RoleBot


class OpenJukeboxPanelButton(ui.Button):
    """一个简单的按钮，用于打开点歌机面板。"""

    def __init__(self, cog: "RoleJukeboxCog"):
        super().__init__(
            label="身份点歌机",
            style=discord.ButtonStyle.primary,
            emoji="🎶",
            custom_id="role_jukebox:open_panel"
        )
        self.cog = cog

    async def callback(self, interaction: Interaction):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("错误：无法获取您的成员信息。", ephemeral=True)
            return

        view = RoleJukeboxView(self.cog, interaction.user)
        await view.update_view()  # Initial build
        await interaction.response.send_message(embed=view.embed, view=view, ephemeral=True)


class RoleJukeboxCog(FeatureCog, name="RoleJukebox"):
    """管理身份组点歌机功能。"""

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [OpenJukeboxPanelButton(self)]

    async def update_safe_roles_cache(self):
        pass

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.jukebox_manager = RoleJukeboxManager()
        self.process_expirations_task.start()
        self.session = aiohttp.ClientSession()

    def cog_unload(self):
        self.process_expirations_task.cancel()
        asyncio.create_task(self.session.close())

    # --- Helper Methods ---
    def get_guild_config(self, guild_id: int) -> Optional[Dict]:
        """安全地获取服务器的点歌机配置。"""
        return config.JUKEBOX_GUILD_CONFIGS.get(guild_id)

    def is_vip(self, member: discord.Member) -> bool:
        """检查成员是否为VIP。"""
        guild_config = self.get_guild_config(member.guild.id)
        if not guild_config or not guild_config.get("vip_user_role_ids"):
            return False
        vip_role_ids = set(guild_config["vip_user_role_ids"])
        member_role_ids = {role.id for role in member.roles}
        return not vip_role_ids.isdisjoint(member_role_ids)

    # --- Slash Commands Group ---
    jukebox = app_commands.Group(
        name="身份组点歌机",
        description="身份组点歌机相关指令",
        guild_ids=[gid for gid in config.JUKEBOX_GUILD_CONFIGS if config.JUKEBOX_GUILD_CONFIGS[gid].get("enabled")]
    )

    @jukebox.command(name="私人面板", description="打开身份组点歌机面板")
    async def panel(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("错误：无法获取您的成员信息。", ephemeral=True)
            return

        view = RoleJukeboxView(self, interaction.user)
        await view.update_view()  # Initial build
        await interaction.response.send_message(embed=view.embed, view=view, ephemeral=True)

    # --- Admin Sub-group ---
    admin = app_commands.Group(name="管理", description="点歌机管理指令", parent=jukebox)

    @admin.command(name="管理面板", description="打开可视化的预设管理面板")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def admin_panel(self, interaction: Interaction):
        if not interaction.guild:
            await interaction.response.send_message("此指令只能在服务器内使用。", ephemeral=True)
            return

        view = PresetAdminView(self, interaction.guild)
        await view.start(interaction, ephemeral=True)

    @admin.command(name="解锁全部", description="强制解锁本服务器所有被锁定的点歌队列")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def force_unlock_all(self, interaction: Interaction):
        """管理员指令，用于立即解除所有队列的变更锁定。"""
        await interaction.response.defer(ephemeral=True)

        unlocked_count = await self.jukebox_manager.force_unlock_all_queues(interaction.guild_id)

        if unlocked_count > 0:
            message = f"✅ 操作成功！已强制解锁 **{unlocked_count}** 个点歌队列。"
        else:
            message = "ℹ️ 操作完成，但当前没有发现任何处于锁定状态的队列。"

        await interaction.followup.send(message, ephemeral=True)

    # --- VIP Sub-group ---
    my = app_commands.Group(name="我的", description="我的专属预设管理", parent=jukebox)

    @my.command(name="添加专属预设", description="添加一个我的专属预设")
    @app_commands.describe(name="预设名称", color="颜色 (HEX格式, 如#FF0000)", icon="可选的表情符号")
    async def add_my_preset(self, interaction: discord.Interaction, name: str, color: str, icon: Optional[str] = None):
        if not self.is_vip(interaction.user):
            await interaction.response.send_message("❌ 此功能仅限权区用户使用。", ephemeral=True)
            return

        try:
            Color.from_str(color)
        except ValueError:
            await interaction.response.send_message("❌ 颜色格式无效，请输入HEX格式 (例如: `#FF5733`)。", ephemeral=True)
            return

        success, msg = await self.jukebox_manager.add_user_preset(interaction.user.id, interaction.guild_id, name, color, icon)
        await interaction.response.send_message(f"✅ {msg}" if success else f"❌ {msg}", ephemeral=True)

    async def _apply_preset_to_role(self, role: discord.Role, preset: dict, reason: str):
        """一个辅助函数，用于将预设应用到身份组，包含图标处理。"""
        name = preset['name']
        color = Color.from_str(preset['color'])
        icon_url = preset.get('icon')
        icon_bytes = None

        self.logger.info(f"以颜色 {color} 和图标 {icon_url} 更新身份组 {role.name} 为 {name}")

        if icon_url:
            try:
                async with self.session.get(icon_url) as resp:
                    if resp.status == 200:
                        icon_bytes = await resp.read()
                    else:
                        self.logger.warning(f"Failed to download icon from {icon_url}, status: {resp.status}")
            except Exception as e:
                self.logger.error(f"Error downloading icon from {icon_url}: {e}")

        await role.edit(name=name, color=color, display_icon=icon_bytes, reason=reason)

    async def user_claim_or_change_queue(self, interaction: Interaction, view: 'RoleJukeboxView'):
        """处理用户点击'变更/点播'按钮的逻辑"""
        success, msg = await self.jukebox_manager.change_or_claim_queue(
            view.guild.id, view.user.id, view.selected_queue_role_id, view.selected_preset
        )
        if success:
            role = view.guild.get_role(view.selected_queue_role_id)
            await self._apply_preset_to_role(role, view.selected_preset, f"{interaction.user} 变更/点播")

            if role not in interaction.user.roles:
                await view.user.add_roles(role, reason="变更/点播身份组")

            await interaction.followup.send(f"✅ {msg}", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True)

        await view.update_view(interaction)

    # --- Background Task ---
    @tasks.loop(seconds=30)
    async def process_expirations_task(self):
        actions = await self.jukebox_manager.process_expirations()
        for action in actions:
            guild = self.bot.get_guild(action['guild_id'])
            role = guild.get_role(action['role_id']) if guild else None
            if not guild or not role:
                self.logger.warning(f"Jukebox: Can't find guild or role for action: {action}")
                continue

            try:
                if action['type'] == 'ROTATE':
                    self.logger.info(f"Jukebox: Rotating role {role.id} in guild {guild.id}")
                    new_preset = action['new_preset']
                    requester = await try_get_member(guild, action['requester_id'])

                    # 身份组外观直接在所有持有者身上改变。

                    # 1. 应用新预设
                    color = Color.from_str(new_preset['color'])
                    await self._apply_preset_to_role(role, action['new_preset'], "点歌队列轮换")

                    # 2. 确保请求者拥有该身份组
                    if requester and role not in requester.roles:
                        await requester.add_roles(role, reason="排队请求生效")

                    # 3. (可选) 通知请求者
                    if requester:
                        try:
                            await requester.send(f"你在服务器 **{guild.name}** 的排队请求已生效！身份组已变更为 **{new_preset['name']}**。")
                        except discord.Forbidden:
                            pass

            except Exception as e:
                self.logger.error(f"Error processing jukebox action {action}: {e}")

    async def _upload_icon_and_get_url(self, guild_id: int, image_bytes: bytes, original_filename: str) -> Optional[str]:
        """将图片二进制数据上传到专用频道并返回永久URL。"""
        guild_config = self.get_guild_config(guild_id)
        if not guild_config or not (channel_id := guild_config.get("icon_storage_channel_id")):
            self.logger.error(f"Guild {guild_id} is missing 'icon_storage_channel_id' in config.")
            return None

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.logger.error(f"Cannot find icon storage channel with ID {channel_id}.")
            return None

        try:
            # 使用 discord.File 对象上传二进制数据
            file = discord.File(io.BytesIO(image_bytes), filename=original_filename)
            message = await channel.send(file=file)

            # 返回上传后附件的永久URL
            return message.attachments[0].url
        except discord.Forbidden:
            self.logger.error(f"Bot lacks permissions to upload to channel {channel_id}.")
            return None
        except Exception as e:
            self.logger.error(f"Failed to upload icon to storage channel: {e}")
            return None

    @process_expirations_task.before_loop
    async def before_tasks(self):
        await self.bot.wait_until_ready()


async def setup(bot: RoleBot):
    """Cog的入口点。"""
    # 确保只在有配置的服务器上注册指令
    await bot.add_cog(RoleJukeboxCog(bot))
