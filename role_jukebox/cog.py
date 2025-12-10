# role_jukebox/cog.py
from __future__ import annotations

import asyncio
import typing
from typing import List

import aiohttp
import discord
from discord import app_commands, ui
from discord.ext import tasks

from role_jukebox.admin_view import AdminDashboardView
from role_jukebox.manager import RoleJukeboxManager
from role_jukebox.models import Preset
from role_jukebox.user_view import UserJukeboxView
from utility.feature_cog import FeatureCog

if typing.TYPE_CHECKING:
    from main import RoleBot


class RoleJukeboxCog(FeatureCog, name="RoleJukebox"):
    """
    身份组自动轮播系统。
    管理员配置轨道（身份组+预设池+间隔），机器人自动在该身份组上循环应用外观。
    """

    async def update_safe_roles_cache(self):
        pass

    def get_main_panel_buttons(self) -> List[discord.ui.Button]:
        """
        [框架方法]
        返回要显示在机器人主控面板（/panel）上的按钮。
        管理员用指令配置，所以这里只提供给用户的入口。
        """
        return [OpenLobbyButton(self)]

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.manager = RoleJukeboxManager()
        self.session = aiohttp.ClientSession()
        self.rotation_task.start()

    def cog_unload(self):
        self.rotation_task.cancel()
        asyncio.create_task(self.session.close())

    async def track_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> List[app_commands.Choice[str]]:
        """为轨道选择提供自动补全列表。"""
        choices = []
        tracks = self.manager.get_all_tracks(interaction.guild_id)
        for track in tracks:
            # 优先显示自定义名称，否则显示身份组名称
            role = interaction.guild.get_role(track.role_id)
            if not role: continue  # 跳过失效的轨道

            display_name = track.name or role.name

            # 简单的模糊搜索
            if current.lower() in display_name.lower():
                choices.append(app_commands.Choice(
                    name=f"{display_name} ({len(track.presets)}个预设)",  # 在选项中提供更多上下文信息
                    value=str(track.role_id)  # value 必须是 string, int, or float
                ))
        return choices[:25]  # Discord 限制最多25个选项

    # --- Commands ---

    jukebox = app_commands.Group(name="身份组轮播", description="身份组外观自动轮播系统")

    @jukebox.command(name="私人面板", description="打开身份组轮播面板")
    async def public_panel(self, interaction: discord.Interaction):
        if not interaction.guild: return
        view = UserJukeboxView(self, interaction.guild)
        await view.show(interaction)

    @jukebox.command(name="管理面板", description="查看和配置轮播轨道 (查看/删除/开关)")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def admin_panel(self, interaction: discord.Interaction):
        if not interaction.guild: return
        view = AdminDashboardView(self, interaction.guild)
        await view.show(interaction)

    @jukebox.command(name="添加预设", description="向轨道添加一个新的外观预设")
    @app_commands.describe(
        track="要添加预设到的轨道",
        name="预设名称",
        color="颜色 (HEX格式，如 #FF0000)",
        icon="上传图标文件 (支持 PNG/JPG/GIF)"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.autocomplete(track=track_autocomplete)
    async def add_preset(self,
                         interaction: discord.Interaction,
                         track: str,
                         name: str,
                         color: str,
                         icon: typing.Optional[discord.Attachment] = None):

        await interaction.response.defer(ephemeral=True)

        try:
            target_role_id = int(track)
        except ValueError:
            return await interaction.followup.send("❌ 无效的轨道选择。", ephemeral=True)

        # 1. 检查轨道是否存在
        track_obj = self.manager.get_track(interaction.guild_id, target_role_id)
        target_role = interaction.guild.get_role(target_role_id)

        if not track_obj or not target_role:
            return await interaction.followup.send("❌ 目标轨道或身份组不存在。", ephemeral=True)

        # 2. 验证颜色
        try:
            discord.Color.from_str(color)
        except ValueError:
            return await interaction.followup.send("❌ 颜色格式无效。", ephemeral=True)

        # 3. 处理图片
        filename = None
        if icon:
            # 限制文件大小 (Discord 身份组图标限制 256kb，虽然我们只是存，但太大也没用)
            if icon.size > 1024 * 1024 * 2:  # 2MB 限制
                return await interaction.followup.send("❌ 图片太大了，请上传小于 2MB 的图片。", ephemeral=True)

            try:
                image_bytes = await icon.read()
                # 简单获取后缀
                ext = icon.filename.split('.')[-1] if '.' in icon.filename else "png"
                filename = await self.manager.save_icon(image_bytes, ext)
            except Exception as e:
                self.logger.error(f"Save icon failed: {e}")
                return await interaction.followup.send("❌ 图片保存失败。", ephemeral=True)

        # 4. 保存预设
        preset = Preset(name=name, color=color, icon_filename=filename)
        await self.manager.add_preset(interaction.guild_id, target_role_id, preset)

        display_name = track_obj.name or target_role.name
        msg = f"✅ 已向 {display_name} 添加预设：**{name}**"
        if filename: msg += " (含图标)"
        return await interaction.followup.send(msg, ephemeral=True)

    @jukebox.command(name="克隆预设", description="从现有的身份组复制外观作为预设")
    @app_commands.describe(
        track="要克隆预设到的目标轨道",
        source_role="提供外观的来源身份组"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.autocomplete(track=track_autocomplete)
    async def clone_preset(self, interaction: discord.Interaction, track: str, source_role: discord.Role):
        await interaction.response.defer(ephemeral=True)

        try:
            target_role_id = int(track)
        except ValueError:
            return await interaction.followup.send("❌ 无效的轨道选择。", ephemeral=True)

        track_obj = self.manager.get_track(interaction.guild_id, target_role_id)
        target_role = interaction.guild.get_role(target_role_id)

        if not track_obj or not target_role:
            return await interaction.followup.send("❌ 目标轨道或身份组不存在。", ephemeral=True)

        filename = None
        if source_role.icon:
            try:
                # 即使是动态头像，Discord 也可以 read() 出来
                icon_bytes = await source_role.icon.read()
                ext = "gif" if source_role.icon.is_animated() else "png"
                filename = await self.manager.save_icon(icon_bytes, ext)
            except Exception as e:
                self.logger.error(f"Clone icon failed: {e}")
                return await interaction.followup.send("⚠️ 克隆图标失败，将只克隆颜色和名称。", ephemeral=True)

        preset = Preset(name=source_role.name, color=str(source_role.color), icon_filename=filename)
        await self.manager.add_preset(interaction.guild_id, target_role_id, preset)

        display_name = track_obj.name or target_role.name
        return await interaction.followup.send(f"✅ 已从 {source_role.name} 克隆预设到 **{display_name}**。", ephemeral=True)

    # --- Rotation Task ---

    @tasks.loop(minutes=1)
    async def rotation_task(self):
        """每分钟检查一次是否有轨道需要轮换。"""
        try:
            # 获取需要执行的操作
            # 注意：get_due_rotations 会更新内存中的时间戳，所以我们需要保存一次
            actions = await asyncio.to_thread(self.manager.get_due_rotations)

            if actions:
                await self.manager.save_data()  # 保存更新后的时间戳和索引

            for guild_id, track, preset in actions:
                await self._apply_preset(guild_id, track.role_id, preset)

        except Exception as e:
            self.logger.error(f"[Jukebox] Rotation task error: {e}")

    async def _apply_preset(self, guild_id: int, role_id: int, preset):
        """执行具体的身份组修改操作。"""
        guild = self.bot.get_guild(guild_id)
        if not guild: return

        role = guild.get_role(role_id)
        if not role:
            # 身份组如果被删了，可以考虑自动删除轨道，或者仅仅打印日志
            self.logger.warning(f"[Jukebox] Role {role_id} not found in {guild.name}.")
            return

        # 下载图标
        icon_bytes = None
        if preset.icon_filename:
            # 这一步是同步IO读取，但因为是本地SSD，通常很快
            # 如果文件很大，可以在 manager 里用 asyncio.to_thread 包装
            icon_bytes = await asyncio.to_thread(self.manager.get_icon_bytes, preset.icon_filename)

        try:
            await role.edit(
                name=preset.name,
                color=discord.Color.from_str(preset.color),
                display_icon=icon_bytes,
                reason=f"Jukebox Rotation: {preset.name}"
            )
        except discord.Forbidden:
            self.logger.warning(f"Missing permission to edit role {role.name} in {guild.name}")
        except Exception as e:
            self.logger.error(f"Failed to edit role {role.id}: {e}")

    @rotation_task.before_loop
    async def before_task(self):
        await self.bot.wait_until_ready()


class OpenLobbyButton(ui.Button):
    def __init__(self, cog: RoleJukeboxCog):
        # 放在主面板上的按钮，负责打开 User View
        super().__init__(
            label="身份点歌机",
            style=discord.ButtonStyle.primary,
            emoji="🎶",
            custom_id="role_jukebox:open_panel"
        )
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        view = UserJukeboxView(self.cog, interaction.guild)
        await view.show(interaction)


async def setup(bot: RoleBot):
    await bot.add_cog(RoleJukeboxCog(bot))
