from __future__ import annotations

import asyncio
from typing import List
from typing import Optional, TYPE_CHECKING, Any

import aiohttp
import discord
from discord import app_commands, ui
from discord.ext import tasks

from role_system.role_jukebox.admin_view import AdminDashboardView
from role_system.role_jukebox.manager import RoleJukeboxManager
from role_system.role_jukebox.models import Preset
from role_system.role_jukebox.user_view import UserJukeboxView
from utility.feature_cog import FeatureCog, PanelEntry

if TYPE_CHECKING:
    from main import RoleBot


class RoleJukeboxCog(FeatureCog, name="RoleJukebox"):
    """
    身份组自动轮播系统。
    管理员配置轨道（身份组+预设池+间隔），机器人自动在该身份组上循环应用外观。
    """

    async def update_safe_roles_cache(self):
        pass

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """
        [框架方法]
        返回要显示在机器人主控面板（/panel）上的按钮。
        管理员用指令配置，所以这里只提供给用户的入口。
        """
        return [
            PanelEntry(
                button=OpenLobbyButton(self),
                description="随着时间变换的有趣身份组。"
            )
        ]

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.manager = RoleJukeboxManager.get_instance(logger=self.logger)
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
        secondary_color="[可选] 副颜色，用于创建渐变效果",
        tertiary_color="[可选] 启用全息模式 (目前输入任意HEX值均可触发，如 #000000)",
        icon="上传图标文件 (支持 PNG/JPG/GIF)"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.autocomplete(track=track_autocomplete)
    async def add_preset(
            self,
            interaction: discord.Interaction,
            track: str,
            name: str,
            color: str,
            secondary_color: Optional[str] = None,
            tertiary_color: Optional[str] = None,
            icon: Optional[discord.Attachment] = None
    ):

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
        preset = Preset(
            name=name,
            color=color,
            secondary_color=secondary_color,
            tertiary_color=tertiary_color,
            icon_filename=filename,
        )
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

        # 检查并克隆副色
        secondary_color_str = str(source_role.secondary_color) if source_role.secondary_color else None
        tertiary_color_str = str(source_role.tertiary_color) if source_role.tertiary_color else None

        preset = Preset(
            name=source_role.name,
            color=str(source_role.color),
            secondary_color=secondary_color_str,
            tertiary_color=tertiary_color_str,
            icon_filename=filename
        )
        await self.manager.add_preset(interaction.guild_id, target_role_id, preset)

        display_name = track_obj.name or target_role.name
        return await interaction.followup.send(f"✅ 已从 {source_role.name} 克隆预设到 **{display_name}**。", ephemeral=True)

    @jukebox.command(name="狂暴模式", description="⚡ 开启限时极速轮播 (慎用！极消耗每日限额)")
    @app_commands.describe(
        track="要加速的轨道",
        duration="持续时间 (秒)，建议不超过 300秒",
        confirm="确认你知道这会消耗大量 API 配额",
        interval="[可选] 狂暴模式下的轮播间隔 (秒)，默认为1秒"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.autocomplete(track=track_autocomplete)
    async def hyper_mode(
            self,
            interaction: discord.Interaction,
            track: str,
            duration: int,
            confirm: bool,
            interval: int = 1
    ):
        """
        开启限时狂暴模式。
        """
        if not confirm:
            return await interaction.response.send_message("❌ 你必须确认你知道这会消耗 API 配额才能使用。", ephemeral=True)

        # 安全限制：防止误操作设太久，直接把号封了
        MAX_DURATION = 900  # 最多 900次切换
        if duration > MAX_DURATION * interval:
            return await interaction.response.send_message(f"❌ 为了安全，狂暴模式一次最多只能持续 {MAX_DURATION} 次切换，而你设置了 {duration // interval} 次。",
                                                           ephemeral=True)

        try:
            target_role_id = int(track)
        except ValueError:
            return await interaction.response.send_message("❌ 无效的轨道。", ephemeral=True)

        track_obj = self.manager.get_track(interaction.guild_id, target_role_id)
        if not track_obj:
            return await interaction.response.send_message("❌ 轨道不存在。", ephemeral=True)

        # 计算油耗
        cost = duration // interval
        daily_limit = 1000
        cost_percent = (cost / daily_limit) * 100

        await interaction.response.defer()

        # 1. 记录原始速度
        original_interval = track_obj.interval_seconds

        # 如果原本的速度已经比狂暴模式还快了，就提示一下
        if original_interval <= interval:
            return await interaction.followup.send(f"⚠️ 该轨道当前速度 ({original_interval}秒/次) 已快于或等于你设置的狂暴速度 ({interval}秒/次)！",
                                                   ephemeral=True)

        # 2. 开启狂暴
        await self.manager.set_hyper_mode(
            interaction.guild_id,
            target_role_id,
            active=True,
            hyper_interval=interval
        )

        target_role = interaction.guild.get_role(target_role_id)
        role_name = target_role.name if target_role else "未知身份组"

        embed = discord.Embed(
            title="⚡ 狂暴模式已启动！",
            description=(
                f"**轨道**: {role_name}\n"
                f"**持续**: {duration} 秒\n"
                f"**频率**: **{interval} 秒/次**\n"
                f"**预计消耗配额**: {cost} 次 (约占每日限额的 {cost_percent:.1f}%，每日限额大约1000，为估算值，可能略微浮动。)\n\n"
                "🔥 *Enjoy the show!*"
            ),
            color=discord.Color.brand_red()
        )
        embed.set_footer(text="倒计时结束后将自动恢复原速")
        await interaction.followup.send(embed=embed)

        # 3. 倒计时等待 (非阻塞方式)
        await asyncio.sleep(duration)

        # 4. 恢复原速
        # 重新获取轨道对象（防止中途被删）
        current_track = self.manager.get_track(interaction.guild_id, target_role_id)
        if current_track:
            await self.manager.set_hyper_mode(interaction.guild_id, target_role_id, active=False, original_interval=original_interval)

            try:
                await interaction.followup.send(
                    f"✅ 狂暴模式结束。**{role_name}** 已恢复为 {original_interval}秒/次 的安全巡航速度。",
                    ephemeral=True
                )
            except:
                pass  # 如果原来消息被删了就算了

    # --- Rotation Task ---

    @tasks.loop(seconds=1)
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

        # --- 获取轨道信息并构建最终名称 ---
        track = self.manager.get_track(guild_id, role_id)
        if not track:
            self.logger.warning(f"Track for role {role_id} not found when applying preset.")
            # 即使轨道数据丢失，也按原计划执行，但不加前缀
            final_name = preset.name
        else:
            if track.name_prefix:
                final_name = f"{track.name_prefix}{preset.name}"
            else:
                final_name = preset.name

        # 确保名称不超过 Discord 100个字符的限制
        final_name = final_name[:100]

        # 下载图标
        icon_bytes = None
        if preset.icon_filename:
            # 这一步是同步IO读取，但因为是本地SSD，通常很快
            # 如果文件很大，可以在 manager 里用 asyncio.to_thread 包装
            icon_bytes = await self.manager.get_icon_bytes(preset.icon_filename)

        try:
            edit_kwargs: dict[str, Any] = {
                'name': final_name,
                'reason': f"Jukebox Rotation: {preset.name}",
            }
            if icon_bytes:
                edit_kwargs['display_icon'] = icon_bytes
            # 根据预设配置决定颜色模式
            if preset.tertiary_color:
                # 全息模式：使用固定的常量值
                # 这些值来自 Discord API 文档
                edit_kwargs['color'] = discord.Colour(11127295)
                edit_kwargs['secondary_color'] = discord.Colour(16759788)
                edit_kwargs['tertiary_color'] = discord.Colour(16761760)
            elif preset.secondary_color:
                # 渐变模式
                edit_kwargs['color'] = discord.Color.from_str(preset.color)
                edit_kwargs['secondary_color'] = discord.Color.from_str(preset.secondary_color)
            else:
                # 单色模式
                edit_kwargs['color'] = discord.Color.from_str(preset.color)

            await role.edit(**edit_kwargs)

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
            label="轮播身份组",
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
