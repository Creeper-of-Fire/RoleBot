from __future__ import annotations

import asyncio
import io
import os
import platform
import zipfile

import config
from core.embed_link.embed_manager import EmbedLinkManager

try:
    import distro

    IS_LINUX = True
except ImportError:
    IS_LINUX = False

import typing
from datetime import datetime, timezone
from typing import Dict, List, Optional

import discord
import psutil
from discord import app_commands
from discord.ext import commands, tasks

from core.main_panel_view import MainPanelView, create_main_panel_ui

if typing.TYPE_CHECKING:
    from main import RoleBot
    from utility.feature_cog import FeatureCog
    from activity_tracker.TrackActivityCog import TrackActivityCog


def _format_bytes(size: int) -> str:
    """将字节大小格式化为 KB, MB, GB 等。"""
    if size < 1024:
        return f"{size} B"
    for unit in ["", "K", "M", "G", "T", "P"]:
        if size < 1024.0:
            # 返回带有两位小数的字符串，例如 "956.00 MB"
            return f"{size:.2f} {unit}B"
        size /= 1024.0
    return f"{size:.2f} PB"


class CoreCog(commands.Cog, name="Core"):
    """
    核心协调Cog。
    - 管理全局的 role_name_cache。
    - 提供主面板入口命令。
    - 周期性地触发所有功能模块的安全缓存更新。
    - 对其他模块的具体实现和配置保持无知。
    """

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger

        self.start_time = datetime.now(timezone.utc)

        self.role_name_cache: Dict[int, str] = {}
        self.feature_cogs: List[FeatureCog] = []

    async def cog_load(self) -> None:
        """当 Cog 被加载时，启动后台任务。"""
        self.logger.info("CoreCog 已加载，正在启动后台任务...")
        self._update_all_caches_task.start()
        self.update_registered_embeds_task.start()
        self._backup_data_task.start()

    def cog_unload(self):
        self._update_all_caches_task.cancel()
        self.update_registered_embeds_task.cancel()
        self._backup_data_task.cancel()

    @tasks.loop(hours=1)
    async def _update_all_caches_task(self):
        """每小时调用所有已注册功能模块的缓存更新方法。"""
        self.logger.info("开始执行每小时的全局安全缓存更新...")
        if not self.feature_cogs:
            self.logger.warning("没有功能模块注册到 CoreCog，缓存更新任务跳过。")
            return

        # 使用 ayncio.gather 并行执行所有模块的更新
        results = await asyncio.gather(
            *[cog.update_safe_roles_cache() for cog in self.feature_cogs],
            return_exceptions=True
        )

        for cog, result in zip(self.feature_cogs, results):
            if isinstance(result, Exception):
                self.logger.error(f"模块 {cog.qualified_name} 在更新缓存时发生错误: {result}", exc_info=result)

        self.logger.info("每小时全局安全缓存更新完毕。")

    @commands.Cog.listener()
    async def on_ready(self):
        """当 Cog 准备就绪时，注册持久化视图。"""
        # 注意：MainPanelView 的构造函数需要一个 cog 实例，
        # 尽管它现在大部分功能都分散了，但为了向后兼容和简单性，
        # 我们可以暂时传入 CoreCog 自身或任一其他 Cog。
        # 更好的做法是重构 MainPanelView，使其不依赖任何特定的 feature cog。
        # 这里我们暂时传入 CoreCog。
        self.bot.add_view(MainPanelView(self))  # MainPanelView 现在由 CoreCog 负责
        self.logger.info("核心模块已就绪，主控制面板持久化视图已注册。")

    @tasks.loop(minutes=15)
    async def update_registered_embeds_task(self):
        """定时刷新所有已注册的EmbedLinkManager。"""
        self.bot.logger.info("开始刷新所有已注册的Embed链接...")
        managers = EmbedLinkManager.get_all_managers()
        if not managers:
            self.bot.logger.info("没有已注册的Embed链接管理器，跳过刷新。")
            return

        for manager in managers:
            await manager.refresh_from_config()
        self.bot.logger.info(f"已完成对 {len(managers)} 个管理器的刷新。")

    @tasks.loop(hours=12)
    async def _backup_data_task(self):
        """每12小时自动备份 data 目录。"""
        self.logger.info("开始执行计划的数据备份任务...")

        if not config.BACKUP_CHANNEL_ID:
            self.logger.warning("未配置 BACKUP_CHANNEL_ID，自动备份任务跳过。")
            return

        channel = self.bot.get_channel(config.BACKUP_CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel):
            self.logger.error(f"找不到备份频道 ID: {config.BACKUP_CHANNEL_ID} 或该频道不是文本频道。")
            return

        try:
            backup_file, message = await self._create_backup_zip()

            if backup_file:
                await channel.send(
                    content=f"📦 **自动数据备份**\n"
                            f"已于 {discord.utils.format_dt(datetime.now(), style='F')} 完成 `data` 目录的自动备份。",
                    file=backup_file
                )
                self.logger.info(f"自动数据备份成功，文件已发送至频道 #{channel.name}。")
            else:
                # message 将包含原因，例如 "目录为空"
                await channel.send(f"ℹ️ **自动数据备份状态**\n{message}")
                self.logger.info(f"自动数据备份跳过: {message}")
        except discord.errors.Forbidden:
            self.logger.error(f"机器人没有权限在频道 #{channel.name} ({channel.id}) 发送消息或上传文件。")
        except Exception as e:
            self.logger.error(f"执行自动数据备份任务时发生未知错误: {e}", exc_info=True)
            try:
                await channel.send(f"❌ **自动数据备份失败**\n执行备份时发生严重错误，请检查机器人日志。")
            except Exception:
                pass  # 避免在无法发送错误消息时出现级联错误

    @_backup_data_task.before_loop
    @update_registered_embeds_task.before_loop
    @_update_all_caches_task.before_loop
    async def before_cache_update_task(self):
        """在任务开始前，等待机器人就绪并执行一次初始缓存。"""
        await self.bot.wait_until_ready()
        # 确保在第一次循环前，所有 feature_cogs 都已注册
        # setup_hook 是更稳妥的地方，但这里延迟一下也能工作
        await asyncio.sleep(5)
        self.logger.info("CoreCog 已就绪，准备执行首次缓存更新...")

    core_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨核心", description="机器人核心管理与状态指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @core_group.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        """发送一个公共的身份组管理入口面板。"""
        embed, view = create_main_panel_ui(self)
        await interaction.response.send_message(embed=embed, view=view)

    async def link_module_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """为配置指令提供模块键的自动补全。"""
        keys = EmbedLinkManager.get_registered_keys()
        return [
            app_commands.Choice(name=key, value=key)
            for key in keys if current.lower() in key.lower()
        ]

    @core_group.command(name="配置embed链接", description="配置一个模块使用的Discord消息链接")
    @app_commands.describe(module="要配置的模块名", url="指向Discord消息的URL (留空以清除)")
    @app_commands.autocomplete(module=link_module_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def config_embed_link(self, interaction: discord.Interaction, module: str, url: typing.Optional[str] = None):
        """配置或清除一个模块的消息链接。"""
        manager = EmbedLinkManager.get_manager(module)
        if not manager:
            await interaction.response.send_message(f"❌ 错误：找不到名为 `{module}` 的模块。可用模块: `{'`, `'.join(EmbedLinkManager.get_registered_keys())}`",
                                                    ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            if url:
                await manager.set_from_url(url)
                await interaction.edit_original_response(content=f"✅ 成功！模块 `{module}` 的链接已更新。新的Embed已加载。")
            else:
                await manager.clear_config()
                await interaction.edit_original_response(content=f"🗑️ 成功！模块 `{module}` 的链接配置已被清除。它现在将显示默认内容。")
        except ValueError as e:
            await interaction.edit_original_response(content=f"❌ 错误: {e}")
        except Exception as e:
            self.bot.logger.error(f"配置模块 '{module}' 时发生未知错误: {e}")
            await interaction.edit_original_response(content=f"❌ 发生未知错误，请检查日志。")

    @core_group.command(name="系统状态", description="显示机器人和服务器的实时系统信息。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def system_status(self, interaction: discord.Interaction):
        """
        显示一个包含详细系统和 Redis 信息的监控面板。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)

        # --- 1. 获取进程和机器人信息 ---
        process = psutil.Process()
        try:
            mem_info = process.memory_full_info()
            bot_mem_uss = mem_info.uss
            bot_mem_rss = mem_info.rss
        except AttributeError:
            mem_info = process.memory_info()
            bot_mem_rss = mem_info.rss
            bot_mem_uss = bot_mem_rss

        # --- 2. 获取系统资源信息 ---
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_info = psutil.virtual_memory()

        # --- 3. 获取操作系统信息 ---
        os_display_name = ""
        kernel_display = ""
        os_ver_display = ""
        if IS_LINUX:
            os_display_name = distro.name()
            kernel_display = f"Linux {platform.release()}"
            os_ver_display = f"Linux ({distro.name()} {distro.version()})"
        else:
            os_display_name = platform.system()
            kernel_display = platform.release()
            os_ver_display = f"{platform.system()} {platform.version()}"

        # --- 4. 构建 Embed ---
        embed = discord.Embed(
            title="🤖 系统信息",
            color=discord.Color.from_rgb(107, 222, 122),
            timestamp=discord.utils.utcnow()
        )
        if self.bot.user.display_avatar:
            embed.set_thumbnail(url=self.bot.user.display_avatar.url)

        # Section 1: System Info
        embed.add_field(name="🖥️ 系统名称", value=f"{os_display_name}", inline=True)
        embed.add_field(name="🔧 内核版本", value=f"{kernel_display}", inline=True)
        embed.add_field(name="💻 操作系统版本", value=f"{os_ver_display}", inline=True)

        # Section 2: Resources
        embed.add_field(name="🐍 Python 版本", value=f"{platform.python_version()}", inline=True)
        embed.add_field(name="🔥 CPU 使用率", value=f"{cpu_usage}%", inline=True)
        embed.add_field(
            name="🧠 系统内存",
            value=f"{ram_info.percent}%\n"
                  f"({_format_bytes(ram_info.used)} / {_format_bytes(ram_info.total)})",
            inline=True
        )

        # Section 3: Bot Info
        uptime = datetime.now(timezone.utc) - self.start_time
        days, remainder = divmod(int(uptime.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{days}天 {hours}时 {minutes}分"

        embed.add_field(name="📊 Bot 内存 (独占)", value=f"{_format_bytes(bot_mem_uss)}", inline=True)
        embed.add_field(name="📈 Bot 内存 (常驻)", value=f"{_format_bytes(bot_mem_rss)}", inline=True)
        embed.add_field(name="👥 缓存用户数", value=f"{len(self.bot.users)}", inline=True)
        embed.add_field(name="⏱️ 机器人运行时长", value=f"{uptime_str}", inline=False)

        # --- 5. 获取并添加 Redis 统计信息 ---
        # 动态获取 TrackActivityCog 实例
        activity_cog: typing.Optional[TrackActivityCog] = self.bot.get_cog("TrackActivity")

        if activity_cog:
            # 获取 Redis 统计信息
            redis_stats = await activity_cog.get_redis_stats()
            if redis_stats:
                redis_info_str = (
                    f"**版本:** {redis_stats['version']}\n"
                    f"**运行时长:** {redis_stats['uptime']}\n"
                    f"**内存占用:** {redis_stats['memory']}\n"
                    f"**客户端数:** {redis_stats['clients']}\n"
                    f"**总键数 (DB0):** {redis_stats['keys']}"
                )
                embed.add_field(name="🗄️ Redis 状态", value=redis_info_str, inline=True)
            else:
                embed.add_field(name="🗄️ Redis 状态", value="无法获取统计信息。", inline=True)

            # 获取内部缓存统计
            this_dtos, total_dtos = activity_cog.get_processor_cache_stats(guild=interaction.guild)
            cache_info_str = (
                f"**当前服务器频道信息缓存 (DTOs):** {this_dtos}\n"
                f"**全部服务器频道信息缓存 (DTOs):** {total_dtos}"
            )
            embed.add_field(name="🧠 活跃度模块缓存", value=cache_info_str, inline=True)

        else:
            # 如果 TrackActivityCog 未加载，则不显示 Redis 部分
            pass

        embed.set_footer(text=f"{self.bot.user.name} 系统监控")

        await interaction.followup.send(embed=embed)

    async def _create_backup_zip(self) -> tuple[Optional[discord.File], str]:
        """
        打包 data 目录并返回一个 discord.File 对象和一条消息。
        如果目录为空或不存在，返回 (None, "无需备份的原因")。
        如果打包失败，返回 (None, "错误信息")。
        """
        data_dir = "data"

        if not os.path.isdir(data_dir) or not os.listdir(data_dir):
            return None, f"ℹ️ `{data_dir}` 目录不存在或为空，无需备份。"

        memory_file = io.BytesIO()
        try:
            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, files in os.walk(data_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, data_dir)
                        zf.write(file_path, arcname)
        except Exception as e:
            self.logger.error(f"创建数据备份 ZIP 时发生错误: {e}", exc_info=True)
            return None, f"❌ 创建备份失败: `{e}`"

        memory_file.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.bot.user.name}的数据备份_{timestamp}.zip"
        backup_file = discord.File(memory_file, filename=filename)

        return backup_file, filename

    @core_group.command(name="获取数据备份", description="打包并发送 data 目录下的所有数据文件。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backup_data(self, interaction: discord.Interaction):
        """
        创建一个包含 'data' 目录下所有文件的 zip 压缩包，并私密地发送给命令使用者。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)

        self.logger.info(
            f"数据备份操作触发: "
            f"用户: {interaction.user} ({interaction.user.id}), "
            f"服务器: {interaction.guild.name} ({interaction.guild.id})"
        )

        backup_file, message = await self._create_backup_zip()

        if backup_file:
            await interaction.followup.send(
                content=f"📦 {interaction.user.mention}，这是您请求的数据备份文件：",
                file=backup_file,
                ephemeral=False
            )
        else:
            # message 包含原因 (例如，目录为空、出错等)
            await interaction.followup.send(content=message, ephemeral=True)

    def register_feature_cog(self, cog: FeatureCog):
        """允许其他功能模块向核心Cog注册自己。"""
        if asyncio.iscoroutinefunction(cog.update_safe_roles_cache):
            self.feature_cogs.append(cog)
            self.logger.info(f"功能模块 {cog.qualified_name} 已成功注册到 CoreCog。")
        else:
            self.logger.error(f"尝试注册的模块 {cog.qualified_name} 未实现 'update_safe_roles_cache' 异步方法，注册失败。")


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CoreCog(bot))
