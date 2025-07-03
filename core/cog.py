from __future__ import annotations

import asyncio
import os
import platform
import typing
from datetime import datetime, timezone
from typing import Dict, List

import discord
import psutil
from discord import app_commands
from discord.ext import commands, tasks

import config
from core.main_panel_view import MainPanelView
from utility.helpers import create_progress_bar

if typing.TYPE_CHECKING:
    from main import RoleBot
    from utility.feature_cog import FeatureCog


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

    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger

        self.start_time = datetime.now(timezone.utc)

        self.role_name_cache: Dict[int, str] = {}
        self.feature_cogs: List[FeatureCog] = []
        self._update_all_caches_task.start()

    def cog_unload(self):
        self._update_all_caches_task.cancel()

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

    @_update_all_caches_task.before_loop
    async def before_cache_update_task(self):
        """在任务开始前，等待机器人就绪并执行一次初始缓存。"""
        await self.bot.wait_until_ready()
        # 确保在第一次循环前，所有 feature_cogs 都已注册
        # setup_hook 是更稳妥的地方，但这里延迟一下也能工作
        await asyncio.sleep(5)
        self.logger.info("CoreCog 已就绪，准备执行首次缓存更新...")

    def register_feature_cog(self, cog: FeatureCog):
        """允许其他功能模块向核心Cog注册自己。"""
        if asyncio.iscoroutinefunction(cog.update_safe_roles_cache):
            self.feature_cogs.append(cog)
            self.logger.info(f"功能模块 {cog.qualified_name} 已成功注册到 CoreCog。")
        else:
            self.logger.error(f"尝试注册的模块 {cog.qualified_name} 未实现 'update_safe_roles_cache' 异步方法，注册失败。")

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

    rolebot_group = app_commands.Group(name=config.COMMAND_GROUP_NAME, description="机器人核心管理与状态指令")

    @rolebot_group.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        """发送一个公共的身份组管理入口面板。"""
        # 此命令现在不关心任何具体配置，只是发送面板
        embed = discord.Embed(title="✨ 身份组自助中心 ✨", description="欢迎来到身份组自助中心！\n\n点击下方的按钮来管理你的身份组或查询状态。",
                              color=discord.Color.blurple())
        embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")

        # MainPanelView 的 __init__ 需要修改，以动态地从 bot 获取 cogs
        view = MainPanelView(self)
        await interaction.response.send_message(embed=embed, view=view)

    @rolebot_group.command(name="刷新成员缓存", description="【非常耗时！注意！】手动拉取服务器所有成员信息到机器人缓存中（带进度条）。")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    async def refresh_member_cache(self, interaction: discord.Interaction):
        """
        手动触发从 Discord API 拉取服务器所有成员，并显示实时进度条。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)
        guild = interaction.guild
        if not guild:
            await interaction.edit_original_response(content="❌ 无法获取服务器信息。")
            return

        total_members = guild.member_count
        if total_members == 0:
            await interaction.edit_original_response(content="✅ 服务器中没有成员。")
            return

        self.logger.info(f"服务器 '{guild.name}' (ID: {guild.id}) 由 {interaction.user} 手动触发了成员缓存刷新。")

        # 初始进度条消息
        embed = discord.Embed(
            title="⏳ 正在刷新成员缓存...",
            description=f"正在从服务器拉取 **{total_members}** 名成员的信息...",
            color=discord.Color.blue()
        )
        embed.add_field(name="进度", value=create_progress_bar(0, total_members), inline=False)
        await interaction.edit_original_response(embed=embed)

        fetched_count = 0
        last_update_count = 0

        # 使用异步迭代器逐个获取成员
        try:
            async for member in guild.fetch_members(limit=None):
                fetched_count += 1
                # 为了避免过于频繁地编辑消息（API限速），我们每获取一定数量的成员或进度变化超过5%时才更新
                if fetched_count - last_update_count >= 100 or fetched_count == total_members:
                    last_update_count = fetched_count

                    embed.description = f"正在处理成员: **{fetched_count} / {total_members}**"
                    embed.set_field_at(
                        index=0,  # 更新第一个字段
                        name="进度",
                        value=create_progress_bar(fetched_count, total_members),
                        inline=False
                    )
                    await interaction.edit_original_response(embed=embed)
                    # 稍微暂停一下，给API一点喘息空间
                    await asyncio.sleep(0.1)

        except Exception as e:
            self.logger.error(f"刷新成员缓存时发生错误: {e}", exc_info=True)
            error_embed = discord.Embed(
                title="❌ 刷新中断",
                description=f"在处理过程中发生错误。\n`{e}`",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(embed=error_embed)
            return

        # 任务完成后的最终消息
        final_embed = discord.Embed(
            title="✅ 成员缓存刷新完成",
            description=f"成功将 **{fetched_count}** 名（共 {total_members} 名）成员的信息同步到了机器人缓存中。",
            color=discord.Color.green()
        )
        final_embed.set_footer(text=f"当前缓存成员数: {len(guild.members)}")
        await interaction.edit_original_response(embed=final_embed)

    @rolebot_group.command(name="系统状态", description="显示机器人和服务器的实时系统信息。")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    async def system_status(self, interaction: discord.Interaction):
        """显示一个包含详细系统信息的监控面板。"""
        await interaction.response.defer(ephemeral=False, thinking=True)

        # --- 1. 获取进程和机器人信息 ---
        process = psutil.Process()
        # memory_full_info() 在某些系统上比 memory_info() 提供更多信息
        # 它在 Linux 和 Windows 上都可用
        try:
            mem_info = process.memory_full_info()
            bot_mem_rss = mem_info.rss  # 常驻内存
            bot_mem_uss = mem_info.uss  # 独占内存（作为“已分配”的代表）
        except AttributeError:  # 在某些权限受限或不支持的系统上回退
            mem_info = process.memory_info()
            bot_mem_rss = mem_info.rss
            bot_mem_uss = bot_mem_rss  # 如果无法获取uss, 就让两个值相等

        # --- 2. 获取系统信息 ---
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_info = psutil.virtual_memory()

        # --- 3. 获取操作系统信息 ---
        # os.uname() 在 Windows 上不可用，所以我们做个兼容处理
        if hasattr(os, "uname"):
            uname = os.uname()
            os_name = f"{uname.sysname}"
            kernel_ver = f"{uname.release}"
            os_ver = f"{uname.version}"
        else:  # For Windows
            os_name = platform.system()
            kernel_ver = platform.release()
            os_ver = platform.version()

        # --- 4. 构建 Embed ---
        embed = discord.Embed(
            title="💻 系统信息",
            color=discord.Color.green(),
            timestamp=discord.utils.utcnow()
        )
        if self.bot.user.display_avatar:
            embed.set_thumbnail(url=self.bot.user.display_avatar.url)

        # 匹配您截图的布局
        embed.add_field(name="🖥️ 系统名称", value=f"`{os_name}`", inline=True)
        embed.add_field(name="🔧 内核版本", value=f"`{kernel_ver}`", inline=True)
        # 为了更美观地显示，可以截断过长的 os_ver
        os_ver_short = (os_ver[:45] + '...') if len(os_ver) > 45 else os_ver
        embed.add_field(name=" OS 版本", value=f"`{os_ver_short}`", inline=True)

        # 您的截图是Rust, 但项目是Python, 所以显示Python版本
        embed.add_field(name="🐍 Python 版本", value=f"`{platform.python_version()}`", inline=True)
        embed.add_field(name="🔥 CPU 使用率", value=f"`{cpu_usage}%`", inline=True)
        embed.add_field(
            name="🧠 系统内存",
            value=f"`{ram_info.percent}%` ({_format_bytes(ram_info.used)} / {_format_bytes(ram_info.total)})",
            inline=True
        )

        # 添加一个空行字段来强制换行，以实现更好的布局
        embed.add_field(name="\u200b", value="\u200b", inline=False)

        embed.add_field(name="📊 Bot 内存 (独占)", value=f"`{_format_bytes(bot_mem_uss)}`", inline=True)
        embed.add_field(name="📈 Bot 内存 (常驻)", value=f"`{_format_bytes(bot_mem_rss)}`", inline=True)

        embed.add_field(name="👥 缓存用户数", value=f"`{len(self.bot.users)}`", inline=True)

        # 计算运行时间
        uptime = datetime.now(timezone.utc) - self.start_time
        days, remainder = divmod(int(uptime.total_seconds()), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{days}天 {hours}时 {minutes}分"
        embed.add_field(name="⏱️ 机器人运行时长", value=f"`{uptime_str}`", inline=True)

        embed.set_footer(text="机器人系统监控")

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(CoreCog(bot))
