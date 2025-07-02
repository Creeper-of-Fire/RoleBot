from __future__ import annotations

import asyncio
import typing
from typing import Dict, List

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
from core.main_panel_view import MainPanelView
from utility.helpers import create_progress_bar

if typing.TYPE_CHECKING:
    from main import RoleBot
    from utility.feature_cog import FeatureCog


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

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
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

    @app_commands.command(name="刷新成员缓存", description="【非常耗时！注意！】手动拉取服务器所有成员信息到机器人缓存中（带进度条）。")
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


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(CoreCog(bot))
