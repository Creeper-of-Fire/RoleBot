# activity_tracker/cog.py

from __future__ import annotations

import asyncio
import collections
import time
import typing
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands, ui
from discord.ext import commands

import config
from activity_tracker.data_manager import DataManager, BEIJING_TZ
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from main import RoleBot

# --- 【新】热力图表情符号定义 ---
# 0条: ⬜, 1-5条: 🟨, 6-15条: 🟩, 16-30条: 🟦, 31+条: 🟥
HEATMAP_EMOJIS = {
    0: '⬜',
    1: '🟨',
    6: '🟩',
    16: '🟦',
    31: '🟥'
}
HEATMAP_THRESHOLDS = sorted(HEATMAP_EMOJIS.keys())

# --- 【新】每页显示的最大频道数 ---
MAX_CHANNELS_PER_PAGE = 10


# ===================================================================
# 1. 持久化视图和按钮
# ===================================================================

class ActivityReportPaginationView(ui.View):
    """
    用于活跃度报告的翻页视图。
    【已优化】现在支持层级排序，将子频道显示在父频道下方。
    """

    def __init__(self, cog: 'TrackActivityCog', user: discord.Member, guild: discord.Guild,
                 total_messages: int, all_channel_data: list[tuple[int, int]], heatmap_data: dict[str, int],
                 days_window: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.guild = guild
        self.total_messages = total_messages
        self.all_channel_data = all_channel_data  # 原始数据
        self.heatmap_data = heatmap_data
        self.days_window = days_window
        self.current_page = 0
        self.channels_per_page = MAX_CHANNELS_PER_PAGE

        # 将总页数和按钮的初始化推迟到数据排序后进行
        self.sorted_display_data: typing.Optional[list[tuple[discord.abc.GuildChannel, int]]] = None
        self.total_pages = 1

    async def _build_and_sort_data_if_needed(self):
        """
        如果需要，则构建一个按层级（父频道 -> 子频道）排序的数据列表。
        这个方法只在第一次生成Embed时运行一次。
        """
        if self.sorted_display_data is not None:
            return

        top_level_channels = {}  # {channel_obj: count}
        threads_by_parent = collections.defaultdict(list)  # {parent_id: [(thread_obj, count), ...]}

        # 1. 异步获取所有频道对象并进行分组
        for channel_id, count in self.all_channel_data:
            channel = self.guild.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.cog.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden):
                    continue  # 跳过无法获取的频道

            if isinstance(channel, discord.Thread) and channel.parent:
                threads_by_parent[channel.parent.id].append((channel, count))
            else:  # 文本频道, 论坛频道, 或无父级信息的孤立子频道
                top_level_channels[channel] = count

        # 2. 按消息数对顶级频道进行排序
        sorted_top_level = sorted(top_level_channels.items(), key=lambda item: item[1], reverse=True)

        # 3. 构建最终的、扁平化的、有序的显示列表
        final_list = []
        for channel, count in sorted_top_level:
            final_list.append((channel, count))
            # 检查此顶级频道下是否有子频道
            if channel.id in threads_by_parent:
                # 对其下的子频道按消息数排序
                sorted_threads = sorted(threads_by_parent[channel.id], key=lambda item: item[1], reverse=True)
                final_list.extend(sorted_threads)

        self.sorted_display_data = final_list

        # 4. 基于排序后的列表长度，更新分页信息
        self.total_pages = (len(self.sorted_display_data) + self.channels_per_page - 1) // self.channels_per_page
        if self.total_pages == 0:
            self.total_pages = 1
        self._update_buttons()

    def _update_buttons(self):
        """根据当前页更新按钮状态。"""
        # 确保按钮已经被添加到视图中
        if not hasattr(self, 'previous_page'):
            return

        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1

        if self.total_pages <= 1:
            self.previous_page.disabled = True
            self.next_page.disabled = True

    async def _create_embed(self) -> discord.Embed:
        """生成当前页的活跃度报告 Embed。"""
        # 在首次调用时，异步构建和排序数据
        await self._build_and_sort_data_if_needed()

        embed = discord.Embed(
            title=f"📊 {self.user.display_name} 的活跃度报告",
            description=f"这是你在过去 **{self.days_window}** 天内的活跃概览。",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="总消息数", value=f"`{self.total_messages}` 条", inline=False)

        heatmap_text = self.cog._render_heatmap_text(self.heatmap_data, self.days_window)
        if heatmap_text:
            embed.add_field(name="近况热力图 (消息数/天)", value=heatmap_text, inline=False)

        # --- 使用排序和分组后的数据进行分页 ---
        start_index = self.current_page * self.channels_per_page
        end_index = min(start_index + self.channels_per_page, len(self.sorted_display_data))

        channels_on_page = self.sorted_display_data[start_index:end_index]

        if channels_on_page:
            channel_list_text = []
            for channel, count in channels_on_page:
                if isinstance(channel, discord.Thread):
                    parent_name = f"({channel.parent.name})" if channel.parent else ""
                    channel_list_text.append(f"└ {channel.mention} {parent_name}: `{count}` 条")
                else:
                    channel_list_text.append(f"{channel.mention}: `{count}` 条")

            embed.add_field(
                name=f"分频道消息数 (第 {self.current_page + 1}/{self.total_pages} 页)",
                value="\n".join(channel_list_text),
                inline=False
            )
        else:
            embed.add_field(name="分频道消息数", value="暂无符合条件的频道消息记录。", inline=False)

        embed.set_footer(text=f"数据统计时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        return embed

    @ui.button(label="上一页", style=discord.ButtonStyle.secondary, custom_id="activity_report_prev")
    async def previous_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.edit_original_response(embed=await self._create_embed(), view=self)

    @ui.button(label="下一页", style=discord.ButtonStyle.secondary, custom_id="activity_report_next")
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.edit_original_response(embed=await self._create_embed(), view=self)

    async def on_timeout(self):
        for item in self.children:
            if isinstance(item, ui.Button):
                item.disabled = True
        try:
            if hasattr(self, 'message') and self.message:
                await self.message.edit(view=self)
        except discord.NotFound:
            pass


class StatsPaginationView(ui.View):
    """
    用于 get_activity_stats 命令的翻页视图。
    支持层级排序和分页。
    """

    def __init__(self, cog: 'TrackActivityCog', guild: discord.Guild, total_stat: int,
                 metric_name_display: str, all_channel_data: list[tuple[int, int]],
                 days_window: int, scope_description: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild
        self.total_stat = total_stat
        self.metric_name_display = metric_name_display
        self.total_value_display = f"`{total_stat}` 位" if "用户" in metric_name_display else f"`{total_stat}` 条"
        self.all_channel_data = all_channel_data
        self.days_window = days_window
        self.scope_description = scope_description

        self.current_page = 0
        self.channels_per_page = MAX_CHANNELS_PER_PAGE
        self.sorted_display_data: typing.Optional[list[tuple[discord.abc.GuildChannel, int]]] = None
        self.total_pages = 1

    async def _build_and_sort_data_if_needed(self):
        """如果需要，则构建一个按层级排序的数据列表。仅运行一次。"""
        if self.sorted_display_data is not None:
            return

        top_level_channels = {}
        threads_by_parent = collections.defaultdict(list)

        for channel_id, count in self.all_channel_data:
            channel = self.guild.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.cog.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden):
                    continue

            if isinstance(channel, discord.Thread) and channel.parent:
                threads_by_parent[channel.parent.id].append((channel, count))
            else:
                top_level_channels[channel] = count

        sorted_top_level = sorted(top_level_channels.items(), key=lambda item: item[1], reverse=True)

        final_list = []
        for channel, count in sorted_top_level:
            final_list.append((channel, count))
            if channel.id in threads_by_parent:
                sorted_threads = sorted(threads_by_parent[channel.id], key=lambda item: item[1], reverse=True)
                final_list.extend(sorted_threads)

        self.sorted_display_data = final_list

        self.total_pages = (len(self.sorted_display_data) + self.channels_per_page - 1) // self.channels_per_page
        if self.total_pages == 0:
            self.total_pages = 1
        self._update_buttons()

    def _update_buttons(self):
        """更新按钮状态。"""
        if not hasattr(self, 'previous_page'):
            return

        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1

        if self.total_pages <= 1:
            self.previous_page.disabled = True
            self.next_page.disabled = True

    async def _create_embed(self) -> discord.Embed:
        """生成当前页的统计报告 Embed。"""
        await self._build_and_sort_data_if_needed()

        embed = discord.Embed(
            title=f"📈 活跃度统计报告 - {self.days_window} 天",
            color=discord.Color.dark_green(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.description = f"在 {self.scope_description} 中，过去 **{self.days_window}** 天的活跃度概览："
        embed.add_field(name=f"**总计 {self.metric_name_display}**", value=self.total_value_display, inline=False)

        start_index = self.current_page * self.channels_per_page
        end_index = min(start_index + self.channels_per_page, len(self.sorted_display_data))
        channels_on_page = self.sorted_display_data[start_index:end_index]

        if channels_on_page:
            channel_list_text = []
            for channel, count in channels_on_page:
                if isinstance(channel, discord.Thread):
                    parent_name = f"({channel.parent.name})" if channel.parent else ""
                    channel_list_text.append(f"└ {channel.mention} {parent_name}: `{count}` 条消息")
                else:
                    channel_list_text.append(f"{channel.mention}: `{count}` 条消息")

            embed.add_field(
                name=f"分频道消息数 (第 {self.current_page + 1}/{self.total_pages} 页)",
                value="\n".join(channel_list_text),
                inline=False
            )
        else:
            embed.add_field(name="分频道消息数", value="没有找到任何消息记录。", inline=False)

        embed.set_footer(text=f"统计时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        return embed

    @ui.button(label="上一页", style=discord.ButtonStyle.secondary, custom_id="stats_report_prev")
    async def previous_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.edit_original_response(embed=await self._create_embed(), view=self)

    @ui.button(label="下一页", style=discord.ButtonStyle.secondary, custom_id="stats_report_next")
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.edit_original_response(embed=await self._create_embed(), view=self)

    async def on_timeout(self):
        for item in self.children:
            if isinstance(item, ui.Button):
                item.disabled = True
        try:
            if hasattr(self, 'message') and self.message:
                await self.message.edit(view=self)
        except discord.NotFound:
            pass

class ActivityRoleView(ui.View):
    """
    包含“检查我的活跃度”、“查看报告”和“移除角色”按钮的持久化视图。
    """

    def __init__(self, cog: 'TrackActivityCog'):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="检查活跃度 & 申领身份组", style=discord.ButtonStyle.success, custom_id="check_activity_role")
    async def check_activity_button(self, interaction: discord.Interaction, button: ui.Button):
        """
        当用户点击按钮时，检查他们的活跃度并执行相应操作。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild

        guild_cfg = self.cog.config.get("guild_configs", {}).get(guild.id)
        if not guild_cfg:
            await interaction.followup.send("❌ 此服务器尚未配置活跃度角色功能。", ephemeral=True)
            return

        target_role_id = guild_cfg.get("target_role_id")
        message_threshold = guild_cfg.get("message_threshold")
        days_window = guild_cfg.get("days_window")
        target_role = guild.get_role(target_role_id)

        if not all([target_role_id, message_threshold, days_window, target_role]):
            await interaction.followup.send("❌ 服务器配置不完整，请联系管理员。", ephemeral=True)
            return

        # --- 从 Redis 聚合数据 ---
        # 优化：使用新的辅助方法来获取总消息数
        total_message_count, _ = await self.cog._get_user_activity_summary(
            guild, member.id, days_window, guild_cfg
        )

        has_role = target_role in member.roles
        is_eligible = total_message_count >= message_threshold

        # --- 生成响应消息 ---
        status_emoji = "✅" if is_eligible else "❌"
        status_text = "符合" if is_eligible else "不符合"

        embed = discord.Embed(
            title="活跃度检查结果",
            description=f"你好，{member.mention}！\n这是你在过去 **{days_window}** 天内的活跃度报告：",
            color=discord.Color.green() if is_eligible else discord.Color.orange()
        )
        embed.add_field(name="统计消息数", value=f"`{total_message_count}` 条", inline=True)
        embed.add_field(name="要求消息数", value=f"`{message_threshold}` 条", inline=True)
        embed.add_field(name="资格状态", value=f"**{status_emoji} {status_text}**", inline=True)

        # --- 角色操作逻辑 ---
        action_taken_text = ""
        if is_eligible:
            if not has_role:
                try:
                    await member.add_roles(target_role, reason=f"用户通过面板申领活跃度角色")
                    action_taken_text = f"\n🎉 **已为你授予 `{target_role.name}` 角色！**"
                    self.cog.logger.info(f"用户 {member.display_name} 申领了 '{target_role.name}' 角色。")
                except discord.Forbidden:
                    action_taken_text = f"\n⚠️ 我没有权限为你添加角色，请联系管理员。"
            else:
                action_taken_text = f"\n👍 你已拥有该角色，无需额外操作。"
        else:  # 不符合条件
            if has_role:
                try:
                    await member.remove_roles(target_role, reason=f"用户通过面板确认不活跃并移除角色")
                    action_taken_text = f"\nℹ️ 你当前不满足活跃条件，已为你移除 `{target_role.name}` 角色。"
                    self.cog.logger.info(f"用户 {member.display_name} 移除了不满足条件的 '{target_role.name}' 角色。")
                except discord.Forbidden:
                    action_taken_text = f"\n⚠️ 我没有权限为你移除角色，请联系管理员。"
            else:
                action_taken_text = f"\n💪 请继续努力，达到要求后即可申领！"

        embed.description += action_taken_text
        await interaction.followup.send(embed=embed, ephemeral=True)

    @ui.button(label="查看我的活跃报告", style=discord.ButtonStyle.primary, custom_id="view_activity_report")
    async def view_report_button(self, interaction: discord.Interaction, button: ui.Button):
        """
        当用户点击按钮时，发送详细的活跃度报告（含频道分布和热力图）。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild

        guild_cfg = self.cog.config.get("guild_configs", {}).get(guild.id)
        if not guild_cfg:
            await interaction.followup.send("❌ 此服务器尚未配置活跃度角色功能。", ephemeral=True)
            return

        days_window = guild_cfg.get("days_window")
        if not days_window:
            await interaction.followup.send("❌ 服务器配置不完整，请联系管理员。", ephemeral=True)
            return

        total_messages, channel_data = await self.cog._get_user_activity_summary(
            guild, member.id, days_window, guild_cfg
        )
        heatmap_data = await self.cog._generate_heatmap_data(
            guild, member.id, days_window
        )

        view = ActivityReportPaginationView(self.cog, member, guild, total_messages, channel_data, heatmap_data, days_window)
        # 发送初始的报告消息
        view.message = await interaction.followup.send(embed=await view._create_embed(), view=view, ephemeral=True)

    @ui.button(label="移除我的活跃度身份组", style=discord.ButtonStyle.danger, custom_id="remove_activity_role")
    async def remove_role_button(self, interaction: discord.Interaction, button: ui.Button):
        """
        当用户点击按钮时，移除他们的活跃度角色。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild

        guild_cfg = self.cog.config.get("guild_configs", {}).get(guild.id)
        if not guild_cfg:
            await interaction.followup.send("❌ 此服务器尚未配置活跃度角色功能。", ephemeral=True)
            return

        target_role_id = guild_cfg.get("target_role_id")
        target_role = guild.get_role(target_role_id)

        if not target_role:
            await interaction.followup.send("❌ 服务器配置不完整，请联系管理员。", ephemeral=True)
            return

        if target_role not in member.roles:
            await interaction.followup.send(f"ℹ️ 你没有 `{target_role.name}` 角色，无需移除。", ephemeral=True)
            return

        # 确认视图
        confirm_view = ConfirmationView(interaction.user)
        await interaction.followup.send(
            f"⚠️ **警告！** 您确定要移除您的 `{target_role.name}` 活跃度角色吗？",
            view=confirm_view,
            ephemeral=True
        )

        await confirm_view.wait()

        if confirm_view.value:
            try:
                await member.remove_roles(target_role, reason=f"用户通过面板主动移除活跃度角色")
                await interaction.edit_original_response(content=f"✅ **成功移除！** 你的 `{target_role.name}` 角色已被移除。", view=None)
                self.cog.logger.info(f"用户 {member.display_name} 主动移除了 '{target_role.name}' 角色。")
            except discord.Forbidden:
                await interaction.edit_original_response(content=f"⚠️ 我没有权限为你移除角色，请联系管理员。", view=None)
            except Exception as e:
                await interaction.edit_original_response(content=f"❌ 移除角色时发生错误: `{e}`", view=None)
        elif confirm_view.value is False:
            await interaction.edit_original_response(content="❌ 操作已取消。", view=None)
        else:  # 超时
            await interaction.edit_original_response(content="⏰ 操作超时，已自动取消。", view=None)


# ===================================================================
# 2. 主 Cog 类
# ===================================================================

class TrackActivityCog(commands.Cog, name="TrackActivity"):
    """
    通过 Redis 跟踪用户消息活动，并提供手动回填和面板申领的功能。
    """

    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        self.config = config.ACTIVITY_TRACKER_CONFIG

        self.data_manager = DataManager(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            logger=bot.logger
        )

        self._has_run_startup_task = False  # Cog内部的状态标志，用于确保启动任务只运行一次

    # --- 【新】Cog 生命周期方法 ---
    async def cog_load(self):
        """Cog 加载时执行的操作"""
        self.logger.info(f"Cog '{self.qualified_name}' 加载完成。")
        self.bot.add_view(ActivityRoleView(self))

    # --- 【新】使用 Cog 内部的 on_ready 监听器来处理启动任务 ---
    @commands.Cog.listener()
    async def on_ready(self):
        """
        当 bot 准备就绪时，执行一次性的启动任务。
        这个监听器是 Cog 的一部分，比在 setup 中定义更健壮。
        """
        # 等待内部缓存完全加载
        await self.bot.wait_until_ready()

        if not await self.data_manager.check_connection():
            self.bot.logger.error("Redis 连接失败，活跃度追踪模块将无法正常工作。不加载 TrackActivityCog。")
            self.cog_check = lambda ctx: False
            return

        if not self._has_run_startup_task:
            self.logger.info("检测到首次启动，准备执行启动时回填任务...")
            # 使用 create_task 在后台运行，不阻塞 on_ready
            self.bot.loop.create_task(self._startup_backfill_task_body())
            self._has_run_startup_task = True

    async def _startup_backfill_task_body(self):
        """
        启动时自动回填任务的具体逻辑。
        【已修复】现在会自动处理因上次异常关闭而残留的"僵尸锁"。
        """
        startup_backfill_cfg = self.config.get("startup_backfill", {})
        if not startup_backfill_cfg.get("enabled", False):
            self.logger.info("启动时回填任务未启用，跳过。")
            return

        guild_id = startup_backfill_cfg.get("guild_id")
        report_channel_id = startup_backfill_cfg.get("report_channel_id")
        duration_minutes = startup_backfill_cfg.get("duration_minutes")

        if not all([guild_id, report_channel_id, duration_minutes]) or duration_minutes <= 0:
            self.logger.error("启动时回填配置不完整或无效。请检查 'startup_backfill' 配置。")
            return

        guild = self.bot.get_guild(guild_id)
        report_channel = None
        if guild:
            report_channel = guild.get_channel(report_channel_id)

        if not guild or not report_channel or not isinstance(report_channel, discord.TextChannel):
            self.logger.error(f"启动时回填：无法找到服务器 {guild_id} 或报告频道 {report_channel_id}，或其不是文本频道。跳过。")
            return

        # --- 【代码修复】---
        # 检查回填任务是否已在运行。如果是，假定它是陈旧的锁并强制解锁。
        is_running = await self.data_manager.is_backfill_locked(guild.id)
        if is_running:
            self.logger.warning(f"服务器 '{guild.name}' 上检测到一个可能由上次异常关闭导致的回填锁。将强制解锁并继续执行启动任务。")
            await report_channel.send(f"⚠️ **回填锁重置！**\n检测到可能由先前异常中断导致的回填锁。系统将自动重置该锁并开始本次启动回填任务。")
            await self.data_manager.unlock_backfill(guild.id)
        # --- 【修复结束】---

        self.logger.info(f"正在执行启动时自动回填任务，服务器: {guild.name}, 持续时间: {duration_minutes} 分钟, 报告频道: #{report_channel.name}")

        end_datetime = datetime.now(timezone.utc)
        start_datetime = end_datetime - timedelta(minutes=duration_minutes)

        await report_channel.send(
            f"🤖 **自动回填任务启动！**\n我将在后台开始拉取服务器 `{guild.name}` 过去 `{duration_minutes}` 分钟的历史消息。进度和结果将在此频道更新。")

        # 现在调用 self._backfill_guild_history
        await self._backfill_guild_history(
            guild=guild,
            target_channel=report_channel,
            start_datetime=start_datetime,
            end_datetime=end_datetime
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """实时记录用户发送的每一条消息到对应频道的键"""
        if message.author.bot or not message.guild:
            return
        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg:
            return

        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        # Check if the channel itself is ignored
        if message.channel.id in ignored_channels:
            return

        # Check if the channel's category is ignored
        # For Threads, message.channel.category_id is None, so check parent's category
        if isinstance(message.channel, discord.Thread):
            if message.channel.parent and message.channel.parent.category_id in ignored_categories:
                return
        elif message.channel.category_id and message.channel.category_id in ignored_categories:
            return

        retention_days = guild_cfg.get("data_retention_days", 90)
        await self.data_manager.record_message(
            guild_id=message.guild.id,
            channel_id=message.channel.id,  # This is correct, it will be the thread ID for threads
            user_id=message.author.id,
            message_id=message.id,
            created_at_timestamp=message.created_at.timestamp(),
            retention_days=retention_days
        )

    # --- 【代码修改】恢复为简单、快速的同步版本，不再获取已归档帖子以提高性能 ---
    async def _get_relevant_channels(self, guild: discord.Guild, guild_cfg: dict,
                                     target_channel: typing.Optional[discord.abc.Messageable] = None,
                                     target_category: typing.Optional[discord.CategoryChannel] = None) -> list[
        typing.Union[discord.TextChannel, discord.ForumChannel, discord.Thread]]:
        """
        获取一个服务器内所有符合条件（未被忽略、有权限）的可发送消息的频道列表。
        【性能优化】此版本只从缓存中获取活跃频道和帖子，不主动请求已归档帖子，以加快数据收集速度。
        """
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        all_messageable_channels: list[typing.Union[discord.TextChannel, discord.ForumChannel, discord.Thread]] = []

        # 优先处理特定频道或类别
        if target_channel:
            if not target_channel.permissions_for(guild.me).read_message_history:
                self.logger.warning(f"无法访问 {target_channel.name} 的历史消息，跳过。")
                return []
            if target_channel.id in ignored_channels:
                self.logger.info(f"频道 {target_channel.name} 被忽略，跳过。")
                return []
            if isinstance(target_channel, discord.Thread):
                if target_channel.parent and target_channel.parent.category_id in ignored_categories:
                    self.logger.info(f"子频道 {target_channel.name} 的父频道类别被忽略，跳过。")
                    return []
            elif target_channel.category_id and target_channel.category_id in ignored_categories:
                self.logger.info(f"频道 {target_channel.name} 的类别被忽略，跳过。")
                return []
            # 如果是论坛，需要把它和它的活跃帖子都加进去
            if isinstance(target_channel, discord.ForumChannel):
                all_messageable_channels.append(target_channel)
                for thread in target_channel.threads:
                    if thread.id not in ignored_channels and thread.permissions_for(guild.me).read_message_history:
                        all_messageable_channels.append(thread)
                return all_messageable_channels
            return [target_channel]

        if target_category:
            if target_category.id in ignored_categories:
                self.logger.info(f"类别 {target_category.name} 被忽略，跳过。")
                return []

            # 获取类别下的所有文本和论坛频道
            for channel in target_category.channels:
                if isinstance(channel, (discord.TextChannel, discord.ForumChannel)):
                    if channel.id not in ignored_channels and channel.permissions_for(guild.me).read_message_history:
                        all_messageable_channels.append(channel)
                        # 如果是论坛频道，其下的活跃帖子也应纳入统计
                        if isinstance(channel, discord.ForumChannel):
                            for thread in channel.threads:
                                if thread.id not in ignored_channels and thread.permissions_for(guild.me).read_message_history:
                                    all_messageable_channels.append(thread)
            return all_messageable_channels

        # 如果没有指定特定频道或类别，则获取整个服务器所有相关的可发送消息频道
        for channel in guild.channels:
            if isinstance(channel, (discord.TextChannel, discord.ForumChannel)):
                if channel.id not in ignored_channels \
                        and (not channel.category_id or channel.category_id not in ignored_categories) \
                        and channel.permissions_for(guild.me).read_message_history:
                    all_messageable_channels.append(channel)
            # ForumChannel 的 Threads 会在 guild.threads 中单独处理，防止重复

        for thread in guild.threads:  # guild.threads 只包含活跃帖子
            # 检查 thread.parent 是否存在（有些旧的或特殊情况可能没有）
            # 并检查其父频道的类别是否被忽略，或者线程本身是否被忽略
            if thread.id not in ignored_channels \
                    and (not thread.parent or not thread.parent.category_id or thread.parent.category_id not in ignored_categories) \
                    and thread.permissions_for(guild.me).read_message_history:
                all_messageable_channels.append(thread)

        # 去重
        final_channels = []
        seen_ids = set()
        for ch in all_messageable_channels:
            if ch.id not in seen_ids:
                final_channels.append(ch)
                seen_ids.add(ch.id)

        return final_channels

    async def _get_user_activity_summary(self, guild: discord.Guild, user_id: int, days_window: int, guild_cfg: dict) -> tuple[int, list[tuple[int, int]]]:
        """
        获取用户在指定天数窗口内的总消息数和分频道消息数。
        返回 (总消息数, [(channel_id, count), ...])
        """
        # 从 DataManager 获取所有该用户的频道活动，不进行过滤
        raw_channel_counts = await self.data_manager.get_user_activity_summary(
            guild_id=guild.id,
            user_id=user_id,
            days_window=days_window
        )

        total_message_count = 0
        channel_counts: list[tuple[int, int]] = []

        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        # 【代码修改】在 Cog 层进行过滤
        for channel_id, count in raw_channel_counts:
            # 尝试从缓存获取频道对象，如果不在缓存中，则通过API获取
            channel_obj = guild.get_channel(channel_id)
            if not channel_obj:
                try:
                    channel_obj = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden):
                    # 如果频道不存在或无权限，跳过
                    self.logger.warning(f"用户 {user_id} 在未知/无权限频道 {channel_id} 有活动，已跳过统计。")
                    continue

            # 应用忽略规则
            if channel_obj.id in ignored_channels:
                continue

            is_ignored_category = False
            if isinstance(channel_obj, discord.Thread):
                if channel_obj.parent and channel_obj.parent.category_id in ignored_categories:
                    is_ignored_category = True
            elif channel_obj.category_id and channel_obj.category_id in ignored_categories:
                is_ignored_category = True

            if is_ignored_category:
                continue

            channel_counts.append((channel_id, count))
            total_message_count += count

        channel_counts.sort(key=lambda x: x[1], reverse=True)
        return total_message_count, channel_counts

    # --- 辅助方法：生成热力图数据 ---
    async def _generate_heatmap_data(self, guild: discord.Guild, user_id: int, days_window: int) -> dict[str, int]:
        """
        获取用户在指定天数窗口内每天的消息数，用于热力图。
        返回 {'YYYY-MM-DD': count, ...}
        """
        # 从 DataManager 获取所有该用户的消息时间戳，不进行过滤
        raw_messages_data = await self.data_manager.get_heatmap_data(
            guild_id=guild.id,
            user_id=user_id,
            days_window=days_window
        )

        heatmap_counts = collections.defaultdict(int)

        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        # 【代码修改】在 Cog 层进行过滤和聚合
        for channel_id, timestamp in raw_messages_data:
            # 尝试从缓存获取频道对象，如果不在缓存中，则通过API获取
            channel_obj = guild.get_channel(channel_id)
            if not channel_obj:
                try:
                    channel_obj = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden):
                    continue  # 如果频道不存在或无权限，跳过

            # 应用忽略规则
            if channel_obj.id in ignored_channels:
                continue

            is_ignored_category = False
            if isinstance(channel_obj, discord.Thread):
                if channel_obj.parent and channel_obj.parent.category_id in ignored_categories:
                    is_ignored_category = True
            elif channel_obj.category_id and channel_obj.category_id in ignored_categories:
                is_ignored_category = True

            if is_ignored_category:
                continue

            dt_utc8 = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone(BEIJING_TZ)
            date_str = dt_utc8.strftime('%Y-%m-%d')
            heatmap_counts[date_str] += 1

        return heatmap_counts

    # --- 【新】辅助方法：渲染热力图文本 ---
    @staticmethod
    def _render_heatmap_text(heatmap_data: dict[str, int], days_window: int) -> str:
        """
        将热力图数据转换为表情符号字符串。
        """
        # 从今天开始回溯 days_window 天
        today_utc8 = datetime.now(BEIJING_TZ)
        heatmap_lines = []

        current_date = today_utc8 - timedelta(days=days_window - 1)  # 从最早的日期开始

        # 创建一个表示日期的列表，填充所有天的表情
        daily_emojis = []
        for i in range(days_window):
            date_str = current_date.strftime('%Y-%m-%d')
            count = heatmap_data.get(date_str, 0)

            # 根据消息数量选择表情
            emoji = HEATMAP_EMOJIS[0]  # 默认是 0 条的方块
            for threshold in reversed(HEATMAP_THRESHOLDS):
                if count >= threshold:
                    emoji = HEATMAP_EMOJIS[threshold]
                    break
            daily_emojis.append(emoji)
            current_date += timedelta(days=1)

        # 将日历线分成多行，每行14天
        rows = []
        for i in range(0, len(daily_emojis), 14):
            rows.append("".join(daily_emojis[i:i + 14]))

        heatmap_output = []
        current_date_display = today_utc8 - timedelta(days=days_window - 1)

        for i, emoji in enumerate(daily_emojis):
            # 每隔一段时间显示日期，或者每行开始显示日期
            if i % 7 == 0:  # 每7天或行首显示日期
                if i != 0: heatmap_output.append("\n")  # 换行
                heatmap_output.append(f"`{current_date_display.strftime('%m-%d')}`: ")

            heatmap_output.append(emoji)
            current_date_display += timedelta(days=1)

        # 添加图例
        legend_items = []
        for threshold in sorted(HEATMAP_EMOJIS.keys()):
            emoji = HEATMAP_EMOJIS[threshold]
            if threshold == 0:
                legend_items.append(f"{emoji} 0")
            elif threshold == 1:
                legend_items.append(f"{emoji} 1-5")
            elif threshold == 6:
                legend_items.append(f"{emoji} 6-15")
            elif threshold == 16:
                legend_items.append(f"{emoji} 16-30")
            elif threshold == 31:
                legend_items.append(f"{emoji} 31+")

        if not daily_emojis:
            return "暂无消息记录。"

        return "\n" + "".join(heatmap_output) + "\n\n**图例:** " + " ".join(legend_items)

    # --- 指令组 ---
    class ActivityGroup(app_commands.Group):
        def __init__(self, *args, **kwargs):
            super().__init__(
                name="用户活跃度",
                description="用户活动追踪相关指令",
                guild_ids=[gid for gid in config.GUILD_IDS],
                default_permissions=discord.Permissions(manage_roles=True),
                *args,
                **kwargs
            )

    activity_group = ActivityGroup()

    @activity_group.command(name="发送活跃度身份组领取面板", description="发送一个活跃度角色申领面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        """管理员指令，用于发送一个公共的、可交互的面板。"""
        await interaction.response.defer()
        guild = interaction.guild

        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        target_role = guild.get_role(guild_cfg.get("target_role_id", 0))
        message_threshold = guild_cfg.get("message_threshold", "N/A")
        days_window = guild_cfg.get("days_window", "N/A")

        if not target_role:
            await interaction.followup.send("❌ **发送失败!**\n请先在配置文件中正确设置本服务器的 `target_role_id`。", ephemeral=True)
            return

        embed = discord.Embed(
            title="✨ 社区助力者 - 活跃度认证 ✨",
            description=(
                "保持社区的活力，感谢有你！\n\n"
                "点击下方的按钮，系统将检查你近期的活跃度。如果达标，你将可以申领或继续持有专属的 "
                f"{target_role.mention} 角色。"
            ),
            color=target_role.color or discord.Color.blurple()
        )
        embed.add_field(
            name="认证标准",
            value=f"在过去 **{days_window}** 天内，发送消息达到 **{message_threshold}** 条。",
            inline=False
        )
        embed.set_footer(text="所有检查和操作都只有你自己可见。")

        view = ActivityRoleView(self)
        await interaction.followup.send(embed=embed, view=view)

    @activity_group.command(name="管理活动数据", description="【管理员】管理本服务器的活动数据。")
    @app_commands.describe(action="要执行的操作。")
    @app_commands.choices(action=[
        app_commands.Choice(name="强制解锁回填任务", value="force_unlock"),
        app_commands.Choice(name="清除本服所有活动数据", value="clear_guild_data")
    ])
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_activity_data(self, interaction: discord.Interaction, action: str):
        guild = interaction.guild

        if action == "force_unlock":
            is_locked = await self.data_manager.is_backfill_locked(guild.id)
            if not is_locked:
                await interaction.response.send_message("ℹ️ 本服务器的回填任务当前未被锁定，无需解锁。", ephemeral=True)
                return

            await self.data_manager.unlock_backfill(guild.id)
            self.logger.warning(f"服务器 '{guild.name}' 的回填任务被 {interaction.user} 强制解锁。")
            await interaction.response.send_message("✅ **强制解锁成功！**\n现在可以重新运行 `手动拉取` 指令了。", ephemeral=True)

        elif action == "clear_guild_data":
            view = ConfirmationView(author=interaction.user)
            await interaction.response.send_message(
                "⚠️ **警告！** 您确定要清除本服务器**所有**用户的活动数据吗？\n\n"
                "此操作将删除所有已记录的消息时间戳，**且不可撤销**。\n"
                "（但可以通过重新运行回填任务来恢复）",
                view=view,
                ephemeral=True
            )

            await view.wait()

            if view.value:
                await interaction.edit_original_response(content="⏳ 正在清除数据，请稍候...", view=None)
                deleted_count = await self.data_manager.delete_guild_activity_data(guild.id)
                if deleted_count >= 0:
                    await interaction.edit_original_response(content=f"✅ **操作完成！**\n成功清除了 `{deleted_count}` 条与本服务器相关的用户活动数据。",
                                                             view=None)
                else:
                    await interaction.edit_original_response(content=f"❌ 清除数据时发生错误，请查看日志。", view=None)
            elif view.value is False:
                await interaction.edit_original_response(content="❌ 操作已取消。", view=None)
            else:  # 超时
                await interaction.edit_original_response(content="⏰ 操作超时，已自动取消。", view=None)

    @staticmethod
    def _parse_flexible_date(date_str: str) -> typing.Optional[datetime]:
        """
        尝试以多种格式解析日期字符串 (YYYY-MM-DD, MM-DD, DD)，并假定输入为 UTC+8 时区。
        返回一个 timezone-aware 的 UTC datetime 对象，如果所有格式都失败则返回 None。
        """
        now = datetime.now(BEIJING_TZ)  # 使用北京时间作为当前时间基准
        parsed_dt = None

        try:
            parsed_dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            pass

        if not parsed_dt:
            try:
                parsed_dt = datetime.strptime(date_str, "%m-%d").replace(year=now.year)
            except ValueError:
                pass

        if not parsed_dt:
            try:
                parsed_dt = datetime.strptime(date_str, "%d").replace(year=now.year, month=now.month)
            except ValueError:
                pass

        if parsed_dt:
            # 将解析出的 naive datetime 本地化为 UTC+8，然后转换为 UTC
            return BEIJING_TZ.localize(parsed_dt).astimezone(timezone.utc)

        return None

    @activity_group.command(name="手动拉取历史消息", description="手动拉取指定时间范围/频道的历史消息以填充活动数据。")
    @app_commands.describe(
        start_date="🔍 开始日期 (格式: YYYY-MM-DD, MM-DD, 或 DD, 时区: UTC+8) - 与 '回溯' 选项互斥。",
        end_date="🔍 结束日期 (格式同上, 默认为今天, 时区: UTC+8) - 与 '回溯' 选项互斥。",
        hours_ago="⏰ 从现在开始回溯的小时数 (例如: 24, 48)。用于快速同步最新数据。与 '日期' 选项互斥。",
        minutes_ago="⏱️ 从现在开始回溯的分钟数 (例如: 60, 300)。用于快速同步最新数据。与 '日期' 选项互斥。",
        channel="🎯 【可选】只扫描此特定频道 (文本频道/子频道/论坛频道)。"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(
            self,
            interaction: discord.Interaction,
            start_date: typing.Optional[str] = None,
            end_date: typing.Optional[str] = None,
            hours_ago: typing.Optional[int] = None,
            minutes_ago: typing.Optional[int] = None,
            channel: typing.Optional[typing.Union[discord.TextChannel, discord.Thread, discord.ForumChannel]] = None
    ):
        guild = interaction.guild
        now_utc = datetime.now(timezone.utc)

        is_running = await self.data_manager.is_backfill_locked(guild.id)
        if is_running:
            await interaction.response.send_message("❌ 此服务器上已经有一个回填任务正在运行。", ephemeral=True)
            return

        # --- 【新】参数解析逻辑 ---
        start_datetime: datetime = now_utc
        end_datetime: datetime = now_utc
        display_range_str = ""

        # 检查参数组合的有效性
        date_params_provided = (start_date is not None) or (end_date is not None)
        time_ago_params_provided = (hours_ago is not None) or (minutes_ago is not None)

        if date_params_provided and time_ago_params_provided:
            await interaction.response.send_message(
                "❌ **参数冲突！**\n您不能同时使用 `开始日期/结束日期` 组合和 `回溯时间 (hours_ago/minutes_ago)` 组合。请选择一种方式指定时间范围。",
                ephemeral=True
            )
            return

        if not (date_params_provided or time_ago_params_provided):
            await interaction.response.send_message(
                "❌ **缺少时间范围参数！**\n请指定 `开始日期` (及可选的 `结束日期`)，或指定 `hours_ago` (或 `minutes_ago`) 来定义回填范围。",
                ephemeral=True
            )
            return

        # 处理 "回溯" 方式
        if time_ago_params_provided:
            if hours_ago is not None and minutes_ago is not None:
                await interaction.response.send_message(
                    "❌ **参数冲突！**\n您不能同时指定 `hours_ago` 和 `minutes_ago`。请选择一个更精细的粒度。",
                    ephemeral=True
                )
                return

            if hours_ago is not None:
                if hours_ago <= 0:
                    await interaction.response.send_message("❌ `hours_ago` 必须是正整数。", ephemeral=True)
                    return
                delta = timedelta(hours=hours_ago)
            elif minutes_ago is not None:
                if minutes_ago <= 0:
                    await interaction.response.send_message("❌ `minutes_ago` 必须是正整数。", ephemeral=True)
                    return
                delta = timedelta(minutes=minutes_ago)
            else:  # 这段理论上不会触发，因为 time_ago_params_provided 已检查
                await interaction.response.send_message("❌ 请指定 `hours_ago` 或 `minutes_ago`。", ephemeral=True)
                return

            end_datetime = now_utc
            start_datetime = now_utc - delta

            # 为了显示，我们将它们转换到北京时间进行格式化
            start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            end_display = end_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            display_range_str = f"从 **{start_display}** 到 **{end_display}**"

        # 处理 "日期范围" 方式
        elif date_params_provided:
            if start_date is None:
                await interaction.response.send_message("❌ 使用日期范围模式时，`start_date` 是必需的。", ephemeral=True)
                return

            start_datetime = self._parse_flexible_date(start_date)
            if not start_datetime:
                await interaction.response.send_message("❌ **开始日期格式错误！**\n请使用 `YYYY-MM-DD`, `MM-DD`, 或 `DD` 格式。", ephemeral=True)
                return

            if end_date:
                parsed_end = self._parse_flexible_date(end_date)
                if not parsed_end:
                    await interaction.response.send_message("❌ **结束日期格式错误！**\n请使用 `YYYY-MM-DD`, `MM-DD`, 或 `DD` 格式。", ephemeral=True)
                    return
                end_datetime = parsed_end + timedelta(days=1, microseconds=-1)  # 结束于当天的 23:59:59.999999 (UTC)
            else:
                end_datetime = now_utc  # 如果没有指定结束日期，默认为当前 UTC 时间

            if start_datetime >= end_datetime:
                await interaction.response.send_message("❌ **错误**：开始日期必须在结束日期之前。", ephemeral=True)
                return

            # 为用户显示 UTC+8 格式的日期
            start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d')
            end_display = end_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d')
            display_range_str = f"从 **{start_display}** 到 **{end_display}**"

        # --- 统一的后续处理 ---
        target_description = f"服务器 **{guild.name}** 的所有可读频道"
        if channel:
            if isinstance(channel, discord.Thread):
                target_description = f"子频道 {channel.mention}"
            elif isinstance(channel, discord.ForumChannel):
                target_description = f"论坛频道 {channel.mention}"
            else:
                target_description = f"频道 {channel.mention}"

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n\n"
            f"我将开始拉取 {display_range_str} 之间，在 {target_description} 的历史消息。请关注此频道以获取进度更新。",
            ephemeral=False
        )

        # Pass interaction.channel as the target for updates
        self.bot.loop.create_task(self._backfill_guild_history(
            guild=guild,
            target_channel=interaction.channel,  # Now it's interaction.channel
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            single_channel=channel
        ))

    async def _backfill_guild_history(self, guild: discord.Guild, target_channel: discord.TextChannel,
                                      start_datetime: datetime, end_datetime: datetime,
                                      single_channel: typing.Optional[typing.Union[discord.TextChannel, discord.Thread, discord.ForumChannel]] = None):
        """
        负责回填指定时间范围内的历史消息。
        现在接受一个 discord.TextChannel 对象来发送更新，而不是直接修改 interaction.response。
        """
        await self.data_manager.lock_backfill(guild.id)
        self.logger.info(
            f"服务器 '{guild.name}' 开始历史消息回填任务。范围: "
            f"{start_datetime.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_datetime.strftime('%Y-%m-%d %H:%M:%S')} (UTC)"
            f"。目标: {'单个频道' if single_channel else '全服'}。报告频道: #{target_channel.name}"
        )

        start_time = time.time()
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})

        channels_to_scan = []
        if single_channel:
            channels_to_scan = await self._get_relevant_channels(guild, guild_cfg, target_channel=single_channel)
        else:
            channels_to_scan = await self._get_relevant_channels(guild, guild_cfg)

        total_channels = len(channels_to_scan)
        if total_channels == 0:
            await target_channel.send("⚠️ 没有找到任何可扫描的频道（可能所有频道都被忽略或无权限）。任务已取消。")
            await self.data_manager.unlock_backfill(guild.id)
            return

        total_messages_processed, total_messages_added, channels_scanned = 0, 0, 0
        last_update_time, progress_message = time.time(), None

        try:
            redis_pipe = self.data_manager.redis.pipeline()  # 获取 Redis 客户端的 pipeline
            messages_in_pipe = 0
            for channel in channels_to_scan:
                channels_scanned += 1
                try:
                    # --- 【代码修复】---
                    # 检查当前频道是否为论坛频道。如果是，则跳过。
                    # 因为论坛频道本身没有消息历史，它的帖子(Thread)已经被 _get_relevant_channels 单独收集并会在此循环中被处理。
                    if isinstance(channel, discord.ForumChannel):
                        self.logger.info(f"[{guild.name}] 跳过论坛频道容器 #{channel.name}，其帖子将作为独立子频道进行扫描。")
                        continue
                    # --- 【修复结束】---

                    # 使用 after 和 before 参数来精确控制时间范围
                    async for message in channel.history(limit=None, after=start_datetime, before=end_datetime, oldest_first=False):
                        if message.author.bot: continue  # 过滤掉机器人消息
                        total_messages_processed += 1

                        await self.data_manager.add_message_to_pipeline(
                            redis_pipe,
                            guild_id=guild.id,
                            channel_id=message.channel.id,  # 确保记录的是消息实际所在的频道ID (可能是thread ID)
                            user_id=message.author.id,
                            message_id=message.id,
                            created_at_timestamp=message.created_at.timestamp()
                        )
                        messages_in_pipe += 1
                        total_messages_added += 1

                        if messages_in_pipe >= 500:
                            await self.data_manager.execute_pipeline(redis_pipe)
                            redis_pipe = self.data_manager.redis.pipeline()  # 重置管道
                            messages_in_pipe = 0
                            await asyncio.sleep(0.1)  # 避免阻塞

                        current_time = time.time()
                        if current_time - last_update_time > 2:  # Update progress every 2 seconds
                            embed = self._create_progress_embed(
                                guild, start_time, total_channels, channels_scanned,
                                channel.name, total_messages_processed, total_messages_added,
                                start_datetime, end_datetime, bool(single_channel)
                            )
                            if progress_message:
                                try:
                                    await progress_message.edit(embed=embed)
                                except (discord.NotFound, discord.HTTPException):
                                    # If original message gone, send a new one
                                    progress_message = await target_channel.send(embed=embed)
                            else:
                                # First time sending progress message
                                progress_message = await target_channel.send(embed=embed)
                            last_update_time = current_time
                except discord.Forbidden:
                    self.logger.warning(f"[{guild.name}] 无法访问频道 #{channel.name} 的历史记录，已跳过。")
                except Exception as e:
                    self.logger.error(f"[{guild.name}] 扫描频道 #{channel.name} 时发生错误: {e}", exc_info=True)

            if messages_in_pipe > 0:
                await self.data_manager.execute_pipeline(redis_pipe)

            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"服务器 '{guild.name}' 的历史消息回填任务完成。耗时: {duration:.2f}秒")

            # 最终报告也显示 UTC+8 日期和时间
            start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            end_display = end_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')

            final_embed = discord.Embed(
                title="✅ 历史消息回填完成",
                description=f"成功为服务器 **{guild.name}** 拉取了从 **{start_display}** 到 **{end_display}** 的历史消息。",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            final_embed.add_field(name="总耗时", value=f"{duration:.2f} 秒", inline=True)
            final_embed.add_field(name="扫描频道数", value=f"{channels_scanned}/{total_channels}", inline=True)
            final_embed.add_field(name="处理消息总数", value=f"{total_messages_processed}", inline=True)
            final_embed.add_field(name="有效消息写入数", value=f"{total_messages_added}", inline=True)
            if progress_message:
                try:
                    await progress_message.edit(embed=final_embed, view=None)  # Disable view if any
                except (discord.NotFound, discord.HTTPException):
                    await target_channel.send(embed=final_embed)
            else:
                await target_channel.send(embed=final_embed)
        except Exception as e:
            self.logger.critical(f"服务器 '{guild.name}' 的回填任务发生严重错误并中断: {e}", exc_info=True)
            error_embed = discord.Embed(title="❌ 回填任务异常中断", description=f"发生严重错误: `{e}`", color=discord.Color.red())
            await target_channel.send(embed=error_embed)
        finally:
            await self.data_manager.unlock_backfill(guild.id)

    @staticmethod
    def _create_progress_embed(guild, start_time, total_channels, channels_scanned, current_channel_name, processed_count, added_count, start_dt, end_dt,
                               is_single_channel: bool):
        elapsed_time = time.time() - start_time
        start_display = start_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        end_display = end_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')

        scan_target_text = f"({channels_scanned}/{total_channels})" if not is_single_channel else ""

        embed = discord.Embed(
            title="⏳ 正在回填历史消息...",
            description=f"服务器 **{guild.name}** 的回填任务正在进行中。\n**时间范围:** `{start_display}` 至 `{end_display}` (UTC+8)",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="当前进度", value=f"正在扫描频道 **#{current_channel_name}** {scan_target_text}", inline=False)
        embed.add_field(name="已处理消息", value=f"`{processed_count}`", inline=True)
        embed.add_field(name="已写入 Redis", value=f"`{added_count}`", inline=True)
        embed.add_field(name="已用时", value=f"{int(elapsed_time)} 秒", inline=True)
        embed.set_footer(text="请耐心等待，这可能需要很长时间...")
        return embed

    # --- 统计活跃度 (重构为更通用) ---
    @activity_group.command(name="统计活跃度", description="统计指定范围和指标的活跃度数据。")
    @app_commands.describe(
        scope="📊 统计范围：服务器、特定频道、或特定频道类别。",
        metric="📈 统计指标：独立活跃用户数，或总消息数。",
        days_window="⏱️ 回溯天数 (例如: 7, 30)。",
        target_channel="🎯 (仅当范围为'频道'时使用) 要统计的特定频道 (文本/子频道/论坛)。",
        target_category="📁 (仅当范围为'类别'时使用) 要统计的频道类别。"
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="整个服务器", value="guild"),
            app_commands.Choice(name="特定频道", value="channel"),
            app_commands.Choice(name="特定频道类别", value="category")
        ],
        metric=[
            app_commands.Choice(name="独立活跃用户数", value="distinct_users"),
            app_commands.Choice(name="总消息数", value="total_messages")
        ]
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def get_activity_stats(
            self,
            interaction: discord.Interaction,
            scope: str,
            metric: str,
            days_window: int = 7,
            target_channel: typing.Optional[typing.Union[discord.TextChannel, discord.Thread, discord.ForumChannel]] = None,
            target_category: typing.Optional[discord.CategoryChannel] = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild

        if days_window <= 0:
            await interaction.followup.send("❌ `回溯天数` 必须是正整数。", ephemeral=True)
            return

        # 参数合法性检查
        if scope == "channel" and not target_channel:
            await interaction.followup.send("❌ 当统计范围为 `特定频道` 时，`target_channel` 不能为空。", ephemeral=True)
            return
        if scope == "category" and not target_category:
            await interaction.followup.send("❌ 当统计范围为 `特定频道类别` 时，`target_category` 不能为空。", ephemeral=True)
            return
        if scope == "guild" and (target_channel or target_category):
            await interaction.followup.send("❌ 当统计范围为 `整个服务器` 时，`target_channel` 和 `target_category` 必须为空。", ephemeral=True)
            return

        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        scope_description = ""

        # 从 DataManager 获取所有原始活动数据
        raw_all_activity_data = await self.data_manager.get_channel_activity_summary(
            guild_id=guild.id,
            days_window=days_window
        )

        total_overall_count = 0
        channel_message_counts = collections.defaultdict(int)
        distinct_users_global = set()

        # 【代码修改】在 Cog 层进行过滤和聚合
        for user_id, user_channels_data in raw_all_activity_data.items():
            for channel_id, count in user_channels_data.items():
                # 尝试从缓存获取频道对象，如果不在缓存中，则通过API获取
                channel_obj = guild.get_channel(channel_id)
                if not channel_obj:
                    try:
                        channel_obj = await self.bot.fetch_channel(channel_id)
                    except (discord.NotFound, discord.Forbidden):
                        continue  # 如果频道不存在或无权限，跳过

                # Step 1: 应用配置中的忽略规则
                if channel_obj.id in ignored_channels:
                    continue

                is_ignored_category = False
                if isinstance(channel_obj, discord.Thread):
                    if channel_obj.parent and channel_obj.parent.category_id in ignored_categories:
                        is_ignored_category = True
                elif channel_obj.category_id and channel_obj.category_id in ignored_categories:
                    is_ignored_category = True

                if is_ignored_category:
                    continue

                # Step 2: 根据命令参数 (scope, target_channel, target_category) 进行过滤
                should_include_channel = False
                if scope == "guild":
                    should_include_channel = True
                    scope_description = f"整个服务器的**所有**可读频道（含子频道和论坛频道）"
                elif scope == "channel":
                    if target_channel and channel_obj.id == target_channel.id:
                        should_include_channel = True
                        if isinstance(target_channel, discord.Thread):
                            scope_description = f"子频道 {target_channel.mention}"
                        elif isinstance(target_channel, discord.ForumChannel):
                            scope_description = f"论坛频道 {target_channel.mention}"
                        else:
                            scope_description = f"频道 {target_channel.mention}"
                elif scope == "category":
                    if target_category:
                        if isinstance(channel_obj, discord.Thread):
                            if channel_obj.parent and channel_obj.parent.category_id == target_category.id:
                                should_include_channel = True
                        elif channel_obj.category_id == target_category.id:
                            should_include_channel = True
                    scope_description = f"频道类别 **{target_category.name}** 下所有可读频道（含子频道和论坛频道）"

                if not should_include_channel:
                    continue  # 如果不符合指定范围，跳过

                # Step 3: 累加符合条件的计数
                channel_message_counts[channel_id] += count

                if metric == "distinct_users":
                    distinct_users_global.add(user_id)
                elif metric == "total_messages":
                    total_overall_count += count

        # 确定最终的总数
        if metric == "distinct_users":
            total_overall_count = len(distinct_users_global)

        # 如果 scope_description 仍然为空，说明没有找到任何符合条件的频道
        if not scope_description:
            # 这种情况可能发生在 target_channel/target_category 找不到，或者被忽略了
            if scope == "channel" and target_channel:
                await interaction.followup.send(f"❌ 无法统计 {target_channel.mention}，可能没有权限，或者该频道/其类别被忽略。", ephemeral=True)
            elif scope == "category" and target_category:
                await interaction.followup.send(f"❌ 无法统计频道类别 **{target_category.name}**，可能其被忽略，或者该类别下没有可统计频道。", ephemeral=True)
            else:  # 这种情况通常不会发生，除非 guild 没有可读频道
                await interaction.followup.send(f"❌ 在服务器中没有找到任何可以统计的频道。请检查配置和机器人权限。", ephemeral=True)
            return

        # --- 使用新的翻页视图 ---
        view = StatsPaginationView(
            cog=self,
            guild=guild,
            total_stat=total_overall_count,  # total_overall_count 现在是根据 metric 来的
            metric_name_display=("独立活跃用户数" if metric == "distinct_users" else "总消息数"),
            all_channel_data=list(channel_message_counts.items()),  # 将 defaultdict 转换为列表
            days_window=days_window,
            scope_description=scope_description
        )

        # 异步创建初始 Embed
        initial_embed = await view._create_embed()

        # 发送带视图的响应
        view.message = await interaction.followup.send(embed=initial_embed, view=view, ephemeral=True)


async def setup(bot: RoleBot):
    """Cog的入口点。"""
    await bot.add_cog(TrackActivityCog(bot))
