# activity_tracker/cog.py

from __future__ import annotations

import asyncio
import collections
import time
import typing
import uuid
from datetime import datetime, timedelta, timezone

import discord
import pytz
import redis.asyncio as redis
from discord import app_commands, ui
from discord.ext import commands
from redis import exceptions

import config
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from main import RoleBot

# --- 定义时区常量 ---
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

# --- Redis 键名模板 ---
CHANNEL_ACTIVITY_KEY_TEMPLATE = "activity:{guild_id}:{channel_id}:{user_id}"
ACTIVE_BACKFILLS_KEY = "active_backfills"

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
    """

    def __init__(self, cog: 'TrackActivityCog', user: discord.Member, guild: discord.Guild,
                 total_messages: int, all_channel_data: list[tuple[int, int]], heatmap_data: dict[str, int],
                 days_window: int):
        super().__init__(timeout=300)  # 报告视图可以有超时
        self.cog = cog
        self.user = user
        self.guild = guild
        self.total_messages = total_messages
        self.all_channel_data = all_channel_data  # 格式: [(channel_id, count), ...]
        self.heatmap_data = heatmap_data  # 格式: {'YYYY-MM-DD': count, ...}
        self.days_window = days_window
        self.current_page = 0

        self.channels_per_page = MAX_CHANNELS_PER_PAGE
        self.total_pages = (len(self.all_channel_data) + self.channels_per_page - 1) // self.channels_per_page
        if self.total_pages == 0:  # 至少有一页，即使没有频道数据
            self.total_pages = 1

        self._update_buttons()

    def _update_buttons(self):
        """根据当前页更新按钮状态。"""
        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1

        # 如果只有一页，禁用所有翻页按钮
        if self.total_pages <= 1:
            self.previous_page.disabled = True
            self.next_page.disabled = True

    def _create_embed(self) -> discord.Embed:
        """生成当前页的活跃度报告 Embed。"""
        embed = discord.Embed(
            title=f"📊 {self.user.display_name} 的活跃度报告",
            description=f"这是你在过去 **{self.days_window}** 天内的活跃概览。",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(name="总消息数", value=f"`{self.total_messages}` 条", inline=False)

        # --- 添加热力图 ---
        heatmap_text = self.cog._render_heatmap_text(self.heatmap_data, self.days_window)
        if heatmap_text:
            embed.add_field(name="近况热力图 (消息数/天)", value=heatmap_text, inline=False)

        # --- 添加分页频道列表 ---
        start_index = self.current_page * self.channels_per_page
        end_index = min(start_index + self.channels_per_page, len(self.all_channel_data))

        channels_on_page = self.all_channel_data[start_index:end_index]

        if channels_on_page:
            channel_list_text = []
            for channel_id, count in channels_on_page:
                channel = self.guild.get_channel(channel_id)
                if channel:
                    channel_list_text.append(f"{channel.mention}: `{count}` 条")
                else:
                    channel_list_text.append(f"未知频道 (`{channel_id}`): `{count}` 条")

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
            await interaction.edit_original_response(embed=self._create_embed(), view=self)

    @ui.button(label="下一页", style=discord.ButtonStyle.secondary, custom_id="activity_report_next")
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.edit_original_response(embed=self._create_embed(), view=self)

    async def on_timeout(self):
        # 禁用所有按钮
        for item in self.children:
            if isinstance(item, ui.Button):
                item.disabled = True
        try:
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
            guild.id, member.id, days_window, guild_cfg
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
            guild.id, member.id, days_window, guild_cfg
        )
        heatmap_data = await self.cog._generate_heatmap_data(
            guild.id, member.id, days_window, guild_cfg
        )

        view = ActivityReportPaginationView(self.cog, member, guild, total_messages, channel_data, heatmap_data, days_window)
        # 发送初始的报告消息
        view.message = await interaction.followup.send(embed=view._create_embed(), view=view, ephemeral=True)

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

        if confirm_view.value is True:
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
        self.redis = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)
        self.bot.loop.create_task(self.check_redis_connection())
        # 注册持久化视图
        self.bot.add_view(ActivityRoleView(self))

    async def check_redis_connection(self):
        """在启动时异步检查 Redis 连接。"""
        try:
            await self.redis.ping()
            self.logger.info("成功连接到 Redis 服务器 (异步客户端)。")
        except exceptions.ConnectionError as e:
            self.logger.critical(f"无法连接到 Redis，活动追踪模块将无法工作！错误: {e}")
            self.cog_check = lambda ctx: False  # 禁用整个 cog

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """实时记录用户发送的每一条消息到对应频道的键"""
        if message.author.bot or not message.guild:
            return
        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg:
            return
        ignored_channels = guild_cfg.get("ignored_channels", [])
        ignored_categories = guild_cfg.get("ignored_categories", [])
        if message.channel.id in ignored_channels or (message.channel.category_id and message.channel.category_id in ignored_categories):
            return

        key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
            guild_id=message.guild.id,
            channel_id=message.channel.id,
            user_id=message.author.id
        )

        async with self.redis.pipeline() as pipe:
            await pipe.zadd(key, {str(message.id): message.created_at.timestamp()})
            retention_days = guild_cfg.get("data_retention_days", 90)
            cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp()
            await pipe.zremrangebyscore(key, '-inf', cutoff_timestamp)
            await pipe.execute()

    # --- 【新】辅助方法：获取用户活跃度概览 ---
    async def _get_user_activity_summary(self, guild_id: int, user_id: int, days_window: int, guild_cfg: dict) -> tuple[int, list[tuple[int, int]]]:
        """
        获取用户在指定天数窗口内的总消息数和分频道消息数。
        返回 (总消息数, [(channel_id, count), ...])
        """
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        guild = self.bot.get_guild(guild_id)
        if not guild: return 0, []

        channels_to_check = [
            c for c in guild.text_channels
            if c.id not in ignored_channels and not (c.category_id and c.category_id in ignored_categories)
        ]

        total_message_count = 0
        channel_counts: list[tuple[int, int]] = []
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()

        # 批处理每个频道的 ZCOUNT 请求
        pipe = self.redis.pipeline()
        key_channel_map = {}
        for channel in channels_to_check:
            key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=channel.id, user_id=user_id)
            await pipe.zcount(key, cutoff_timestamp, '+inf')
            key_channel_map[key] = channel.id

        results = await pipe.execute()

        for i, count in enumerate(results):
            channel_id = channels_to_check[i].id  # 保持顺序一致
            if count > 0:
                channel_counts.append((channel_id, count))
                total_message_count += count

        # 按消息数降序排列
        channel_counts.sort(key=lambda x: x[1], reverse=True)

        return total_message_count, channel_counts

    # --- 【新】辅助方法：生成热力图数据 ---
    async def _generate_heatmap_data(self, guild_id: int, user_id: int, days_window: int, guild_cfg: dict) -> dict[str, int]:
        """
        获取用户在指定天数窗口内每天的消息数，用于热力图。
        返回 {'YYYY-MM-DD': count, ...}
        """
        heatmap_counts = collections.defaultdict(int)

        # 计算 UTC 时间范围
        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(days=days_window)

        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        guild = self.bot.get_guild(guild_id)
        if not guild: return {}

        channels_to_check = [
            c for c in guild.text_channels
            if c.id not in ignored_channels and not (c.category_id and c.category_id in ignored_categories)
        ]

        # 批处理每个频道的 ZRANGEBYSCORE 请求
        pipe = self.redis.pipeline()
        for channel in channels_to_check:
            key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=channel.id, user_id=user_id)
            await pipe.zrangebyscore(key, start_utc.timestamp(), end_utc.timestamp(), withscores=True)

        results = await pipe.execute()

        for channel_messages in results:
            for _, timestamp in channel_messages:
                # 将 UTC 时间戳转换为 UTC+8 时区的日期
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

        # 定义星期几标签
        # Monday=0, Sunday=6
        day_labels = ["一", "二", "三", "四", "五", "六", "日"]

        # 创建一个空的热力图网格 (7行，每行 days_window/7 列)
        # 为了简单，直接按天显示，不严格按周对齐，但会显示星期几。

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

        # 添加星期几标签 (从最早的日期开始，并确保长度匹配)
        # 这里为了简化，我们只在热力图下方加一个提示，不严格对齐周几网格。
        # 如果需要严格对齐，需要更复杂的逻辑来计算每个月的起始星期几和补白。

        # 简化版：直接列出每天的方块，并在前面加日期
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
            is_locked = await self.redis.sismember(ACTIVE_BACKFILLS_KEY, str(guild.id))
            if not is_locked:
                await interaction.response.send_message("ℹ️ 本服务器的回填任务当前未被锁定，无需解锁。", ephemeral=True)
                return

            await self.redis.srem(ACTIVE_BACKFILLS_KEY, str(guild.id))
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

            if view.value is True:
                await interaction.edit_original_response(content="⏳ 正在清除数据，请稍候...", view=None)
                deleted_count = await self._delete_guild_activity_data(guild.id)
                await interaction.edit_original_response(content=f"✅ **操作完成！**\n成功清除了 `{deleted_count}` 条与本服务器相关的用户活动数据。")
            elif view.value is False:
                await interaction.edit_original_response(content="❌ 操作已取消。", view=None)
            else:
                await interaction.edit_original_response(content="⏰ 操作超时，已自动取消。", view=None)

    async def _delete_guild_activity_data(self, guild_id: int) -> int:
        """
        使用 SCAN_ITER 安全地查找并删除一个服务器的所有活动数据键。
        返回被删除的键的数量。
        """
        pattern = f"activity:{guild_id}:*"
        self.logger.warning(f"开始为服务器 {guild_id} 清除活动数据，匹配模式: {pattern}")

        keys_to_delete = [key async for key in self.redis.scan_iter(pattern)]

        if not keys_to_delete:
            self.logger.info(f"服务器 {guild_id} 没有找到需要清除的活动数据。")
            return 0

        await self.redis.delete(*keys_to_delete)

        self.logger.warning(f"成功为服务器 {guild_id} 清除了 {len(keys_to_delete)} 个键。")
        return len(keys_to_delete)

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
        channel="🎯 【可选】只扫描此特定频道。"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(
            self,
            interaction: discord.Interaction,
            start_date: typing.Optional[str] = None,
            end_date: typing.Optional[str] = None,
            hours_ago: typing.Optional[int] = None,
            minutes_ago: typing.Optional[int] = None,
            channel: typing.Optional[discord.TextChannel] = None
    ):
        guild = interaction.guild
        now_utc = datetime.now(timezone.utc)

        is_running = await self.redis.sismember(ACTIVE_BACKFILLS_KEY, str(guild.id))
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
            target_description = f"频道 {channel.mention}"

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n\n"
            f"我将开始拉取 {display_range_str} 之间，在 {target_description} 的历史消息。",
            ephemeral=False
        )

        self.bot.loop.create_task(self._backfill_guild_history(interaction, start_datetime, end_datetime, channel))

    async def _backfill_guild_history(self, interaction: discord.Interaction, start_datetime: datetime, end_datetime: datetime,
                                      single_channel: typing.Optional[discord.TextChannel] = None):
        guild = interaction.guild
        channel_to_report = interaction.channel

        await self.redis.sadd(ACTIVE_BACKFILLS_KEY, str(guild.id))
        self.logger.info(
            f"服务器 '{guild.name}' 开始历史消息回填任务。范围: "
            f"{start_datetime.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_datetime.strftime('%Y-%m-%d %H:%M:%S')} (UTC)"
            f"。由 {interaction.user} 触发。目标: {'单个频道' if single_channel else '全服'}"
        )

        start_time = time.time()
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        total_messages_processed, total_messages_added, channels_scanned = 0, 0, 0
        last_update_time, progress_message = time.time(), None

        try:
            if single_channel:
                if single_channel.permissions_for(guild.me).read_message_history:
                    channels_to_scan = [single_channel]
                else:
                    channels_to_scan = []
                    await channel_to_report.send(f"⚠️ 我没有权限读取 {single_channel.mention} 的历史消息，任务跳过。")
            else:
                channels_to_scan = [
                    c for c in guild.text_channels
                    if c.id not in ignored_channels
                       and not (c.category_id and c.category_id in ignored_categories)
                       and c.permissions_for(guild.me).read_message_history
                ]
            total_channels = len(channels_to_scan)

            async with self.redis.pipeline() as pipe:
                messages_in_pipe = 0
                for channel in channels_to_scan:
                    channels_scanned += 1
                    try:
                        # 使用 after 和 before 参数来精确控制时间范围
                        async for message in channel.history(limit=None, after=start_datetime, before=end_datetime, oldest_first=False):
                            if message.author.bot: continue
                            total_messages_processed += 1
                            key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
                                guild_id=guild.id,
                                channel_id=channel.id,
                                user_id=message.author.id
                            )
                            await pipe.zadd(key, {str(message.id): message.created_at.timestamp()})
                            messages_in_pipe += 1
                            total_messages_added += 1
                            if messages_in_pipe >= 500:
                                await pipe.execute()
                                messages_in_pipe = 0
                                await asyncio.sleep(0.1)

                            current_time = time.time()
                            if current_time - last_update_time > 30:
                                embed = self._create_progress_embed(
                                    guild, start_time, total_channels, channels_scanned,
                                    channel.name, total_messages_processed, total_messages_added,
                                    start_datetime, end_datetime, bool(single_channel)
                                )
                                if progress_message:
                                    try:
                                        await progress_message.edit(embed=embed)
                                    except (discord.NotFound, discord.HTTPException):
                                        progress_message = await channel_to_report.send(embed=embed)
                                else:
                                    progress_message = await channel_to_report.send(embed=embed)
                                last_update_time = current_time
                    except discord.Forbidden:
                        self.logger.warning(f"[{guild.name}] 无法访问频道 #{channel.name} 的历史记录，已跳过。")
                    except Exception as e:
                        self.logger.error(f"[{guild.name}] 扫描频道 #{channel.name} 时发生错误: {e}", exc_info=True)

                if messages_in_pipe > 0:
                    await pipe.execute()

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
            await channel_to_report.send(embed=final_embed)
        except Exception as e:
            self.logger.critical(f"服务器 '{guild.name}' 的回填任务发生严重错误并中断: {e}", exc_info=True)
            error_embed = discord.Embed(title="❌ 回填任务异常中断", description=f"发生严重错误: `{e}`", color=discord.Color.red())
            await channel_to_report.send(embed=error_embed)
        finally:
            await self.redis.srem(ACTIVE_BACKFILLS_KEY, str(guild.id))

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


async def setup(bot: RoleBot):
    """Cog的入口点。"""
    await bot.add_cog(TrackActivityCog(bot))