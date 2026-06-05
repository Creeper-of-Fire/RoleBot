# activity_tracker/cog.py
from __future__ import annotations

import asyncio
import collections
import io
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING, Dict, Union

import discord
import emoji
from discord import app_commands, Guild
from discord.ext import commands

import config
from activity_tracker.data_manager import DataManager, BEIJING_TZ
from activity_tracker.blacklist_data_manager import BlacklistDataManager
from activity_tracker.logic import ActivityProcessor, UserReportData
from activity_tracker.views import ActivityRoleView, ReportEmbeds, UserReportDetailView
from utility.permison import is_super_admin, is_admin
from utility.views import ConfirmationView

if TYPE_CHECKING:
    from main import RoleBot


@dataclass
class BlacklistPunishmentResult:
    """黑名单处罚执行结果。子步骤失败不影响主流程。"""
    role_removed: bool = False
    role_remove_failed: bool = False
    dm_sent: bool = False
    dm_failed: bool = False
    announced: bool = False
    announce_failed: bool = False


class TrackActivityCog(commands.Cog, name="TrackActivity"):
    """
    【控制器】协调 DataManager, ActivityProcessor 和 Views 来实现活动追踪功能。
    """

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger
        self.config = config.ACTIVITY_TRACKER_CONFIG
        self.data_manager = DataManager(
            host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, logger=bot.logger
        )
        self.blacklist_manager:BlacklistDataManager = BlacklistDataManager.get_instance(logger=bot.logger)
        # 用于防止并发回填任务的内存锁
        self._backfill_locks: set[int] = set(config.GUILD_IDS)
        # 用于节流更新最后同步时间戳
        self._last_timestamp_update: Dict[int, float] = {}
        self.TIMESTAMP_UPDATE_INTERVAL = 60

        self._processors: Dict[int, ActivityProcessor] = {}

        # 注册右键菜单：消息右键 → 把发出者加入刷屏黑名单
        self.ctx_menu = app_commands.ContextMenu(
            name="添加到刷屏黑名单",
            callback=self._blacklist_add_context_impl,
        )
        is_admin()(self.ctx_menu)
        self.bot.tree.add_command(self.ctx_menu)

    def _build_blacklist_embed(
        self,
        *,
        title: str,
        color: discord.Color,
        target: discord.abc.User,
        reason: str,
        expiry_dt: datetime,
        guild: discord.Guild,
        operator: Optional[discord.abc.User] = None,
        show_server_field: bool = False,
        execution_lines: Optional[list[str]] = None,
        context_message_jump: Optional[str] = None,
        target_left_guild: bool = False,
    ) -> discord.Embed:
        """统一构造黑名单相关 embed。reason 已由业务层兜底为非空字符串。"""
        embed = discord.Embed(
            title=title,
            description=f"用户 {target.mention} 因刷屏行为被加入 30 天黑名单。",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="用户", value=f"{target.mention} (`{target.id}`)", inline=False)
        embed.add_field(name="原因", value=reason, inline=False)
        embed.add_field(
            name="到期时间",
            value=expiry_dt.strftime("%Y-%m-%d %H:%M (UTC+8)"),
            inline=False,
        )
        if operator is not None:
            embed.add_field(name="处理人", value=operator.mention, inline=True)
        if show_server_field:
            embed.add_field(name="服务器", value=guild.name, inline=True)
        if context_message_jump:
            embed.add_field(name="来源消息", value=f"[跳转到消息]({context_message_jump})", inline=False)
        if target_left_guild:
            embed.add_field(name="提示", value="⚠️ 该用户已不在服务器内，仅记录黑名单。", inline=False)
        if execution_lines:
            embed.add_field(name="执行结果", value="\n".join(execution_lines), inline=False)
        return embed

    async def _execute_blacklist_punishment(
        self,
        interaction: discord.Interaction,
        user: discord.abc.User,
        reason: str,
    ) -> BlacklistPunishmentResult:
        """写黑名单 -> 移除活跃角色 -> 发私信 -> 发公示。任一子步骤失败不影响主流程。"""
        result = BlacklistPunishmentResult()
        guild = interaction.guild
        if guild is None:
            return result

        self.blacklist_manager.add_to_blacklist(guild.id, user.id, reason=reason)

        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        target_role_id = guild_cfg.get("target_role_id")
        member = guild.get_member(user.id)
        if target_role_id and isinstance(member, discord.Member):
            target_role = guild.get_role(target_role_id)
            if target_role and target_role in member.roles:
                try:
                    await member.remove_roles(target_role, reason=f"被加入刷屏黑名单：{reason}")
                    result.role_removed = True
                except (discord.Forbidden, discord.HTTPException) as e:
                    self.logger.warning(f"无法移除 {user.id} 的活跃角色：{e}")
                    result.role_remove_failed = True

        expiry_dt = datetime.now(BEIJING_TZ) + timedelta(days=30)
        try:
            await user.send(
                f"您在服务器 **{guild.name}** 因刷屏行为被加入黑名单，\n"
                f"30 天内无法通过面板领取活跃度身份组。\n"
                f"原因：{reason}\n"
                f"到期时间：{expiry_dt:%Y-%m-%d %H:%M} (UTC+8)"
            )
            result.dm_sent = True
        except discord.Forbidden:
            self.logger.warning(f"无法私信用户 {user.id}，他们可能关闭了私信。")
            result.dm_failed = True
        except discord.HTTPException as e:
            self.logger.error(f"私信用户 {user.id} 时发生 HTTP 错误：{e}")
            result.dm_failed = True

        announce_id = self.config.get("blacklist", {}).get("announce_channel_id")
        if announce_id:
            try:
                announce_ch = guild.get_channel(announce_id) or await self.bot.fetch_channel(announce_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                self.logger.error(f"获取处罚公示区频道失败：{e}")
                announce_ch = None
            if announce_ch and isinstance(announce_ch, discord.abc.Messageable):
                embed = self._build_blacklist_embed(
                    title="🚫 刷屏处罚公示",
                    color=discord.Color.red(),
                    target=user,
                    reason=reason,
                    expiry_dt=expiry_dt,
                    guild=guild,
                    operator=interaction.user,
                    show_server_field=True,
                )
                try:
                    await announce_ch.send(embed=embed)
                    result.announced = True
                except (discord.Forbidden, discord.HTTPException) as e:
                    self.logger.error(f"发送至处罚公示区失败：{e}")
                    result.announce_failed = True
            else:
                result.announce_failed = True
        else:
            result.announce_failed = True

        return result

    def _get_processor(self, guild: discord.Guild) -> Optional[ActivityProcessor]:
        """
        【新增】获取或创建并缓存一个服务器的 ActivityProcessor 实例。
        这是所有需要 Processor 的地方的统一入口。
        """
        if guild.id not in self._processors:
            guild_cfg = self.config.get("guild_configs", {}).get(guild.id)
            if not guild_cfg:
                return None
            self._processors[guild.id] = ActivityProcessor(self.bot, guild, self.data_manager, guild_cfg)
        return self._processors[guild.id]

    def _get_shared_guild_ids(self, guild_id: int) -> list[int]:
        """获取与指定服务器共享活跃度的所有服务器ID（包含自身）。"""
        guild_cfg = self.config.get("guild_configs", {}).get(guild_id, {})
        group_name = guild_cfg.get("shared_activity_group")
        if not group_name:
            return [guild_id]
        return self.config.get("shared_activity_groups", {}).get(group_name, [guild_id])

    async def _get_merged_report_data(self, guild_id: int, user_id: int, days_window: int) -> UserReportData:
        """聚合共通组内所有服务器的报告数据。"""
        merged_channel_activity: dict[int, int] = collections.defaultdict(int)
        merged_heatmap: dict[str, int] = collections.defaultdict(int)
        total = 0
        for gid in self._get_shared_guild_ids(guild_id):
            g = self.bot.get_guild(gid)
            if not g:
                continue
            proc = self._get_processor(g)
            if not proc:
                continue
            report = await proc.generate_user_report_data(user_id, days_window)
            total += report.total_messages
            for date, count in report.heatmap_data.items():
                merged_heatmap[date] += count
            for channel_id, count in report.channel_activity:
                merged_channel_activity[channel_id] += count
        return UserReportData(
            total_messages=total,
            channel_activity=list(merged_channel_activity.items()),
            heatmap_data=dict(merged_heatmap),
        )

    async def _resolve_report_channel(self, guild_cfg: dict) -> Optional[discord.abc.Messageable]:
        """根据配置解析回填通知频道（支持跨服务器）。"""
        rc = guild_cfg.get("report_channel")
        if not rc:
            return None
        try:
            g = self.bot.get_guild(rc["guild_id"]) or await self.bot.fetch_guild(rc["guild_id"])
            return g.get_channel(rc["channel_id"]) or await g.fetch_channel(rc["channel_id"])
        except (discord.NotFound, discord.Forbidden, KeyError):
            return None

    @commands.Cog.listener()
    async def on_guild_remove(self, guild: discord.Guild):
        """【新增】当机器人离开服务器时，清理相关资源。"""
        if guild.id in self._processors:
            del self._processors[guild.id]
            self.logger.info(f"已从服务器 '{guild.name}' (ID: {guild.id}) 移除，清理了其 ActivityProcessor 实例。")

    # --- Cog 生命周期与事件监听 ---

    async def cog_load(self):
        """Cog 加载时执行的操作，注册持久化视图。"""
        self.logger.info(f"Cog '{self.qualified_name}' 加载完成。")
        self.bot.add_view(ActivityRoleView(self))

    @commands.Cog.listener()
    async def on_ready(self):
        """当 bot 准备就绪时，检查 Redis 连接并执行一次性的启动任务。"""
        await self.bot.wait_until_ready()
        if not await self.data_manager.check_connection():
            self.logger.error("Redis 连接失败，活跃度追踪模块将无法正常工作。")
            # 禁用本 Cog 的所有指令
            self.activity_group.interaction_check = lambda i: False
            return

        self.logger.info("Bot is ready. Creating startup incremental sync task...")
        self.bot.loop.create_task(self._incremental_sync_on_startup())

    @staticmethod
    def is_not_valid_message(message: discord.Message) -> tuple[bool, Optional[str]]:
        """
        判断消息是否为有效消息（非刷屏消息）。
        有效消息包括：有意义的文字内容、或图文混合内容。
        无效消息（刷屏）包括：纯表情包（Unicode/自定义）、纯图片/文件、纯标点符号等。

        :param message: 消息
        :return: (是否应该过滤, 过滤原因)
        """
        content = message.content
        embeds = message.embeds
        has_attachments = bool(message.attachments)

        # 去除首尾空白
        text = content.strip()

        # 检查是否有图片相关的嵌入内容
        has_image_embed = False
        if embeds:
            for embed in embeds:
                if embed.type in ('image', 'gifv') or (embed.image or embed.thumbnail or embed.video):
                    has_image_embed = True
                    break

        # 情况1: 纯图片/文件（没有文字内容）
        if not text and (has_attachments or has_image_embed):
            return True, "纯图片/文件"

        # 情况2: 只有文字内容
        if text and not has_attachments:
            # 移除 Discord 自定义表情 (格式: <:name:id> 或 <a:animated_name:id>)
            # 静态表情: <:emoji_name:123456789>
            # 动态表情: <a:emoji_name:123456789>
            text_without_discord_emoji = re.sub(r'<a?:\w+:\d+>', '', text)
            # 移除所有emoji，看是否还有剩余字符
            text_without_unicode_emoji = emoji.replace_emoji(text_without_discord_emoji, '')
            # # 也移除常见的装饰性字符（如零宽连接符、变体选择器等）
            # text_without_emoji = re.sub(r'[\uFE00-\uFE0F\u200D\uE0000-\uE007F]', '', text_without_unicode_emoji)
            text_without_emoji = text_without_unicode_emoji.strip()

            # 如果移除emoji后没有剩余字符，说明是纯表情
            if not text_without_emoji:
                return True, "纯表情包"

        # 情况3: 既有文字又有图片/表情
        # 这种情况保留，不算刷屏
        return False, None

    async def _should_track_message(self, message: discord.Message, processor: 'ActivityProcessor') -> bool:
        """统一的消息过滤入口。on_message 和回填共用此方法。"""
        # BOT消息或非公会消息
        if message.author.bot or not message.guild:
            return False
        # 垃圾消息过滤（纯表情/纯图片）
        if self.is_not_valid_message(message)[0]:
            return False
        # 频道过滤
        if not await processor.is_channel_included(message.channel.id, message.channel):
            return False
        # BOT对话过滤（回复BOT 或 @提及BOT）
        if message.reference:
            ref_msg = message.reference.cached_message
            if ref_msg and ref_msg.author.bot:
                return False
        if any(m.bot for m in message.mentions):
            return False
        return True

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """实时记录用户发送的每一条消息。"""
        if not message.guild:
            return

        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg:
            return

        processor = self._get_processor(message.guild)
        if not await self._should_track_message(message, processor):
            return

        retention_days = guild_cfg.get("data_retention_days", 90)
        message_ts = message.created_at.timestamp()

        await self.data_manager.record_message(
            guild_id=message.guild.id, channel_id=message.channel.id, user_id=message.author.id,
            message_id=message.id, created_at_timestamp=message_ts, retention_days=retention_days
        )
        await self._throttled_update_sync_timestamp(message.guild.id, message_ts)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        """消息删除时从 Redis 中移除对应记录。仅处理缓存内的消息，不覆盖 raw 事件。"""
        if not message.guild or message.author.bot:
            return
        if not self.config.get("guild_configs", {}).get(message.guild.id):
            return
        await self.data_manager.remove_message(
            message.guild.id, message.channel.id, message.author.id, message.id
        )

    # --- 视图回调处理方法 (公共接口) ---

    async def handle_check_activity(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的"检查活跃度"按钮点击。"""
        guild, member = interaction.guild, interaction.user
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})

        if not all(k in guild_cfg for k in ["target_role_id", "message_threshold", "claim_days_window"]):
            await interaction.followup.send("❌ 服务器配置不完整，请联系管理员。", ephemeral=True)
            return

        target_role = guild.get_role(guild_cfg["target_role_id"])
        if not target_role:
            await interaction.followup.send("❌ 配置中的目标角色未找到，请联系管理员。", ephemeral=True)
            return

        # 聚合共通组内所有服务器的数据
        daily_cap = guild_cfg.get("daily_message_cap", 999)
        report_data = await self._get_merged_report_data(guild.id, member.id, guild_cfg["claim_days_window"])
        total = report_data.total_messages
        counted = sum(min(c, daily_cap) for c in report_data.heatmap_data.values())
        is_blacklisted, blacklisted_until = self.blacklist_manager.is_blacklisted(guild.id, member.id)

        is_eligible = counted >= guild_cfg["message_threshold"] and not is_blacklisted
        has_role = target_role in member.roles
        action_text = ""
        try:
            if is_blacklisted:
                action_text = "\n🚫 您目前处于刷屏黑名单中，无法领取活跃度身份组。"
            elif is_eligible and not has_role:
                await member.add_roles(target_role, reason="通过面板申领活跃角色")
                action_text = f"\n🎉 **已为您授予 `{target_role.name}` 角色！**"
            elif has_role:
                action_text = "\n👍 您已拥有该角色，无需操作。"
            else:
                action_text = "\n💪 请继续努力！"
        except discord.Forbidden:
            action_text = "\n⚠️ 我没有权限为您操作角色，请联系管理员。"

        embed = ReportEmbeds.create_check_activity_embed(
            member, guild_cfg["claim_days_window"], total, counted, daily_cap,
            guild_cfg["message_threshold"], action_text, blacklisted_until=blacklisted_until if is_blacklisted else None
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def handle_view_report(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的"查看报告"按钮点击。"""
        guild, member = interaction.guild, interaction.user
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        if not (days_window := guild_cfg.get("report_days_window")):
            await interaction.followup.send("❌ 服务器配置不完整。", ephemeral=True)
            return

        # 聚合共通组内所有服务器的报告数据
        report_data = await self._get_merged_report_data(guild.id, member.id, days_window)

        processor = self._get_processor(guild)
        sorted_display_data = await processor.process_and_sort_for_display(report_data.channel_activity)

        is_blacklisted, blacklisted_until = self.blacklist_manager.is_blacklisted(guild.id, member.id)
        daily_cap = guild_cfg.get("daily_message_cap", 999)
        threshold = guild_cfg.get("message_threshold", 0)

        embed_template = ReportEmbeds.create_user_report_embed_template(
            member, days_window, report_data,
            daily_cap=daily_cap, threshold=threshold,
            blacklisted_until=blacklisted_until if is_blacklisted else None
        )

        pagination_view = UserReportDetailView(
            embed_template=embed_template,
            sorted_display_data=sorted_display_data,
            field_name="分频道消息数",
            value_suffix="条"
        )
        await pagination_view.start(interaction, ephemeral=True)

    async def handle_remove_role(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的"移除角色"按钮点击。"""
        guild, member = interaction.guild, interaction.user
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        if not (target_role_id := guild_cfg.get("target_role_id")) or not (target_role := guild.get_role(target_role_id)):
            await interaction.followup.send("❌ 服务器配置不完整。", ephemeral=True)
            return

        if target_role not in member.roles:
            await interaction.followup.send(f"ℹ️ 您没有 `{target_role.name}` 角色。", ephemeral=True)
            return

        confirm_view = ConfirmationView(interaction.user)
        await interaction.followup.send(f"⚠️ 确定要移除您的 `{target_role.name}` 角色吗？", view=confirm_view, ephemeral=True)
        await confirm_view.wait()

        if confirm_view.value:
            try:
                await member.remove_roles(target_role, reason="用户通过面板主动移除")
                await interaction.edit_original_response(content=f"✅ 已移除 `{target_role.name}` 角色。", view=None)
            except discord.Forbidden:
                await interaction.edit_original_response(content="⚠️ 我没有权限为您移除角色。", view=None)
        else:  # Cancelled or Timeout
            await interaction.edit_original_response(content="❌ 操作已取消。", view=None)

    # --- 内部辅助与核心执行器 (回填等管理任务) ---

    async def _incremental_sync_on_startup(self):
        """在机器人启动时，为每个配置的服务器执行增量数据同步。"""
        for guild_id, guild_cfg in self.config.get("guild_configs", {}).items():
            if not guild_cfg.get("enabled", True): continue

            guild = self.bot.get_guild(guild_id)
            if not guild:
                self.logger.warning(f"无法找到服务器 {guild_id}，跳过启动时增量同步。")
                continue

            try:
                last_sync_ts = await self.data_manager.get_last_sync_timestamp(guild.id)
                now_utc = datetime.now(timezone.utc)

                report_channel = await self._resolve_report_channel(guild_cfg)

                if last_sync_ts is None:
                    await self._update_sync_timestamp(guild.id, now_utc.timestamp(), force=True)
                    if report_channel:
                        await report_channel.send(f"👋 **首次启动**：已设置当前时间为初始同步点。如需历史数据，请使用 `/用户活跃度 手动拉取历史消息` 指令。")
                    continue

                start_datetime = datetime.fromtimestamp(last_sync_ts, tz=timezone.utc)
                if report_channel:
                    start_disp = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    await report_channel.send(f"🤖 **自动增量同步启动**：开始补全自 `{start_disp}` (UTC+8) 以来的离线消息。")

                # 派发后台回填任务
                self.bot.loop.create_task(self._backfill_guild_history(
                    guild=guild,
                    target_channel=report_channel,  # 用于发送最终报告
                    start_datetime=start_datetime,
                    end_datetime=now_utc
                ))
                await asyncio.sleep(1)  # 避免同时启动多个任务造成拥堵
            except Exception as e:
                self.logger.critical(f"为服务器 {guild.id} 执行启动时同步任务时发生错误: {e}", exc_info=True)

    async def _throttled_update_sync_timestamp(self, guild_id: int, timestamp: float):
        """节流地更新最后同步时间戳。"""
        now = time.time()
        last_update = self._last_timestamp_update.get(guild_id, 0)
        if now - last_update > self.TIMESTAMP_UPDATE_INTERVAL:
            await self._update_sync_timestamp(guild_id, timestamp)
            self._last_timestamp_update[guild_id] = now

    async def _update_sync_timestamp(self, guild_id: int, timestamp: float, force: bool = False):
        """安全地更新最后同步时间戳，除非被回填任务锁定。"""
        if guild_id in self._backfill_locks and not force:
            return
        await self.data_manager.set_last_sync_timestamp(guild_id, timestamp)

    async def _backfill_guild_history(self, guild: discord.Guild,
                                      target_channel: Optional[discord.abc.Messageable],
                                      start_datetime: datetime, end_datetime: datetime,
                                      single_channel: Optional[Union[discord.TextChannel, discord.Thread, discord.ForumChannel]] = None):
        """【核心执行器】负责回填历史消息，是所有同步任务的唯一入口。"""
        try:
            self._backfill_locks.add(guild.id)
            self.logger.info(f"服务器 '{guild.name}' 开始历史消息回填任务。内存锁已激活。")

            start_time = time.time()

            processor = self._get_processor(guild)
            if not processor:
                self.logger.warning(f"无法为服务器 {guild.id} 获取 ActivityProcessor，中止回填任务。")
                return

            scannable_channel_ids = await processor.get_scannable_channels(single_channel)

            if not scannable_channel_ids:
                if target_channel:
                    await target_channel.send("⚠️ **任务取消**：没有找到任何符合条件的可扫描频道。")
                self._backfill_locks.remove(guild.id)
                return

            total_messages_added, last_update_time = 0, time.time()
            progress_message = None
            redis_pipe = self.data_manager.redis.pipeline()
            messages_in_pipe = 0

            for i, channel_id in enumerate(scannable_channel_ids):
                channel = None
                try:
                    # 使用 fetch 来获取最新的频道对象。这会进行一次API调用（如果不在d.py缓存中）。
                    # 这是必要的，因为我们需要 history() 方法。
                    channel = guild.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)

                    # 再次确认类型，因为 fetch_channel 可能返回其他类型
                    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
                        self.logger.debug(f"频道 {channel_id} 不是文本频道或帖子，跳过。")
                        continue
                    async for message in channel.history(limit=None, after=start_datetime, before=end_datetime):
                        if not await self._should_track_message(message, processor):
                            continue

                        total_messages_added += 1
                        messages_in_pipe += 1
                        await self.data_manager.add_message_to_pipeline(
                            redis_pipe, guild.id, message.channel.id, message.author.id,
                            message.id, message.created_at.timestamp()
                        )
                        if messages_in_pipe >= 500:
                            await self.data_manager.execute_pipeline(redis_pipe)
                            redis_pipe = self.data_manager.redis.pipeline()
                            messages_in_pipe = 0
                            await asyncio.sleep(0.05)  # 短暂让步

                        if target_channel and time.time() - last_update_time > 5:
                            embed = self._create_progress_embed(guild, start_time, len(scannable_channel_ids), i + 1, channel.name, total_messages_added,
                                                                bool(single_channel))
                            if progress_message:
                                await progress_message.edit(embed=embed)
                            else:
                                progress_message = await target_channel.send(embed=embed)
                            last_update_time = time.time()
                except discord.Forbidden:
                    self.logger.warning(f"[{guild.name}] 无法访问频道 #{channel.name}，已跳过。")
                except Exception as e:
                    self.logger.error(f"[{guild.name}] 扫描频道 #{channel.name} 时出错: {e}", exc_info=True)

            if messages_in_pipe > 0: await self.data_manager.execute_pipeline(redis_pipe)

            # 只有全服扫描（非指定单个频道）时才更新同步时间戳
            if single_channel is None:
                await self._update_sync_timestamp(guild.id, end_datetime.timestamp(), force=True)
                ts_update_msg = "\n**全局同步时间点已更新。**"
            else:
                ts_update_msg = "\n**注意：本次为部分回填，全局同步时间点未更新。**"

            duration = time.time() - start_time
            if target_channel:
                final_embed = self._create_final_embed(
                    "✅ 历史消息回填完成", guild.name, duration, len(scannable_channel_ids), total_messages_added,
                    start_datetime, end_datetime, ts_update_msg
                )
                if progress_message:
                    await progress_message.edit(embed=final_embed, view=None)
                else:
                    await target_channel.send(embed=final_embed)

        except Exception as e:
            self.logger.critical(f"服务器 '{guild.name}' 的回填任务发生严重错误: {e}", exc_info=True)
            if target_channel: await target_channel.send(f"❌ **回填任务异常中断**: `{e}`")
        finally:
            if guild.id in self._backfill_locks: self._backfill_locks.remove(guild.id)
            self.logger.info(f"服务器 '{guild.name}' 的回填任务结束，内存锁已释放。")

    activity_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨活跃", description="用户活动追踪相关指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @activity_group.command(name="发送面板", description="发送一个活跃度角色申领面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        guild_cfg = self.config.get("guild_configs", {}).get(interaction.guild.id, {})
        target_role = interaction.guild.get_role(guild_cfg.get("target_role_id", 0))
        if not target_role:
            await interaction.followup.send("❌ 请先在配置文件中正确设置 `target_role_id`。", ephemeral=True)
            return

        embed = discord.Embed(
            title="✨ 社区活跃度认证 ✨",
            description=f"点击按钮来检查、申领或移除您的 {target_role.mention} 活跃身份组。",
            color=target_role.color or discord.Color.blurple()
        )
        claim_days = guild_cfg.get('claim_days_window', 'N/A')
        threshold = guild_cfg.get('message_threshold', 'N/A')
        daily_cap = guild_cfg.get('daily_message_cap', 'N/A')
        embed.add_field(
            name="认证标准",
            value=(f"过去 **{claim_days}** 天内，发送非纯表情/纯图片的有效消息达到 **{threshold}** 条"
                   f"（每日最多计入 **{daily_cap}** 条，即至少需要 **{int(threshold // daily_cap)}** 天活跃）。\n"
                   f"与 BOT 的对话（回复或@提及BOT）不计入。"),
            inline=False
        )
        embed.set_footer(text="所有操作仅您自己可见。")
        await interaction.followup.send(embed=embed, view=ActivityRoleView(self))

    # --- 刷屏黑名单命令 ---

    @activity_group.command(name="刷屏黑名单-添加", description="【管理员】将用户加入刷屏黑名单（30天），自动移除活跃角色。")
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.describe(user="要加入黑名单的用户", reason='处罚原因（可选，默认为"刷屏"）')
    async def blacklist_add(self, interaction: discord.Interaction, user: discord.Member, reason: Optional[str] = None):
        await interaction.response.defer()
        reason = (reason or "刷屏").strip()

        result = await self._execute_blacklist_punishment(interaction, user, reason)

        expiry_dt = datetime.now(BEIJING_TZ) + timedelta(days=30)
        guild_cfg = self.config.get("guild_configs", {}).get(interaction.guild.id, {})
        target_role = interaction.guild.get_role(guild_cfg.get("target_role_id", 0))

        execution_lines: list[str] = []
        if result.role_removed and target_role:
            execution_lines.append(f"✅ 已移除其 `{target_role.name}` 角色")
        elif result.role_remove_failed:
            execution_lines.append("⚠️ 活跃角色移除失败：权限不足")
        if result.dm_sent:
            execution_lines.append("✅ 私信提醒已发送")
        elif result.dm_failed:
            execution_lines.append("⚠️ 私信提醒发送失败：对方可能关闭了私信")
        if result.announced:
            execution_lines.append("✅ 处罚信息已发送至公示区")
        elif result.announce_failed:
            execution_lines.append("⚠️ 处罚信息未发送至公示区（请检查配置或权限）")

        embed = self._build_blacklist_embed(
            title="🚫 刷屏黑名单 — 已添加",
            color=discord.Color.red(),
            target=user,
            reason=reason,
            expiry_dt=expiry_dt,
            guild=interaction.guild,
            execution_lines=execution_lines,
        )
        await interaction.followup.send(embed=embed)

    async def _blacklist_add_context_impl(self, interaction: discord.Interaction, message: discord.Message):
        """右键菜单的实际实现，被模块级 _blacklist_add_context_callback 调用。"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        if message.guild is None:
            await interaction.followup.send("❌ 只能在服务器内使用。", ephemeral=True)
            return

        author = message.author
        reason = f"刷屏（右键菜单来源：{message.channel.mention}）"

        target_left_guild = message.guild.get_member(author.id) is None
        result = await self._execute_blacklist_punishment(interaction, author, reason)

        expiry_dt = datetime.now(BEIJING_TZ) + timedelta(days=30)
        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id, {})
        target_role = message.guild.get_role(guild_cfg.get("target_role_id", 0))

        execution_lines: list[str] = []
        if target_left_guild:
            pass
        elif result.role_removed and target_role:
            execution_lines.append(f"✅ 已移除其 `{target_role.name}` 角色")
        elif result.role_remove_failed:
            execution_lines.append("⚠️ 活跃角色移除失败：权限不足")
        if result.dm_sent:
            execution_lines.append("✅ 私信提醒已发送")
        elif result.dm_failed:
            execution_lines.append("⚠️ 私信提醒发送失败：对方可能关闭了私信")
        if result.announced:
            execution_lines.append("✅ 处罚信息已发送至公示区")
        elif result.announce_failed:
            execution_lines.append("⚠️ 处罚信息未发送至公示区（请检查配置或权限）")

        embed = self._build_blacklist_embed(
            title="🚫 刷屏黑名单 — 已添加",
            color=discord.Color.red(),
            target=author,
            reason=reason,
            expiry_dt=expiry_dt,
            guild=message.guild,
            execution_lines=execution_lines,
            context_message_jump=message.jump_url,
            target_left_guild=target_left_guild,
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def blacklist_user_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        """从黑名单中过滤当前仍在服务器内的成员，供「刷屏黑名单-移除」自动补全。"""
        entries = self.blacklist_manager.get_all_blacklisted(interaction.guild.id)
        current_lower = current.lower()
        choices: list[app_commands.Choice[str]] = []
        for user_id, expiry in entries:
            member = interaction.guild.get_member(user_id)
            if member is None:
                continue
            name = member.display_name
            uid_str = str(member.id)
            if current_lower and current_lower not in name.lower() and current_lower not in uid_str:
                continue
            days_left = max(0.0, (expiry - time.time()) / 86400)
            choices.append(app_commands.Choice(
                name=f"{name}（剩余 {days_left:.1f} 天）",
                value=uid_str,
            ))
            if len(choices) >= 25:
                break
        return choices

    @activity_group.command(name="刷屏黑名单-移除", description="【管理员】将用户从刷屏黑名单中移除。")
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.autocomplete(user=blacklist_user_autocomplete)
    @app_commands.describe(user="要移除黑名单的用户（从黑名单下拉列表中选择）")
    async def blacklist_remove(self, interaction: discord.Interaction, user: str):
        await interaction.response.defer()
        try:
            user_id = int(user)
        except ValueError:
            await interaction.followup.send("❌ 无效的用户标识。", ephemeral=True)
            return
        member = interaction.guild.get_member(user_id)
        if member is None:
            await interaction.followup.send("❌ 该用户已不在服务器中，无法显示身份组信息。", ephemeral=True)
            return

        removed = self.blacklist_manager.remove_from_blacklist(interaction.guild.id, user_id)

        if removed:
            embed = discord.Embed(
                title="✅ 刷屏黑名单 — 已移除",
                description=f"已将 {member.mention} 从刷屏黑名单中移除。",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="ℹ️ 刷屏黑名单",
                description=f"{member.mention} 不在黑名单中。",
                color=discord.Color.orange()
            )
        await interaction.followup.send(embed=embed)

    @activity_group.command(name="刷屏黑名单-查看", description="【管理员】查看当前服务器的刷屏黑名单列表。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def blacklist_list(self, interaction: discord.Interaction):
        await interaction.response.defer()
        entries = self.blacklist_manager.get_all_blacklisted(interaction.guild.id)

        if not entries:
            embed = discord.Embed(
                title="📋 刷屏黑名单",
                description="当前没有黑名单用户。",
                color=discord.Color.blue()
            )
        else:
            lines = []
            for user_id, expiry in entries:
                member = interaction.guild.get_member(user_id)
                name = member.mention if member else f"`{user_id}`"
                expiry_dt = datetime.fromtimestamp(expiry, tz=BEIJING_TZ)
                lines.append(f"{name} — 到期：{expiry_dt:%Y-%m-%d %H:%M}")
            embed = discord.Embed(
                title=f"📋 刷屏黑名单 ({len(entries)} 人)",
                description="\n".join(lines),
                color=discord.Color.red()
            )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @activity_group.command(name="管理或删除活动数据", description="【管理员】管理本服务器的活动数据。")
    @is_super_admin()
    @app_commands.describe(action="要执行的操作。")
    @app_commands.choices(action=[
        app_commands.Choice(name="【推荐】强制结束并解锁回填任务", value="finalize_and_unlock"),
        app_commands.Choice(name="【危险】清除本服所有活动数据", value="clear_guild_data"),
        app_commands.Choice(name="【一次性】为旧数据重建索引", value="rebuild_indexes")
    ])
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_activity_data(self, interaction: discord.Interaction, action: str):
        guild_id = interaction.guild_id

        if action == "finalize_and_unlock":
            if guild_id not in self._backfill_locks:
                await interaction.response.send_message("ℹ️ 本服务器当前没有正在运行的回填任务。", ephemeral=True)
                return

            await self._update_sync_timestamp(guild_id, datetime.now(timezone.utc).timestamp(), force=True)
            self._backfill_locks.remove(guild_id)
            self.logger.warning(f"服务器 {guild_id} 的回填任务被 {interaction.user} 强制结束并解锁。")
            await interaction.response.send_message("✅ 已将同步时间点更新至当前，并移除了回填锁。", ephemeral=True)

        elif action == "clear_guild_data":
            view = ConfirmationView(author=interaction.user)
            await interaction.response.send_message("⚠️ **危险操作！** 此操作将清除本服所有已记录的用户活动数据且不可撤销，确定吗？", view=view, ephemeral=True)
            await view.wait()
            if view.value:
                await interaction.edit_original_response(content="⏳ 正在清除数据...", view=None)
                deleted_count = await self.data_manager.delete_guild_activity_data(guild_id)
                await interaction.edit_original_response(content=f"✅ 操作完成！成功清除了 `{deleted_count}` 条数据。", view=None)
            else:
                await interaction.edit_original_response(content="❌ 操作已取消。", view=None)

        elif action == "rebuild_indexes":
            if guild_id in self._backfill_locks:
                await interaction.response.send_message("❌ 回填任务正在运行，请稍后再试。", ephemeral=True)
                return

            view = ConfirmationView(author=interaction.user)
            await interaction.response.send_message("⚠️ **高负载操作！** 此操作会扫描所有相关数据并重建索引，耗时较长。确定吗？", view=view, ephemeral=True)
            await view.wait()
            if view.value:
                self._backfill_locks.add(guild_id)
                self.logger.warning(f"用户 {interaction.user} 启动了服务器 {interaction.guild.name} 的索引重建任务。")
                await interaction.edit_original_response(content="✅ 索引重建任务已在后台启动，完成后会通知您。", view=None)

                start_time = time.time()
                try:
                    scanned, created = await self.data_manager.rebuild_indexes_for_guild(guild_id)
                    duration = time.time() - start_time
                    await interaction.followup.send(f"🎉 **索引重建完成！**\n耗时: `{duration:.2f}` 秒, 扫描键: `{scanned}`, 创建索引: `{created}`")
                except Exception as e:
                    self.logger.critical(f"重建索引时发生严重错误: {e}", exc_info=True)
                    await interaction.followup.send(f"❌ **索引重建失败！** 错误: `{e}`")
                finally:
                    if guild_id in self._backfill_locks: self._backfill_locks.remove(guild_id)
            else:
                await interaction.edit_original_response(content="❌ 操作已取消。", view=None)

    @staticmethod
    def _parse_flexible_date(date_str: str) -> Optional[datetime]:
        """尝试以多种格式解析日期字符串，返回 UTC datetime 对象。"""
        now = datetime.now(BEIJING_TZ)
        formats = ["%Y-%m-%d", "%m-%d", "%d"]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if fmt == "%m-%d":
                    dt = dt.replace(year=now.year)
                elif fmt == "%d":
                    dt = dt.replace(year=now.year, month=now.month)
                return BEIJING_TZ.localize(dt).astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    @activity_group.command(name="回填", description="手动拉取指定时间范围/频道的历史消息以填充活动数据。")
    @app_commands.describe(
        start_date="开始日期 (格式: YYYY-MM-DD, MM-DD, 或 DD, 时区: UTC+8)。",
        end_date="结束日期 (同上, 默认为今天)。",
        hours_ago="从现在回溯的小时数 (与日期选项互斥)。",
        channel="【可选】只扫描此特定频道/类别。",
        entire_group="【可选】同时回填整个共通活跃度组内的所有服务器。"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(
            self, interaction: discord.Interaction,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            hours_ago: Optional[int] = None,
            channel: Optional[Union[discord.TextChannel, discord.Thread, discord.ForumChannel, discord.CategoryChannel]] = None,
            entire_group: bool = False
    ):
        guild = interaction.guild

        if any(gid in self._backfill_locks for gid in self._get_shared_guild_ids(guild.id)):
            await interaction.response.send_message("❌ 此服务器或共通组内已有回填任务正在运行。", ephemeral=True)
            return

        now_utc = datetime.now(timezone.utc)
        start_dt, end_dt = None, now_utc

        if hours_ago:
            if start_date or end_date:
                await interaction.response.send_message("❌ 不能同时使用日期和回溯小时数。", ephemeral=True)
                return
            start_dt = now_utc - timedelta(hours=hours_ago)
        else:
            if not start_date:
                await interaction.response.send_message("❌ 必须提供 `start_date` 或 `hours_ago`。", ephemeral=True)
                return
            start_dt = self._parse_flexible_date(start_date)
            if not start_dt:
                await interaction.response.send_message("❌ 开始日期格式错误。", ephemeral=True)
                return
            if end_date:
                parsed_end = self._parse_flexible_date(end_date)
                if not parsed_end:
                    await interaction.response.send_message("❌ 结束日期格式错误。", ephemeral=True)
                    return
                end_dt = parsed_end + timedelta(days=1, microseconds=-1)
            if start_dt >= end_dt:
                await interaction.response.send_message("❌ 开始日期必须在结束日期之前。", ephemeral=True)
                return

        guild_ids = self._get_shared_guild_ids(guild.id) if entire_group else [guild.id]

        start_disp = start_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        end_disp = end_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        if entire_group and len(guild_ids) > 1:
            target_disp = f"共通组（{len(guild_ids)} 个服务器）"
        else:
            target_disp = f"频道/类别 {channel.mention}" if channel else f"服务器 **{interaction.guild.name}**"

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n我将开始拉取从 `{start_disp}` 到 `{end_disp}` (UTC+8) 在 {target_disp} 的消息。",
            ephemeral=False
        )

        for gid in guild_ids:
            g = self.bot.get_guild(gid)
            if not g:
                continue
            if gid in self._backfill_locks:
                continue

            g_cfg = self.config.get("guild_configs", {}).get(gid, {})
            report_channel = await self._resolve_report_channel(g_cfg) or interaction.channel

            self.bot.loop.create_task(self._backfill_guild_history(
                guild=g, target_channel=report_channel,
                start_datetime=start_dt, end_datetime=end_dt,
                single_channel=channel if gid == guild.id else None
            ))
            await asyncio.sleep(1)

    @staticmethod
    def _create_progress_embed(guild, start_time, total, scanned, current_name, added, is_single):
        elapsed = time.time() - start_time
        scan_target = f"({scanned}/{total})" if not is_single else ""
        embed = discord.Embed(
            title="⏳ 正在回填历史消息...",
            description=f"服务器 **{guild.name}** 回填进行中...",
            color=discord.Color.blue()
        )
        embed.add_field(name="当前进度", value=f"正在扫描 **#{current_name}** {scan_target}", inline=False)
        embed.add_field(name="已写入", value=f"{added} 条", inline=True)
        embed.add_field(name="已用时", value=f"{int(elapsed)} 秒", inline=True)
        return embed

    @staticmethod
    def _create_final_embed(title, guild_name, duration, total_ch, added, start_dt, end_dt, footer):
        start_disp = start_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        end_disp = end_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        embed = discord.Embed(
            title=title,
            description=f"为 **{guild_name}** 拉取了从 `{start_disp}` 到 `{end_disp}` (UTC+8) 的消息。{footer}",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="总耗时", value=f"{duration:.2f} 秒", inline=True)
        embed.add_field(name="扫描频道数", value=f"{total_ch}", inline=True)
        embed.add_field(name="写入消息数", value=f"{added}", inline=True)
        return embed

    role_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨通用",
        description=f"{config.COMMAND_GROUP_NAME}的通用机器人指令，只需要可以查看消息就能使用",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(read_messages=True),
    )

    @role_group.command(name="统计活跃度", description="统计指定范围和指标的活跃度数据。")
    @app_commands.describe(
        scope="统计范围：服务器、特定频道、或特定频道类别。",
        metric="统计指标：独立活跃用户数，或总消息数。",
        days_window="回溯天数 (例如: 7, 30)。",
        target_channel="[可选] 要统计的特定频道/类别。"
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="整个服务器", value="guild"),
            app_commands.Choice(name="特定频道/类别", value="channel")
        ],
        metric=[
            app_commands.Choice(name="独立活跃用户数", value="distinct_users"),
            app_commands.Choice(name="总消息数", value="total_messages")
        ]
    )
    @app_commands.checks.has_permissions(read_messages=True)
    async def get_activity_stats(
            self, interaction: discord.Interaction, scope: str, metric: str,
            days_window: int = 7,
            target_channel: Optional[Union[discord.TextChannel, discord.Thread, discord.ForumChannel, discord.CategoryChannel]] = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild

        if scope == "channel" and not target_channel:
            await interaction.followup.send("❌ 当范围为 `特定频道/类别` 时，必须指定 `target_channel`。", ephemeral=True)
            return

        # 1. 获取全量原始数据
        raw_all_activity = await self.data_manager.get_channel_activity_summary(guild.id, days_window)
        if not raw_all_activity:
            await interaction.followup.send("在指定时间范围内没有找到任何活动记录。", ephemeral=True)
            return

        # 2. 实例化 Processor 并预热缓存
        processor = self._get_processor(guild)
        if not processor:
            await interaction.followup.send("❌ 服务器配置不存在，请联系管理员。", ephemeral=True)
            return
        all_channel_ids = {cid for user_data in raw_all_activity.values() for cid in user_data.keys()}

        # 批量获取DTO以预热缓存
        dto_tasks = [processor.get_or_fetch_channel_info(cid) for cid in all_channel_ids]
        await asyncio.gather(*dto_tasks)

        # 3. 在 Python 端进行范围筛选和聚合
        scoped_channel_msg_counts = collections.defaultdict(int)
        scoped_channel_users = collections.defaultdict(set)

        target_channel_ids = set()
        if scope == "channel" and target_channel:
            # 获取目标范围内的所有子频道ID

            relevant_channels = await processor.get_scannable_channels(target_channel)
            target_channel_ids = {cid for cid in relevant_channels}

        for user_id, user_channels_data in raw_all_activity.items():
            for channel_id, count in user_channels_data.items():
                if not await processor.is_channel_included(channel_id): continue
                if scope == "channel" and channel_id not in target_channel_ids: continue

                scoped_channel_msg_counts[channel_id] += count
                scoped_channel_users[channel_id].add(user_id)

        if not scoped_channel_msg_counts:
            await interaction.followup.send("在您指定的范围内没有找到任何符合条件的活动记录。", ephemeral=True)
            return

        # 4. 确定要排序的数据和总计
        data_to_sort, total_stat = [], 0
        if metric == "total_messages":
            data_to_sort = list(scoped_channel_msg_counts.items())
            total_stat = sum(scoped_channel_msg_counts.values())
        else:  # distinct_users
            data_to_sort = [(cid, len(users)) for cid, users in scoped_channel_users.items()]
            all_users = set.union(*scoped_channel_users.values()) if scoped_channel_users else set()
            total_stat = len(all_users)

        # 5. 使用 Processor 排序
        sorted_display_data = await processor.process_and_sort_for_display(data_to_sort)

        # 6. 构建 Embed 和 View
        scope_desc = f"服务器 {guild.name}" if scope == "guild" else f"频道/类别 {target_channel.name}"
        metric_name = "总消息数" if metric == "total_messages" else "独立活跃用户数"
        value_suffix = "条" if metric == "total_messages" else "位"

        embed_template = discord.Embed(
            title=f"📈 活跃度统计报告 ({days_window}天)",
            description=f"在 **{scope_desc}** 中，过去 **{days_window}** 天的活跃度概览：",
            color=discord.Color.dark_green()
        )
        embed_template.add_field(name=f"**总计{metric_name}**", value=f"`{total_stat}` {value_suffix}", inline=False)
        embed_template.set_footer(text=f"统计于 {datetime.now(BEIJING_TZ):%Y-%m-%d %H:%M:%S}")

        view = UserReportDetailView(
            embed_template=embed_template,
            sorted_display_data=sorted_display_data,
            field_name=f"分频道{metric_name}",
            value_suffix=f"{value_suffix}"
        )
        await view.start(interaction, ephemeral=True)

    async def get_redis_stats(self) -> Optional[dict]:
        """公共接口，用于从其 DataManager 获取 Redis 统计信息。"""
        return await self.data_manager.get_redis_info()

    def get_processor_cache_stats(self, guild: Guild) -> tuple[int, int]:
        """
        【新增】公共接口，获取当前服务器/所有 ActivityProcessor 实例中缓存的 DTO 总数。
        返回 (this_dtos, total_dtos)
        """
        if not self._processors:
            return 0, 0

        this_dtos = len(self._get_processor(guild).channel_info_cache)
        total_dtos = sum(
            len(processor.channel_info_cache) for processor in self._processors.values()
        )
        return this_dtos, total_dtos

    @activity_group.command(name="导出数据", description="【管理员】将会员活动数据导出为 CSV 文件。")
    @app_commands.describe(
        aggregation_level="数据聚合的时间粒度。按天聚合文件更小，按小时聚合更详细。",
        compression="是否对导出的 CSV 文件进行 Gzip 压缩，推荐在数据量大时使用。"
    )
    @app_commands.choices(
        aggregation_level=[
            app_commands.Choice(name="按天聚合 (推荐)", value="daily"),
            app_commands.Choice(name="按小时聚合", value="hourly"),
        ],
        compression=[
            app_commands.Choice(name="Gzip 压缩 (.csv.gz)", value="gzip"),
            app_commands.Choice(name="不压缩 (.csv)", value="none"),
        ]
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def export_data(self, interaction: discord.Interaction,
                          aggregation_level: str,
                          compression: str):
        """
        处理数据导出请求。这是一个高负载操作，会流式处理 Redis 数据。
        """
        # if interaction.guild_id in self._backfill_locks:
        #     await interaction.response.send_message("❌ 回填任务正在运行，为避免性能问题，请稍后再导出。", ephemeral=True)
        #     return

        await interaction.response.defer(ephemeral=True, thinking=True)

        self.logger.warning(
            f"用户 {interaction.user} (ID: {interaction.user.id}) 请求从服务器 {interaction.guild.name} 导出活动数据。聚合级别: {aggregation_level}, 压缩: {compression}")

        try:
            start_time = time.time()
            use_compression = compression == "gzip"

            # 调用 DataManager 的核心导出功能
            file_bytes, filename = await self.data_manager.generate_activity_csv(
                interaction.guild_id,
                aggregation_level,
                use_compression
            )

            duration = time.time() - start_time

            if not file_bytes:
                await interaction.followup.send("ℹ️ 在服务器中没有找到任何可导出的活动数据。", ephemeral=True)
                return

            # 使用 io.BytesIO 将字节流包装成文件对象
            file_buffer = io.BytesIO(file_bytes)
            discord_file = discord.File(fp=file_buffer, filename=filename)

            # 发送消息和文件
            await interaction.followup.send(
                f"✅ **数据导出成功！**\n"
                f"耗时: `{duration:.2f}` 秒。\n"
                f"文件已生成，请查收。导出的数据仅包含 `时间点`, `频道ID`, `用户ID` 和 `消息数`，不含任何消息内容。",
                file=discord_file,
                ephemeral=True
            )

        except Exception as e:
            self.logger.critical(f"导出服务器 {interaction.guild.id} 的活动数据时发生严重错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ **导出失败！** 发生了一个内部错误，请检查机器人后台日志。", ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(TrackActivityCog(bot))
