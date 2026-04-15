# activity_tracker/cog.py
from __future__ import annotations

import asyncio
import collections
import io
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING, Dict, Union

import discord
import emoji
from discord import app_commands, Guild
from discord.ext import commands

import config
from activity_tracker.data_manager import DataManager, BEIJING_TZ
from activity_tracker.logic import ActivityProcessor
from activity_tracker.views import ActivityRoleView, ReportEmbeds, UserReportDetailView
from utility.permison import is_super_admin
from utility.views import ConfirmationView

if TYPE_CHECKING:
    from main import RoleBot


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
        # 用于防止并发回填任务的内存锁
        self._backfill_locks: set[int] = set(config.GUILD_IDS)
        # 用于节流更新最后同步时间戳
        self._last_timestamp_update: Dict[int, float] = {}
        self.TIMESTAMP_UPDATE_INTERVAL = 60

        self._processors: Dict[int, ActivityProcessor] = {}

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

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        【重构】实时记录用户发送的每一条消息。
        现在调用 ActivityProcessor 的中央过滤方法来决定是否记录。
        """
        if message.author.bot or not message.guild:
            return

        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg:
            return

        should_filter, _ = self.is_not_valid_message(message)
        if should_filter:
            return

        # 1. 实例化处理器 (轻量级操作)
        processor = self._get_processor(message.guild)

        # 2. 使用中央过滤逻辑，并传入 message.channel 对象来预热缓存，避免API调用
        if not await processor.is_channel_included(message.channel.id, message.channel):
            return

        # 3. 如果通过过滤，则记录数据
        retention_days = guild_cfg.get("data_retention_days", 90)
        message_ts = message.created_at.timestamp()

        await self.data_manager.record_message(
            guild_id=message.guild.id, channel_id=message.channel.id, user_id=message.author.id,
            message_id=message.id, created_at_timestamp=message_ts, retention_days=retention_days
        )
        await self._throttled_update_sync_timestamp(message.guild.id, message_ts)

    # --- 视图回调处理方法 (公共接口) ---

    async def handle_check_activity(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的“检查活跃度”按钮点击。"""
        guild, member = interaction.guild, interaction.user
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})

        # 简化配置检查
        if not all(k in guild_cfg for k in ["target_role_id", "message_threshold", "days_window"]):
            await interaction.followup.send("❌ 服务器配置不完整，请联系管理员。", ephemeral=True)
            return

        target_role = guild.get_role(guild_cfg["target_role_id"])
        if not target_role:
            await interaction.followup.send("❌ 配置中的目标角色未找到，请联系管理员。", ephemeral=True)
            return

        processor = self._get_processor(guild)
        total_messages, _ = await processor.get_user_activity_summary(member.id, guild_cfg["days_window"])

        is_eligible = total_messages >= guild_cfg["message_threshold"]
        has_role = target_role in member.roles
        action_text = ""
        try:
            if is_eligible and not has_role:
                await member.add_roles(target_role, reason="通过面板申领活跃角色")
                action_text = f"\n🎉 **已为您授予 `{target_role.name}` 角色！**"
            # elif not is_eligible and has_role:
            #     await member.remove_roles(target_role, reason="通过面板确认不活跃并移除")
            #     action_text = f"\nℹ️ 您不满足条件，已移除 `{target_role.name}` 角色。"
            # elif is_eligible and has_role:
            elif has_role:
                action_text = "\n👍 您已拥有该角色，无需操作。"
            else:
                action_text = "\n💪 请继续努力！"
        except discord.Forbidden:
            action_text = "\n⚠️ 我没有权限为您操作角色，请联系管理员。"

        embed = ReportEmbeds.create_check_activity_embed(
            member, guild_cfg["days_window"], total_messages, guild_cfg["message_threshold"], action_text
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def handle_view_report(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的“查看报告”按钮点击。"""
        guild, member = interaction.guild, interaction.user
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        if not (days_window := guild_cfg.get("days_window")):
            await interaction.followup.send("❌ 服务器配置不完整。", ephemeral=True)
            return

        processor = self._get_processor(guild)
        report_data = await processor.generate_user_report_data(member.id, days_window)
        sorted_display_data = await processor.process_and_sort_for_display(report_data.channel_activity)

        embed_template = ReportEmbeds.create_user_report_embed_template(member, days_window, report_data)

        pagination_view = UserReportDetailView(
            embed_template=embed_template,
            sorted_display_data=sorted_display_data,
            field_name="分频道消息数",
            value_suffix="条"
        )
        await pagination_view.start(interaction, ephemeral=True)

    async def handle_remove_role(self, interaction: discord.Interaction):
        """处理来自 ActivityRoleView 的“移除角色”按钮点击。"""
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

                report_channel = None
                if report_channel_id := guild_cfg.get("report_channel_id"):
                    report_channel = guild.get_channel(report_channel_id) or await guild.fetch_channel(report_channel_id)

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
                        if message.author.bot:
                            continue
                        should_filter, _ = self.is_not_valid_message(message)
                        if should_filter:
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
        embed.add_field(
            name="认证标准",
            value=f"过去 **{guild_cfg.get('days_window', 'N/A')}** 天内，发送非纯表情/纯图片的有效消息达到 **{guild_cfg.get('message_threshold', 'N/A')}** 条。",
            inline=False
        )
        embed.set_footer(text="所有操作仅您自己可见。")
        await interaction.followup.send(embed=embed, view=ActivityRoleView(self))

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
        channel="【可选】只扫描此特定频道/类别。"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(
            self, interaction: discord.Interaction,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            hours_ago: Optional[int] = None,
            channel: Optional[Union[discord.TextChannel, discord.Thread, discord.ForumChannel, discord.CategoryChannel]] = None
    ):
        if interaction.guild_id in self._backfill_locks:
            await interaction.response.send_message("❌ 此服务器上已经有一个回填任务正在运行。", ephemeral=True)
            return

        guild = interaction.guild

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

        start_disp = start_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        end_disp = end_dt.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        target_disp = f"频道/类别 {channel.mention}" if channel else f"服务器 **{interaction.guild.name}**"

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n我将开始拉取从 `{start_disp}` 到 `{end_disp}` (UTC+8) 在 {target_disp} 的消息。",
            ephemeral=False
        )

        if guild.id in self._backfill_locks:
            self.logger.warning(f"服务器 '{guild.name}' 尝试启动回填任务，但任务已被锁定。")
            if interaction.channel:
                await interaction.channel.send("⚠️ **任务中止**：服务器上已有另一个回填任务正在运行。")
            return

        self.bot.loop.create_task(self._backfill_guild_history(
            guild=interaction.guild, target_channel=interaction.channel,
            start_datetime=start_dt, end_datetime=end_dt,
            single_channel=channel
        ))

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
