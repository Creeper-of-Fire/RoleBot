# activity_tracker/cog.py

from __future__ import annotations

import asyncio
import time
import typing
from datetime import datetime, timedelta, timezone

import discord
import redis.asyncio as redis
from discord import app_commands, ui
from discord.ext import commands
from redis import exceptions

import config
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from main import RoleBot

# --- Redis 键名模板 ---
ACTIVITY_KEY_TEMPLATE = "activity:{guild_id}:{user_id}"
ACTIVE_BACKFILLS_KEY = "active_backfills"


# ===================================================================
# 1. 持久化视图和按钮
# ===================================================================

class ActivityRoleView(ui.View):
    """
    包含“检查我的活跃度”按钮的持久化视图。
    """

    def __init__(self, cog: 'TrackActivityCog'):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="检查我的活跃度 & 申领/移除角色", style=discord.ButtonStyle.success, custom_id="check_activity_role")
    async def check_activity_button(self, interaction: discord.Interaction, button: ui.Button):
        """
        当用户点击按钮时，检查他们的活跃度并执行相应操作。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild

        # --- 获取配置 ---
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

        # --- 查询 Redis ---
        key = ACTIVITY_KEY_TEMPLATE.format(guild_id=guild.id, user_id=member.id)
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()
        message_count = await self.cog.redis.zcount(key, cutoff_timestamp, '+inf')

        has_role = target_role in member.roles
        is_eligible = message_count >= message_threshold

        # --- 生成响应消息 ---
        status_emoji = "✅" if is_eligible else "❌"
        status_text = "符合" if is_eligible else "不符合"

        embed = discord.Embed(
            title="活跃度检查结果",
            description=f"你好，{member.mention}！\n这是你在过去 **{days_window}** 天内的活跃度报告：",
            color=discord.Color.green() if is_eligible else discord.Color.orange()
        )
        embed.add_field(name="统计消息数", value=f"`{message_count}` 条", inline=True)
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

        # 实例化的是异步客户端
        # 注意这里的 redis 是我们导入的 redis.asyncio
        self.redis = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)
        self.bot.loop.create_task(self.check_redis_connection())

        # 注册持久化视图
        self.bot.add_view(ActivityRoleView(self))

    async def check_redis_connection(self):
        """在启动时异步检查 Redis 连接。"""
        try:
            # 所有 Redis 调用都需要 await
            await self.redis.ping()
            self.logger.info("成功连接到 Redis 服务器 (异步客户端)。")
        except exceptions.ConnectionError as e:
            self.logger.critical(f"无法连接到 Redis，活动追踪模块将无法工作！错误: {e}")
            # 让整个 cog 失效
            self.cog_check = lambda ctx: False

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """实时记录用户发送的每一条消息（逻辑不变）"""
        if message.author.bot or not message.guild:
            return
        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg:
            return
        ignored_channels = guild_cfg.get("ignored_channels", [])
        ignored_categories = guild_cfg.get("ignored_categories", [])
        if message.channel.id in ignored_channels or (message.channel.category_id and message.channel.category_id in ignored_categories):
            return

        key = ACTIVITY_KEY_TEMPLATE.format(guild_id=message.guild.id, user_id=message.author.id)

        async with self.redis.pipeline() as pipe:
            await pipe.zadd(key, {str(message.id): message.created_at.timestamp()})
            retention_days = guild_cfg.get("data_retention_days", 90)
            cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp()
            await pipe.zremrangebyscore(key, '-inf', cutoff_timestamp)

            await pipe.execute()

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

        # 获取配置以在面板上显示信息
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

            # 等待用户点击按钮
            await view.wait()

            if view.value is True:  # 用户点击了确认
                await interaction.edit_original_response(content="⏳ 正在清除数据，请稍候...", view=None)
                deleted_count = await self._delete_guild_activity_data(guild.id)
                await interaction.edit_original_response(content=f"✅ **操作完成！**\n成功清除了 `{deleted_count}` 条与本服务器相关的用户活动数据。")
            elif view.value is False:  # 用户点击了取消
                await interaction.edit_original_response(content="❌ 操作已取消。", view=None)
            else:  # 超时
                await interaction.edit_original_response(content="⏰ 操作超时，已自动取消。", view=None)

    async def _delete_guild_activity_data(self, guild_id: int) -> int:
        """
        使用 SCAN_ITER 安全地查找并删除一个服务器的所有活动数据键。
        返回被删除的键的数量。
        """
        pattern = f"activity:{guild_id}:*"
        self.logger.warning(f"开始为服务器 {guild_id} 清除活动数据，匹配模式: {pattern}")

        # 异步迭代器获取所有匹配的键
        keys_to_delete = [key async for key in self.redis.scan_iter(pattern)]

        if not keys_to_delete:
            self.logger.info(f"服务器 {guild_id} 没有找到需要清除的活动数据。")
            return 0

        # 使用 pipeline 批量删除，效率更高
        await self.redis.delete(*keys_to_delete)

        self.logger.warning(f"成功为服务器 {guild_id} 清除了 {len(keys_to_delete)} 个键。")
        return len(keys_to_delete)

    @staticmethod
    def _parse_flexible_date(date_str: str) -> typing.Optional[datetime]:
        """
        尝试以多种格式解析日期字符串 (YYYY-MM-DD, MM-DD, DD)。
        返回一个 timezone-aware 的 datetime 对象，如果所有格式都失败则返回 None。
        """
        now = datetime.now(timezone.utc)

        # 尝试 YYYY-MM-DD
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

        # 尝试 MM-DD (使用当前年份)
        try:
            dt = datetime.strptime(date_str, "%m-%d")
            return dt.replace(year=now.year, tzinfo=timezone.utc)
        except ValueError:
            pass

        # 尝试 DD (使用当前年份和月份)
        try:
            dt = datetime.strptime(date_str, "%d")
            return dt.replace(year=now.year, month=now.month, tzinfo=timezone.utc)
        except ValueError:
            pass

        return None

    @activity_group.command(name="手动拉取历史消息-开始", description="手动拉取指定时间范围内的历史消息以填充活动数据。")
    @app_commands.describe(
        start_date="开始日期 (格式: YYYY-MM-DD, MM-DD, 或 DD)",
        end_date="结束日期 (格式同上, 默认为今天)"
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(self, interaction: discord.Interaction, start_date: str, end_date: str = None):
        guild = interaction.guild

        is_running = await self.redis.sismember(ACTIVE_BACKFILLS_KEY, str(guild.id))
        if is_running:
            await interaction.response.send_message("❌ 此服务器上已经有一个回填任务正在运行。", ephemeral=True)
            return

        start_datetime = self._parse_flexible_date(start_date)
        if not start_datetime:
            await interaction.response.send_message("❌ **开始日期格式错误！**\n请使用 `YYYY-MM-DD`, `MM-DD`, 或 `DD` 格式。", ephemeral=True)
            return

        end_datetime = datetime.now(timezone.utc)
        if end_date:
            parsed_end = self._parse_flexible_date(end_date)
            if not parsed_end:
                await interaction.response.send_message("❌ **结束日期格式错误！**\n请使用 `YYYY-MM-DD`, `MM-DD`, 或 `DD` 格式。", ephemeral=True)
                return
            # 结束日期需要到当天的最后一秒
            end_datetime = parsed_end + timedelta(days=1)

        if start_datetime >= end_datetime:
            await interaction.response.send_message("❌ **错误**：开始日期必须在结束日期之前。", ephemeral=True)
            return

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n\n"
            f"我将开始拉取从 **{start_datetime.strftime('%Y-%m-%d')}** 到 **{end_datetime.strftime('%Y-%m-%d')}** 的历史消息。",
            ephemeral=False
        )

        self.bot.loop.create_task(self._backfill_guild_history(interaction, start_datetime, end_datetime))

    async def _backfill_guild_history(self, interaction: discord.Interaction, start_datetime: datetime, end_datetime: datetime):
        guild = interaction.guild
        channel_to_report = interaction.channel

        await self.redis.sadd(ACTIVE_BACKFILLS_KEY, str(guild.id))
        self.logger.info(
            f"服务器 '{guild.name}' 开始历史消息回填任务。范围: "
            f"{start_datetime.strftime('%Y-%m-%d')} 至 {end_datetime.strftime('%Y-%m-%d')}"
            f"。由 {interaction.user} 触发。"
        )

        start_time = time.time()
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        # 【改动】获取忽略类别配置
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        total_messages_processed, total_messages_added, channels_scanned = 0, 0, 0
        last_update_time, progress_message = time.time(), None

        try:
            # 【改动】在筛选频道时，同时检查类别ID
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
                        # 【改动】使用 after 和 before 参数来精确控制时间范围
                        async for message in channel.history(limit=None, after=start_datetime, before=end_datetime, oldest_first=False):
                            if message.author.bot: continue
                            total_messages_processed += 1
                            key = ACTIVITY_KEY_TEMPLATE.format(guild_id=guild.id, user_id=message.author.id)
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
                                    start_datetime, end_datetime
                                )
                                if progress_message and (current_time - progress_message.created_at.timestamp() < 600):
                                    try:
                                        await progress_message.edit(embed=embed)
                                    except discord.NotFound:
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

            final_embed = discord.Embed(
                title="✅ 历史消息回填完成",
                description=f"成功为服务器 **{guild.name}** 拉取了从 **{start_datetime.strftime('%Y-%m-%d')}** 到 **{end_datetime.strftime('%Y-%m-%d')}** 的历史消息。",
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
    def _create_progress_embed(guild, start_time, total_channels, channels_scanned, current_channel_name, processed_count, added_count, start_dt, end_dt):
        elapsed_time = time.time() - start_time
        embed = discord.Embed(
            title="⏳ 正在回填历史消息...",
            description=f"服务器 **{guild.name}** 的回填任务正在进行中。\n**时间范围:** `{start_dt.strftime('%Y-%m-%d')}` 至 `{end_dt.strftime('%Y-%m-%d')}`",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="当前进度", value=f"正在扫描频道 **#{current_channel_name}** ({channels_scanned}/{total_channels})", inline=False)
        embed.add_field(name="已处理消息", value=f"`{processed_count}`", inline=True)
        embed.add_field(name="已写入 Redis", value=f"`{added_count}`", inline=True)
        embed.add_field(name="已用时", value=f"{int(elapsed_time)} 秒", inline=True)
        embed.set_footer(text="请耐心等待，这可能需要很长时间...")
        return embed


async def setup(bot: RoleBot):
    """Cog的入口点。"""
    await bot.add_cog(TrackActivityCog(bot))