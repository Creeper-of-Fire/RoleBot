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

    def __init__(self, cog: 'ActivityTrackerCog'):
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

class ActivityTrackerCog(commands.Cog, name="ActivityTracker"):
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

        guild_id = message.guild.id
        guild_cfg = self.config.get("guild_configs", {}).get(guild_id)

        if not guild_cfg or message.channel.id in guild_cfg.get("ignored_channels", []):
            return

        user_id = message.author.id
        timestamp = message.created_at.timestamp()
        key = ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)

        async with self.redis.pipeline() as pipe:
            await pipe.zadd(key, {str(message.id): timestamp})

            retention_days = guild_cfg.get("data_retention_days", 90)
            cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp()
            await pipe.zremrangebyscore(key, '-inf', cutoff_timestamp)

            await pipe.execute()

    # --- 指令组 ---
    activity_group = app_commands.Group(
        name="用户活跃度",
        description="用户活动追踪相关指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @activity_group.command(name="活跃度身份组领取面板", description="发送一个活跃度角色申领面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_activity_panel(self, interaction: discord.Interaction):
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

    @activity_group.command(name="手动拉取历史消息", description="手动拉取历史消息以填充活动数据。")
    @app_commands.describe(days="要拉取多少天内的历史消息（默认30天）")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def backfill_history(self, interaction: discord.Interaction, days: int = 30):
        """手动回填指令（逻辑不变）"""
        guild = interaction.guild

        is_running = await self.redis.sismember(ACTIVE_BACKFILLS_KEY, str(guild.id))
        if is_running:
            await interaction.response.send_message("❌ 此服务器上已经有一个回填任务正在运行。请等待其完成后再试。", ephemeral=True)
            return

        await interaction.response.send_message("✅ **历史消息回填任务已启动！**\n\n我将在后台努力工作，并将进度持续发送到本频道。这可能需要很长时间，请耐心等待。",
                                                ephemeral=False)

        self.bot.loop.create_task(self._backfill_guild_history(interaction, days))

    async def _backfill_guild_history(self, interaction: discord.Interaction, days: int):
        guild = interaction.guild
        channel_to_report = interaction.channel

        await self.redis.sadd(ACTIVE_BACKFILLS_KEY, str(guild.id))
        self.logger.info(f"服务器 '{guild.name}' 开始历史消息回填任务，范围: {days}天。由 {interaction.user} 触发。")

        start_time = time.time()
        after_timestamp = datetime.now(timezone.utc) - timedelta(days=days)
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))

        total_messages_processed, total_messages_added, channels_scanned = 0, 0, 0
        last_update_time, progress_message = time.time(), None

        try:
            channels_to_scan = [c for c in guild.text_channels if c.id not in ignored_channels and c.permissions_for(guild.me).read_message_history]
            total_channels = len(channels_to_scan)

            async with self.redis.pipeline() as pipe:
                messages_in_pipe = 0
                for channel in channels_to_scan:
                    channels_scanned += 1
                    try:
                        async for message in channel.history(limit=None, after=after_timestamp, oldest_first=False):
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
                                embed = self._create_progress_embed(guild, start_time, total_channels, channels_scanned, channel.name, total_messages_processed,
                                                                    total_messages_added)
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
                description=f"成功为服务器 **{guild.name}** 拉取了过去 **{days}** 天的历史消息。",
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

    def _create_progress_embed(self, guild, start_time, total_channels, channels_scanned, current_channel_name, processed_count, added_count):
        """辅助函数，用于创建统一格式的进度条 Embed（逻辑不变）"""
        elapsed_time = time.time() - start_time
        embed = discord.Embed(
            title="⏳ 正在回填历史消息...",
            description=f"服务器 **{guild.name}** 的回填任务正在进行中。",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="当前进度", value=f"正在扫描频道 **#{current_channel_name}** ({channels_scanned}/{total_channels})", inline=False)
        embed.add_field(name="已处理消息", value=f"`{processed_count}`", inline=True)
        embed.add_field(name="已写入 Redis", value=f"`{added_count}`", inline=True)
        embed.add_field(name="已用时", value=f"{int(elapsed_time)} 秒", inline=True)
        embed.set_footer(text="请耐心等待，这可能需要很长时间...")
        return embed


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(ActivityTrackerCog(bot))
