# honor_system/post_module.py
from __future__ import annotations

import asyncio
import datetime
import time
import typing
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

import config
import config_data
from activity_tracker_db.activity_data_manager import ActivityDataManager
from honor_system.data_manager.honor_data_manager import HonorDataManager

if typing.TYPE_CHECKING:
    from main import RoleBot


class HonorPostModuleCog(commands.Cog, name="HonorPostModule"):
    """【荣誉子模块】管理与成员发帖相关的荣誉。"""

    def __init__(self, bot: 'RoleBot'):
        self.running_backfill_tasks: typing.Dict[int, asyncio.Task] = {}
        self.logger = bot.logger
        self.bot = bot
        self.honor_data_manager = HonorDataManager.getDataManager(logger=bot.logger)
        self.activity_data_manager = ActivityDataManager.getDataManager(logger=bot.logger)

    # --- 核心荣誉授予逻辑 ---
    async def _process_thread_for_honor(self, thread: discord.Thread):
        """
        【核心处理逻辑】处理单个帖子，检查并授予相应的荣誉。
        此函数被 on_thread_create 和回填命令共同调用。
        """
        if not isinstance(thread.parent, discord.ForumChannel):
            return

        # 有时 owner 是 None，特别是在处理旧帖子时
        try:
            author = thread.owner
        except (discord.NotFound, AttributeError):
            self.logger.warning(f"无法获取帖子 T:{thread.id} 的所有者，跳过荣誉处理。")
            return

        if not author or author.bot:
            return

        # 1. 处理基础活动荣誉
        event_cfg = config_data.HONOR_CONFIG.get(thread.guild.id, {}).get("event_honor", {})
        if event_cfg.get("enabled") and thread.parent.id in event_cfg.get("target_forum_ids", []):
            # 使用帖子的创建时间而不是当前时间，以确保回填的准确性
            thread_creation_time_utc = thread.created_at
            tz = ZoneInfo("Asia/Shanghai")
            thread_creation_time_local = thread_creation_time_utc.astimezone(tz)

            start_time = datetime.datetime.fromisoformat(event_cfg["start_time"]).replace(tzinfo=tz)
            end_time = datetime.datetime.fromisoformat(event_cfg["end_time"]).replace(tzinfo=tz)

            if start_time <= thread_creation_time_local <= end_time:
                honor_uuid_to_grant = event_cfg.get("honor_uuid")
                if honor_uuid_to_grant:
                    granted_honor_def = self.honor_data_manager.grant_honor(author.id, honor_uuid_to_grant)
                    if granted_honor_def:
                        self.logger.info(f"[活动荣誉] 用户 {author} ({author.id}) 因帖子 T:{thread.id} 获得了荣誉 '{granted_honor_def.name}'")

        # 2. 处理高级里程碑荣誉
        milestone_cfg = config_data.HONOR_CONFIG.get(thread.guild.id, {}).get("milestone_honor", {})
        if milestone_cfg.get("enabled") and thread.parent.id in milestone_cfg.get("target_forum_ids", []):
            # a. 记录帖子 (如果不存在)
            self.activity_data_manager.add_tracked_post(thread.id, author.id, thread.parent.id)

            # b. 检查里程碑
            post_count = self.activity_data_manager.get_user_post_count(author.id)
            milestones = milestone_cfg.get("milestones", {})

            # 倒序检查
            for count_req_str, honor_uuid in sorted(milestones.items(), key=lambda item: int(item[0]), reverse=True):
                count_req = int(count_req_str)
                if post_count >= count_req:
                    granted_honor_def = self.honor_data_manager.grant_honor(author.id, honor_uuid)
                    if granted_honor_def:
                        self.logger.info(f"[里程碑荣誉] 用户 {author} ({author.id}) 发帖数达到 {count_req}，获得了荣誉 '{granted_honor_def.name}'")
                    # 找到第一个达成的里程碑并授予后就停止
                    break

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        """监听新帖子创建事件，实时授予荣誉"""
        await self._process_thread_for_honor(thread)

    post_group = app_commands.Group(
        name="荣誉头衔丨发帖头衔",
        description="管理发帖头衔",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    # --- 历史荣誉回填功能 ---
    @post_group.command(name="回填荣誉", description="扫描论坛历史帖子并根据当前规则补发荣誉。")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_roles=True)
    async def rescan_honors(self, interaction: discord.Interaction):
        """扫描历史帖子以补发荣誉，并提供进度。"""
        await interaction.response.defer(ephemeral=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 并发控制：如果已有任务在运行，取消它
        if guild.id in self.running_backfill_tasks:
            old_task = self.running_backfill_tasks[guild.id]
            if not old_task.done():
                self.logger.warning(f"服务器 {guild.name} 请求新的回填任务，正在取消旧任务...")
                old_task.cancel()
                try:
                    await old_task
                except asyncio.CancelledError:
                    pass  # 预料之中的取消
                await interaction.edit_original_response(content="⚠️ 已取消上一个正在进行的回填任务，即将开始新的任务...")
                await asyncio.sleep(2)  # 给用户一点反应时间

        # 创建并注册新任务
        await interaction.edit_original_response(content="回填任务已开始。")
        task = self.bot.loop.create_task(self._backfill_honor_task(interaction.channel, guild))
        self.running_backfill_tasks[guild.id] = task

    async def _backfill_honor_task(self, target_channel: discord.abc.Messageable, guild: discord.Guild):
        """【核心执行器】负责回填历史荣誉，是回填命令的唯一入口。"""
        start_time = time.time()
        progress_message: typing.Optional[discord.Message] = None

        try:
            # 1. 聚合所有目标版块ID
            guild_config = config_data.HONOR_CONFIG.get(guild.id, {})
            event_cfg = guild_config.get("event_honor", {})
            milestone_cfg = guild_config.get("milestone_honor", {})

            target_forum_ids = set()
            if event_cfg.get("enabled"):
                target_forum_ids.update(event_cfg.get("target_forum_ids", []))
            if milestone_cfg.get("enabled"):
                target_forum_ids.update(milestone_cfg.get("target_forum_ids", []))

            if not target_forum_ids:
                await target_channel.send("❌ **任务中止**：在配置中没有找到任何需要扫描的目标论坛版块。")
                return

            # 2. 获取所有帖子
            self.logger.info(f"[{guild.name}] 开始回填荣誉任务。目标版块ID: {target_forum_ids}")
            initial_embed = discord.Embed(title="⏳ 荣誉回填任务初始化中...", description="正在收集中... 请稍候。", color=discord.Color.blue())
            progress_message = await target_channel.send(embed=initial_embed)

            all_threads = []
            for forum_id in target_forum_ids:
                forum = guild.get_channel(forum_id) or await guild.fetch_channel(forum_id)
                if not isinstance(forum, discord.ForumChannel):
                    self.logger.warning(f"[{guild.name}] 配置的ID {forum_id} 不是一个有效的论坛版块，已跳过。")
                    continue

                forum = typing.cast(discord.ForumChannel, forum)

                # 获取活跃帖子
                all_threads.extend(forum.threads)
                # 获取归档帖子
                try:
                    async for thread in forum.archived_threads(limit=None):
                        all_threads.append(thread)
                except discord.Forbidden:
                    self.logger.error(f"无法获取版块 '{forum.name}' 的归档帖子，权限不足。")

            total_threads = len(all_threads)
            self.logger.info(f"[{guild.name}] 共找到 {total_threads} 个帖子需要处理。")

            # 3. 循环处理并更新进度
            processed_count = 0
            last_update_time = time.time()

            for thread in all_threads:
                try:
                    await self._process_thread_for_honor(thread)
                except Exception as e:
                    self.logger.error(f"处理帖子 T:{thread.id} 时发生错误: {e}", exc_info=True)

                processed_count += 1

                # 每5秒或处理了20个帖子后更新一次进度，避免过于频繁的API调用
                if time.time() - last_update_time > 5 or processed_count % 20 == 0:
                    progress_embed = self._create_backfill_progress_embed(
                        guild, start_time, total_threads, processed_count, thread.parent.name
                    )
                    await progress_message.edit(embed=progress_embed)
                    last_update_time = time.time()
                    await asyncio.sleep(0.1)  # 短暂让步，避免速率限制

            # 4. 发送最终报告
            duration = time.time() - start_time
            final_embed = self._create_backfill_final_embed(guild, duration, total_threads)
            await progress_message.edit(embed=final_embed)
            self.logger.info(f"[{guild.name}] 荣誉回填任务完成。耗时 {duration:.2f} 秒，处理了 {total_threads} 个帖子。")

        except asyncio.CancelledError:
            self.logger.warning(f"[{guild.name}] 回填任务被手动取消。")
            if progress_message:
                await progress_message.edit(content="🛑 **任务已取消**。", embed=None, view=None)
        except Exception as e:
            self.logger.critical(f"[{guild.name}] 回填任务发生严重错误: {e}", exc_info=True)
            if progress_message:
                error_embed = discord.Embed(
                    title="❌ 任务异常中断",
                    description=f"在执行过程中发生严重错误，任务已停止。\n```\n{e}\n```",
                    color=discord.Color.red()
                )
                await progress_message.edit(embed=error_embed)
        finally:
            # 任务结束（无论成功、失败或取消），都从字典中移除
            _ = self.running_backfill_tasks.pop(guild.id, None)

    @staticmethod
    def _create_backfill_progress_embed(guild: discord.Guild, start_time: float, total: int, current: int, current_forum: str) -> discord.Embed:
        """创建进度更新的 Embed"""
        progress = current / total if total > 0 else 0
        bar_length = 20
        filled_length = int(bar_length * progress)
        bar = '█' * filled_length + '─' * (bar_length - filled_length)

        elapsed_time = time.time() - start_time

        embed = discord.Embed(
            title=f"⚙️ 正在回填 {guild.name} 的荣誉...",
            description=f"进度: **{current} / {total}** ({progress:.1%})\n`{bar}`",
            color=discord.Color.gold()
        )
        embed.add_field(name="当前扫描版块", value=f"#{current_forum}", inline=True)
        embed.add_field(name="已用时", value=f"{int(elapsed_time)} 秒", inline=True)
        embed.set_footer(text="正在扫描所有历史帖子，这可能需要一些时间...")
        return embed

    @staticmethod
    def _create_backfill_final_embed(guild: discord.Guild, duration: float, total_processed: int) -> discord.Embed:
        """创建任务完成的 Embed"""
        embed = discord.Embed(
            title=f"✅ {guild.name} 荣誉回填完成",
            description="已根据最新规则扫描所有相关历史帖子，并补发了应得的荣誉。",
            color=discord.Color.green()
        )
        embed.add_field(name="总处理帖子数", value=str(total_processed), inline=True)
        embed.add_field(name="总耗时", value=f"{duration:.2f} 秒", inline=True)
        embed.set_footer(text="现在用户的荣誉数据已是最新状态。")
        return embed

async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(HonorPostModuleCog(bot))