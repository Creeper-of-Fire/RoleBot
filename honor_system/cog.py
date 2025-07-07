# honor_system/cog.py
from __future__ import annotations

import asyncio
import datetime
import time
import typing
from zoneinfo import ZoneInfo

import discord
from discord import app_commands, ui
from discord.ext import commands

import config_data
from utility.feature_cog import FeatureCog
from .data_manager import HonorDataManager
from .models import HonorDefinition

if typing.TYPE_CHECKING:
    from main import RoleBot


# --- 视图定义 (无变动) ---
class HonorManageView(ui.View):
    def __init__(self, cog: 'HonorCog', member: discord.Member, guild: discord.Guild):
        super().__init__(timeout=180)
        self.cog = cog
        self.member = member
        self.guild = guild
        self.message: typing.Optional[discord.Message] = None
        self.build_view()

    def build_view(self):
        self.clear_items()
        user_honors_earned = self.cog.data_manager.get_user_honors(self.member.id)
        if not user_honors_earned:
            return

        member_role_ids = {role.id for role in self.member.roles}
        options = []
        for uh_instance in user_honors_earned:
            honor_def = uh_instance.definition
            if honor_def.role_id is None:
                continue

            is_equipped_now = honor_def.role_id in member_role_ids
            equip_emoji = "✅" if is_equipped_now else "🔘"

            options.append(discord.SelectOption(
                label=f"{equip_emoji} {honor_def.name}",
                description=honor_def.description[:80],
                value=honor_def.uuid
            ))

        if not options:
            return

        honor_select = ui.Select(
            placeholder="选择一个荣誉来佩戴或卸下身份组...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="honor_select"
        )
        honor_select.callback = self.on_honor_select
        self.add_item(honor_select)

    async def on_honor_select(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_honor_uuid = interaction.data["values"][0]

        selected_honor_def = next(
            (hd for hd in self.cog.data_manager.get_all_honor_definitions(self.guild.id)
             if hd.uuid == selected_honor_uuid),
            None
        )

        if not selected_honor_def or selected_honor_def.role_id is None:
            await interaction.followup.send("❌ 选择的荣誉无效或未关联身份组。", ephemeral=True)
            await self.update_display(interaction)
            return

        role_id_int: int = typing.cast(int, selected_honor_def.role_id)
        target_role = self.guild.get_role(role_id_int)
        if not target_role:
            await interaction.followup.send(f"⚠️ 荣誉 **{selected_honor_def.name}** 关联的身份组(ID:{selected_honor_def.role_id})已不存在。", ephemeral=True)
            await self.update_display(interaction)
            return

        member_has_role = target_role in self.member.roles
        try:
            if member_has_role:
                await self.member.remove_roles(target_role, reason=f"用户卸下荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"☑️ 已卸下荣誉 **{selected_honor_def.name}** 并移除身份组。", ephemeral=True)
            else:
                await self.member.add_roles(target_role, reason=f"用户佩戴荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"✅ 已佩戴荣誉 **{selected_honor_def.name}** 并获得身份组！", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ 操作失败！我没有足够的权限来为你添加/移除身份组。请确保我的角色高于此荣誉的身份组。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"佩戴/卸下荣誉身份组时发生错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)

        self.member = await self.guild.fetch_member(self.member.id)
        await self.update_display(interaction)

    async def update_display(self, interaction: discord.Interaction):
        self.build_view()
        embed = self.cog.create_honor_embed(self.member, self.guild)
        await interaction.edit_original_response(embed=embed, view=self)

    async def on_timeout(self):
        if self.message:
            for item in self.children:
                item.disabled = True
            await self.message.edit(content="*这个荣誉面板已超时，请重新使用 `/荣誉面板` 命令。*", view=self)


# --- 主Cog ---
class HonorCog(FeatureCog, name="Honor"):
    """管理荣誉系统"""

    def __init__(self, bot: RoleBot):
        super().__init__(bot)  # 调用父类 (FeatureCog) 的构造函数
        self.data_manager = HonorDataManager()
        self.running_backfill_tasks: typing.Dict[int, asyncio.Task] = {}
        # 安全缓存，用于存储此模块管理的所有身份组ID
        self.safe_honor_role_ids: set[int] = set()

        self.bot.loop.create_task(self.synchronize_all_honor_definitions())

    # --- FeatureCog 接口实现 ---

    async def update_safe_roles_cache(self):
        """
        [接口实现] 从荣誉定义中更新此模块管理的安全身份组缓存。
        """
        self.logger.info(f"模块 '{self.qualified_name}' 开始更新安全身份组缓存...")

        new_cache = set()

        # 从数据库中获取所有荣誉定义
        all_honor_defs = []
        with self.data_manager.get_db() as db:
            all_honor_defs = db.query(HonorDefinition).filter(HonorDefinition.is_archived == False).all()

        if not all_honor_defs:
            self.logger.info(f"模块 '{self.qualified_name}' 没有找到任何荣誉定义。")
            self.safe_honor_role_ids = new_cache
            return

        for honor_def in all_honor_defs:
            if honor_def.role_id:
                new_cache.add(honor_def.role_id)

        self.safe_honor_role_ids = new_cache
        self.logger.info(f"模块 '{self.qualified_name}' 安全缓存更新完毕，共加载 {len(self.safe_honor_role_ids)} 个身份组。")

    def get_main_panel_buttons(self) -> typing.Optional[typing.List[discord.ui.Button]]:
        """
        [接口实现] 返回一个用于主面板的 "我的荣誉墙" 按钮。
        """

        async def honor_panel_callback(interaction: discord.Interaction):
            # 这是原 /荣誉面板 命令的所有逻辑
            await interaction.response.defer(ephemeral=True)
            member = typing.cast(discord.Member, interaction.user)
            guild = typing.cast(discord.Guild, interaction.guild)

            embed = self.create_honor_embed(member, guild)
            view = HonorManageView(self, member, guild)

            # 使用 followup 发送，因为已经 defer
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message

        honor_button = ui.Button(
            label="我的荣誉墙（临时测试）",
            style=discord.ButtonStyle.secondary,
            emoji="🏆",
            custom_id="honor_cog:show_honor_panel"
        )
        honor_button.callback = honor_panel_callback

        return [honor_button]

    async def synchronize_all_honor_definitions(self):
        await self.bot.wait_until_ready()
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")
        all_config_uuids = set()
        for guild_id, guild_config in config_data.HONOR_CONFIG.items():
            for honor_def in guild_config.get("definitions", []):
                all_config_uuids.add(honor_def['uuid'])
        with self.data_manager.get_db() as db:
            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                for config_def in guild_config.get("definitions", []):
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_def['uuid']).one_or_none()
                    if db_def:
                        db_def.name = config_def['name']
                        db_def.description = config_def['description']
                        db_def.role_id = config_def.get('role_id')
                        db_def.icon_url = config_def.get('icon_url')
                        db_def.guild_id = guild_id
                        db_def.is_archived = False
                    else:
                        new_def = HonorDefinition(
                            uuid=config_def['uuid'], guild_id=guild_id, name=config_def['name'],
                            description=config_def['description'], role_id=config_def.get('role_id'),
                            icon_url=config_def.get('icon_url'),
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_def['name']}")
            db_uuids_to_check = db.query(HonorDefinition.uuid).filter(HonorDefinition.is_archived == False).all()
            db_uuids_set = {uuid_tuple[0] for uuid_tuple in db_uuids_to_check}
            uuids_to_archive = db_uuids_set - all_config_uuids
            if uuids_to_archive:
                self.logger.warning(f"发现 {len(uuids_to_archive)} 个需要归档的荣誉...")
                db.query(HonorDefinition).filter(HonorDefinition.uuid.in_(uuids_to_archive)).update({"is_archived": True})
            db.commit()
        self.logger.info("HonorCog: 荣誉定义同步完成。")

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
                    granted_honor_def = self.data_manager.grant_honor(author.id, honor_uuid_to_grant)
                    if granted_honor_def:
                        self.logger.info(f"[活动荣誉] 用户 {author} ({author.id}) 因帖子 T:{thread.id} 获得了荣誉 '{granted_honor_def.name}'")

        # 2. 处理高级里程碑荣誉
        milestone_cfg = config_data.HONOR_CONFIG.get(thread.guild.id, {}).get("milestone_honor", {})
        if milestone_cfg.get("enabled") and thread.parent.id in milestone_cfg.get("target_forum_ids", []):
            # a. 记录帖子 (如果不存在)
            self.data_manager.add_tracked_post(thread.id, author.id, thread.parent.id)

            # b. 检查里程碑
            post_count = self.data_manager.get_user_post_count(author.id)
            milestones = milestone_cfg.get("milestones", {})

            # 倒序检查
            for count_req_str, honor_uuid in sorted(milestones.items(), key=lambda item: int(item[0]), reverse=True):
                count_req = int(count_req_str)
                if post_count >= count_req:
                    granted_honor_def = self.data_manager.grant_honor(author.id, honor_uuid)
                    if granted_honor_def:
                        self.logger.info(f"[里程碑荣誉] 用户 {author} ({author.id}) 发帖数达到 {count_req}，获得了荣誉 '{granted_honor_def.name}'")
                    # 找到第一个达成的里程碑并授予后就停止
                    break

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        """监听新帖子创建事件，实时授予荣誉"""
        await self._process_thread_for_honor(thread)

    # --- 荣誉展示与管理 ---
    def create_honor_embed(self, member: discord.Member, guild: discord.Guild) -> discord.Embed:
        all_definitions = self.data_manager.get_all_honor_definitions(guild.id)
        user_honor_instances = self.data_manager.get_user_honors(member.id)
        member_role_ids = {role.id for role in member.roles}
        owned_honor_definitions_map = {uh.honor_uuid: uh.definition for uh in user_honor_instances}

        equipped_honors_lines, unequipped_owned_honors_lines = [], []
        pure_achievement_honors_lines, unearned_honors_lines = [], []

        for definition in all_definitions:
            honor_line_text = f"**{definition.name}**\n*└ {definition.description}*"
            if definition.role_id is not None:
                honor_line_text = f"<@&{definition.role_id}>\n*└ {definition.description}*"

            if definition.uuid in owned_honor_definitions_map:
                if definition.role_id is not None:
                    if definition.role_id in member_role_ids:
                        equipped_honors_lines.append(honor_line_text)
                    else:
                        unequipped_owned_honors_lines.append(honor_line_text)
                else:
                    pure_achievement_honors_lines.append(honor_line_text)
            else:
                unearned_honors_lines.append(honor_line_text)

        embed = discord.Embed(title=f"{member.display_name}的荣誉墙", color=member.color)
        if member.display_avatar:
            embed.set_thumbnail(url=member.display_avatar.url)

        if not user_honor_instances and not all_definitions:
            embed.description = "目前没有可用的荣誉定义。请联系管理员添加。"
        elif not user_honor_instances:
            embed.description = "你还没有获得任何荣誉哦！查看下方待解锁荣誉，多多参与社区活动吧！"
        elif all_definitions and len(user_honor_instances) == len(all_definitions) and not unearned_honors_lines:
            embed.description = "🎉 你已经解锁了所有可用的荣誉！恭喜你！"
        else:
            embed.description = "你已获得部分荣誉。请查看下方已佩戴、未佩戴的荣誉，或探索待解锁的更多荣誉。"

        if equipped_honors_lines:
            embed.add_field(name="✅ 已佩戴荣誉", value="\n\n".join(equipped_honors_lines), inline=False)
        if unequipped_owned_honors_lines:
            embed.add_field(name="☑️ 未佩戴荣誉 (可佩戴身份组)", value="\n\n".join(unequipped_owned_honors_lines), inline=False)
        if pure_achievement_honors_lines:
            embed.add_field(name="✨ 纯粹成就荣誉 (无身份组)", value="\n\n".join(pure_achievement_honors_lines), inline=False)
        if unearned_honors_lines:
            embed.add_field(name="💡 待解锁荣誉", value="\n\n".join(unearned_honors_lines), inline=False)

        embed.set_footer(text="佩戴/卸下荣誉需使用下方的下拉选择器进行操作。")
        return embed

    # --- 新增：历史荣誉回填功能 ---
    @app_commands.command(name="回填荣誉", description="扫描论坛历史帖子并根据当前规则补发荣誉。")
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
        progress_message = None

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


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
