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


class GenericHierarchicalPaginationView(ui.View):
    """
    【新】一个通用的、可复用的层级分页视图。
    接收已经完全处理和排序好的数据，只负责分页和展示。
    """

    def __init__(self, interaction: discord.Interaction, embed_template: discord.Embed,
                 sorted_display_data: list[tuple[discord.abc.GuildChannel, int]],
                 field_name: str, value_suffix: str):
        super().__init__(timeout=300)
        self.interaction = interaction
        self.embed_template = embed_template
        self.sorted_display_data = sorted_display_data
        self.field_name = field_name
        self.value_suffix = value_suffix

        self.current_page = 0
        self.channels_per_page = MAX_CHANNELS_PER_PAGE
        self.total_pages = (len(self.sorted_display_data) + self.channels_per_page - 1) // self.channels_per_page
        if self.total_pages == 0:
            self.total_pages = 1

        self.message: typing.Optional[discord.Message] = None
        self._update_buttons()

    def _update_buttons(self):
        """根据当前页更新按钮状态。"""
        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1
        if self.total_pages <= 1:
            self.previous_page.style = discord.ButtonStyle.secondary
            self.next_page.style = discord.ButtonStyle.secondary
            self.previous_page.disabled = True
            self.next_page.disabled = True

    def _create_page_embed(self) -> discord.Embed:
        """生成当前页的 Embed。"""
        # 从模板克隆一个新的 embed，避免修改原始模板
        embed = self.embed_template.copy()

        # 移除可能存在的旧分页字段，以便重新生成
        # 倒序遍历以安全地移除元素
        for i in range(len(embed.fields) - 1, -1, -1):
            if embed.fields[i].name and embed.fields[i].name.startswith(self.field_name):
                embed.remove_field(i)

        start_index = self.current_page * self.channels_per_page
        end_index = min(start_index + self.channels_per_page, len(self.sorted_display_data))
        channels_on_page = self.sorted_display_data[start_index:end_index]

        if channels_on_page:
            channel_list_text = []
            for channel, count in channels_on_page:
                if isinstance(channel, discord.Thread):
                    parent_name = f"({channel.parent.name})" if channel.parent else ""
                    # 使用一个细微的缩进来表示层级
                    channel_list_text.append(f"  └ {channel.mention} {parent_name}: `{count}` {self.value_suffix}")
                else:
                    channel_list_text.append(f"**{channel.mention}**: `{count}` {self.value_suffix}")

            field_title = f"{self.field_name} (第 {self.current_page + 1}/{self.total_pages} 页)"
            embed.add_field(name=field_title, value="\n".join(channel_list_text), inline=False)
        elif self.current_page == 0:  # 只有在第一页且没有数据时才显示这个
            embed.add_field(name=self.field_name, value="没有找到任何符合条件的记录。", inline=False)

        return embed

    async def start(self):
        """发送初始消息并启动视图。"""
        embed = self._create_page_embed()
        self.message = await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)

    @ui.button(label="上一页", style=discord.ButtonStyle.primary)
    async def previous_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.edit_original_response(embed=self._create_page_embed(), view=self)

    @ui.button(label="下一页", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.edit_original_response(embed=self._create_page_embed(), view=self)

    async def on_timeout(self):
        self.previous_page.disabled = True
        self.next_page.disabled = True
        try:
            if self.message:
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
        【已重构】使用新的通用分页视图。
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

        # 1. 获取原始数据
        total_messages, channel_data = await self.cog._get_user_activity_summary(
            guild, member.id, days_window, guild_cfg
        )
        heatmap_data = await self.cog._generate_heatmap_data(
            guild, member.id, days_window
        )

        # 2. 【新】调用通用方法处理和排序数据
        sorted_display_data = await self.cog._process_and_sort_activity_data(guild, channel_data)

        # 3. 创建 Embed 模板
        embed_template = discord.Embed(
            title=f"📊 {member.display_name} 的活跃度报告",
            description=f"这是你在过去 **{days_window}** 天内的活跃概览。",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed_template.add_field(name="总消息数", value=f"`{total_messages}` 条", inline=False)
        heatmap_text = self.cog._render_heatmap_text(heatmap_data, days_window)
        if heatmap_text:
            embed_template.add_field(name="近况热力图 (消息数/天)", value=heatmap_text, inline=False)
        embed_template.set_footer(text=f"数据统计时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")

        # 4. 实例化并启动新的通用视图
        view = GenericHierarchicalPaginationView(
            interaction=interaction,
            embed_template=embed_template,
            sorted_display_data=sorted_display_data,
            field_name="分频道消息数",
            value_suffix="条"
        )
        await view.start()

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

        # --- 【已修改】使用内存锁替代 Redis 持久化锁 ---
        self._backfill_locks: set[int] = set()

        # --- 【已修改】启动任务状态标志，用于解决时间戳竞争问题 ---
        self._startup_sync_complete = False

        # --- 用于 on_message 时间戳更新的节流控制 ---
        self._last_timestamp_update: typing.Dict[int, float] = {}
        self.TIMESTAMP_UPDATE_INTERVAL = 60

    # --- Cog 生命周期方法 ---
    async def cog_load(self):
        """Cog 加载时执行的操作"""
        self.logger.info(f"Cog '{self.qualified_name}' 加载完成。")
        self.bot.add_view(ActivityRoleView(self))

    # --- 使用 Cog 内部的 on_ready 监听器来处理启动任务 ---
    @commands.Cog.listener()
    async def on_ready(self):
        """
        当 bot 准备就绪时，执行一次性的启动任务。
        【已重构】现在会基于持久化的时间戳来执行增量同步。
        """
        # 等待内部缓存完全加载
        await self.bot.wait_until_ready()

        if not await self.data_manager.check_connection():
            self.logger.error("Redis 连接失败，活跃度追踪模块将无法正常工作。")
            self.cog_check = lambda ctx: False
            return

        # self._startup_sync_complete 确保这个逻辑只在机器人生命周期中运行一次
        if not self._startup_sync_complete:
            self.logger.info("检测到首次启动，准备执行增量同步任务...")
            # 使用 create_task 在后台运行，不阻塞 on_ready
            self.bot.loop.create_task(self._incremental_sync_on_startup())

    async def _incremental_sync_on_startup(self):
        """
        【已重构和修正】在机器人启动时，为每个配置的服务器执行增量数据同步。
        该方法会读取最后同步时间戳，并回填从该时间点到现在的缺失数据。
        所有的进度和结果都会发送到配置文件中指定的报告频道。
        """
        try:
            # 遍历在 config.py 中配置的所有 guild_configs
            for guild_id, guild_cfg in self.config.get("guild_configs", {}).items():
                if not guild_cfg.get("enabled", True):
                    self.logger.info(f"[Guild {guild_id}] 活动追踪功能未启用，跳过启动时同步。")
                    continue

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    self.logger.error(f"无法找到服务器 {guild_id}，跳过该服务器的增量同步。")
                    continue

                report_channel_id = guild_cfg.get("report_channel_id")
                report_channel = None
                if report_channel_id:
                    report_channel = guild.get_channel(report_channel_id)
                    if not report_channel or not isinstance(report_channel, discord.TextChannel):
                        self.logger.error(f"服务器 {guild.name} 的报告频道 {report_channel_id} 无效。无法发送通知。")
                        report_channel = None
                else:
                    self.logger.warning(f"服务器 {guild.name} 未配置 'report_channel_id'，将无法发送启动同步通知。")

                # --- 【已修改】检查内存回填锁 ---
                if guild.id in self._backfill_locks:
                    self.logger.warning(f"服务器 {guild.name} 检测到内存回填锁，本次启动时增量同步任务已跳过。")
                    if report_channel:
                        await report_channel.send(f"⚠️ **启动同步跳过！**\n检测到服务器当前有另一个回填任务正在进行，本次自动增量同步已取消。")
                    continue

                last_sync_ts = await self.data_manager.get_last_sync_timestamp(guild.id)
                now_utc = datetime.now(timezone.utc)

                if last_sync_ts is None:
                    self.logger.warning(
                        f"服务器 {guild.name} 没有找到最后同步时间戳。将设置当前时间为初始时间戳。"
                    )
                    if report_channel:
                        await report_channel.send(
                            f"👋 **首次启动初始化**\n"
                            f"为了获取历史数据，请管理员使用 `/用户活跃度 手动拉取历史消息` 指令进行一次初始回填。"
                        )
                    await self.data_manager.set_last_sync_timestamp(guild.id, now_utc.timestamp())
                    continue

                start_datetime = datetime.fromtimestamp(last_sync_ts, tz=timezone.utc)
                if (now_utc - start_datetime).total_seconds() < 60:
                    self.logger.info(f"服务器 {guild.name} 离线时间很短，无需执行增量同步。")
                    continue

                self.logger.info(f"为服务器 {guild.name} 执行增量同步，范围: {start_datetime.isoformat()} -> {now_utc.isoformat()}")
                if report_channel:
                    start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    end_display = now_utc.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
                    await report_channel.send(
                        f"🤖 **自动增量同步启动！**\n"
                        f"开始补全从 `{start_display}` 到 `{end_display}` (UTC+8) 的历史消息。"
                    )

                await self._backfill_guild_history(
                    guild=guild,
                    target_channel=report_channel,
                    start_datetime=start_datetime,
                    end_datetime=now_utc,
                    single_channel=None
                )
                await asyncio.sleep(1)
        finally:
            # --- 【已修改】无论成功或失败，都在最后标记启动任务完成 ---
            self.logger.info("所有服务器的启动时增量同步流程已结束。")
            self._startup_sync_complete = True

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        实时记录用户发送的每一条消息，并节流更新“最后同步时间戳”。
        """
        if message.author.bot or not message.guild:
            return

        guild_cfg = self.config.get("guild_configs", {}).get(message.guild.id)
        if not guild_cfg or not guild_cfg.get("enabled", True):
            return

        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))
        if message.channel.id in ignored_channels: return
        category_id_to_check = message.channel.parent.category_id if isinstance(message.channel,
                                                                                discord.Thread) and message.channel.parent else message.channel.category_id
        if category_id_to_check in ignored_categories: return

        retention_days = guild_cfg.get("data_retention_days", 90)
        message_ts = message.created_at.timestamp()
        await self.data_manager.record_message(
            guild_id=message.guild.id,
            channel_id=message.channel.id,
            user_id=message.author.id,
            message_id=message.id,
            created_at_timestamp=message_ts,
            retention_days=retention_days
        )

        # --- 【已修改】节流更新最后同步时间戳的逻辑 ---
        # 1. 启动任务完成前，绝不更新时间戳
        if not self._startup_sync_complete:
            return
        # 2. 如果当前服务器正在回填，也绝不更新时间戳
        if message.guild.id in self._backfill_locks:
            return

        now = time.time()
        last_update = self._last_timestamp_update.get(message.guild.id, 0)

        if now - last_update > self.TIMESTAMP_UPDATE_INTERVAL:
            await self.data_manager.set_last_sync_timestamp(message.guild.id, message_ts)
            self._last_timestamp_update[message.guild.id] = now

    async def _process_and_sort_activity_data(
            self,
            guild: discord.Guild,
            activity_data: list[tuple[int, int]]
    ) -> list[tuple[discord.abc.GuildChannel, int]]:
        """
        【新】通用的数据处理和层级排序辅助方法。
        接收频道ID和计数的元组列表，返回按父子频道层级排序的频道对象和计数的列表。
        【已重构】以正确处理子频道并按聚合活跃度排序。
        """
        if not activity_data:
            return []

        # 1. 为所有活动频道及其父频道建立缓存
        all_channel_ids = {cid for _, _ in activity_data}
        channel_cache = await self._build_channel_cache(guild, all_channel_ids)

        parent_ids_to_fetch = set()
        for channel in channel_cache.values():
            if channel and isinstance(channel, discord.Thread) and channel.parent_id:
                if channel.parent_id not in channel_cache:
                    parent_ids_to_fetch.add(channel.parent_id)
        if parent_ids_to_fetch:
            parent_cache = await self._build_channel_cache(guild, parent_ids_to_fetch)
            channel_cache.update(parent_cache)

        # 2. 将数据按层级分组
        top_level_activity = {}
        threads_by_parent = collections.defaultdict(list)

        for channel_id, count in activity_data:
            channel = channel_cache.get(channel_id)
            if not channel:
                continue

            if isinstance(channel, discord.Thread) and channel.parent:
                threads_by_parent[channel.parent_id].append((channel, count))
            else:
                top_level_activity[channel] = count

        # 3. 计算用于排序的聚合计数 (自身消息 + 子频道消息总和)
        all_top_level_channels = {}

        # 添加有直接活动的频道
        for channel, count in top_level_activity.items():
            all_top_level_channels[channel] = all_top_level_channels.get(channel, 0) + count

        # 添加子频道的计数到其父频道
        for parent_id, children in threads_by_parent.items():
            parent_channel = channel_cache.get(parent_id)
            if parent_channel:
                children_total_count = sum(c for _, c in children)
                all_top_level_channels[parent_channel] = all_top_level_channels.get(parent_channel, 0) + children_total_count

        # 4. 按聚合计数对顶级频道进行排序
        sorted_parents = sorted(
            all_top_level_channels.items(),
            key=lambda item: item[1],
            reverse=True
        )

        # 5. 构建最终的、扁平化的、有序的显示列表
        final_sorted_list = []
        for parent_obj, _ in sorted_parents:
            # 如果父频道本身有消息，则将其添加到列表中
            if parent_obj in top_level_activity:
                final_sorted_list.append((parent_obj, top_level_activity[parent_obj]))

            # 添加该父频道下的所有子频道（已按其自身计数排序）
            if parent_obj.id in threads_by_parent:
                sorted_threads = sorted(threads_by_parent[parent_obj.id], key=lambda item: item[1], reverse=True)
                final_sorted_list.extend(sorted_threads)

        return final_sorted_list

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
        【已性能优化】获取用户在指定天数窗口内的总消息数和分频道消息数。
        使用批量频道缓存避免循环内API调用。
        """
        # 1. 从 DataManager 获取原始数据 (非常快)
        raw_channel_counts = await self.data_manager.get_user_activity_summary(
            guild_id=guild.id,
            user_id=user_id,
            days_window=days_window
        )
        if not raw_channel_counts:
            return 0, []

        # 2. 【新】构建批量频道缓存
        all_channel_ids = {channel_id for channel_id, count in raw_channel_counts}
        channel_cache = await self._build_channel_cache(guild, all_channel_ids)

        # 3. 在内存中高效处理和过滤
        total_message_count = 0
        filtered_channel_counts: list[tuple[int, int]] = []
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        for channel_id, count in raw_channel_counts:
            channel_obj = channel_cache.get(channel_id)
            if not channel_obj:
                # 无法获取频道对象，跳过
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

            filtered_channel_counts.append((channel_id, count))
            total_message_count += count

        filtered_channel_counts.sort(key=lambda x: x[1], reverse=True)
        return total_message_count, filtered_channel_counts

    # --- 辅助方法：生成热力图数据 ---
    async def _generate_heatmap_data(self, guild: discord.Guild, user_id: int, days_window: int) -> dict[str, int]:
        """
        【已性能优化】获取用户在指定天数窗口内每天的消息数，用于热力图。
        使用批量频道缓存避免循环内API调用。
        """
        # 1. 从 DataManager 获取原始数据 (非常快)
        raw_messages_data = await self.data_manager.get_heatmap_data(
            guild_id=guild.id,
            user_id=user_id,
            days_window=days_window
        )
        if not raw_messages_data:
            return {}

        # 2. 【新】构建批量频道缓存
        all_channel_ids = {channel_id for channel_id, timestamp in raw_messages_data}
        channel_cache = await self._build_channel_cache(guild, all_channel_ids)

        # 3. 在内存中高效处理和过滤
        heatmap_counts = collections.defaultdict(int)
        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        for channel_id, timestamp in raw_messages_data:
            channel_obj = channel_cache.get(channel_id)
            if not channel_obj:
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
        app_commands.Choice(name="【危险】清除本服所有活动数据", value="clear_guild_data"),
        app_commands.Choice(name="【一次性】为旧数据重建索引", value="rebuild_indexes")
    ])
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_activity_data(self, interaction: discord.Interaction, action: str):
        guild = interaction.guild

        if action == "force_unlock":
            # --- 【已修改】使用内存锁 ---
            if guild.id not in self._backfill_locks:
                await interaction.response.send_message("ℹ️ 本服务器的回填任务当前未被锁定，无需解锁。", ephemeral=True)
                return

            self._backfill_locks.remove(guild.id)
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

        # --- 【新】处理索引重建的逻辑 ---
        elif action == "rebuild_indexes":
            # --- 【已修改】使用内存锁 ---
            if guild.id in self._backfill_locks:
                await interaction.response.send_message("❌ 此服务器上有一个回填任务正在运行，请等待其完成后再重建索引。", ephemeral=True)
                return

            view = ConfirmationView(author=interaction.user)
            await interaction.response.send_message(
                "⚠️ **注意！** 您将要为本服务器的所有历史活动数据重建索引。\n\n"
                "这是一个**高负载、耗时较长**的操作，期间会扫描所有相关的 Redis 键。\n"
                "仅在从旧版数据结构迁移后，或怀疑索引不完整时执行此操作。\n\n"
                "**确定要开始吗？**",
                view=view,
                ephemeral=True
            )

            await view.wait()

            if view.value:
                # --- 【已修改】使用内存锁 ---
                self._backfill_locks.add(guild.id)
                self.logger.warning(f"用户 {interaction.user} 启动了服务器 {guild.name} 的索引重建任务。")

                await interaction.edit_original_response(
                    content="✅ **索引重建任务已启动！**\n我正在后台扫描数据并建立索引，完成后会在此处通知您。",
                    view=None
                )

                start_time = time.time()
                try:
                    scanned_keys, created_indexes = await self.data_manager.rebuild_indexes_for_guild(guild.id)
                    duration = time.time() - start_time
                    self.logger.info(f"服务器 {guild.id} 索引重建成功，耗时 {duration:.2f} 秒。")
                    await interaction.followup.send(
                        (
                            f"🎉 **索引重建完成！**\n"
                            f"**服务器:** `{guild.name}`\n"
                            f"**总耗时:** `{duration:.2f}` 秒\n"
                            f"**扫描的活动数据键:** `{scanned_keys}`\n"
                            f"**创建的新索引条目:** `{created_indexes}`"
                        ),
                        ephemeral=False
                    )
                except Exception as e:
                    self.logger.critical(f"为服务器 {guild.id} 重建索引时发生严重错误: {e}", exc_info=True)
                    await interaction.followup.send(f"❌ **索引重建失败！**\n发生严重错误: `{e}`", ephemeral=False)
                finally:
                    # --- 【已修改】使用内存锁 ---
                    if guild.id in self._backfill_locks:
                        self._backfill_locks.remove(guild.id)

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

        # --- 【已修改】使用内存锁 ---
        if guild.id in self._backfill_locks:
            await interaction.response.send_message("❌ 此服务器上已经有一个回填任务正在运行。", ephemeral=True)
            return

        # --- 参数解析逻辑 (保持不变) ---
        start_datetime: datetime = now_utc
        end_datetime: datetime = now_utc
        display_range_str = ""

        date_params_provided = (start_date is not None) or (end_date is not None)
        time_ago_params_provided = (hours_ago is not None) or (minutes_ago is not None)

        if date_params_provided and time_ago_params_provided:
            await interaction.response.send_message("❌ **参数冲突！**\n不能同时使用日期和回溯时间。", ephemeral=True)
            return
        if not (date_params_provided or time_ago_params_provided):
            await interaction.response.send_message("❌ **缺少时间范围参数！**\n请指定日期或回溯时间。", ephemeral=True)
            return

        if time_ago_params_provided:
            delta = timedelta()
            if hours_ago is not None and hours_ago > 0:
                delta = timedelta(hours=hours_ago)
            elif minutes_ago is not None and minutes_ago > 0:
                delta = timedelta(minutes=minutes_ago)
            else:
                await interaction.response.send_message("❌ `hours_ago` 或 `minutes_ago` 必须是正整数。", ephemeral=True)
                return
            start_datetime = now_utc - delta
        elif date_params_provided:
            if start_date is None:
                await interaction.response.send_message("❌ 使用日期范围时，`start_date` 是必需的。", ephemeral=True)
                return
            start_datetime = self._parse_flexible_date(start_date)
            if not start_datetime:
                await interaction.response.send_message("❌ **开始日期格式错误！**", ephemeral=True)
                return
            if end_date:
                parsed_end = self._parse_flexible_date(end_date)
                if not parsed_end:
                    await interaction.response.send_message("❌ **结束日期格式错误！**", ephemeral=True)
                    return
                end_datetime = parsed_end + timedelta(days=1, microseconds=-1)
            if start_datetime >= end_datetime:
                await interaction.response.send_message("❌ **错误**：开始日期必须在结束日期之前。", ephemeral=True)
                return

        start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        end_display = end_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
        display_range_str = f"从 **{start_display}** 到 **{end_display}**"

        target_description = f"服务器 **{guild.name}** 的所有可读频道"
        if channel:
            target_description = f"频道 {channel.mention}"

        await interaction.response.send_message(
            f"✅ **历史消息回填任务已启动！**\n\n"
            f"我将开始拉取 {display_range_str} 之间，在 {target_description} 的历史消息。",
            ephemeral=False
        )

        self.bot.loop.create_task(self._backfill_guild_history(
            guild=guild,
            target_channel=interaction.channel,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            single_channel=channel
        ))

    async def _backfill_guild_history(self, guild: discord.Guild,
                                      target_channel: typing.Optional[discord.TextChannel],
                                      start_datetime: datetime, end_datetime: datetime,
                                      single_channel: typing.Optional[typing.Union[discord.TextChannel, discord.Thread, discord.ForumChannel]] = None):
        """
        【核心执行器】负责回填指定时间范围内的历史消息。
        【已修改】使用内存锁。
        """
        # --- 【已修改】使用内存锁 ---
        if guild.id in self._backfill_locks:
            self.logger.warning(f"服务器 '{guild.name}' 尝试启动回填任务，但任务已被锁定，本次请求中止。")
            if target_channel:
                await target_channel.send("⚠️ **任务中止**：服务器上已有另一个回填任务正在运行。")
            return

        try:
            # 1. 任务初始化与锁定
            self._backfill_locks.add(guild.id)
            self.logger.info(f"服务器 '{guild.name}' 开始历史消息回填任务。内存锁已激活。")

            start_time = time.time()
            guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
            channels_to_scan = await self._get_relevant_channels(guild, guild_cfg, target_channel=single_channel)
            total_channels = len(channels_to_scan)

            if total_channels == 0:
                if target_channel:
                    await target_channel.send("⚠️ **任务取消**：没有找到任何可扫描的频道。")
                self.logger.warning(f"服务器 '{guild.name}' 回填任务因找不到可扫描频道而中止。")
                return

            total_messages_processed, total_messages_added, channels_scanned = 0, 0, 0
            last_update_time, progress_message = time.time(), None

            # 2. 核心处理循环
            redis_pipe = self.data_manager.redis.pipeline()
            messages_in_pipe = 0

            for channel in channels_to_scan:
                channels_scanned += 1
                try:
                    if isinstance(channel, discord.ForumChannel): continue
                    async for message in channel.history(limit=None, after=start_datetime, before=end_datetime):
                        if message.author.bot: continue
                        total_messages_processed += 1
                        total_messages_added += 1
                        await self.data_manager.add_message_to_pipeline(
                            redis_pipe, guild.id, message.channel.id, message.author.id,
                            message.id, message.created_at.timestamp()
                        )
                        messages_in_pipe += 1
                        if messages_in_pipe >= 500:
                            await self.data_manager.execute_pipeline(redis_pipe)
                            redis_pipe = self.data_manager.redis.pipeline()
                            messages_in_pipe = 0
                            await asyncio.sleep(0.1)

                        current_time = time.time()
                        if target_channel and (current_time - last_update_time > 5):
                            embed = self._create_progress_embed(guild, start_time, total_channels, channels_scanned, channel.name, total_messages_processed,
                                                                total_messages_added, start_datetime, end_datetime, bool(single_channel))
                            if progress_message:
                                try:
                                    await progress_message.edit(embed=embed)
                                except discord.HTTPException:
                                    progress_message = await target_channel.send(embed=embed)
                            else:
                                progress_message = await target_channel.send(embed=embed)
                            last_update_time = current_time
                except discord.Forbidden:
                    self.logger.warning(f"[{guild.name}] 无法访问频道 #{channel.name}，已跳过。")
                except Exception as e:
                    self.logger.error(f"[{guild.name}] 扫描频道 #{channel.name} 时发生错误: {e}", exc_info=True)

            if messages_in_pipe > 0: await self.data_manager.execute_pipeline(redis_pipe)

            # 3. 任务成功完成
            if single_channel is None:
                await self.data_manager.set_last_sync_timestamp(guild.id, end_datetime.timestamp())
                timestamp_update_message = "\n**全局同步时间点已更新。**"
            else:
                timestamp_update_message = "\n**注意：本次为部分回填，全局同步时间点未更新。**"

            duration = time.time() - start_time
            if target_channel:
                start_display = start_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
                end_display = end_datetime.astimezone(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
                final_embed = discord.Embed(
                    title="✅ 历史消息回填完成",
                    description=f"成功为 **{guild.name}** 拉取了从 **{start_display}** 到 **{end_display}** (UTC+8) 的消息。{timestamp_update_message}",
                    color=discord.Color.green(), timestamp=datetime.now(timezone.utc)
                )
                final_embed.add_field(name="总耗时", value=f"{duration:.2f} 秒", inline=True)
                final_embed.add_field(name="扫描频道数", value=f"{channels_scanned}/{total_channels}", inline=True)
                final_embed.add_field(name="写入消息数", value=f"{total_messages_added}", inline=True)
                if progress_message:
                    try:
                        await progress_message.edit(embed=final_embed, view=None)
                    except discord.HTTPException:
                        await target_channel.send(embed=final_embed)
                else:
                    await target_channel.send(embed=final_embed)

        except Exception as e:
            # 4. 任务失败
            self.logger.critical(f"服务器 '{guild.name}' 的回填任务发生严重错误: {e}", exc_info=True)
            if target_channel:
                await target_channel.send(f"❌ **回填任务异常中断**: `{e}`\n**【重要】同步时间戳未更新，以便重试。**")
        finally:
            # 5. 任务收尾，必须释放锁
            if guild.id in self._backfill_locks:
                self._backfill_locks.remove(guild.id)
            self.logger.info(f"服务器 '{guild.name}' 的回填任务结束，内存锁已释放。")

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
        """
        【核心统计指令】根据指定的范围 (全服/频道/类别) 和指标 (消息数/用户数) 生成活跃度报告。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild

        if days_window <= 0:
            await interaction.followup.send("❌ `回溯天数` 必须是正整数。", ephemeral=True)
            return
        if scope == "channel" and not target_channel:
            await interaction.followup.send("❌ 当统计范围为 `特定频道` 时，`target_channel` 不能为空。", ephemeral=True)
            return
        if scope == "category" and not target_category:
            await interaction.followup.send("❌ 当统计范围为 `特定频道类别` 时，`target_category` 不能为空。", ephemeral=True)
            return

        raw_all_activity_data = await self.data_manager.get_channel_activity_summary(
            guild_id=guild.id,
            days_window=days_window
        )
        if not raw_all_activity_data:
            await interaction.followup.send("在指定时间范围内没有找到任何活动记录。", ephemeral=True)
            return

        all_channel_ids_in_data = {
            cid for user_data in raw_all_activity_data.values() for cid in user_data.keys()
        }
        channel_cache = await self._build_channel_cache(guild, all_channel_ids_in_data)

        guild_cfg = self.config.get("guild_configs", {}).get(guild.id, {})
        ignored_channels = set(guild_cfg.get("ignored_channels", []))
        ignored_categories = set(guild_cfg.get("ignored_categories", []))

        scoped_channel_distinct_users = collections.defaultdict(set)
        scoped_channel_message_counts = collections.defaultdict(int)
        scoped_global_distinct_users = set()
        scope_description = ""

        for user_id, user_channels_data in raw_all_activity_data.items():
            for channel_id, count in user_channels_data.items():
                channel_obj = channel_cache.get(channel_id)
                if not channel_obj: continue

                category_id_to_check = channel_obj.parent.category_id if isinstance(channel_obj,
                                                                                    discord.Thread) and channel_obj.parent else channel_obj.category_id
                if category_id_to_check in ignored_categories or channel_obj.id in ignored_channels: continue

                should_include = False
                if scope == "guild":
                    should_include = True
                    scope_description = f"整个服务器"
                elif scope == "channel" and target_channel:
                    if isinstance(target_channel, discord.ForumChannel):
                        if (isinstance(channel_obj, discord.Thread) and channel_obj.parent_id == target_channel.id) or channel_obj.id == target_channel.id:
                            should_include = True;
                            scope_description = f"论坛频道 {target_channel.mention}"
                    elif channel_obj.id == target_channel.id:
                        should_include = True;
                        scope_description = f"频道 {target_channel.mention}"
                elif scope == "category" and target_category and category_id_to_check == target_category.id:
                    should_include = True;
                    scope_description = f"频道类别 **{target_category.name}**"

                if not should_include: continue

                scoped_channel_message_counts[channel_id] += count
                scoped_channel_distinct_users[channel_id].add(user_id)
                scoped_global_distinct_users.add(user_id)

        if not scope_description:
            await interaction.followup.send(f"在您指定的范围内没有找到任何符合条件的活动记录。", ephemeral=True)
            return

        channel_values_to_sort, total_overall_stat = {}, 0
        if metric == "total_messages":
            channel_values_to_sort = scoped_channel_message_counts
            total_overall_stat = sum(scoped_channel_message_counts.values())
        elif metric == "distinct_users":
            for cid, users in scoped_channel_distinct_users.items():
                channel_values_to_sort[cid] = len(users)
            total_overall_stat = len(scoped_global_distinct_users)

        sorted_display_data = await self._process_and_sort_activity_data(guild, list(channel_values_to_sort.items()))

        metric_name_display = "总消息数" if metric == "total_messages" else "独立活跃用户数"
        value_suffix = "条消息" if metric == "total_messages" else "位用户"
        total_value_display_suffix = "条" if metric == "total_messages" else "位"

        embed_template = discord.Embed(
            title=f"📈 活跃度统计报告 - {days_window} 天",
            color=discord.Color.dark_green(),
            timestamp=datetime.now(timezone.utc)
        )
        embed_template.description = f"在 **{scope_description}** 中，过去 **{days_window}** 天的活跃度概览："
        embed_template.add_field(name=f"**总计 {metric_name_display}**", value=f"`{total_overall_stat}` {total_value_display_suffix}", inline=False)
        embed_template.set_footer(text=f"统计时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")

        view = GenericHierarchicalPaginationView(
            interaction=interaction, embed_template=embed_template,
            sorted_display_data=sorted_display_data, field_name=f"分频道{metric_name_display}",
            value_suffix=value_suffix
        )
        await view.start()

    # --- 【性能优化】核心辅助方法：批量构建频道对象缓存 ---
    async def _build_channel_cache(
            self,
            guild: discord.Guild,
            channel_ids: typing.Set[int]
    ) -> typing.Dict[int, typing.Optional[discord.abc.GuildChannel]]:
        """
        高效地为一组 channel_id 构建一个频道对象缓存。
        优先从 guild.channels/threads 缓存获取，对未找到的进行一次性批量 API 请求。
        返回一个 {channel_id: channel_object | None} 的字典。
        """
        channel_cache: typing.Dict[int, typing.Optional[discord.abc.GuildChannel]] = {}
        ids_to_fetch = set()

        # 第一遍：从机器人内部缓存快速查找
        for cid in channel_ids:
            channel = guild.get_channel(cid)
            if channel:
                channel_cache[cid] = channel
            else:
                # 如果在缓存中找不到，则记录下来准备批量获取
                ids_to_fetch.add(cid)

        if not ids_to_fetch:
            return channel_cache  # 所有频道都在缓存中，直接返回

        self.logger.info(f"频道缓存未命中 {len(ids_to_fetch)} 个ID，准备从API获取...")

        # 第二遍：批量从 API 获取未缓存的频道
        async def fetch_one(channel_id):
            try:
                return await self.bot.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden):
                self.logger.warning(f"无法获取频道 {channel_id} (可能已删除或无权限)。")
                return None

        # 并发执行所有 fetch 操作
        fetch_tasks = [fetch_one(cid) for cid in ids_to_fetch]
        fetched_channels = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for channel in fetched_channels:
            if isinstance(channel, discord.abc.GuildChannel):
                channel_cache[channel.id] = channel
            elif channel is None:
                pass
            elif isinstance(channel, Exception):
                self.logger.error(f"批量获取频道对象时出现未处理的异常: {channel}", exc_info=channel)

        for cid in ids_to_fetch:
            if cid not in channel_cache:
                channel_cache[cid] = None

        return channel_cache


async def setup(bot: RoleBot):
    """Cog的入口点。"""
    await bot.add_cog(TrackActivityCog(bot))
