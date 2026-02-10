# cogs/heartbeat_cog.py
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional,TYPE_CHECKING

import discord
from discord import app_commands, Embed
from discord.ext import commands, tasks

import config
from information.data_manager import HeartbeatDataManager, HeartbeatInfo

from utility.helpers import format_duration_hms, BEIJING_TZ
from utility.permison import is_admin

if TYPE_CHECKING:
    from main import RoleBot

INFORMATION_GROUP_NAME = "服务器资讯"


def _last_update_of_message(message: discord.Message) -> datetime:
    """获取消息的最后更新时间（编辑时间或创建时间）。"""
    return message.edited_at or message.created_at


class HeartbeatInformationCog(commands.Cog, name="Heartbeat Information"):
    """一个用于创建和管理实时更新资讯的模块。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.data_manager = HeartbeatDataManager()
        # 存储每个心跳资讯的动态任务 (键仍为 target_message_id 的字符串形式)
        self.active_tasks: Dict[str, tasks.Loop] = {}

    async def cog_load(self):
        """Cog加载时，加载数据并为现有记录启动任务。"""
        await self.data_manager.load_data()
        for info in self.data_manager.get_all_heartbeats():
            if info.target_message_id:  # 只有有目标消息ID的才启动心跳任务
                self._start_heartbeat_task(info)

    async def cog_unload(self):
        """Cog卸载时，取消所有正在运行的任务。"""
        for task in self.active_tasks.values():
            task.cancel()
        self.active_tasks.clear()

    async def _fetch_source_message(self, info: HeartbeatInfo) -> Optional[discord.Message]:
        """根据HeartbeatInfo获取源消息，支持特定消息和频道最新消息。"""
        try:
            source_guild = self.bot.get_guild(info.source_guild_id) or await self.bot.fetch_guild(info.source_guild_id)
            source_channel = source_guild.get_channel(info.source_channel_id) or await source_guild.fetch_channel(info.source_channel_id)

            if info.is_channel_feed:
                # 获取频道最新消息
                async for msg in source_channel.history(limit=1):
                    return msg
                return None  # 频道无消息
            elif info.source_message_id:
                # 获取特定消息
                return await source_channel.fetch_message(info.source_message_id)
            else:
                self.bot.logger.error(f"心跳资讯 {info.key} 配置错误：既不是频道订阅也不是特定消息。")
                return None
        except (discord.NotFound, discord.Forbidden):
            raise  # 重新抛出以便上层处理，例如移除任务
        except Exception as e:
            self.bot.logger.error(f"获取源消息时发生未知错误 for {info.key}: {e}")
            raise

    @staticmethod
    def _prepare_target_message_kwargs(
            source_message: discord.Message,
            heartbeat_info: HeartbeatInfo,
            *,
            jump_url: Optional[str] = None
    ) -> tuple[str | None, list[Embed]]:
        """
        根据源消息和模式准备发送/编辑消息的关键字参数。
        增加了标题处理。

        Returns:
            一个包含 'content' 和 'embeds' 键的字典。
        """
        if jump_url:
            _jump_url = jump_url
        else:
            _jump_url = source_message.jump_url

        source_embeds = source_message.embeds
        source_content = source_message.content
        source_attachments = source_message.attachments

        mode_type = "频道订阅" if heartbeat_info.is_channel_feed else "消息同步"
        set_author_name = f"来自 {source_message.author.display_name} 的消息（同步）" if not heartbeat_info.is_channel_feed else f"来自 {source_message.channel.name} 的消息（同步）"

        copy_embeds = [embed.copy() for embed in source_embeds]

        if heartbeat_info.embed_mode and source_content:
            # 如果开启Embed模式，且源消息只有内容没有Embed
            # 就将内容转换成一个Embed
            content_embed = discord.Embed(
                description=source_content,
                color=discord.Color.blue()  # 您可以自定义颜色
            )
            new_content = None
            new_embeds: List[discord.Embed] = [content_embed]
            new_embeds.extend(copy_embeds)
        else:
            title_prefix = f"**{heartbeat_info.title}**\n" if heartbeat_info.title else ""
            new_content = title_prefix + source_content if source_content else title_prefix or None
            new_embeds = copy_embeds

        if source_attachments:
            attachments_text = "\n".join([f"📄 [{att.filename}]({att.url})" for att in source_attachments])
            if len(attachments_text) > 1024:
                attachments_text = attachments_text[:1020] + "..."

            if not new_embeds:
                new_embeds.append(discord.Embed(color=discord.Color.blue()))

            if len(new_embeds[0].fields) < 25:
                new_embeds[0].add_field(name="附件", value=attachments_text, inline=False)


        if len(new_embeds) > 0:
            first_embed = new_embeds[0]
            # 更新Embed的作者信息和footer
            old_author = first_embed.author
            author_name = old_author.name or set_author_name
            author_icon_url = old_author.icon_url or source_message.author.display_avatar
            author_url = _jump_url
            first_embed.set_author(name=author_name, url=author_url, icon_url=author_icon_url)
            first_embed.set_footer(
                text=f"{mode_type} | 使用`/{INFORMATION_GROUP_NAME}`指令转发 | 检测频率： {format_duration_hms(heartbeat_info.update_interval_seconds)} | 源消息更新于")
            first_embed.timestamp = _last_update_of_message(source_message)

            # 如果有标题，尝试添加到Embed的title，如果已经有title，则考虑前缀
            if heartbeat_info.title:
                if first_embed.title:
                    first_embed.title = f"{heartbeat_info.title}: {first_embed.title}"
                else:
                    first_embed.title = heartbeat_info.title

        # --- 3. 最终的超限检查与“牺牲”逻辑 ---
        if len(new_embeds) > 10:
            # 创建一个专门的警告Embed
            warning_embed = discord.Embed(
                title="⚠️ 内容超限，部分信息未显示",
                description=f"源消息包含的内容过多（超过10个Embed），因此仅显示前9个。\n\n"
                            f"**[点击此处查看完整原始消息]({_jump_url})**",
                color=discord.Color.orange()  # 使用醒目的橙色
            )
            # 牺牲：保留前9个，然后将警告Embed作为第10个
            new_embeds = new_embeds[:9] + [warning_embed]

        return new_content, new_embeds

    def _create_task_coro(self, info: HeartbeatInfo):
        """创建一个闭包，捕获info变量，用于任务的coroutine。"""

        async def update_message():
            try:
                source_message = await self._fetch_source_message(info)
                if not source_message:
                    # 频道无消息或配置错误，跳过此次更新
                    return

                # 检查源消息是否更新
                if _last_update_of_message(source_message) == info.last_update:
                    return

                target_guild = self.bot.get_guild(info.target_guild_id) or await self.bot.fetch_guild(info.target_guild_id)
                target_channel = target_guild.get_channel(info.target_channel_id) or await target_guild.fetch_channel(info.target_channel_id)
                target_message = await target_channel.fetch_message(info.target_message_id)

                # 准备新的embeds和content
                new_content, new_embeds = self._prepare_target_message_kwargs(source_message, info)

                # 更新消息
                await target_message.edit(
                    content=new_content,
                    embeds=new_embeds,
                    allowed_mentions=discord.AllowedMentions.none())

                # 更新HeartbeatInfo中的last_update并保存
                info.last_update = _last_update_of_message(source_message)
                await self.data_manager.update_heartbeat(info)

            except discord.NotFound:
                # 如果源或目标消息/频道被删除，则停止并移除此任务
                self.bot.logger.warning(f"心跳资讯 {info.target_message_id} (标题: {info.title}) 的源/目标实体已不存在，将自动移除。")
                await self._stop_and_remove_heartbeat(info.target_message_id, f"源/目标实体已删除")
            except discord.Forbidden:
                self.bot.logger.error(f"心跳资讯 {info.target_message_id} (标题: {info.title}) 更新失败：权限不足。将自动移除。")
                await self._stop_and_remove_heartbeat(info.target_message_id, f"机器人权限不足")
            except Exception as e:
                self.bot.logger.error(f"更新心跳资讯 {info.target_message_id} (标题: {info.title}) 时发生未知错误: {e}")

        return update_message

    def _start_heartbeat_task(self, info: HeartbeatInfo):
        """根据HeartbeatInfo创建一个新的后台任务并启动它。"""
        if not info.target_message_id:
            self.bot.logger.warning(f"尝试启动无目标消息ID的心跳任务: {info.title}。跳过。")
            return

        key = str(info.target_message_id)
        if key in self.active_tasks:
            self.bot.logger.warning(f"尝试启动已存在的心跳任务: {info.title} (ID: {key})。将先停止旧任务。")
            self.active_tasks[key].cancel()

        # TODO 由于速率限制，现在取消实时更新功能，之后转为可发送限时信息
        return
        # 1. 创建任务的协程
        coro = self._create_task_coro(info)

        # 2. 用 tasks.loop 装饰器包装它
        new_task = tasks.loop(seconds=info.update_interval_seconds)(coro)

        # 3. 为这个新任务动态地附加一个 before_loop
        #    这确保任务在开始循环前，机器人一定是 ready 状态
        async def before_loop_waiter():
            await self.bot.wait_until_ready()

        new_task.before_loop(before_loop_waiter)

        # 4. 存储并直接启动任务
        self.active_tasks[key] = new_task
        new_task.start()
        self.bot.logger.info(f"已调度心跳资讯任务: {info.title} (ID: {key})，间隔: {info.update_interval_seconds}s")

    async def _stop_and_remove_heartbeat(self, target_message_id: int, reason: str):
        """停止任务，从数据管理器中移除记录，并尝试通知创建者。"""
        key = str(target_message_id)

        # 停止任务
        if key in self.active_tasks:
            self.active_tasks[key].cancel()
            del self.active_tasks[key]

        # 从数据文件移除
        info = await self.data_manager.remove_heartbeat(target_message_id)

        if info:
            self.bot.logger.info(f"心跳资讯 {info.title} (ID: {key}) 因 '{reason}' 被移除。")
            try:
                channel_id = info.target_channel_id
                message_id = info.target_message_id
                if channel_id and message_id:  # 确保有目标消息才尝试编辑
                    channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
                    message = await channel.fetch_message(message_id)
                    current_content = message.content or ""
                    await message.edit(
                        content=f"⚠️ 本心跳资讯 (『{info.title}』) 已停止同步，最后更新时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}。\n" + current_content
                    )
            except (discord.NotFound, discord.Forbidden, ValueError) as e:
                self.bot.logger.warning(f"无法编辑目标消息 {target_message_id} 告知停止：{str(e)}")
        else:
            self.bot.logger.warning(f"尝试移除不存在的心跳资讯 {key}")

    # --- Slash Commands ---

    information_group = app_commands.Group(
        name=f"心跳资讯", description="心跳资讯相关指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(read_messages=True)
    )

    @information_group.command(name="添加", description="在当前频道创建一个实时更新的资讯消息 (基于特定消息)")
    @app_commands.describe(
        source_url="要同步的源消息的URL",
        title="资讯的标题 (用于识别)",
        interval_seconds="更新间隔（秒），最小为1",
        embed_mode="如果源消息只有文本，是否自动转换为Embed (默认为是)"
    )
    @is_admin()
    @app_commands.checks.has_permissions(read_messages=True)
    async def heartbeat_create_message(
            self,
            interaction: discord.Interaction,
            source_url: str,
            title: str,
            interval_seconds: int = 60,  # 默认间隔60秒
            embed_mode: bool = True
    ):
        await interaction.response.defer(ephemeral=True)

        target_channel = interaction.channel

        if interval_seconds < 1:
            await interaction.followup.send("❌ 错误：更新间隔不能小于1秒。", ephemeral=True)
            return

        if not title:
            await interaction.followup.send("❌ 错误：标题不能为空。", ephemeral=True)
            return

        if self.data_manager.get_heartbeat_by_title(title, interaction.guild_id):
            await interaction.followup.send(f"❌ 错误：本服务器已存在标题为 `{title}` 的资讯。", ephemeral=True)
            return

        # 解析URL
        match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)', source_url)
        if not match:
            await interaction.followup.send("❌ 错误：无效的Discord消息URL格式。", ephemeral=True)
            return

        source_guild_id, source_channel_id, source_message_id = map(int, match.groups())

        # 验证源消息
        try:
            source_guild = self.bot.get_guild(source_guild_id) or await self.bot.fetch_guild(source_guild_id)
            source_channel = source_guild.get_channel(source_channel_id) or await source_guild.fetch_channel(source_channel_id)
            source_message = await source_channel.fetch_message(source_message_id)
        except (discord.NotFound, discord.Forbidden) as e:
            await interaction.followup.send(f"❌ 错误：无法访问源消息。请确保URL正确且机器人有权限访问。\n`{e}`", ephemeral=True)
            return

        # 发送初始消息作为目标
        try:
            target_message: discord.Message = await target_channel.send(content="心跳资讯：正在准备消息中……")
        except discord.Forbidden:
            await interaction.followup.send(f"❌ 错误：机器人没有权限在 `{target_channel.name}` 频道发送消息。", ephemeral=True)
            return

        # 创建并存储记录
        new_info = HeartbeatInfo(
            source_guild_id=source_guild_id,
            source_channel_id=source_channel_id,
            source_message_id=source_message_id,
            is_channel_feed=False,  # 不是频道订阅
            target_guild_id=interaction.guild_id,
            target_channel_id=target_channel.id,
            target_message_id=target_message.id,
            update_interval_seconds=interval_seconds,
            created_by=interaction.user.id,
            last_update=_last_update_of_message(source_message),
            embed_mode=embed_mode,
            title=title
        )

        # 使用辅助函数来获取要发送的内容
        new_content, new_embeds = self._prepare_target_message_kwargs(source_message, new_info)

        await asyncio.sleep(1)  # 稍作等待，确保消息已发送

        await target_message.edit(
            content=new_content,
            embeds=new_embeds,
            allowed_mentions=discord.AllowedMentions.none()
        )

        await self.data_manager.add_heartbeat(new_info)

        # 启动后台更新任务
        self._start_heartbeat_task(new_info)

        await interaction.followup.send(f"✅ 成功！心跳资讯『**{title}**』已创建在 {target_channel.mention}。\n"
                                        f"它将每 {interval_seconds} 秒更新一次。\n"
                                        f"资讯链接: {target_message.jump_url}", ephemeral=True)

    @information_group.command(name="发送频道订阅", description="订阅一个频道，将其最新消息作为资讯实时更新")
    @app_commands.describe(
        source_channel_url="要订阅的源频道的URL",
        title="资讯的标题 (用于识别)",
        interval_seconds="更新间隔（秒），最小为1",
        embed_mode="如果源消息只有文本，是否自动转换为Embed (默认为是)"
    )
    @is_admin()
    async def heartbeat_create_channel_feed(
            self,
            interaction: discord.Interaction,
            source_channel_url: str,
            title: str,
            interval_seconds: int = 60,  # 默认间隔60秒
            embed_mode: bool = True
    ):
        await interaction.response.defer(ephemeral=True)

        target_channel = interaction.channel

        if interval_seconds < 1:
            await interaction.followup.send("❌ 错误：更新间隔不能小于1秒。", ephemeral=True)
            return

        if not title:
            await interaction.followup.send("❌ 错误：标题不能为空。", ephemeral=True)
            return

        if self.data_manager.get_heartbeat_by_title(title, interaction.guild_id):
            await interaction.followup.send(f"❌ 错误：本服务器已存在标题为 `{title}` 的资讯。", ephemeral=True)
            return

        # 解析URL，只需要频道ID
        match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)', source_channel_url)
        if not match:
            await interaction.followup.send("❌ 错误：无效的Discord频道URL格式。", ephemeral=True)
            return

        source_guild_id, source_channel_id = map(int, match.groups())

        # 验证源频道
        try:
            source_guild = self.bot.get_guild(source_guild_id) or await self.bot.fetch_guild(source_guild_id)
            source_channel = source_guild.get_channel(source_channel_id) or await source_guild.fetch_channel(source_channel_id)
            if not isinstance(source_channel, (discord.TextChannel, discord.Thread)):
                await interaction.followup.send(f"❌ 错误：`{source_channel.name}` 不是一个文本频道。", ephemeral=True)
                return
        except (discord.NotFound, discord.Forbidden) as e:
            await interaction.followup.send(f"❌ 错误：无法访问源频道。请确保URL正确且机器人有权限访问。\n`{e}`", ephemeral=True)
            return

        # 获取源频道最新消息作为初始内容
        initial_source_message = None
        try:
            async for msg in source_channel.history(limit=1):
                initial_source_message = msg
                break
        except (discord.NotFound, discord.Forbidden) as e:
            await interaction.followup.send(f"❌ 错误：无法获取源频道的最新消息。请确保机器人有权限读取。\n`{e}`", ephemeral=True)
            return

        if not initial_source_message:
            await interaction.followup.send(f"⚠️ 注意：源频道 `{source_channel.name}` 当前没有消息，心跳资讯将在有新消息时开始更新。", ephemeral=True)

        # 发送初始消息作为目标
        try:
            target_message: discord.Message = await target_channel.send(content="心跳资讯：正在准备消息中……")
        except discord.Forbidden:
            await interaction.followup.send(f"❌ 错误：机器人没有权限在 `{target_channel.name}` 频道发送消息。", ephemeral=True)
            return

        # 创建并存储记录
        new_info = HeartbeatInfo(
            source_guild_id=source_guild_id,
            source_channel_id=source_channel_id,
            source_message_id=None,  # 频道订阅模式下，不指定特定消息ID
            is_channel_feed=True,  # 标记为频道订阅
            target_guild_id=interaction.guild_id,
            target_channel_id=target_channel.id,
            target_message_id=target_message.id,
            update_interval_seconds=interval_seconds,
            created_by=interaction.user.id,
            last_update=_last_update_of_message(initial_source_message) if initial_source_message else datetime.min,
            embed_mode=embed_mode,
            title=title
        )

        if initial_source_message:
            new_content, new_embeds = self._prepare_target_message_kwargs(initial_source_message, new_info)
            await asyncio.sleep(1)  # 稍作等待
            await target_message.edit(
                content=new_content,
                embeds=new_embeds,
                allowed_mentions=discord.AllowedMentions.none()
            )

        await self.data_manager.add_heartbeat(new_info)
        self._start_heartbeat_task(new_info)

        await interaction.followup.send(f"✅ 成功！频道订阅『**{title}**』已创建在 {target_channel.mention}。\n"
                                        f"它将每 {interval_seconds} 秒更新一次 `{source_channel.name}` 频道的最新消息。\n"
                                        f"资讯链接: {target_message.jump_url}", ephemeral=True)

    async def _autocomplete_heartbeat_titles(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """为心跳资讯标题提供自动补全。"""
        all_heartbeats = self.data_manager.get_all_heartbeats()
        # 仅显示当前服务器的资讯标题
        server_heartbeats = [info for info in all_heartbeats if info.target_guild_id == interaction.guild_id]

        titles = []
        for info in server_heartbeats:
            if info.title and current.lower() in info.title.lower():
                titles.append(app_commands.Choice(name=info.title, value=info.title))

        return titles[:25]  # Discord 限制为25个选项

    @information_group.command(name="移除", description="移除一个心跳资讯")
    @app_commands.describe(title="要移除的资讯标题")
    @app_commands.autocomplete(title=_autocomplete_heartbeat_titles)
    @is_admin()
    async def heartbeat_remove(self, interaction: discord.Interaction, title: str):
        await interaction.response.defer(ephemeral=True)

        info = self.data_manager.get_heartbeat_by_title(title, interaction.guild_id)
        if not info:
            await interaction.followup.send(f"❌ 错误：在本服务器上找不到标题为 `{title}` 的资讯。", ephemeral=True)
            return

        if not info.target_message_id:
            # 理论上所有心跳资讯都应有 target_message_id，但为了安全考虑
            await interaction.followup.send(f"❌ 错误：资讯『{title}』没有关联的目标消息ID，无法移除任务。", ephemeral=True)
            return

        # 停止任务并移除数据
        await self._stop_and_remove_heartbeat(info.target_message_id, f"由用户 {interaction.user} 手动移除")

        await interaction.followup.send(f"✅ 成功！标题为『**{title}**』的资讯已被移除。", ephemeral=True)

    information_general_group = app_commands.Group(
        name=f"服务器资讯", description="调取并发送资讯",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(read_messages=True)
    )

    @information_general_group.command(name="列表", description="列出本服务器上所有正在运行的心跳资讯")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def heartbeat_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        all_heartbeats = self.data_manager.get_all_heartbeats()
        server_heartbeats = [info for info in all_heartbeats if info.target_guild_id == interaction.guild_id]

        if not server_heartbeats:
            await interaction.followup.send("本服务器上当前没有正在运行的心跳资讯。", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"服务器 '{interaction.guild.name}' 的心跳资讯列表",
            color=discord.Color.blue()
        )

        description_lines = []
        for i, info in enumerate(server_heartbeats, 1):
            mode_type = "频道订阅" if info.is_channel_feed else "消息同步"
            # 兼容旧数据，如果target_message_id为空则不显示链接
            target_link = f"[跳转到资讯]({info.target_url})" if info.target_message_id else "无目标消息"

            line = (
                f"**{i}.** **『{info.title or '无标题'}』** ({mode_type})\n"
                f"   - **{target_link}** (ID: `{info.target_message_id or 'N/A'}`)\n"
                f"   - **来源**: {f'[点击查看]({info.source_url})' if info.source_message_id else f'<#{info.source_channel_id}> (最新消息)'}\n"
                f"   - **目标频道**: <#{info.target_channel_id}>\n"
                f"   - **间隔**: {info.update_interval_seconds} 秒\n"
                f"   - **模式**: {'自动Embed' if info.embed_mode else '直接同步'}\n"
                f"   - **创建者**: <@{info.created_by}>"
            )
            description_lines.append(line)

        embed.description = "\n\n".join(description_lines)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @information_general_group.command(name="调取", description="调取资讯，以私人形式展示")
    @app_commands.describe(title="要发送的资讯标题")
    @app_commands.autocomplete(title=_autocomplete_heartbeat_titles)
    @app_commands.checks.has_permissions(read_messages=True)
    async def send_info_once(self, interaction: discord.Interaction, title: str):
        await self._send_info_once(interaction, title, is_private=True)

    @information_general_group.command(name="发送", description="发送资讯，以公开形式展示")
    @app_commands.describe(title="要发送的资讯标题")
    @app_commands.autocomplete(title=_autocomplete_heartbeat_titles)
    @app_commands.checks.has_permissions(read_messages=True)
    async def send_info_once(self, interaction: discord.Interaction, title: str):
        await self._send_info_once(interaction, title, is_private=False)

    async def _send_info_once(self, interaction: discord.Interaction, title: str, *, is_private: bool):
        await interaction.response.defer(ephemeral=True, thinking=True)

        info = self.data_manager.get_heartbeat_by_title(title, interaction.guild_id)
        if not info:
            await interaction.followup.send(f"❌ 错误：在本服务器上找不到标题为 `{title}` 的资讯。", ephemeral=True)
            return

        try:
            source_message = await self._fetch_source_message(info)
            if not source_message:
                await interaction.followup.send(f"⚠️ 无法获取资讯『{title}』的源内容 (频道可能无消息或消息已删除)。", ephemeral=True)
                return
        except (discord.NotFound, discord.Forbidden) as e:
            await interaction.followup.send(f"❌ 错误：无法访问资讯『{title}』的源内容。请确保机器人有权限访问。\n`{e}`", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"❌ 发生未知错误：无法获取资讯『{title}』的源内容。\n`{e}`", ephemeral=True)
            return

        # 准备发送参数
        new_content, new_embeds = self._prepare_target_message_kwargs(source_message, info)
        # 调整 footer，表明这是一次性发送

        for embed in new_embeds:
            embed.set_footer(text=f"使用 `/{INFORMATION_GROUP_NAME}` 指令调取 | 由 {interaction.user.display_name} 发送的资讯快照 | 源消息更新于",
                             icon_url=interaction.user.avatar.url)
            embed.timestamp = _last_update_of_message(source_message)

        if not is_private:
            try:
                await interaction.channel.send(
                    content=new_content,
                    embeds=new_embeds,
                    allowed_mentions=discord.AllowedMentions.none()
                )
                await interaction.followup.send(f"✅ 成功发送资讯『**{title}**』到 {interaction.channel.mention}。", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send(f"❌ 错误：机器人没有权限在 `{interaction.channel.name}` 频道发送消息。", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ 发送资讯时发生未知错误: {e}", ephemeral=True)
        else:
            try:
                await interaction.edit_original_response(
                    content=new_content,
                    embeds=new_embeds,
                    allowed_mentions=discord.AllowedMentions.none()
                )
            except Exception as e:
                await interaction.followup.send(f"❌ 发送资讯时发生未知错误: {e}", ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(HeartbeatInformationCog(bot))
