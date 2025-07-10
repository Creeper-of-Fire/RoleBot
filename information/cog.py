# cogs/heartbeat_cog.py

import re
from typing import Dict

import discord
from discord import app_commands
from discord.ext import commands, tasks

# 导入我们的数据类和管理器
from data_manager import HeartbeatDataManager, HeartbeatInfo


class HeartbeatInformationCog(commands.Cog, name="Heartbeat Information"):
    """一个用于创建和管理实时更新资讯的模块。"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.data_manager = HeartbeatDataManager()
        # 存储每个心跳资讯的动态任务
        self.active_tasks: Dict[str, tasks.Loop] = {}

    async def cog_load(self):
        """Cog加载时，加载数据并为现有记录启动任务。"""
        await self.data_manager.load_data()
        for info in self.data_manager.get_all_heartbeats():
            self._start_heartbeat_task(info)

    async def cog_unload(self):
        """Cog卸载时，取消所有正在运行的任务。"""
        for task in self.active_tasks.values():
            task.cancel()
        self.active_tasks.clear()

    @staticmethod
    def _prepare_target_message_kwargs(source_message: discord.Message, embed_mode: bool) -> Dict[str, Any]:
        """
        根据源消息和模式准备发送/编辑消息的关键字参数。

        Returns:
            一个包含 'content' 和 'embeds' 键的字典。
        """
        source_embeds = source_message.embeds
        source_content = source_message.content

        # 这是新模式的核心逻辑
        if embed_mode and not source_embeds and source_content:
            # 如果开启Embed模式，且源消息只有内容没有Embed
            # 就将内容转换成一个Embed
            embed = discord.Embed(
                description=source_content,
                color=discord.Color.blue()  # 您可以自定义颜色
            )
            # 添加作者和时间戳，让资讯更具上下文
            embed.set_author(name=f"来自 {source_message.author.display_name}", icon_url=source_message.author.display_avatar)
            embed.timestamp = source_message.created_at

            return {"content": None, "embeds": [embed]}
        else:
            # 否则，使用默认行为：直接复制内容和Embeds
            return {
                "content": source_content if source_content else None,
                "embeds": [embed.copy() for embed in source_embeds]
            }

    def _create_task_coro(self, info: HeartbeatInfo):
        """创建一个闭包，捕获info变量，用于任务的coroutine。"""

        async def update_message():
            try:
                source_guild = self.bot.get_guild(info.source_guild_id) or await self.bot.fetch_guild(info.source_guild_id)
                source_channel = source_guild.get_channel(info.source_channel_id) or await source_guild.fetch_channel(info.source_channel_id)
                source_message = await source_channel.fetch_message(info.source_message_id)

                target_guild = self.bot.get_guild(info.target_guild_id) or await self.bot.fetch_guild(info.target_guild_id)
                target_channel = target_guild.get_channel(info.target_channel_id) or await target_guild.fetch_channel(info.target_channel_id)
                target_message = await target_channel.fetch_message(info.target_message_id)

                # 准备新的embeds和content
                edit_kwargs = self._prepare_target_message_kwargs(source_message, info.embed_mode)

                # 更新消息
                await target_message.edit(**edit_kwargs)

            except discord.NotFound:
                # 如果源或目标消息被删除，则停止并移除此任务
                self.bot.logger.warning(f"心跳资讯 {info.key} 的源或目标消息已不存在，将自动移除。")
                await self._stop_and_remove_heartbeat(info.target_message_id, f"源或目标消息已删除")
            except discord.Forbidden:
                self.bot.logger.error(f"心跳资讯 {info.key} 更新失败：权限不足。将自动移除。")
                await self._stop_and_remove_heartbeat(info.target_message_id, f"机器人权限不足")
            except Exception as e:
                self.bot.logger.error(f"更新心跳资讯 {info.key} 时发生未知错误: {e}")

        return update_message

    def _start_heartbeat_task(self, info: HeartbeatInfo):
        """根据HeartbeatInfo创建一个新的后台任务并启动它。"""
        if info.key in self.active_tasks:
            self.bot.logger.warning(f"尝试启动已存在的心跳任务: {info.key}。将先停止旧任务。")
            self.active_tasks[info.key].cancel()

        # 使用工厂函数创建任务，以确保每个任务都有独立的coroutine实例和状态
        coro = self._create_task_coro(info)
        new_task = tasks.loop(seconds=info.update_interval_seconds)(coro)

        # 在启动任务前，等待机器人准备就绪
        async def start_after_ready():
            await self.bot.wait_until_ready()
            if not new_task.is_running():
                new_task.start()
                self.bot.logger.info(f"已启动心跳资讯任务: {info.key}，间隔: {info.update_interval_seconds}s")

        self.active_tasks[info.key] = new_task
        self.bot.loop.create_task(start_after_ready())

    async def _stop_and_remove_heartbeat(self, target_message_id: int, reason: str):
        """停止任务，从数据管理器中移除记录，并尝试通知创建者。"""
        key = str(target_message_id)

        # 停止任务
        if key in self.active_tasks:
            self.active_tasks[key].cancel()
            del self.active_tasks[key]

        # 从数据文件移除
        info = await self.data_manager.remove_heartbeat(target_message_id)

        # 尝试通知创建者
        if info:
            try:
                creator = await self.bot.fetch_user(info.created_by)
                await creator.send(f"⚠️ 您创建的一个心跳资讯已被自动移除。\n"
                                   f"**原因**: {reason}\n"
                                   f"**源消息**: <{info.source_url}>\n"
                                   f"**目标位置**: <{info.target_url}>")
            except (discord.NotFound, discord.Forbidden):
                self.bot.logger.warning(f"无法通知心跳资讯 {key} 的创建者 (ID: {info.created_by})。")

    # --- Slash Commands ---

    @app_commands.command(name="心跳资讯_添加", description="创建一个实时更新的资讯消息")
    @app_commands.describe(
        source_url="要同步的源消息的URL",
        channel="要将资讯发送到的频道",
        interval_seconds="更新间隔（秒），最小为5",
        embed_mode="如果源消息只有文本，是否自动转换为Embed (默认为是)"
    )
    @app_commands.checks.has_permissions(manage_messages=True)
    async def heartbeat_create(
            self,
            interaction: discord.Interaction,
            source_url: str,
            channel: discord.TextChannel,
            interval_seconds: int,
            embed_mode: bool = True
    ):
        await interaction.response.defer(ephemeral=True)

        if interval_seconds < 5:
            await interaction.followup.send("❌ 错误：更新间隔不能小于5秒。", ephemeral=True)
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
            source_channel = await source_guild.fetch_channel(source_channel_id)
            source_message = await source_channel.fetch_message(source_message_id)
        except (discord.NotFound, discord.Forbidden) as e:
            await interaction.followup.send(f"❌ 错误：无法访问源消息。请确保URL正确且机器人有权限访问。\n`{e}`", ephemeral=True)
            return

        # 发送初始消息作为目标
        try:
            # 使用辅助函数来获取要发送的内容
            send_kwargs = self._prepare_target_message_kwargs(source_message, embed_mode)
            target_message = await channel.send(
                **send_kwargs,
                allowed_mentions=discord.AllowedMentions.none()
            )
        except discord.Forbidden:
            await interaction.followup.send(f"❌ 错误：机器人没有权限在 `{channel.name}` 频道发送消息。", ephemeral=True)
            return

        # 创建并存储记录
        new_info = HeartbeatInfo(
            source_guild_id=source_guild_id,
            source_channel_id=source_channel_id,
            source_message_id=source_message_id,
            target_guild_id=interaction.guild_id,
            target_channel_id=channel.id,
            target_message_id=target_message.id,
            update_interval_seconds=interval_seconds,
            created_by=interaction.user.id,
            embed_mode=embed_mode
        )
        await self.data_manager.add_heartbeat(new_info)

        # 启动后台更新任务
        self._start_heartbeat_task(new_info)

        await interaction.followup.send(f"✅ 成功！心跳资讯已创建在 {channel.mention}。\n"
                                        f"它将每 {interval_seconds} 秒更新一次。\n"
                                        f"资讯链接: {target_message.jump_url}", ephemeral=True)

    @app_commands.command(name="心跳资讯_列表", description="列出本服务器上所有正在运行的心跳资讯")
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
            mode_text = "自动Embed" if info.embed_mode else "直接同步"
            line = (
                f"**{i}.** [跳转到资讯]({info.target_url}) (ID: `{info.target_message_id}`)\n"
                f"   - **来源**: [点击查看]({info.source_url})\n"
                f"   - **频道**: <#{info.target_channel_id}>\n"
                f"   - **间隔**: {info.update_interval_seconds} 秒\n"
                f"   - **模式**: {mode_text}\n"
                f"   - **创建者**: <@{info.created_by}>"
            )
            description_lines.append(line)

        embed.description = "\n\n".join(description_lines)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="心跳资讯_移除", description="移除一个心跳资讯")
    @app_commands.describe(target_message_id="要移除的资讯消息的ID")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def heartbeat_remove(self, interaction: discord.Interaction, target_message_id: str):
        await interaction.response.defer(ephemeral=True)

        try:
            msg_id_int = int(target_message_id)
        except ValueError:
            await interaction.followup.send("❌ 错误：消息ID必须是一个数字。", ephemeral=True)
            return

        info = self.data_manager.get_heartbeat(msg_id_int)
        if not info or info.target_guild_id != interaction.guild_id:
            await interaction.followup.send("❌ 错误：在本服务器上找不到具有该ID的心跳资讯。", ephemeral=True)
            return

        # 停止任务并移除数据
        await self._stop_and_remove_heartbeat(msg_id_int, f"由用户 {interaction.user} 手动移除")

        await interaction.followup.send(f"✅ 成功！ID为 `{target_message_id}` 的心跳资讯已被移除。", ephemeral=True)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(HeartbeatInformationCog(bot))
