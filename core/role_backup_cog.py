# cogs/backup_cog.py

from __future__ import annotations

import asyncio
import io
import json
import typing
import zipfile
from datetime import datetime, timezone
from functools import partial

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
from utility.helpers import create_progress_bar

if typing.TYPE_CHECKING:
    from main import RoleBot


class BackupCog(commands.Cog, name="Backup"):
    """
    负责服务器身份组数据备份的专用模块。
    - 定期自动备份所有身份组的元数据和成员列表。
    - 提供手动触发备份和成员缓存刷新的功能。
    - 将备份结果发送到指定的频道。
    """

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger

        # 从配置加载ID
        self.guild_id = config.BACKUP_GUILD_ID
        self.channel_id = config.BACKUP_CHANNEL_ID

        self.backup_guild: discord.Guild | None = None
        self.backup_channel: discord.TextChannel | None = None

        # 我们不再在这里等待 ready，而是在 on_ready 事件中获取 guild 和 channel
        if config.ENABLE_ROLE_BACKUPS and self.guild_id and self.channel_id:
            self.auto_backup_task.start()
        else:
            self.logger.warning("备份功能未启用或配置不完整，将不会启动自动备份任务。")

    @commands.Cog.listener()
    async def on_ready(self):
        """
        当机器人准备就绪后，安全地获取 Guild 和 Channel 对象。
        这是进行初始化操作的正确位置。
        """
        if not config.ENABLE_ROLE_BACKUPS:
            return

        # 此时 bot 的缓存已经就绪
        self.backup_guild = self.bot.get_guild(self.guild_id)
        self.backup_channel = self.bot.get_channel(self.channel_id)

        if not self.backup_guild:
            self.logger.error(f"找不到配置的备份服务器 (ID: {self.guild_id})。备份功能将无法运行。")
            # 如果找不到服务器，则取消任务以防出错
            self.auto_backup_task.cancel()
        elif not self.backup_channel:
            self.logger.error(f"找不到配置的备份频道 (ID: {self.channel_id})。备份功能将无法运行。")
            # 同样，找不到频道也取消任务
            self.auto_backup_task.cancel()
        else:
            self.logger.info(f"备份模块已就绪，目标服务器: '{self.backup_guild.name}', 目标频道: '#{self.backup_channel.name}'")

    def cog_unload(self):
        """Cog卸载时，取消任务。"""
        self.auto_backup_task.cancel()

    # --- 核心备份逻辑 ---

    def _blocking_create_backup_data(self, guild: discord.Guild) -> dict:
        """
        [同步/阻塞] 生成包含服务器所有身份组信息的字典。
        这个函数包含 CPU 密集型操作，应该在 executor 中运行。
        """
        self.logger.info(f"开始在后台线程为服务器 '{guild.name}' 生成身份组备份数据...")

        backup_data = {
            "backup_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "guild_id": guild.id,
            "guild_name": guild.name,
            "roles": []
        }
        # 从 position 高的（顶部的）角色开始备份
        sorted_roles = sorted(guild.roles, key=lambda r: r.position, reverse=True)

        for role in sorted_roles:
            if role.is_default():  # 跳过 @everyone
                continue

            role_data = {
                "id": role.id,
                "name": role.name,
                "color": role.color.value,
                "secondary_color": role.secondary_color.value if role.secondary_color is not None else None,
                "tertiary_color": role.tertiary_color.value if role.tertiary_color is not None else None,
                "hoist": role.hoist,
                "position": role.position,
                "permissions": role.permissions.value,
                "mentionable": role.mentionable,
                "is_bot_managed": role.managed,
                # 存储成员ID列表，这是最高效的方式
                "member_ids": [member.id for member in role.members]
            }
            backup_data["roles"].append(role_data)

        self.logger.info(f"后台身份组数据生成完毕，共处理了 {len(backup_data['roles'])} 个身份组。")
        return backup_data

    async def _create_backup_data_async(self, guild: discord.Guild) -> dict:
        """
        [异步] 调用阻塞的数据生成函数，使其在后台线程池中运行，避免阻塞事件循环。
        """
        # 使用 run_in_executor 将阻塞函数放到后台线程执行
        loop = self.bot.loop
        # 我们使用 partial 来包装函数和它的参数
        func = partial(self._blocking_create_backup_data, guild)
        backup_data = await loop.run_in_executor(None, func)
        return backup_data

    def _blocking_create_zip_file(self, backup_data: dict, backup_type: str, guild_name: str) -> tuple[io.BytesIO, str]:
        """
        [同步/阻塞] 将备份数据打包成一个压缩的内存文件对象。
        这个函数包含 CPU 密集型和 I/O 型操作，应该在 executor 中运行。
        """
        self.logger.info("开始在后台线程中创建 ZIP 备份文件...")
        # 在内存中创建文件
        json_bytes = json.dumps(backup_data, indent=2).encode('utf-8')
        memory_file = io.BytesIO()

        # 创建ZIP压缩包
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('role_backup.json', json_bytes)

        memory_file.seek(0)

        # 准备文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{backup_type}_backup_{guild_name}_{timestamp}.zip"

        self.logger.info(f"后台 ZIP 文件创建完毕: {filename}")
        return memory_file, filename

    async def _create_backup_file_async(self, backup_data: dict, backup_type: str) -> discord.File:
        """
        [异步] 调用阻塞的 ZIP 文件创建函数，并返回一个 discord.File 对象。
        """
        loop = self.bot.loop
        # 同样使用 partial 包装函数和参数
        func = partial(self._blocking_create_zip_file, backup_data, backup_type, self.backup_guild.name)
        memory_file, filename = await loop.run_in_executor(None, func)

        return discord.File(memory_file, filename=filename)

    # --- 自动化任务 ---

    @tasks.loop(hours=config.LIGHT_BACKUP_INTERVAL_HOURS)
    async def auto_backup_task(self):
        """每小时执行一次，根据时间决定是轻量备份还是重量备份。"""
        if not self.backup_guild or not self.backup_channel:
            self.logger.warning("自动备份任务跳过，因为服务器或频道对象无效。")
            return

        current_hour = datetime.now(timezone.utc).hour
        is_full_backup_time = (current_hour % config.FULL_BACKUP_INTERVAL_HOURS == 0)

        backup_type = "FULL" if is_full_backup_time else "LIGHT"
        self.logger.info(f"开始执行自动 {backup_type} 备份...")

        try:
            # 如果是重量备份时间，先刷新成员缓存
            if is_full_backup_time:
                await self._perform_member_cache_refresh(interaction=None)  # 内部调用，无交互

            # 1. 生成备份数据
            data = await self._create_backup_data_async(self.backup_guild)

            # 2. 创建文件
            backup_file = await self._create_backup_file_async(data, backup_type)

            # 3. 发送到频道
            role_count = len(data['roles'])
            total_members_in_roles = sum(len(r['member_ids']) for r in data['roles'])
            await self.backup_channel.send(
                f"✅ **自动 {backup_type} 备份完成**\n"
                f"📅 `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`\n"
                f"- **备份身份组数:** `{role_count}`\n"
                f"- **总身份组人次:** `{total_members_in_roles}`",
                file=backup_file
            )
            self.logger.info(f"自动 {backup_type} 备份成功并已发送到频道 {self.backup_channel.name}。")

        except Exception as e:
            self.logger.error(f"自动 {backup_type} 备份失败: {e}", exc_info=True)
            try:
                await self.backup_channel.send(f"❌ **自动 {backup_type} 备份失败!**\n错误: `{e}`")
            except Exception as send_e:
                self.logger.error(f"向备份频道发送失败通知时也发生错误: {send_e}")

    @auto_backup_task.before_loop
    async def before_auto_backup(self):
        """在任务开始前等待机器人就绪。"""
        await self.bot.wait_until_ready()
        self.logger.info("备份Cog已就绪，自动备份任务即将开始。")

    # --- 指令 ---

    backup_admin_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨备份", description="数据备份相关指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @backup_admin_group.command(name="手动身份组备份", description="立即执行一次完整的身份组备份。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manual_backup(self, interaction: discord.Interaction):
        """手动触发一次完整的身份组备份，包含刷新成员缓存。"""
        if interaction.guild_id != self.guild_id:
            await interaction.response.send_message("❌ 此指令只能在主服务器执行。", ephemeral=True)
            return

        await interaction.response.send_message("⏳ 正在启动手动完全备份... 这可能需要一些时间。", ephemeral=True)

        try:
            # 1. 刷新缓存 (这里我们直接调用内部方法，并将通知发送到备份频道)
            await self._perform_member_cache_refresh(interaction)

            # 2. 生成备份数据
            data = await self._create_backup_data_async(self.backup_guild)

            # 3. 创建文件
            backup_file = await self._create_backup_file_async(data, "MANUAL")

            # 4. 发送到备份频道，并@用户
            await self.backup_channel.send(
                f"✅ **手动备份完成** (由 {interaction.user.mention} 触发)",
                file=backup_file
            )
            await interaction.followup.send("✅ 手动备份已成功完成并发送至备份频道！", ephemeral=True)

        except Exception as e:
            self.logger.error(f"手动备份失败: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 手动备份失败: `{e}`", ephemeral=True)

    # 这是从 CoreCog 移过来的功能，并被重构为内部可调用
    async def _perform_member_cache_refresh(self, interaction: discord.Interaction | None):
        """
        内部使用的成员缓存刷新逻辑。
        如果提供了 interaction, 会在命令处回应进度。
        否则，会将进度发送到备份通知频道。
        """
        guild = self.backup_guild
        if not guild: return

        total_members = guild.member_count
        if total_members == 0: return

        self.logger.info(f"服务器 '{guild.name}' 开始成员缓存刷新。")

        # 决定向哪里发送消息
        responder = interaction.followup if interaction else self.backup_channel
        original_message = None

        # 初始进度条消息
        embed = discord.Embed(
            title="⏳ 正在刷新成员缓存...",
            description=f"目标: **{total_members}** 名成员。",
            color=discord.Color.blue()
        )
        embed.add_field(name="进度", value=create_progress_bar(0, total_members), inline=False)

        # 发送初始消息
        if interaction:
            # 如果是手动命令触发，则私密回应
            await interaction.edit_original_response(content=None, embed=embed)
            original_message = await interaction.original_response()
        else:
            # 如果是自动任务，则公开发布到备份频道
            original_message = await responder.send(embed=embed)

        fetched_count = 0
        last_update_count = 0

        try:
            async for member in guild.fetch_members(limit=None):
                fetched_count += 1
                if fetched_count - last_update_count >= 100 or fetched_count == total_members:
                    last_update_count = fetched_count
                    embed.description = f"正在处理: **{fetched_count} / {total_members}**"
                    embed.set_field_at(0, name="进度", value=create_progress_bar(fetched_count, total_members), inline=False)
                    await original_message.edit(embed=embed)
                    await asyncio.sleep(0.1)

        except Exception as e:
            self.logger.error(f"刷新成员缓存时发生错误: {e}", exc_info=True)
            error_embed = discord.Embed(title="❌ 刷新中断", description=f"发生错误: `{e}`", color=discord.Color.red())
            await original_message.edit(embed=error_embed)
            raise  # 重新抛出异常，让调用方知道失败了

        final_embed = discord.Embed(
            title="✅ 成员缓存刷新完成",
            description=f"成功同步 **{fetched_count} / {total_members}** 名成员信息。",
            color=discord.Color.green()
        )
        final_embed.set_footer(text=f"当前缓存成员数: {len(guild.members)}")
        await original_message.edit(embed=final_embed)

    @backup_admin_group.command(name="刷新成员缓存", description="【耗时】手动拉取服务器所有成员信息到机器人缓存中。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def refresh_member_cache_command(self, interaction: discord.Interaction):
        """这是面向用户的斜杠命令，它调用内部的刷新逻辑。"""
        if interaction.guild_id != self.guild_id:
            await interaction.response.send_message("❌ 此指令只能在主服务器执行。", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False, thinking=True)
        await self._perform_member_cache_refresh(interaction)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(BackupCog(bot))
