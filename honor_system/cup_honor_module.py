# honor_system/cup_honor_module.py
from __future__ import annotations

import datetime
import json
import logging
import os
import re
import threading
import typing
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config_data
from utility.views import ConfirmationView
from .honor_data_manager import HonorDataManager
from .models import UserHonor

if typing.TYPE_CHECKING:
    from main import RoleBot

DATA_FILE_PATH = os.path.join('data', 'cup_honor_notified.json')


class NotificationStateManager:
    """
    一个单例类，用于管理已发送通知的杯赛荣誉状态，并将其持久化到JSON文件中。
    """
    _instance = None
    _lock = threading.Lock()

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.notified_uuids: set[str] = set()
        self._ensure_data_file()
        self.load_state()

    @classmethod
    def get_instance(cls, logger: logging.Logger) -> 'NotificationStateManager':
        """获取本类的单例实例。"""
        if cls._instance is None:
            if cls._instance is None:
                cls._instance = cls(logger)
        return cls._instance

    def _ensure_data_file(self):
        """确保数据文件和目录存在。"""
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
        if not os.path.exists(DATA_FILE_PATH):
            with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump([], f)  # 初始为空列表

    def load_state(self):
        """从JSON文件加载已通知的UUID列表。"""
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.notified_uuids = set(data)
                    self.logger.info(f"成功从 {DATA_FILE_PATH} 加载了 {len(self.notified_uuids)} 条已通知荣誉记录。")
            except (IOError, json.JSONDecodeError) as e:
                self.logger.error(f"无法加载杯赛荣誉通知状态: {e}", exc_info=True)
                self.notified_uuids = set()

    def _save_state(self):
        """将当前状态保存到JSON文件。"""
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                    # JSON不支持set，需要转换为list
                    json.dump(list(self.notified_uuids), f, indent=4)
            except IOError as e:
                self.logger.error(f"无法保存杯赛荣誉通知状态: {e}", exc_info=True)

    def add_notified(self, honor_uuid: str):
        """将一个UUID标记为已通知，并立即保存。"""
        if honor_uuid not in self.notified_uuids:
            self.notified_uuids.add(honor_uuid)
            self._save_state()
            self.logger.info(f"已将荣誉 {honor_uuid} 标记为已通知并持久化。")

    def has_been_notified(self, honor_uuid: str) -> bool:
        """检查一个UUID是否已被通知。"""
        return honor_uuid in self.notified_uuids


class CupHonorModuleCog(commands.Cog, name="CupHonorModule"):
    """【荣誉子模块】管理手动的、有时效性的杯赛头衔。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger
        self.honor_data_manager = HonorDataManager.getDataManager(logger=self.logger)
        # 用于存储已发送过通知的荣誉UUID，防止重复提醒
        self.notification_manager = NotificationStateManager.get_instance(logger=self.logger)
        self.expiration_check_loop.start()

    def cog_unload(self):
        """当Cog被卸载时，取消后台任务。"""
        self.expiration_check_loop.cancel()

    # --- 后台任务：检查过期的杯赛头衔 ---
    async def _perform_expiration_check(self):
        """
        执行一次完整的杯赛头衔到期检查。
        此方法被启动任务和定时循环共同调用。
        """
        self.logger.info("正在执行杯赛头衔到期检查...")
        try:
            now_aware = datetime.datetime.now(ZoneInfo("Asia/Shanghai"))

            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                cup_cfg = guild_config.get("cup_honor", {})
                if not cup_cfg.get("enabled"):
                    continue

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    self.logger.warning(f"无法找到服务器 {guild_id}，跳过杯赛头衔检查。")
                    continue

                await self._check_guild_for_expired_titles(guild, cup_cfg, now_aware)
        except Exception as e:
            self.logger.error(f"杯赛头衔到期检查任务发生未知错误: {e}", exc_info=True)

    @tasks.loop(hours=24)
    async def expiration_check_loop(self):
        """每天运行一次，检查是否有杯赛头衔到期，并通知管理员。"""
        await self._perform_expiration_check()

    # --- [核心改动] 3. 修改 before_loop，在启动时也调用辅助方法 ---
    @expiration_check_loop.before_loop
    async def before_expiration_check(self):
        """在任务开始前，等待机器人完全准备好，并立即执行一次检查。"""
        await self.bot.wait_until_ready()
        self.logger.info("机器人已就绪。正在执行启动时的杯赛头衔到期检查...")
        await self._perform_expiration_check()

    @staticmethod
    def _extract_cup_titles_from_definitions(guild_config: dict) -> dict:
        """
        从主荣誉定义列表中提取所有杯赛头衔。
        通过检查每个定义中是否存在 `cup_honor` 键来实现。

        Args:
            guild_config: 单个服务器的 HONOR_CONFIG[guild_id] 配置字典。

        Returns:
            一个字典，格式为 {honor_uuid: {"expiration_date": "YYYY-MM-DD..."}}，
            以便与模块内其他逻辑兼容。
        """
        cup_titles = {}
        definitions = guild_config.get("definitions", [])
        for honor_def in definitions:
            cup_info = honor_def.get("cup_honor")
            # 确保 cup_info 是一个字典并且包含 expiration_date
            if isinstance(cup_info, dict) and "expiration_date" in cup_info:
                honor_uuid = honor_def.get("uuid")
                if honor_uuid:
                    cup_titles[honor_uuid] = {
                        "expiration_date": cup_info["expiration_date"]
                    }
        return cup_titles

    async def _check_guild_for_expired_titles(self, guild: discord.Guild, cup_cfg: dict, now: datetime.datetime):
        """处理单个服务器的过期检查逻辑。"""
        guild_config = config_data.HONOR_CONFIG.get(guild.id, {})
        titles = self._extract_cup_titles_from_definitions(guild_config)
        notification_cfg = cup_cfg.get("notification", {})

        if not titles or not notification_cfg.get("channel_id") or not notification_cfg.get("admin_role_id"):
            self.logger.warning(f"服务器 {guild.name} 的杯赛头衔配置不完整，跳过。")
            return

        for honor_uuid, title_info in titles.items():
            if self.notification_manager.has_been_notified(honor_uuid):
                continue  # 已处理过，跳过

            try:
                # --- 【核心修改】---
                # 1. 从配置中解析日期字符串
                exp_date_str = title_info["expiration_date"]
                parsed_date = datetime.datetime.fromisoformat(exp_date_str)

                # 2. 如果解析出的日期不带时区信息 (naive)，则强制赋予中国时区。
                #    这允许在config中写 "2025-09-01T00:00:00" 而不是必须带 "+08:00"。
                if parsed_date.tzinfo is None:
                    expiration_date = parsed_date.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
                else:
                    expiration_date = parsed_date  # 如果已带时区，则尊重它
            except (ValueError, KeyError) as e:
                self.logger.error(f"无法解析荣誉 {honor_uuid} 的过期时间: {e}")
                continue

            if now >= expiration_date:
                self.logger.info(f"荣誉 {honor_uuid} 在服务器 {guild.name} 已过期，开始检查用户...")
                await self._notify_admin_for_expired_honor(guild, honor_uuid, expiration_date, notification_cfg)
                self.notification_manager.add_notified(honor_uuid)

    async def _notify_admin_for_expired_honor(self, guild: discord.Guild, honor_uuid: str, exp_date: datetime.datetime,
                                              notify_cfg: dict):
        """为单个过期的荣誉构建并发送通知。
        此版本逻辑基于数据库记录，并确保即使没有成员佩戴身份组也会发送通知。
        """
        # 1. 获取荣誉定义
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            self.logger.warning(f"荣誉 {honor_uuid} 定义无效或未关联身份组，无法发送到期通知。")
            return

        role = guild.get_role(honor_def.role_id)

        # 2. 从数据库获取所有拥有此荣誉的用户，并检查哪些人仍佩戴对应身份组
        members_to_action = []
        with self.honor_data_manager.get_db() as db:
            # 查找所有被授予该荣誉的用户记录
            user_honor_records = db.query(UserHonor).filter(UserHonor.honor_uuid == honor_uuid).all()

            # 仅当身份组实际存在时，才检查哪些成员仍需处理
            if role:
                for record in user_honor_records:
                    member = guild.get_member(record.user_id)
                    # 检查成员是否仍在服务器且拥有该身份组
                    if member and role in member.roles:
                        members_to_action.append(member)

        # 3. 获取通知所需的对象
        notification_channel = guild.get_channel(notify_cfg["channel_id"]) or await guild.fetch_channel(notify_cfg["channel_id"])
        admin_role = guild.get_role(notify_cfg["admin_role_id"])

        if not notification_channel or not admin_role:
            self.logger.error(f"无法在服务器 {guild.name} 中找到通知频道或管理员身份组。")
            return

        # 4. 构建并发送通知 (无论是否有人需要处理)
        embed = discord.Embed(
            title="🏆 杯赛头衔身份组到期提醒",
            color=discord.Color.orange()
        )
        embed.set_footer(text=f"荣誉: {honor_def.name} | UUID: {honor_uuid}")

        # 根据是否有人需要处理来定制消息
        if members_to_action:
            embed.description = (
                f"以下成员佩戴的荣誉身份组 {role.mention} "
                f"已于 `{exp_date.strftime('%Y-%m-%d')}` 到期。\n"
                f"请管理员手动移除他们的身份组，其荣誉勋章将被永久保留。"
            )
            member_mentions = " ".join([m.mention for m in members_to_action])
            embed.add_field(name="需要处理的成员列表", value=member_mentions, inline=False)
        else:
            role_mention = role.mention if role else f"`{honor_def.name}` (身份组可能已被删除)"
            embed.description = (
                f"荣誉 **{honor_def.name}** (关联身份组: {role_mention}) "
                f"已于 `{exp_date.strftime('%Y-%m-%d')}` 到期。"
            )
            embed.add_field(
                name="状态检查",
                value="根据数据库记录，当前没有成员佩戴此身份组。",
                inline=False
            )
            embed.add_field(
                name="建议操作",
                value="管理员可以考虑从服务器的身份组列表中删除此身份组，以保持列表整洁。",
                inline=False
            )

        try:
            await notification_channel.send(content=admin_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[admin_role]))
            self.logger.info(f"已在服务器 {guild.name} 发送关于荣誉 {honor_def.name} 的到期通知。")
        except discord.Forbidden:
            self.logger.error(f"无法在频道 {notification_channel.name} 发送通知，权限不足。")

    # --- 管理员指令 ---
    cup_honor_group = app_commands.Group(
        name="杯赛头衔", description="管理特殊的杯赛头衔",
        guild_only=True, default_permissions=discord.Permissions(manage_roles=True)
    )

    async def honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """为杯赛荣誉UUID参数提供自动补全选项。"""
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        cup_honor_titles = self._extract_cup_titles_from_definitions(guild_config)
        cup_honor_uuids = list(cup_honor_titles.keys())

        if not cup_honor_uuids:
            return []

        all_defs = self.honor_data_manager.get_all_honor_definitions(interaction.guild_id)
        defs_map = {d.uuid: d for d in all_defs}

        choices = []
        for uuid in cup_honor_uuids:
            honor_def = defs_map.get(uuid)
            if honor_def:
                choice_name = f"{honor_def.name} ({honor_def.uuid[:8]})"
                if current.lower() in choice_name.lower():
                    choices.append(app_commands.Choice(name=choice_name, value=uuid))

        return choices[:25]

    @cup_honor_group.command(name="授予", description="为用户手动授予一个杯赛头衔及其身份组。")
    @app_commands.describe(member="要授予头衔的成员", honor_uuid="要授予的杯赛头衔")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def grant(self, interaction: discord.Interaction, member: discord.Member, honor_uuid: str):
        await interaction.response.defer(ephemeral=True)

        # 1. 验证荣誉UUID是否已在配置中
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        cup_honor_titles = self._extract_cup_titles_from_definitions(guild_config)
        if honor_uuid not in cup_honor_titles:
            await interaction.followup.send("❌ **操作失败**：这个荣誉不是一个已配置的杯赛头衔。", ephemeral=True)
            return

        # 2. 尝试授予荣誉（核心数据库操作）
        #    - 如果成功授予，granted_def 会是 honor_def 对象
        #    - 如果用户已拥有，granted_def 会是 None
        granted_def = self.honor_data_manager.grant_honor(member.id, honor_uuid)
        honor_def = granted_def or self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)

        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        # 3. 根据授予结果决定后续操作和响应
        response_lines = []

        if not granted_def:
            # 情况A: 用户已拥有此荣誉
            response_lines.append(f"☑️ {member.mention} 已拥有荣誉 **{honor_def.name}**。")
            response_lines.append("ℹ️ 未进行身份组操作，因为用户已持有该荣誉。")

            await interaction.followup.send("\n".join(response_lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return  # 操作到此结束

        # 情况B: 成功为用户新授予了荣誉
        response_lines.append(f"🏅 已为 {member.mention} 授予荣誉 **{honor_def.name}**。")

        # 4. 尝试授予关联的身份组 (仅在荣誉是新授予时执行)
        if not honor_def.role_id:
            response_lines.append(f"⚠️ **提示**：此荣誉未关联任何身份组，无需佩戴。")
        else:
            role = interaction.guild.get_role(honor_def.role_id)
            if not role:
                response_lines.append(f"❌ **警告**：荣誉已授予，但在服务器中未找到对应的身份组 (ID: {honor_def.role_id})。请联系管理员检查配置。")
            elif role in member.roles:
                response_lines.append(f"☑️ 用户已佩戴身份组 {role.mention}。")
            else:
                try:
                    await member.add_roles(role, reason=f"由 {interaction.user} 手动授予杯赛头衔")
                    response_lines.append(f"✅ 已为用户佩戴身份组 {role.mention}。")
                except discord.Forbidden:
                    response_lines.append(f"❌ **权限不足**：荣誉已授予，但我无法为用户添加身份组 {role.mention}。")
                except Exception as e:
                    self.logger.error(f"为用户 {member} 添加杯赛角色 {role.name} 时出错: {e}", exc_info=True)
                    response_lines.append(f"❌ **未知错误**：荣誉已授予，但添加身份组时发生错误。")

        # 5. 发送最终的合并报告
        await interaction.followup.send("\n".join(response_lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @cup_honor_group.command(name="从身份组同步", description="将一个杯赛头衔授予所有拥有对应身份组的成员。")
    @app_commands.describe(honor_uuid="选择要同步的杯赛头衔，机器人将为拥有其身份组的成员补发荣誉。")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def sync_from_role(self, interaction: discord.Interaction, honor_uuid: str):
        """
        一个便捷工具，用于将荣誉授予所有已拥有对应身份组的成员。
        这对于修复那些被手动赋予身份组但未记录荣誉的成员很有用。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 验证荣誉和身份组
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        if not honor_def.role_id:
            await interaction.followup.send(f"❌ **操作失败**：荣誉 **{honor_def.name}** 没有关联任何身份组，无法进行同步。", ephemeral=True)
            return

        role = guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(f"❌ **错误**：在服务器中找不到与荣誉关联的身份组（ID: {honor_def.role_id}）。", ephemeral=True)
            return

        # 2. 获取成员并处理
        members_with_role = role.members
        if not members_with_role:
            await interaction.followup.send(f"🤷 **无需操作**：没有找到任何成员拥有 {role.mention} 身份组。", ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())
            return

        newly_granted_count = 0
        already_had_count = 0

        for member in members_with_role:
            if member.bot:
                continue

            # grant_honor 方法如果成功授予则返回定义，如果已存在则返回None
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted_count += 1
            else:
                already_had_count += 1

        self.logger.info(
            f"管理员 {interaction.user} 在服务器 {guild.name} "
            f"对荣誉 '{honor_def.name}' 执行了从身份组同步操作。 "
            f"新授予: {newly_granted_count}, 已拥有: {already_had_count}."
        )

        # 3. 发送报告
        embed = discord.Embed(
            title="✅ 荣誉同步完成",
            description=f"已为所有拥有 {role.mention} 身份组的成员检查并补发了荣誉 **{honor_def.name}**。",
            color=discord.Color.green()
        )
        embed.add_field(name="总共检查成员", value=f"`{len(members_with_role)}` 人", inline=True)
        embed.add_field(name="新授予荣誉", value=f"`{newly_granted_count}` 人", inline=True)
        embed.add_field(name="本就拥有荣誉", value=f"`{already_had_count}` 人", inline=True)
        embed.set_footer(text="此操作确保了所有拥有身份组的成员都在荣誉系统中正确记录。")

        await interaction.followup.send(embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @cup_honor_group.command(name="批量授予", description="批量授予一个杯赛头衔给多个用户。")
    @app_commands.describe(
        honor_uuid="要授予的杯赛头衔。",
        user_ids="【模式一】要授予的用户的ID，用英文逗号分隔。",
        message_link="【模式二】包含目标用户的消息链接，将授予所有被提及的用户。"
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def bulk_grant(
            self,
            interaction: discord.Interaction,
            honor_uuid: str,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ):
        """批量授予杯赛头衔，支持从ID列表或消息链接中解析用户。"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 输入验证
        if not user_ids and not message_link:
            await interaction.followup.send("❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True)
            return
        if user_ids and message_link:
            await interaction.followup.send("❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True)
            return

        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        # 2. 收集用户
        try:
            members_to_process, error_logs = await self._parse_members_from_input(guild, user_ids, message_link)
        except (ValueError, IOError) as e:
            await interaction.followup.send(f"❌ **操作失败**：{e}", ephemeral=True)
            return

        if not members_to_process:
            final_message = "🤷 **操作终止**：未找到任何有效的、非机器人的用户进行操作。"
            if error_logs:
                final_message += "\n\n**解析遇到的问题：**\n" + "\n".join(error_logs)
            await interaction.followup.send(final_message, ephemeral=True)
            return

        # 3. 确认环节
        member_mentions = " ".join([m.mention for m in members_to_process])
        if len(member_mentions) > 1000:
            member_mentions = f"共 {len(members_to_process)} 人，列表过长已省略。"

        embed = discord.Embed(
            title="⚠️ 批量授予确认",
            description=f"你即将为以下 **{len(members_to_process)}** 位成员授予荣誉：\n**{honor_def.name}**",
            color=discord.Color.orange()
        )
        embed.add_field(name="目标成员", value=member_mentions, inline=False)
        embed.set_footer(text="请确认操作。此操作将在后台进行。")

        view = ConfirmationView(author=interaction.user)
        # 将消息对象存入视图，以便超时后编辑
        view.message = await interaction.followup.send(
            embed=embed, view=view, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
        )
        await view.wait()

        # 4. 执行或取消
        if view.value is None:  # 超时
            return
        if not view.value:
            await interaction.edit_original_response(content="操作已取消。", embed=None, view=None)
            return

        await interaction.edit_original_response(content="⚙️ 正在处理，请稍候...", embed=None, view=None)

        newly_granted, already_had, role_added, role_failed = [], [], [], []
        role = guild.get_role(honor_def.role_id) if honor_def.role_id else None

        for member in members_to_process:
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted.append(member)
            else:
                already_had.append(member)

            if role and role not in member.roles:
                try:
                    await member.add_roles(role, reason=f"由 {interaction.user} 批量授予杯赛头衔")
                    role_added.append(member)
                except discord.Forbidden:
                    role_failed.append(member)
                except Exception:
                    role_failed.append(member)

        # 5. 最终报告
        final_embed = discord.Embed(
            title="✅ 批量授予完成",
            description=f"已完成对 **{honor_def.name}** 荣誉的批量授予操作。",
            color=discord.Color.green()
        )
        final_embed.add_field(name="总处理人数", value=f"`{len(members_to_process)}` 人", inline=False)
        final_embed.add_field(name="新授予荣誉", value=f"`{len(newly_granted)}` 人", inline=True)
        final_embed.add_field(name="本已拥有", value=f"`{len(already_had)}` 人", inline=True)

        role_status_parts = []
        if role:
            role_status_parts.append(f"新佩戴: `{len(role_added)}`")
            if role_failed:
                role_status_parts.append(f"失败: `{len(role_failed)}`")
            role_status = " | ".join(role_status_parts)
        else:
            role_status = "未关联身份组"

        final_embed.add_field(name="身份组状态", value=role_status, inline=True)

        if error_logs:
            final_embed.add_field(name="解析警告", value="\n".join(error_logs[:5]), inline=False)

        await interaction.edit_original_response(content="", embed=final_embed)

    @staticmethod
    async def _parse_members_from_input(
            guild: discord.Guild,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ) -> Tuple[Set[discord.Member], List[str]]:
        """
        [辅助函数] 从用户ID列表或消息链接中解析成员。
        返回一个包含成员对象的集合和一份错误/警告日志。
        """
        members_to_process: Set[discord.Member] = set()
        error_logs: List[str] = []

        if user_ids:
            id_list = {uid.strip() for uid in user_ids.split(',')}
            for uid_str in id_list:
                if not uid_str.isdigit():
                    error_logs.append(f"无效ID格式: `{uid_str}`")
                    continue
                try:
                    # 使用 get_member 优先从缓存获取，失败再 fetch
                    member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                    if not member.bot:
                        members_to_process.add(member)
                except discord.NotFound:
                    error_logs.append(f"未找到用户: `{uid_str}`")

        elif message_link:
            match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)', message_link)
            if not match or int(match.group(1)) != guild.id:
                raise ValueError("无效的消息链接，或链接不属于本服务器。")

            channel_id, message_id = int(match.group(2)), int(match.group(3))
            try:
                channel = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
                message = await channel.fetch_message(message_id)

                all_mentioned_members = set(message.mentions)
                content_to_scan = message.content
                for embed in message.embeds:
                    if embed.description: content_to_scan += "\n" + embed.description
                    for field in embed.fields: content_to_scan += f"\n{field.name}\n{field.value}"

                mentioned_ids = re.findall(r'<@!?(\d+)>', content_to_scan)
                for uid_str in set(mentioned_ids):
                    try:
                        member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                        if member: all_mentioned_members.add(member)
                    except discord.NotFound:
                        error_logs.append(f"消息中提及的用户 `{uid_str}` 未找到。")

                for member in all_mentioned_members:
                    if not member.bot:
                        members_to_process.add(member)

            except (discord.NotFound, discord.Forbidden) as e:
                raise IOError(f"找不到指定的消息/频道，或我没有权限访问它: {e}")

        return members_to_process, error_logs

    @cup_honor_group.command(name="设置最终持有者-危险操作-仅必要时", description="设置头衔的最终持有者，并移除名单外成员的身份组。")
    @app_commands.describe(
        honor_uuid="要操作的杯赛头衔。",
        user_ids="【模式一】最终持有者的ID，用英文逗号分隔。",
        message_link="【模式二】包含最终持有者的消息链接。"
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def set_holders(
            self,
            interaction: discord.Interaction,
            honor_uuid: str,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ):
        """将提供的用户列表设置为荣誉的唯一持有者，并从其他人身上移除对应身份组。"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 输入验证和解析
        if not user_ids and not message_link:
            await interaction.followup.send("❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True)
            return
        if user_ids and message_link:
            await interaction.followup.send("❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True)
            return

        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            await interaction.followup.send(f"❌ **错误**：此荣誉未定义或未关联身份组，无法执行同步操作。", ephemeral=True)
            return

        role = guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(f"❌ **错误**：在服务器中找不到与荣誉关联的身份组 (ID: {honor_def.role_id})。", ephemeral=True)
            return

        try:
            definitive_members, error_logs = await self._parse_members_from_input(guild, user_ids, message_link)
        except (ValueError, IOError) as e:
            await interaction.followup.send(f"❌ **操作失败**：{e}", ephemeral=True)
            return

        # 2. 计算差异
        current_role_holders = set(role.members)

        members_to_add = definitive_members - current_role_holders
        members_to_remove = current_role_holders - definitive_members
        members_to_keep = definitive_members.intersection(current_role_holders)

        if not members_to_add and not members_to_remove:
            final_message = "🤷 **无需操作**：提供的名单与当前身份组持有者完全一致。"
            if error_logs:
                final_message += "\n\n**解析遇到的问题：**\n" + "\n".join(error_logs)
            await interaction.followup.send(final_message, ephemeral=True)
            return

        # 3. 确认环节
        embed = discord.Embed(
            title="‼️ 高危操作确认：设置最终持有者",
            description=f"你即将同步荣誉 **{honor_def.name}** 及其身份组 {role.mention}。\n"
                        f"**提供的名单将被视为唯一合法的持有者名单。**",
            color=discord.Color.red()
        )
        embed.add_field(name="✅ 新增身份组", value=f"`{len(members_to_add)}` 人", inline=True)
        embed.add_field(name="❌ 移除身份组", value=f"`{len(members_to_remove)}` 人", inline=True)
        embed.add_field(name="☑️ 保持不变", value=f"`{len(members_to_keep)}` 人", inline=True)
        embed.set_footer(text="请仔细核对，此操作不可逆！")

        if members_to_remove:
            remove_list_str = " ".join([m.mention for m in members_to_remove])
            if len(remove_list_str) > 1020:
                remove_list_str = f"共 {len(members_to_remove)} 人，列表过长已省略。"
            embed.add_field(name="将被移除身份组的成员", value=remove_list_str, inline=False)

        view = ConfirmationView(author=interaction.user, timeout=120.0)
        view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        await view.wait()

        # 4. 执行或取消
        if view.value is None: return
        if view.value is False:
            await interaction.edit_original_response(content="操作已取消。", embed=None, view=None)
            return

        await interaction.edit_original_response(content="⚙️ **正在执行同步...** 这可能需要一些时间。", embed=None, view=None)

        # 5. 执行操作并记录结果
        newly_granted, role_added_ok, role_removed_ok = 0, 0, 0
        role_add_failed, role_remove_failed = [], []

        # 授予荣誉给所有最终名单成员
        for member in definitive_members:
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted += 1

        # 添加身份组
        for member in members_to_add:
            try:
                await member.add_roles(role, reason=f"由 {interaction.user} 执行“设置持有者”操作")
                role_added_ok += 1
            except Exception:
                role_add_failed.append(member.mention)

        # 移除身份组
        for member in members_to_remove:
            try:
                await member.remove_roles(role, reason=f"由 {interaction.user} 执行“设置持有者”操作")
                role_removed_ok += 1
            except Exception:
                role_remove_failed.append(member.mention)

        # 6. 最终报告
        final_embed = discord.Embed(
            title="✅ 同步操作完成",
            description=f"已根据你的名单，完成对荣誉 **{honor_def.name}** ({role.mention}) 的持有者设置。",
            color=discord.Color.green()
        )
        final_embed.add_field(name="最终持有者总数", value=f"`{len(definitive_members)}` 人", inline=False)
        final_embed.add_field(name="新授予荣誉记录", value=f"`{newly_granted}` 人", inline=True)
        final_embed.add_field(name="新佩戴身份组", value=f"`{role_added_ok}` 人", inline=True)
        final_embed.add_field(name="被移除身份组", value=f"`{role_removed_ok}` 人", inline=True)

        if role_add_failed or role_remove_failed:
            error_details = ""
            if role_add_failed:
                error_details += f"**添加失败 ({len(role_add_failed)}人):** {' '.join(role_add_failed)}\n"
            if role_remove_failed:
                error_details += f"**移除失败 ({len(role_remove_failed)}人):** {' '.join(role_remove_failed)}"
            final_embed.add_field(name="⚠️ 操作失败详情 (通常为权限问题)", value=error_details, inline=False)

        await interaction.edit_original_response(content="", embed=final_embed)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CupHonorModuleCog(bot))
