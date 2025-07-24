# honor_system/cup_honor_module.py
from __future__ import annotations

import datetime
import json
import logging
import os
import threading
import typing
from typing import List
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config_data
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

    async def _check_guild_for_expired_titles(self, guild: discord.Guild, cup_cfg: dict, now: datetime.datetime):
        """处理单个服务器的过期检查逻辑。"""
        titles = cup_cfg.get("titles", {})
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
    cup_honor_group = app_commands.Group(name="杯赛头衔", description="管理特殊的杯赛头衔",
                                         guild_only=True, default_permissions=discord.Permissions(manage_roles=True))

    async def honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """为杯赛荣誉UUID参数提供自动补全选项。"""
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        cup_honor_titles = guild_config.get("cup_honor", {}).get("titles", {})
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

        # 1. 检查UUID是否为已配置的杯赛头衔
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        cup_honor_titles = guild_config.get("cup_honor", {}).get("titles", {})
        if honor_uuid not in cup_honor_titles:
            await interaction.followup.send("❌ **操作失败**：这个荣誉不是一个已配置的杯赛头衔。", ephemeral=True)
            return

        # 2. 授予荣誉记录
        granted_def = self.honor_data_manager.grant_honor(member.id, honor_uuid)
        honor_def = granted_def or self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)

        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        response_lines = []
        if granted_def:
            response_lines.append(f"🏅 已为 {member.mention} 授予荣誉 **{honor_def.name}**。")
        else:
            response_lines.append(f"☑️ {member.mention} 已拥有荣誉 **{honor_def.name}**。")

        # 3. 授予身份组
        if not honor_def.role_id:
            response_lines.append(f"⚠️ **警告**：此荣誉未关联任何身份组。")
            await interaction.followup.send("\n".join(response_lines), ephemeral=True)
            return

        role = interaction.guild.get_role(honor_def.role_id)
        if not role:
            response_lines.append(f"❌ **错误**：未在服务器中找到对应的身份组（ID: {honor_def.role_id}）。")
            await interaction.followup.send("\n".join(response_lines), ephemeral=True)
            return

        if role not in member.roles:
            try:
                await member.add_roles(role, reason=f"由 {interaction.user} 手动授予杯赛头衔")
                response_lines.append(f"✅ 已为用户佩戴身份组 {role.mention}。")
            except discord.Forbidden:
                response_lines.append(f"❌ **权限不足**：我无法为用户添加身份组 {role.mention}。")
            except Exception as e:
                self.logger.error(f"为用户 {member} 添加杯赛角色 {role.name} 时出错: {e}", exc_info=True)
                response_lines.append(f"❌ **未知错误**：添加身份组时发生错误。")
        else:
            response_lines.append(f"☑️ 用户已佩戴身份组 {role.mention}。")

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


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CupHonorModuleCog(bot))
