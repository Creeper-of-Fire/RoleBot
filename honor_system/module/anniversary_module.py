# honor_system/anniversary_module.py
from __future__ import annotations

import asyncio
import datetime
import typing
from typing import Optional
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


class HonorAnniversaryModuleCog(commands.Cog, name="HonorAnniversaryModule"):
    """【荣誉子模块】管理与成员加入时间相关的荣誉。"""

    def __init__(self, bot: 'RoleBot'):
        self.logger = bot.logger
        self.bot = bot
        self.honor_data_manager = HonorDataManager.getDataManager(logger=bot.logger)
        self.activity_data_manager = ActivityDataManager.getDataManager(logger=bot.logger)

    async def check_and_grant_anniversary_honor(self, member: discord.Member, guild: discord.Guild):
        """
        【按需检查】检查用户是否符合多个周年纪念荣誉的条件。
        此函数在用户与荣誉系统交互时被调用。

        新的配置结构示例 (在 config_data.py 中):
        "anniversary_honor": {
            "enabled": True,
            "tiers": [
                {
                    "honor_uuid": "uuid-for-founder",
                    "cutoff_date": "2021-01-01T00:00:00"
                },
                {
                    "honor_uuid": "uuid-for-veteran",
                    "cutoff_date": "2022-01-01T00:00:00"
                }
            ]
        }
        """
        # 1. 获取配置
        guild_config = config_data.HONOR_CONFIG.get(guild.id, {})
        anniversary_cfg = guild_config.get("anniversary_honor", {})

        # 检查功能是否启用以及是否配置了荣誉等级
        if not anniversary_cfg.get("enabled") or not anniversary_cfg.get("tiers"):
            return

        # 2. 确定用于比较的加入时间 (此部分逻辑不变)
        join_date_to_check: Optional[datetime.datetime] = None
        db_record = self.activity_data_manager.get_join_record(member.id, guild.id)
        if db_record:
            join_date_to_check = db_record.joined_at
        elif member.joined_at:
            join_date_to_check = member.joined_at
            self.activity_data_manager.upsert_join_record(member.id, guild.id, member.joined_at)

        if not join_date_to_check:
            return

        # 3. 准备数据以便比较
        try:
            tz = ZoneInfo("Asia/Shanghai")
        except Exception as e:
            self.logger.error(f"无法加载时区 'Asia/Shanghai': {e}")
            return

        join_date_to_check_aware = join_date_to_check.astimezone(tz)
        user_honors = self.honor_data_manager.get_user_honors(member.id)
        user_honor_uuids = {uh.honor_uuid for uh in user_honors}  # 使用集合以提高查找效率

        # 4. 遍历所有荣誉等级，检查并授予
        for tier in anniversary_cfg["tiers"]:
            honor_uuid = tier.get("honor_uuid")
            cutoff_date_str = tier.get("cutoff_date")

            # 检查当前等级的配置是否完整
            if not honor_uuid or not cutoff_date_str:
                self.logger.warning(f"周年纪念荣誉配置中存在无效的等级（缺少 honor_uuid 或 cutoff_date）: {tier}")
                continue

            # 检查用户是否已拥有此荣誉
            if honor_uuid in user_honor_uuids:
                continue  # 已拥有，检查下一个等级

            # 解析截止日期
            try:
                cutoff_date = datetime.datetime.fromisoformat(cutoff_date_str).replace(tzinfo=tz)
            except ValueError as e:
                self.logger.error(f"周年纪念荣誉等级 'UUID {honor_uuid}' 的 cutoff_date 配置错误: {e}")
                continue

            # 比较时间并授予荣誉
            if join_date_to_check_aware < cutoff_date:
                granted_def = self.honor_data_manager.grant_honor(member.id, honor_uuid)
                if granted_def:
                    self.logger.info(
                        f"[周年荣誉] 用户 {member} ({member.id}) 因加入时间 ({join_date_to_check_aware.date()}) 早于 "
                        f"{cutoff_date.date()} 而获得荣誉 '{granted_def.name}'"
                    )

    anniversary_group = app_commands.Group(
        name="荣誉头衔丨周年纪念", description="管理周年纪念荣誉的数据",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @anniversary_group.command(name="scan_members", description="扫描服务器所有成员的加入时间并存入数据库。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def scan_members_joined_at(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        self.logger.info(f"[{guild.name}] 开始执行成员加入时间全量扫描...")

        # 机器人已经将成员缓存，直接使用 guild.members
        all_members = guild.members
        total_members = len(all_members)

        records_to_upsert = []
        for member in all_members:
            if not member.bot and member.joined_at:
                records_to_upsert.append({
                    "user_id": member.id,
                    "guild_id": guild.id,
                    "joined_at": member.joined_at
                })

        if not records_to_upsert:
            await interaction.followup.send("🤷‍♂️ 没有找到任何可以记录的成员信息。")
            return

        try:
            self.activity_data_manager.bulk_upsert_join_records(records_to_upsert)
            self.logger.info(f"[{guild.name}] 成员扫描完成，成功写入/更新 {len(records_to_upsert)} 条记录。")
            await interaction.followup.send(f"✅ **成员扫描完成！**\n成功处理并存储了 **{len(records_to_upsert)}** / {total_members} 位成员的加入时间信息。")
        except Exception as e:
            self.logger.error(f"[{guild.name}] 批量写入加入记录时出错: {e}", exc_info=True)
            await interaction.followup.send(f"❌ **操作失败！**\n在写入数据库时发生错误: `{e}`")

    @anniversary_group.command(name="scan_channel", description="扫描欢迎频道的历史消息来补全加入时间数据。")
    @app_commands.describe(target_channel="选择包含系统欢迎消息的频道")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def scan_welcome_channel(self, interaction: discord.Interaction, target_channel: discord.TextChannel):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        self.logger.info(f"[{guild.name}] 开始扫描频道 #{target_channel.name} 的历史欢迎消息...")

        log_channel = guild.get_channel(interaction.channel_id) or await guild.fetch_channel(interaction.channel_id)

        progress_message: discord.Message = await log_channel.send(f"[{guild.name}] 开始扫描频道 #{target_channel.name} 的历史欢迎消息...")

        records_to_upsert = []
        processed_count = 0
        try:
            async for message in target_channel.history(limit=None):
                processed_count += 1
                if message.type == discord.MessageType.new_member:
                    # message.author 是加入的用户
                    # message.created_at 是消息创建时间，即加入时间
                    if not message.author.bot:
                        records_to_upsert.append({
                            "user_id": message.author.id,
                            "guild_id": guild.id,
                            "joined_at": message.created_at
                        })
                # 短暂更新状态，让用户知道机器人没死
                if processed_count % 500 == 0:
                    embed = discord.Embed(
                        title="扫描欢迎频道的历史消息来补全加入时间数据",
                        description=f"⏳ 正在扫描... 已处理 {processed_count} 条消息，找到 {len(records_to_upsert)} 条加入记录。",
                        color=discord.Color.green(),
                    )
                    if progress_message:
                        await progress_message.edit(content=None, embed=embed)
                    else:
                        progress_message = await log_channel.send(content=None, embed=embed)
                if processed_count % 100 == 0:
                    await asyncio.sleep(0.5)

            if not records_to_upsert:
                await log_channel.send(
                    f"🤷‍♂️ **扫描完成！**\n在频道 **#{target_channel.name}** 中处理了 {processed_count} 条消息，但没有找到任何有效的系统欢迎消息。")
                return

            self.activity_data_manager.bulk_upsert_join_records(records_to_upsert)
            self.logger.info(f"[{guild.name}] 欢迎频道扫描完成，成功写入/更新 {len(records_to_upsert)} 条记录。")
            await log_channel.send(
                f"✅ **频道扫描完成！**\n总共处理了 {processed_count} 条消息，从中提取并存储了 **{len(records_to_upsert)}** 条加入记录。")

        except discord.Forbidden:
            await log_channel.send(f"❌ **权限不足！**\n我没有权限读取频道 **#{target_channel.name}** 的历史消息。请确保我拥有 `阅读消息历史` 权限。")
        except Exception as e:
            self.logger.error(f"[{guild.name}] 扫描欢迎频道时出错: {e}", exc_info=True)
            await log_channel.send(f"❌ **操作失败！**\n在扫描过程中发生错误: `{e}`")


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(HonorAnniversaryModuleCog(bot))
