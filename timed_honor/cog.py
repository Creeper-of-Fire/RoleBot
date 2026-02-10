from __future__ import annotations

import asyncio
import datetime
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import tasks

import config
from activity_tracker.data_manager import DataManager as ActivityDataManager
from honor_system.honor_data_manager import HonorDataManager
from honor_system.models import HonorDefinition, UserHonor
from utility.feature_cog import FeatureCog, PanelEntry
from utility.helpers import try_get_member

from .panel_store import PanelStore
from .requirements_manager import RequirementEvaluateResult, RequirementsManager
from .views import TimedHonorDetailView, TimedHonorPublicOpenPanelView, TimedHonorSelectView

if TYPE_CHECKING:
    from main import RoleBot


class TimedHonorCog(FeatureCog, name="TimedHonor"):
    """限时荣誉 Cog：命令、交互编排、过期清理任务。"""

    timed_honor_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨荣誉丨限时",
        description="限时荣誉管理",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    def __init__(self, bot: "RoleBot"):
        super().__init__(bot)
        self.honor_data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.activity_data_manager = ActivityDataManager(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            logger=self.logger,
        )
        self.requirements_manager = RequirementsManager(
            honor_data_manager=self.honor_data_manager,
            activity_data_manager=self.activity_data_manager,
        )
        self.panel_store = PanelStore(logger=self.logger)

        self.safe_timed_honor_role_ids: Set[int] = set()

    async def cog_load(self) -> None:
        await super().cog_load()
        self.bot.add_view(TimedHonorPublicOpenPanelView(self))
        if not self.expire_cleanup_task.is_running():
            self.expire_cleanup_task.start()
        self.logger.info("TimedHonorCog 已加载，持久化入口按钮与过期任务已启动。")

    def cog_unload(self):
        if self.expire_cleanup_task.is_running():
            self.expire_cleanup_task.cancel()

    async def update_safe_roles_cache(self):
        """更新本模块管理到的角色缓存（供 CoreCog 统一缓存用途）。"""
        self._safe_reload_requirements()

        new_cache: Set[int] = set()
        core_cog = self.core_cog
        for honor_uuid in self.requirements_manager.get_available_honors():
            honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
            if not honor_def:
                self.logger.warning(f"TimedHonor: available honor 不存在定义: {honor_uuid}")
                continue

            if honor_def.role_id:
                new_cache.add(honor_def.role_id)

                if core_cog and core_cog.role_name_cache is not None:
                    guild = self.bot.get_guild(honor_def.guild_id)
                    role = guild.get_role(honor_def.role_id) if guild else None
                    if role:
                        core_cog.role_name_cache[role.id] = role.name

        self.safe_timed_honor_role_ids = new_cache

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        # 本模块按需求走管理员命令召唤，不放入主面板。
        return None

    # -------------------------
    # Admin command
    # -------------------------
    @timed_honor_group.command(name="发送升级面板", description="发送限时荣誉升级入口面板")
    async def send_upgrade_panel(self, interaction: discord.Interaction):
        if not self._is_admin_user(interaction.user.id):
            await self.send_ephemeral(interaction, "❌ 你没有权限执行此命令。")
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await self.send_ephemeral(interaction, "❌ 该命令仅支持在文字频道使用。")
            return

        if not self._is_panel_channel_allowed(interaction.channel.id):
            await self.send_ephemeral(interaction, "❌ 当前频道不在允许发送升级面板的范围内。")
            return

        if not self._safe_reload_requirements():
            await self.send_ephemeral(interaction, "❌ requirements 配置加载失败，请先修正配置文件。")
            return

        embed = self._build_public_panel_embed()
        view = TimedHonorPublicOpenPanelView(self)

        await interaction.response.defer(ephemeral=True)
        msg = await interaction.channel.send(embed=embed, view=view)

        self.panel_store.add_panel(
            message_id=msg.id,
            channel_id=msg.channel.id,
            guild_id=interaction.guild_id,
            created_by=interaction.user.id,
        )

        await interaction.followup.send(
            f"✅ 已发送升级面板到 {interaction.channel.mention}\n消息ID: `{msg.id}`",
            ephemeral=True,
        )

    # -------------------------
    # Views callbacks
    # -------------------------
    async def handle_open_panel(self, interaction: discord.Interaction):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not guild or not member:
            await self.send_ephemeral(interaction, "❌ 该功能只能在服务器内使用。")
            return

        if not self._safe_reload_requirements():
            await self.send_ephemeral(interaction, "❌ requirements 配置加载失败，请联系管理员。")
            return

        await self.cleanup_expired_honors_for_member(member, guild)

        honor_defs = self._get_available_honor_definitions_for_guild(guild)
        if not honor_defs:
            await self.send_ephemeral(interaction, "ℹ️ 当前服务器暂无可领取的限时荣誉。")
            return

        options: List[discord.SelectOption] = []
        for honor_def in honor_defs[:25]:
            desc = (honor_def.description or "无描述").replace("\n", " ")
            options.append(
                discord.SelectOption(
                    label=honor_def.name[:100],
                    description=desc[:100],
                    value=honor_def.uuid,
                )
            )

        view = TimedHonorSelectView(self, owner_id=member.id, options=options)
        embed = self._build_select_panel_embed(guild, len(options))

        await self.send_ephemeral(interaction, embed=embed, view=view)

    async def handle_select_honor(self, interaction: discord.Interaction, honor_uuid: str):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not guild or not member:
            await self.send_ephemeral(interaction, "❌ 该功能只能在服务器内使用。")
            return

        await self.cleanup_expired_honors_for_member(member, guild)

        if honor_uuid not in self.requirements_manager.get_available_honors():
            await self.send_ephemeral(interaction, "❌ 该荣誉未上架或已下架。")
            return

        detail_embed, detail_view = self._build_honor_detail(member, guild, honor_uuid)
        if detail_embed is None or detail_view is None:
            await self.send_ephemeral(interaction, "❌ 荣誉信息异常，请联系管理员。")
            return

        await interaction.response.edit_message(embed=detail_embed, view=detail_view)

    async def handle_claim_honor(self, interaction: discord.Interaction, honor_uuid: str):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not guild or not member:
            await self.send_ephemeral(interaction, "❌ 该功能只能在服务器内使用。")
            return

        await self.cleanup_expired_honors_for_member(member, guild)

        if not self._check_eligible_member(member):
            await self.send_ephemeral(interaction, "❌ 你当前不具备领取资格（未满足基础身份组门槛）。")
            return

        honor_def, role, err = self._resolve_honor_and_role(guild, honor_uuid)
        if err:
            await self.send_ephemeral(interaction, err)
            return

        result: RequirementEvaluateResult = await self.requirements_manager.evaluate(guild, member, honor_uuid)
        if not result.ok:
            await self.send_ephemeral(interaction, RequirementsManager.build_failure_text(result))
            return

        duration_hours = self.requirements_manager.get_duration_hours(honor_uuid)
        if not duration_hours:
            await self.send_ephemeral(interaction, "❌ 领取时长配置缺失，无法领取。")
            return

        granted = self.honor_data_manager.grant_honor_with_duration_hours(member.id, honor_uuid, duration_hours)
        if granted is None:
            await self._refresh_detail_message(interaction, member, guild, honor_uuid)
            await self.send_ephemeral(interaction, "☑️ 你已拥有该限时荣誉，过期后可续期。")
            return

        equip_message = ""
        try:
            if role not in member.roles:
                await member.add_roles(role, reason="用户领取限时荣誉后自动佩戴")
            equip_message = f"并已佩戴身份组 {role.mention}"
        except discord.Forbidden:
            equip_message = "，但自动佩戴失败（机器人权限不足）"
            self.logger.warning(
                f"TimedHonor: 为用户 {member.id} 自动佩戴角色失败，权限不足。role={role.id}, guild={guild.id}"
            )
        except Exception as e:
            equip_message = "，但自动佩戴失败（发生未知错误）"
            self.logger.error(f"TimedHonor: 自动佩戴失败 user={member.id} role={role.id}: {e}", exc_info=True)

        await self._refresh_detail_message(interaction, member, guild, honor_uuid)
        await self.send_ephemeral(interaction, f"🎉 已成功领取 **{honor_def.name}** {equip_message}")

    async def handle_toggle_wear(self, interaction: discord.Interaction, honor_uuid: str):
        guild = interaction.guild
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not guild or not member:
            await self.send_ephemeral(interaction, "❌ 该功能只能在服务器内使用。")
            return

        await self.cleanup_expired_honors_for_member(member, guild)

        honor_def, role, err = self._resolve_honor_and_role(guild, honor_uuid)
        if err:
            await self.send_ephemeral(interaction, err)
            return

        owned_honors = self.honor_data_manager.get_user_honors(member.id)
        if honor_uuid not in {h.honor_uuid for h in owned_honors}:
            await self.send_ephemeral(interaction, "❌ 你当前未拥有该限时荣誉（可能已过期）。")
            return

        message = ""
        try:
            if role in member.roles:
                await member.remove_roles(role, reason="用户主动摘下限时荣誉身份组")
                message = f"✅ 已摘下身份组 {role.mention}"
            else:
                await member.add_roles(role, reason="用户主动佩戴限时荣誉身份组")
                message = f"✅ 已佩戴身份组 {role.mention}"
        except discord.Forbidden:
            message = "❌ 操作失败：机器人权限不足，无法修改该身份组。"
        except Exception as e:
            self.logger.error(f"TimedHonor: toggle wear 失败 user={member.id} role={role.id}: {e}", exc_info=True)
            message = "❌ 操作失败：发生未知错误，请联系管理员。"

        await self._refresh_detail_message(interaction, member, guild, honor_uuid)
        await self.send_ephemeral(interaction, message)

    # -------------------------
    # Expire cleanup
    # -------------------------
    @tasks.loop(minutes=config.TIMED_HONOR_EXPIRE_CHECK_INTERVAL_MINUTES)
    async def expire_cleanup_task(self):
        if not self._safe_reload_requirements():
            return

        all_uuids = self.requirements_manager.get_available_honors()
        if not all_uuids:
            return

        total_deleted = 0
        total_role_removed = 0
        total_role_failed = 0

        for honor_uuid in all_uuids:
            honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
            if not honor_def:
                continue

            expired_holders = self.honor_data_manager.get_expired_honor_holders(honor_uuid)
            if not expired_holders:
                continue

            user_ids = list({x.user_id for x in expired_holders})

            guild = self.bot.get_guild(honor_def.guild_id)
            if guild and honor_def.role_id:
                role = guild.get_role(honor_def.role_id)
                if role:
                    for uid in user_ids:
                        member = await try_get_member(guild, uid)
                        if not member or role not in member.roles:
                            continue

                        try:
                            await member.remove_roles(role, reason="限时荣誉过期自动移除")
                            total_role_removed += 1
                        except Exception as e:
                            total_role_failed += 1
                            self.logger.warning(
                                f"TimedHonor: 过期移除角色失败 guild={guild.id} user={uid} role={role.id}: {e}"
                            )

            deleted = self.honor_data_manager.revoke_expired_honor_records(honor_uuid)
            total_deleted += deleted

            self.logger.info(
                f"TimedHonor: 过期清理 honor={honor_uuid} users={len(user_ids)} deleted={deleted}"
            )

        if total_deleted or total_role_removed or total_role_failed:
            self.logger.info(
                "TimedHonor: 本轮过期清理完成 "
                f"deleted={total_deleted}, role_removed={total_role_removed}, role_failed={total_role_failed}"
            )

    @expire_cleanup_task.before_loop
    async def before_expire_cleanup_task(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(1)

    async def cleanup_expired_honors_for_member(self, member: discord.Member, guild: discord.Guild) -> int:
        """用户交互前的轻量清理：仅处理 timed_honor 上架荣誉。"""
        now = datetime.datetime.utcnow()
        available = set(self.requirements_manager.get_available_honors())
        if not available:
            return 0

        all_honors = self.honor_data_manager.get_user_honors(member.id, include_expired=True)

        member_role_ids = {r.id for r in member.roles}
        deleted_total = 0

        for record in all_honors:
            if record.honor_uuid not in available:
                continue
            if record.expires_at is None or record.expires_at > now:
                continue

            honor_def = record.definition or self.honor_data_manager.get_honor_definition_by_uuid(record.honor_uuid)
            if not honor_def or honor_def.guild_id != guild.id:
                continue

            if honor_def.role_id and honor_def.role_id in member_role_ids:
                role = guild.get_role(honor_def.role_id)
                if role:
                    try:
                        await member.remove_roles(role, reason="限时荣誉过期（用户交互前轻清理）")
                    except Exception as e:
                        self.logger.warning(
                            f"TimedHonor: 用户级清理移除角色失败 guild={guild.id} user={member.id} role={role.id}: {e}"
                        )

            deleted_total += self.honor_data_manager.revoke_expired_honor_for_user(member.id, record.honor_uuid)

        return deleted_total

    # -------------------------
    # Helper methods
    # -------------------------
    async def send_ephemeral(
        self,
        interaction: discord.Interaction,
        content: Optional[str] = None,
        *,
        embed: Optional[discord.Embed] = None,
        view: Optional[discord.ui.View] = None,
    ):
        if interaction.response.is_done():
            await interaction.followup.send(content=content, embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(content=content, embed=embed, view=view, ephemeral=True)

    def _safe_reload_requirements(self) -> bool:
        try:
            self.requirements_manager.reload()
            return True
        except Exception as e:
            self.logger.error(f"TimedHonor: 加载 requirements/available_honors 失败: {e}", exc_info=True)
            return False

    def _is_admin_user(self, user_id: int) -> bool:
        return user_id in set(config.TIMED_HONOR_ADMIN_USER_IDS)

    def _is_panel_channel_allowed(self, channel_id: int) -> bool:
        allowed = config.TIMED_HONOR_PANEL_CHANNEL_IDS
        if not allowed:
            return True
        return channel_id in set(allowed)

    def _check_eligible_member(self, member: discord.Member) -> bool:
        eligible_roles = set(config.TIMED_HONOR_ELIGIBLE_ROLE_IDS)
        if not eligible_roles:
            return True
        user_roles = {r.id for r in member.roles}
        return not user_roles.isdisjoint(eligible_roles)

    def _get_available_honor_definitions_for_guild(self, guild: discord.Guild) -> List[HonorDefinition]:
        result: List[HonorDefinition] = []

        available = self.requirements_manager.get_available_honors()
        for honor_uuid in available:
            req = self.requirements_manager.get_requirement(honor_uuid)
            if not req:
                self.logger.warning(f"TimedHonor: honor={honor_uuid} 已上架但缺失 requirements，已跳过")
                continue

            honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
            if not honor_def:
                self.logger.warning(f"TimedHonor: honor={honor_uuid} 在定义中不存在，已跳过")
                continue
            if honor_def.guild_id != guild.id:
                continue
            if honor_def.is_archived:
                self.logger.warning(f"TimedHonor: honor={honor_uuid} 已归档，不应上架")
                continue

            result.append(honor_def)

        result.sort(key=lambda x: x.name)
        return result

    def _resolve_honor_and_role(
        self,
        guild: discord.Guild,
        honor_uuid: str,
    ) -> Tuple[Optional[HonorDefinition], Optional[discord.Role], Optional[str]]:
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            return None, None, "❌ 荣誉定义不存在。"
        if honor_def.guild_id != guild.id:
            return None, None, "❌ 该荣誉不属于当前服务器。"
        if honor_def.is_archived:
            return None, None, "❌ 该荣誉已归档，无法操作。"

        if not honor_def.role_id:
            return honor_def, None, "❌ 该荣誉未绑定身份组，无法执行佩戴相关操作。"

        role = guild.get_role(honor_def.role_id)
        if not role:
            return honor_def, None, "❌ 该荣誉绑定的身份组不存在，请联系管理员修复配置。"

        return honor_def, role, None

    def _build_public_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🏅 限时荣誉升级中心",
            description=(
                "点击下方按钮打开个人升级面板。\n"
                "- 支持多项限时荣誉同时佩戴\n"
                "- 未过期重复领取不会延长有效期\n"
                "- 过期后可重新领取"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="领取后会自动尝试佩戴对应身份组")
        return embed

    def _build_select_panel_embed(self, guild: discord.Guild, option_count: int) -> discord.Embed:
        embed = discord.Embed(
            title="📌 荣誉升级面板",
            description="请选择一个限时荣誉，查看条件并执行领取/佩戴操作。",
            color=discord.Color.blue(),
        )
        embed.add_field(name="当前服务器", value=guild.name, inline=True)
        embed.add_field(name="可选荣誉数", value=str(option_count), inline=True)
        return embed

    def _build_honor_detail(
        self,
        member: discord.Member,
        guild: discord.Guild,
        honor_uuid: str,
    ) -> Tuple[Optional[discord.Embed], Optional[TimedHonorDetailView]]:
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or honor_def.guild_id != guild.id:
            return None, None

        req = self.requirements_manager.get_requirement(honor_uuid)
        duration_hours = self.requirements_manager.get_duration_hours(honor_uuid)

        active_honors = self.honor_data_manager.get_user_honors(member.id)
        owned_record: Optional[UserHonor] = next((x for x in active_honors if x.honor_uuid == honor_uuid), None)

        role = guild.get_role(honor_def.role_id) if honor_def.role_id else None
        is_wearing = bool(role and role in member.roles)

        embed = discord.Embed(
            title=f"🏆 {honor_def.name}",
            description=honor_def.description or "无描述",
            color=discord.Color.gold() if owned_record else discord.Color.dark_grey(),
        )

        if duration_hours:
            embed.add_field(name="有效时长", value=f"{duration_hours} 小时", inline=True)

        if role:
            embed.add_field(name="对应身份组", value=role.mention, inline=True)
        elif honor_def.role_id:
            embed.add_field(name="对应身份组", value=f"`{honor_def.role_id}` (不存在)", inline=True)
        else:
            embed.add_field(name="对应身份组", value="未绑定", inline=True)

        if owned_record:
            embed.add_field(name="当前状态", value="✅ 已拥有", inline=False)
            embed.add_field(name="剩余有效期", value=self._format_remaining(owned_record.expires_at), inline=False)
        else:
            embed.add_field(name="当前状态", value="🆕 未拥有", inline=False)

        if req:
            cond_count = (
                len(req["prerequisite_honor_all"]) + len(req["prerequisite_honor_any"]) +
                len(req["prerequisite_role_all"]) + len(req["prerequisite_role_any"]) +
                len(req["prerequisite_role_none"]) + len(req["channel_messages"])
            )
            embed.add_field(name="条件项数量", value=str(cond_count), inline=True)

        view = TimedHonorDetailView(
            self,
            owner_id=member.id,
            honor_uuid=honor_uuid,
            owned=owned_record is not None,
            is_wearing=is_wearing,
        )
        return embed, view

    async def _refresh_detail_message(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        guild: discord.Guild,
        honor_uuid: str,
    ):
        refreshed_member = guild.get_member(member.id)
        if refreshed_member is None:
            refreshed_member = await try_get_member(guild, member.id)

        use_member = refreshed_member or member
        embed, view = self._build_honor_detail(use_member, guild, honor_uuid)
        if not embed or not view:
            await self.send_ephemeral(interaction, "⚠️ 无法刷新详情面板。")
            return

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    @staticmethod
    def _format_remaining(expires_at: Optional[datetime.datetime]) -> str:
        if expires_at is None:
            return "永久"

        now = datetime.datetime.utcnow()
        diff = expires_at - now
        if diff.total_seconds() <= 0:
            return "已过期"

        days = diff.days
        hours, rem = divmod(diff.seconds, 3600)
        minutes, _ = divmod(rem, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}天")
        if hours > 0:
            parts.append(f"{hours}小时")
        if minutes > 0:
            parts.append(f"{minutes}分钟")
        if not parts:
            parts.append("不足1分钟")

        return " ".join(parts)
