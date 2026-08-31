"""Honor 身份组过期通用机制（cup_honor + toml honor 合并处理）。

设计原则（用户 2026-08-31 确认）：
- **honor = 永久记录**（SQLAlchemy db 是历史表，不存过期时间）。
- **role = 临时身份**（expiration_date 是配置，给 toml/json 用，**不在 db**）。
- expiration 字段的真相源：
  - 普通 honor → honor toml `HonorDefinitionItem.expiration_date`
  - 杯赛 honor → cup_honors.json `CupHonorDetails.expiration_date`
- bot 检测到期 → 推 `ExpiredHonorNoticeView` 到 notification 频道 → **admin 手动 remove role**
  （unix 哲学：bot 不调 remove_roles）。

合并处理（2026-08-31 改造）：
- 之前 cup_honor 自己在 `cup_honor_module.py` 有 `expiration_check_loop`（杯赛专用），
  HonorExpirationCog 又有通用版（db 驱动）——重复造轮子 + db 反模式。
- 现统一到 HonorExpirationCog.expiration_check_loop，遍历 toml + cup_honors.json 两个源。
- 防重复提醒：NotificationStateManager（cup_honor 已有，json 存已通知列表）。

字限控制（2026-08-31 用户提醒）：
- 创作大会 contributor_role 持有人多（一个 guild 几百人），统一 embed 会爆 Discord 字限。
- view embed **不列 user list**——只给 summary（"N 个成员仍持有 role"），点"补发"按钮按需触发。
- 字段值用 `_truncate_field(value, 1024)` 兜底。
"""
from __future__ import annotations

import datetime as _dt
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple

import discord
from discord import ui
from discord.ext import commands, tasks

from honor_system.cup_honor.cup_honor_json_manager import CupHonorJsonManager
from honor_system.honor_notification_state_data_manager import NotificationStateManager
from honor_system.data_manager.honor_data_manager import HonorDataManager
from honor_system.honor_config_manager import HonorConfigManager

if TYPE_CHECKING:
    from main import RoleBot


# 时区约定：上海时区（naive datetime 视为上海）。
UTC8 = _dt.timezone(_dt.timedelta(hours=8))


def _truncate_field(value: str, limit: int = 1024) -> str:
    """Discord embed field value 上限 1024 字符——超长截断并标注。"""
    if len(value) <= limit:
        return value
    return value[: limit - 20] + "\n... (列表过长已截断)"


class ExpiredHonorNoticeView(ui.View):
    """通用 honor **身份组** 过期通知 view——bot 检测到 role 过期时自动推。

    重要：这是 **role** 过期的提醒，不是 honor 本身的过期——honor 是永久的。
    admin 看到后需要手动到 Discord remove 该身份组。view 自带"补发 honor"按钮，
    适用场景：admin 已经手动 remove role 但发现 db 里某些成员的 honor 记录丢失，
    可用"补发"扫所有持有 role 的成员 grant_honor。

    字限控制：不列 user list，只给 holder 数字。
    """

    def __init__(
        self,
        cog: "HonorExpirationCog",
        guild: discord.Guild,
        honor_name: str,
        honor_uuid: str,
        role: Optional[discord.Role],
        role_holder_count: int,
        admin_role_id: Optional[int] = None,
    ):
        super().__init__(timeout=86400)  # 24h
        self.cog = cog
        self.guild = guild
        self.honor_name = honor_name
        self.honor_uuid = honor_uuid
        self.role = role
        self.role_holder_count = role_holder_count
        self.admin_role_id = admin_role_id

        # role 不存在时禁用补发按钮
        if role is None:
            for child in self.children:
                if isinstance(child, ui.Button):
                    child.disabled = True

    def create_embed(self) -> discord.Embed:
        role_mention = self.role.mention if self.role else "（身份组已被删除）"
        admin_mention = ""
        if self.admin_role_id:
            admin_role = self.guild.get_role(self.admin_role_id)
            if admin_role is not None:
                admin_mention = admin_role.mention

        embed = discord.Embed(
            title="⏰ 身份组过期提醒",
            description=(
                f"荣誉 **{self.honor_name}** 的身份组 {role_mention} 已过期。\n"
                f"**bot 不调 remove_roles**——admin 看到后手动到 Discord remove 身份组。\n"
                f"（honor 是永久记录，**不会**被删除；这里只提醒移除临时身份组）"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(
            name="当前持有该身份组成员",
            value=_truncate_field(f"`{self.role_holder_count}` 人"),
            inline=False,
        )
        embed.set_footer(text=f"Honor UUID: {self.honor_uuid}")
        return embed

    @ui.button(
        label="🔄 补发 honor（按 role 持有者）",
        style=discord.ButtonStyle.primary,
        custom_id="expired_honor:refill_by_role",
    )
    async def refill_by_role(
        self, interaction: discord.Interaction, button: ui.Button
    ):
        """扫 role.members → grant_honor(uuid)。

        适用场景：admin 手动 remove role 之前/之后发现某些成员 honor 记录缺失。
        不影响 role 本身（role 已经过期/被删），只补 db 的 honor 记录。
        """
        if not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message(
                "❌ 你没有 manage_roles 权限", ephemeral=True,
            )
            return

        if self.role is None:
            await interaction.response.send_message(
                "❌ 身份组已被删除，无法扫持有者", ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        members = [m for m in self.role.members if not m.bot]
        granted_count = 0
        already_had_count = 0
        for member in members:
            if self.cog.data_manager.grant_honor(member.id, self.honor_uuid):
                granted_count += 1
            else:
                already_had_count += 1

        report_embed = discord.Embed(
            title="✅ 补发完成",
            description=(
                f"扫描 `{self.role.name}` 的 `{len(members)}` 名持有者 → "
                f"grant_honor `{self.honor_uuid[:8]}…` ({self.honor_name})"
            ),
            color=discord.Color.green(),
        )
        report_embed.add_field(
            name="新授予 honor", value=f"`{granted_count}` 人", inline=True,
        )
        report_embed.add_field(
            name="已持有 honor", value=f"`{already_had_count}` 人", inline=True,
        )

        # 禁用所有按钮（操作已完成）
        for child in self.children:
            child.disabled = True
        try:
            await interaction.edit_original_response(view=self)
        except discord.NotFound:
            pass
        await interaction.followup.send(embed=report_embed, ephemeral=True)


class HonorExpirationCog(commands.Cog, name="HonorExpiration"):
    """通用 honor 身份组过期机制——合并处理 toml + cup_honors.json。

    注意：cup_honor 自己以前的 `expiration_check_loop` 已撤除（2026-08-31）——统一由此 cog 处理。

    实现选择：继承 ``commands.Cog`` 而非 ``FeatureCog``——这是纯后台任务 cog
    （24h 轮询 + 推 notification），没有安全身份组缓存、不进用户主面板，
    不需要 ``FeatureCog`` 的抽象接口。
    """

    def __init__(self, bot: "RoleBot"):
        self.bot = bot
        self.logger = logging.getLogger(self.__class__.__module__)
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.honor_config = HonorConfigManager.get_instance()
        self.cup_honor_manager = CupHonorJsonManager.get_instance(logger=self.logger)
        # 跨 guild 共享：cup_honors.json 是全局的，所以已通知列表也是全局
        self.notification_manager = NotificationStateManager.get_instance(
            logger=self.logger
        )

    async def cog_load(self):
        self.expiration_check_loop.start()

    def cog_unload(self):
        self.expiration_check_loop.cancel()

    # --- 24h 轮询检查（合并处理 toml + cup json） ---

    @tasks.loop(hours=24)
    async def expiration_check_loop(self):
        await self._perform_expiration_check()

    @expiration_check_loop.before_loop
    async def before_expiration_check(self):
        await self.bot.wait_until_ready()

    async def _perform_expiration_check(self):
        """24h 轮询检查所有 honor 的 expiration_date——到期推 ExpiredHonorNoticeView。

        逻辑（按 cup_honor 原版 + 通用化）：
        1. 遍历所有 honor_*.toml 的 guild_id
        2. 对每个 guild：检查 toml cfg.definitions 找 expiration_date <= now 的普通 honor
        3. 对每个 guild：检查 cup_honors.json 找 expiration_date <= now 的杯赛 honor
        4. 合并两个列表，对每个过期 honor 推 notification（如果该 guild 配了 channel）
        5. 防重复：NotificationStateManager（已通知列表，全局共享）

        注意：**HonorExpirationCog 本身不维护 db**。db 是 honor 历史记录表，
        expiration_date 是**配置**字段，永远来自 toml/json。
        """
        self.logger.info("正在执行 honor 过期检查（toml + cup_honors.json 合并）...")
        try:
            now = _dt.datetime.now(UTC8)

            for guild_id in self._iter_configured_guild_ids():
                try:
                    cfg = self.honor_config.get(guild_id)
                except Exception as e:
                    self.logger.error(f"加载 honor toml 失败 guild {guild_id}: {e}")
                    continue
                if cfg is None:
                    continue

                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    self.logger.warning(f"无法找到服务器 {guild_id}，跳过过期检查。")
                    continue

                # 收集本 guild 所有过期 honor（普通 + 杯赛）
                expired_in_guild = self._collect_expired_for_guild(
                    guild, cfg, now
                )

                if not expired_in_guild:
                    continue

                # 通知 channel 走 cfg.cup_honor.notification（事实上的 honor 通知 channel）
                cup_cfg = getattr(cfg, "cup_honor", None)
                notification = getattr(cup_cfg, "notification", None) if cup_cfg else None
                if notification is None or not notification.channel_id:
                    self.logger.warning(
                        f"服务器 {guild.name} 的 honor notification 未配置，跳过"
                    )
                    continue

                channel = guild.get_channel(notification.channel_id)
                admin_role_id = (
                    notification.admin_role_id if notification else None
                )
                if channel is None:
                    continue

                # 推每个过期 honor
                for honor_uuid, name, role in expired_in_guild:
                    if self.notification_manager.has_been_notified(honor_uuid):
                        continue
                    try:
                        holder_count = (
                            len([m for m in role.members if not m.bot])
                            if role
                            else 0
                        )
                        view = ExpiredHonorNoticeView(
                            self, guild, name, honor_uuid, role, holder_count, admin_role_id,
                        )
                        embed = view.create_embed()
                        await channel.send(
                            content=(
                                f"<@&{admin_role_id}>" if admin_role_id else None
                            ),
                            embed=embed,
                            view=view,
                            allowed_mentions=(
                                discord.AllowedMentions(roles=[admin_role_id])
                                if admin_role_id
                                else discord.AllowedMentions.none()
                            ),
                        )
                        await self.notification_manager.add_notified(honor_uuid)
                        self.logger.info(
                            f"Honor expiration: pushed notice for honor {honor_uuid} "
                            f"({name}) to guild {guild_id}"
                        )
                    except discord.Forbidden:
                        self.logger.warning(
                            f"无法在频道 {notification.channel_id} 发提醒"
                        )

        except Exception as e:
            self.logger.error(f"honor 过期检查任务发生未知错误: {e}", exc_info=True)

    def _collect_expired_for_guild(
        self,
        guild: discord.Guild,
        cfg,  # HonorGuildConfig
        now: _dt.datetime,
    ) -> List[Tuple[str, str, Optional[discord.Role]]]:
        """收集本 guild 所有过期 honor（普通 honor + cup_honor 合并），返回 (uuid, name, role) 列表。

        cup_honors.json 是全局共享，**所有** guild 都共享同一份 cup_honor——所以每个 guild
        都各自对 cup_honor 跑一次检查，但 NotificationStateManager 防重复提醒。
        """
        result: List[Tuple[str, str, Optional[discord.Role]]] = []

        # 1. 普通 honor toml
        for item in cfg.definitions:
            if item.expiration_date is None:
                continue
            exp = self._ensure_utc8(item.expiration_date)
            if exp > now:
                continue
            role = guild.get_role(item.role_id) if item.role_id else None
            result.append((item.uuid, item.name, role))

        # 2. 杯赛 honor json（每个 guild 都跑，但 NotificationStateManager 防重复）
        self.cup_honor_manager.load_data()
        for cup_def in self.cup_honor_manager.get_all_cup_honors():
            try:
                exp = self._ensure_utc8(cup_def.cup_honor.expiration_date)
            except AttributeError:
                continue
            if exp > now:
                continue
            role = (
                guild.get_role(cup_def.role_id) if cup_def.role_id else None
            )
            result.append((str(cup_def.uuid), cup_def.name, role))

        return result

    # --- helpers ---

    @staticmethod
    def _ensure_utc8(dt: _dt.datetime) -> _dt.datetime:
        """naive datetime 视为上海时区；aware 保留原 tz。"""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC8)
        return dt

    @staticmethod
    def _iter_configured_guild_ids() -> List[int]:
        """扫描 data/honor_*.toml 文件名，返回所有已配置 guild_id。"""
        data_dir = Path("data")
        if not data_dir.exists():
            return []
        ids: List[int] = []
        for toml_path in data_dir.glob("honor_*.toml"):
            m = re.match(r"honor_(\d+)\.toml", toml_path.name)
            if m:
                ids.append(int(m.group(1)))
        return ids


async def setup(bot: "RoleBot") -> None:
    await bot.add_cog(HonorExpirationCog(bot))


__all__ = [
    "ExpiredHonorNoticeView",
    "HonorExpirationCog",
    "setup",
]
