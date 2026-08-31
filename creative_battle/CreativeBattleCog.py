"""创作大会主 cog —— 唯一 cog，承担按钮 + Modal + 投稿期 if-else 判断 + 推广。

设计原则（按 ``role_bot/AGENTS.md`` + design doc 简化版拍板记录）：

- **unix 哲学**：bot 只做最少的事
  - 投稿面板（每个分区频道各一个）：用户点投稿 → 写 json + add contributor_role + grant_honor
  - 主入口面板：A/B 互斥领取按钮
  - 撤销投稿：admin 命令删 json（**不** remove role）
- **不做**：身份组过期提醒、winner_role、自动 remove、状态机、游客领取面板
  （游客由 admin 用 honor claimable 模块处理）
- **不硬编码 faction key**：所有 faction 处理走 ``for f in cfg.factions`` 遍历
- **投稿期判断 = if-else**：bot 读 toml 的 ``start_date`` / ``end_date``，
  ``now in [start_date, end_date]`` 就接受投稿；否则拒绝。**不是状态机**。
- **互斥**：用户已选 A 阵营后点 B → bot 拒绝（ephemeral 提示），
  不 remove A。如需更换请管理组手动 remove A 后再点 B。
- **黑/白名单 per-faction**：每个阵营独立配，黑名单优先于白名单。
- **grant_honor 直接调**：bot 在投稿成功后调 honor 系统的 ``grant_honor`` 接口
  （uuid 在 toml 的 ``contributor_honor_uuid`` 配置）。

面板机制
--------

- ``MainPanelView``：主入口——动态生成 A 组 / B 组阵营互斥领取按钮
  （admin 手动发到主入口频道）
- ``FactionPanelView``：分区——📨 投稿按钮（admin 手动发到各阵营分区频道）
- 推广 embed 由 ``promotion_loop`` 定时 refresh 到对应频道
  （**仅投稿期内刷新** = if-else now in [start_date, end_date]）
"""
from __future__ import annotations

import datetime as _dt
import logging
import random
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Set

import discord
from discord import app_commands, ui
from discord.ext import commands, tasks

import config
from creative_battle.creative_battle_config_manager import CreativeBattleConfigManager
from creative_battle.creative_battle_models import (
    CreativeBattleGuildConfig,
    FactionConfig,
)
from creative_battle.creative_battle_season_models import (
    GuildSeasonData,
    ParticipantEntry,
    SeasonState,
    SubmissionEntry,
)
from creative_battle.creative_battle_state_manager import CreativeBattleStateManager
from honor_system.data_manager.honor_data_manager import HonorDataManager
from utility.feature_cog import FeatureCog, PanelEntry
from utility.toml_filename_utils import iter_guild_ids_from_toml_files

if TYPE_CHECKING:
    from main import RoleBot


UTC8 = _dt.timezone(_dt.timedelta(hours=8))


# ========================= View =========================


class MainPanelView(ui.View):
    """主入口面板：A 组 / B 组阵营互斥领取按钮（按 cfg.factions 遍历）。

    互斥语义由 ``_handle_join`` 实现：用户已选 A 后点 B → bot 拒绝
    （ephemeral 提示"你已在 A 阵营，如需更换请联系管理组"）。
    """

    def __init__(self, cog: "CreativeBattleCog", factions: List[FactionConfig]) -> None:
        super().__init__(timeout=None)
        self.cog = cog

        # ★ 遍历 factions 动态加按钮（不硬编码 faction key；支持 3+ 组扩展）
        for faction in factions:
            btn = ui.Button(
                label=f"{faction.emoji} 加入 {faction.display_name}",
                style=discord.ButtonStyle.primary,
                custom_id=f"cb:main:join:{faction.key}",
            )
            btn.callback = self._make_join_callback(faction.key)
            self.add_item(btn)

    def _make_join_callback(self, faction_key: str):
        async def _cb(interaction: discord.Interaction) -> None:
            await self.cog._handle_join(interaction, faction_key=faction_key)
        return _cb


class FactionPanelView(ui.View):
    """分区面板：📨 投稿按钮（一个 view 实例绑一个 faction_key）。

    custom_id 动态包含 faction_key——避免两个 view 注册时
    bot.dispatch 无法区分（persistent view 用 custom_id 路由）。
    """

    def __init__(self, cog: "CreativeBattleCog", faction_key: str) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.faction_key = faction_key

        submit_btn = ui.Button(
            label="📨 提交作品",
            style=discord.ButtonStyle.primary,
            custom_id=f"cb:submit:{faction_key}",
        )
        submit_btn.callback = self._on_submit_click
        self.add_item(submit_btn)

    async def _on_submit_click(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(SubmissionModal(self.cog, self.faction_key))


class SubmissionModal(ui.Modal, title="提交作品"):
    """投稿 Modal：标题（必填）+ 描述（可选）。"""

    def __init__(self, cog: "CreativeBattleCog", faction_key: str) -> None:
        super().__init__(title="提交作品")
        self.cog = cog
        self.faction_key = faction_key
        self.title_input = ui.TextInput(
            label="作品标题",
            style=discord.TextStyle.short,
            max_length=100,
            required=True,
        )
        self.description_input = ui.TextInput(
            label="作品描述 / 链接（可选）",
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False,
        )
        self.add_item(self.title_input)
        self.add_item(self.description_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog._handle_submission(
            interaction,
            faction_key=self.faction_key,
            title=self.title_input.value,
            description=self.description_input.value or None,
        )


# ========================= Cog =========================


class CreativeBattleCog(FeatureCog, name="CreativeBattle"):
    """创作大会主 cog（简化版：unix 哲学 + if-else 投稿期 + per-faction 黑/白名单）。"""

    # --- FeatureCog 抽象接口实现 ---

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        # 主面板按钮留 v2（按 design doc §10 推迟）
        return None

    async def update_safe_roles_cache(self) -> None:
        # 本 cog 管理的身份组都是 toml 配置，role_name_cache 由 CoreCog 维护
        return

    # --- 初始化 ---

    def __init__(self, bot: "RoleBot") -> None:
        super().__init__(bot)
        self.config_mgr = CreativeBattleConfigManager.get_instance()
        self.state_mgr = CreativeBattleStateManager.get_instance(logger=self.logger)
        # honor 系统：bot 直接调 grant_honor（仿 claimable_honor_module 模式）
        self.honor_data_manager = HonorDataManager.getDataManager(logger=self.logger)

    async def cog_load(self) -> None:
        """注册 persistent view + 启动 promotion_loop。"""
        await super().cog_load()

        # ★ 遍历所有 guild 的 toml，注册对应 faction 的 view（不硬编码 faction key）
        for guild_id in iter_guild_ids_from_toml_files(Path("data"), "creative_battle_"):
            cfg = self.config_mgr.get(guild_id)
            if cfg is None:
                continue
            # 注册主入口 view（每个 guild 一个实例）
            self.bot.add_view(MainPanelView(self, cfg.factions))
            # 注册分区 view（每个 faction 一个实例）
            for faction in cfg.factions:
                self.bot.add_view(FactionPanelView(self, faction_key=faction.key))

        # 启动 tasks（**不再**有 season_loop / expire_check_loop）
        self.promotion_loop.start()

    def cog_unload(self) -> None:
        self.promotion_loop.cancel()

    # --- 内部辅助 ---

    def _iter_configured_guild_ids(self):
        """遍历所有配置了 creative_battle toml 的 guild_id。"""
        return iter_guild_ids_from_toml_files(Path("data"), "creative_battle_")

    @staticmethod
    def _is_submission_open(cfg: CreativeBattleGuildConfig, now: _dt.date) -> bool:
        """投稿期判断 = if-else（**不是状态机**）。

        ``start_date <= today <= end_date`` 就接受投稿；否则拒绝。
        admin 改 toml 即可调整时间窗——bot 不需要单独状态字段。
        """
        return cfg.meta.start_date <= now <= cfg.meta.end_date

    @staticmethod
    def _member_holds_any_role(member: discord.Member, role_ids: List[int]) -> bool:
        """member 是否持有 role_ids 中的任一身份组（**任一**即 True）。"""
        if not role_ids:
            return False
        held = {r.id for r in member.roles}
        return any(rid in held for rid in role_ids)

    def _check_blacklist_whitelist(
        self,
        member: discord.Member,
        faction: FactionConfig,
    ) -> Optional[str]:
        """per-faction 黑/白名单检查（unix 哲学：黑名单优先于白名单）。

        Returns:
            None 表示通过；非 None = 拒绝原因（中文，给用户看）。
        """
        # 黑名单优先：持任一即拒
        if faction.blacklist_role_ids and self._member_holds_any_role(
            member, faction.blacklist_role_ids
        ):
            return f"❌ 你持有的某个身份组不允许加入/投稿 {faction.display_name}（黑名单）。"

        # 白名单：非空时持任一才允许
        if faction.whitelist_role_ids and not self._member_holds_any_role(
            member, faction.whitelist_role_ids
        ):
            return f"❌ 你未持有加入/投稿 {faction.display_name} 所需的身份组（白名单）。"

        return None

    @staticmethod
    def _other_supporter_role_ids(
        cfg: CreativeBattleGuildConfig, faction: FactionConfig
    ) -> Set[int]:
        """返回"其他阵营的 supporter_role_id"集合——互斥检查用。"""
        return {f.supporter_role_id for f in cfg.factions if f.key != faction.key}

    # --- 按钮 handler：支持者加入（互斥 + 黑/白名单） ---

    async def _handle_join(
        self, interaction: discord.Interaction, faction_key: str
    ) -> None:
        cfg = self.config_mgr.get(interaction.guild.id)
        if cfg is None or not cfg.enabled:
            await interaction.response.send_message(
                "❌ 当前服务器未启用创作大会。", ephemeral=True
            )
            return

        # ★ 遍历 cfg.factions 找匹配（不硬编码 faction key）
        faction = next((f for f in cfg.factions if f.key == faction_key), None)
        if faction is None:
            await interaction.response.send_message(
                f"❌ 阵营 '{faction_key}' 未配置。", ephemeral=True
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message("❌ 找不到成员。", ephemeral=True)
            return

        # ★ 互斥检查：用户已持有"其他阵营的 supporter_role" → 拒绝（**不 remove**）
        other_supporter_ids = self._other_supporter_role_ids(cfg, faction)
        held_other = [r.id for r in member.roles if r.id in other_supporter_ids]
        if held_other:
            held_faction = next(
                (f for f in cfg.factions if f.supporter_role_id in held_other),
                None,
            )
            await interaction.response.send_message(
                f"❌ 你已在 {held_faction.emoji if held_faction else '其他'}"
                f"{held_faction.display_name if held_faction else '阵营'}。"
                f"如需更换阵营，请联系管理组手动移除原身份组后再点击。",
                ephemeral=True,
            )
            return

        # ★ 黑/白名单检查（per-faction）
        reject_msg = self._check_blacklist_whitelist(member, faction)
        if reject_msg:
            await interaction.response.send_message(reject_msg, ephemeral=True)
            return

        # 幂等 add（重复点不会报错）
        supporter_role = interaction.guild.get_role(faction.supporter_role_id)
        if supporter_role is None:
            await interaction.response.send_message(
                f"❌ 身份组 ID `{faction.supporter_role_id}` 在服务器中不存在或已被删除，请联系管理组。",
                ephemeral=True,
            )
            return
        try:
            await member.add_roles(
                supporter_role,
                reason=f"创作大会 {cfg.meta.season_label} 加入 {faction.display_name}",
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Bot 缺少 manage_roles 权限。", ephemeral=True
            )
            return

        state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)
        state.season.supporters[interaction.user.id] = ParticipantEntry(
            user_id=interaction.user.id,
            faction=faction_key,
            joined_at=_dt.datetime.now(UTC8),
            supporter_role_granted=True,
        )
        await self.state_mgr.save_data(state)
        await interaction.response.send_message(
            f"✅ 你已加入 {faction.emoji} {faction.display_name}！", ephemeral=True
        )

    # --- Modal handler：参赛者投稿（if-else 投稿期 + grant_honor） ---

    async def _handle_submission(
        self,
        interaction: discord.Interaction,
        faction_key: str,
        title: str,
        description: Optional[str],
    ) -> None:
        cfg = self.config_mgr.get(interaction.guild.id)
        if cfg is None or not cfg.enabled:
            await interaction.response.send_message(
                "❌ 当前服务器未启用创作大会。", ephemeral=True
            )
            return

        # ★ 投稿期 if-else（不是状态机）
        today = _dt.datetime.now(UTC8).date()
        if not self._is_submission_open(cfg, today):
            await interaction.response.send_message(
                f"❌ 当前不是投稿期（投稿期：{cfg.meta.start_date} ~ {cfg.meta.end_date}）。",
                ephemeral=True,
            )
            return

        faction = next((f for f in cfg.factions if f.key == faction_key), None)
        if faction is None:
            await interaction.response.send_message(
                f"❌ 阵营 '{faction_key}' 未配置。", ephemeral=True
            )
            return

        member = interaction.guild.get_member(interaction.user.id)
        if member is None:
            await interaction.response.send_message("❌ 找不到成员。", ephemeral=True)
            return

        # ★ 投稿者也走黑/白名单检查（per-faction）
        reject_msg = self._check_blacklist_whitelist(member, faction)
        if reject_msg:
            await interaction.response.send_message(reject_msg, ephemeral=True)
            return

        # 写 json（先写状态——即使后续 grant_honor 失败也保留提交记录）
        state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)
        submission = SubmissionEntry(
            user_id=interaction.user.id,
            faction=faction_key,
            title=title,
            description=description,
            submitted_at=_dt.datetime.now(UTC8),
            contributor_role_granted=False,
            honor_granted=False,
        )
        state.season.submissions[submission.submission_id] = submission
        await self.state_mgr.save_data(state)

        # add contributor_role
        contributor_role = interaction.guild.get_role(faction.contributor_role_id)
        if contributor_role is None:
            await interaction.response.send_message(
                f"❌ 参赛身份组 ID `{faction.contributor_role_id}` 在服务器中不存在或已被删除，未能添加。投稿仍已记录，请联系管理组补发身份组。",
                ephemeral=True,
            )
            submission.contributor_role_granted = False
            await self.state_mgr.save_data(state)
            return
        try:
            await member.add_roles(
                contributor_role,
                reason=f"创作大会投稿 {title[:30]}",
            )
            submission.contributor_role_granted = True
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Bot 缺少 manage_roles 权限，未能添加参赛身份组。请联系管理组。",
                ephemeral=True,
            )
            # 仍然标记失败但 json 已记录——admin 可后续手动补
            submission.contributor_role_granted = False
            await self.state_mgr.save_data(state)
            return

        # ★ grant_honor（按 toml contributor_honor_uuid 配置；可选）
        if faction.contributor_honor_uuid:
            try:
                granted_def = self.honor_data_manager.grant_honor(
                    member.id, faction.contributor_honor_uuid
                )
                if granted_def:
                    submission.honor_granted = True
                    self.logger.info(
                        f"创作大会 {cfg.meta.season_label}: {member.id} 投稿 "
                        f"成功 grant_honor {faction.contributor_honor_uuid}"
                    )
                else:
                    self.logger.warning(
                        f"创作大会 {cfg.meta.season_label}: {member.id} 投稿 "
                        f"grant_honor {faction.contributor_honor_uuid} 返回 None（已持有？）"
                    )
            except Exception as e:
                # grant_honor 失败不阻断投稿流程——admin 后续可手动补
                self.logger.warning(
                    f"创作大会 {cfg.meta.season_label}: {member.id} 投稿 "
                    f"grant_honor 失败: {e}"
                )

        await self.state_mgr.save_data(state)
        await interaction.response.send_message(
            f"✅ 作品已提交！\n标题：{title}", ephemeral=True
        )

    # --- Admin 命令组（简化版：发送面板 + 撤销投稿） ---

    admin_group = app_commands.Group(
        name="合战丨核心",
        description="创作大会管理（发送面板 / 撤销投稿）",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    # 自动补全
    async def channel_key_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[str]]:
        cfg = self.config_mgr.get(interaction.guild.id)
        if cfg is None:
            return []
        options = ["main"] + [f.key for f in cfg.factions]

        def _describe(opt: str) -> str:
            if opt == "main":
                return "主入口（A/B 阵营互斥领取）"
            f = next((f for f in cfg.factions if f.key == opt), None)
            return f.display_name if f else opt

        return [
            app_commands.Choice(name=f"{opt}（{_describe(opt)}）", value=opt)
            for opt in options
            if not current or current.lower() in opt.lower()
        ]

    @admin_group.command(
        name="发送面板",
        description="发送面板到对应频道（main=主入口；任意 faction key=该阵营分区投稿面板）",
    )
    @app_commands.describe(
        channel_key="面板类型（自动补全：main 或任意 faction key）",
    )
    @app_commands.autocomplete(channel_key=channel_key_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def cmd_send_panel(
        self, interaction: discord.Interaction, channel_key: str
    ) -> None:
        cfg = self.config_mgr.get(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message(
                "❌ 当前服务器未启用创作大会。", ephemeral=True
            )
            return
        state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)

        if channel_key == "main":
            if not cfg.promotion.main_channel_id:
                await interaction.response.send_message(
                    "❌ main_channel_id 未配置。", ephemeral=True
                )
                return
            channel_id = cfg.promotion.main_channel_id
            embed = self._build_main_embed(cfg, state.season)
            view = MainPanelView(self, cfg.factions)
        else:
            # ★ 遍历 cfg.factions 找匹配（不硬编码）
            faction = next((f for f in cfg.factions if f.key == channel_key), None)
            if faction is None:
                allowed = ", ".join(["main"] + [f.key for f in cfg.factions])
                await interaction.response.send_message(
                    f"❌ channel_key '{channel_key}' 不存在。允许值：{allowed}",
                    ephemeral=True,
                )
                return
            if not faction.submission_channel_id:
                await interaction.response.send_message(
                    f"❌ {faction.display_name} 的 submission_channel_id 未配置。",
                    ephemeral=True,
                )
                return
            channel_id = faction.submission_channel_id
            embed = self._build_faction_embed(cfg, state.season, faction)
            view = FactionPanelView(self, faction_key=faction.key)

        channel = interaction.guild.get_channel(channel_id)
        if channel is None:
            await interaction.response.send_message(
                f"❌ 频道 ID {channel_id} 找不到或 bot 无权限。", ephemeral=True
            )
            return

        msg = await channel.send(embed=embed, view=view)
        state.season.promotion_message_ids[channel_key] = msg.id
        await self.state_mgr.save_data(state)
        await interaction.response.send_message(
            f"✅ {channel_key} 面板已发送到 {channel.mention}", ephemeral=True
        )

    @admin_group.command(
        name="撤销投稿",
        description="从 json 删除指定投稿（**不** remove contributor_role——请到 Discord 手动 remove）",
    )
    @app_commands.describe(
        submission_id="投稿 UUID（submission_id）",
    )
    @app_commands.checks.has_permissions(manage_roles=True)
    async def cmd_revoke_submission(
        self, interaction: discord.Interaction, submission_id: str
    ) -> None:
        cfg = self.config_mgr.get(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message(
                "❌ 当前服务器未启用创作大会。", ephemeral=True
            )
            return
        state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)

        sub = state.season.submissions.get(submission_id)
        if sub is None:
            await interaction.response.send_message(
                f"❌ 找不到 submission_id={submission_id} 的投稿。",
                ephemeral=True,
            )
            return

        # 撤销：只删 json entry，**不** remove contributor_role，**不** 撤 honor
        del state.season.submissions[submission_id]
        await self.state_mgr.save_data(state)

        faction_display = next(
            (f.display_name for f in cfg.factions if f.key == sub.faction), sub.faction,
        )
        await interaction.response.send_message(
            f"✅ 投稿已从 json 撤销（submission_id={submission_id[:8]}…，《{sub.title}》/ {faction_display}）。\n"
            f"⚠️ **bot 没有 remove contributor_role**——请到 Discord 手动 remove 参赛身份组。\n"
            f"⚠️ **bot 没有撤销 grant_honor**——如需撤销 honor，请到 honor 系统的『荣誉头衔丨管理』处理。",
            ephemeral=True,
        )

    # --- Embed builders ---

    def _build_main_embed(
        self, cfg: CreativeBattleGuildConfig, season: SeasonState
    ) -> discord.Embed:
        supporters = self._count_by_faction(
            [(p.faction,) for p in season.supporters.values()]
        )
        submissions = self._count_by_faction(
            [(s.faction,) for s in season.submissions.values()]
        )
        # if-else 投稿期
        today = _dt.datetime.now(UTC8).date()
        is_open = self._is_submission_open(cfg, today)
        status_line = (
            f"🟢 投稿期（{cfg.meta.start_date} ~ {cfg.meta.end_date}）"
            if is_open
            else f"⚫ 非投稿期（{cfg.meta.start_date} ~ {cfg.meta.end_date}）"
        )
        embed = discord.Embed(
            title=f"🎉 {cfg.meta.season_label}",
            description=(
                f"{status_line}\n\n"
                f"点下方按钮加入你要支持的组（A/B **互斥**，如需更换请联系管理组）。\n\n"
                + "\n".join(
                    f"{f.emoji} **{f.display_name}** — 支持 {supporters.get(f.key, 0)} 人 / 参赛 {submissions.get(f.key, 0)} 人"
                    for f in cfg.factions
                )
            ),
            color=discord.Color.orange() if is_open else discord.Color.greyple(),
        )
        return embed

    def _build_faction_embed(
        self,
        cfg: CreativeBattleGuildConfig,
        season: SeasonState,
        faction: FactionConfig,
    ) -> discord.Embed:
        subs = [s for s in season.submissions.values() if s.faction == faction.key]
        supporters = sum(
            1 for p in season.supporters.values() if p.faction == faction.key
        )

        # random N 个投稿
        random_count = min(cfg.promotion.random_count_per_faction, len(subs))
        selected = random.sample(subs, random_count) if subs else []

        today = _dt.datetime.now(UTC8).date()
        is_open = self._is_submission_open(cfg, today)

        embed = discord.Embed(
            title=f"{faction.emoji} {cfg.meta.season_label} — {faction.display_name}",
            description=(
                f"当前支持者：{supporters} 人 / 参赛者：{len(subs)} 人\n"
                + (
                    f"🟢 投稿期内，点下方按钮提交你的作品。"
                    if is_open
                    else f"⚫ 非投稿期（{cfg.meta.start_date} ~ {cfg.meta.end_date}）"
                )
            ),
            color=discord.Color.blue() if is_open else discord.Color.greyple(),
        )
        for sub in selected:
            value = sub.description or "（无描述）"
            embed.add_field(
                name=f"《{sub.title}》",
                value=value[:200],
                inline=False,
            )
        return embed

    @staticmethod
    def _count_by_faction(items) -> dict[str, int]:
        """统计每个 faction 出现次数。"""
        result: dict[str, int] = {}
        for (faction,) in items:
            result[faction] = result.get(faction, 0) + 1
        return result

    # --- Promotion loop（投稿期内 refresh 推广面板） ---

    @tasks.loop(minutes=5)
    async def promotion_loop(self) -> None:
        today = _dt.datetime.now(UTC8).date()
        for guild_id in self._iter_configured_guild_ids():
            cfg = self.config_mgr.get(guild_id)
            if cfg is None or not cfg.enabled:
                continue

            # ★ if-else（不是状态机）：仅投稿期内刷新
            if not self._is_submission_open(cfg, today):
                continue

            state = self.state_mgr.ensure_season(guild_id, cfg.meta.season_id)
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue

            # 1. 主入口
            if cfg.promotion.main_channel_id:
                await self._refresh_promotion(
                    guild,
                    state,
                    "main",
                    cfg.promotion.main_channel_id,
                    embed=self._build_main_embed(cfg, state.season),
                )

            # 2. ★ 遍历每个 faction（不硬编码）
            for faction in cfg.factions:
                if not faction.submission_channel_id:
                    continue
                await self._refresh_promotion(
                    guild,
                    state,
                    faction.key,
                    faction.submission_channel_id,
                    embed=self._build_faction_embed(cfg, state.season, faction),
                )

            await self.state_mgr.save_data(state)

    @promotion_loop.before_loop
    async def before_promotion_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def _refresh_promotion(
        self,
        guild: discord.Guild,
        state: GuildSeasonData,
        channel_key: str,
        channel_id: int,
        embed: discord.Embed,
    ) -> None:
        """单一持久 embed：edit 上次消息，没有则新发。

        bot 不管面板自身生命周期——如果被 admin / 用户删了，下次 loop 检测到 NotFound 重发。
        """
        channel = guild.get_channel(channel_id)
        if channel is None:
            return
        msg_id = state.season.promotion_message_ids.get(channel_key)
        if msg_id is None:
            msg = await channel.send(embed=embed)
            state.season.promotion_message_ids[channel_key] = msg.id
        else:
            try:
                msg = await channel.fetch_message(msg_id)
                await msg.edit(embed=embed)
            except discord.NotFound:
                msg = await channel.send(embed=embed)
                state.season.promotion_message_ids[channel_key] = msg.id


async def setup(bot: "RoleBot") -> None:
    await bot.add_cog(CreativeBattleCog(bot))


__all__ = [
    "MainPanelView",
    "FactionPanelView",
    "SubmissionModal",
    "CreativeBattleCog",
    "setup",
]
