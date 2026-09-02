"""HonorAdminCog——honor 系统的 admin 命令集合。

从 HonorCog 拆出（2026-08-31）—— HonorCog 现在只剩核心：用户"我的荣誉墙"面板入口、
toml/db 同步、安全缓存。

admin 命令（2026-08-31 现状）：
- 管理持有者
- 同步角色荣誉（cmd_sync_role_honors）—— 通用 sync 机制
- 授予（grant_honor_to_member）—— 手动给一个用户授予任意 honor + role
- 批量授予（bulk_grant_honor）—— 从 ID 列表 / 消息链接解析用户批量授予
- 设置最终持有者（set_final_holders）—— 底层危险命令（按用户要求保留）
- 重置通知状态（reset_notification_state）—— 通用工具

本 cog 自带 ``admin_group``（`/荣誉头衔丨管理`），所有命令用 ``@admin_group.command`` 装饰
注册——**标准 pattern**，跟 ``HonorConfigCog.honor_config_group`` / ``CreativeBattleCog.admin_group``
一致。**不**复用 HonorCog 上的 group（早期实现的 ``cog_load`` add_command hack 已删除，
见 ``role_bot/AGENTS.md`` 教训 + 2026-08-31 hotfix）。

设计要点：
- **授予 / 批量授予**：通用 honor 操作——任何 honor（普通 + cup_honor）都能手动授予。
- **设置最终持有者**：保留 `remove_roles` 能力（用户决定为底层危险命令保留）；
  unix 哲学默认不允许，但 admin 用作最后清理手段仍有价值。
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

import discord
from discord import app_commands, ui

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
from honor_system.data_manager.honor_data_manager import HonorDataManager
from honor_system.honor_config_manager import HonorConfigManager
from honor_system.HonorManageView import HonorHoldersManageView
from honor_system.honor_notification_state_data_manager import NotificationStateManager
from shared.ui.views import ConfirmationView
from utility.feature_cog import FeatureCog, PanelEntry

if TYPE_CHECKING:
    from main import RoleBot


class ConfirmSyncView(ui.View):
    """同步角色荣誉前的二次确认 view（ephemeral，60s timeout）。"""

    def __init__(self, on_confirm):
        super().__init__(timeout=60)
        self.on_confirm = on_confirm

    @ui.button(label="✅ 确认同步", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        await self.on_confirm()

    @ui.button(label="❌ 取消", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="❌ 已取消", embed=None, view=self)


class HonorAdminCog(FeatureCog, name="HonorAdmin"):
    """honor 系统的 admin 命令 cog。

    自带 ``admin_group`` = `/荣誉头衔丨管理`，所有命令用 ``@admin_group.command`` 装饰注册。
    兼容 HonorHoldersManageView 的接口契约：data_manager / honor_config / logger / get_guide_embed。
    """

    def __init__(self, bot: "RoleBot"):
        super().__init__(bot)
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.honor_config = HonorConfigManager.get_instance()
        # NotificationStateManager 通用——HonorExpirationCog 共用同一单例
        self.notification_manager = NotificationStateManager.get_instance(logger=self.logger)

    # --- slash command group（标准 pattern：class-level app_commands.Group + @admin_group.command） ---

    admin_group = app_commands.Group(
        name="荣誉头衔丨管理",
        description="荣誉头衔的 admin 管理命令（授予 / 批量授予 / 同步 / 重置通知）。",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    # --- FeatureCog 接口实现 ---

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """HonorAdminCog 不在主面板暴露入口——admin 命令在本 cog 自带的
        ``/荣誉头衔丨管理`` 命令组下（class-level ``admin_group`` + ``@admin_group.command``），
        保持 UX 单一入口。
        """
        return None

    async def update_safe_roles_cache(self) -> None:
        """HonorAdminCog 不直接管理安全身份组——安全 cache 由 HonorCog 维护
        （HonorCog.update_safe_roles_cache 已扫描 honor toml + cup_honor.json）。
        这里保持空实现以满足 FeatureCog 抽象接口。
        """
        return None

    def get_guide_embed(self, guild_id: int) -> discord.Embed:
        """HonorHoldersManageView 用的 honor 活动指引 embed——同 HonorCog 行为。"""
        return (
            EmbedGuidesConfigManager.get_instance()
            .get(guild_id)
            .honor_celebrate_guide.to_embed()
        )

    # --- 配置层 helpers: ``role_sync_honor`` 是 config 真理源, 不应从 ORM 读 ---

    def _cfg_definitions(self, guild_id: int) -> List:
        """返回 guild 的 honor config 中所有 ``definitions`` 项。

        没有 toml 文件时返回空 list (HonorGuildConfig 含 required fields,
        ``get()`` 无 toml 返回 None, 我们直接在调用方容忍空)。
        """
        cfg = self.honor_config.get(guild_id)
        if cfg is None:
            return []
        return cfg.definitions

    def _find_cfg_def_by_uuid(self, guild_id: int, uuid_str: str):
        """按 uuid 在 config 层查找 (返回 HonorDefinitionItem 或 None)。"""
        for d in self._cfg_definitions(guild_id):
            if d.uuid == uuid_str:
                return d
        return None

    def _find_cfg_def_by_role_id(self, guild_id: int, role_id: int):
        """按 role_id 在 config 层查找 (返回 HonorDefinitionItem 或 None)。

        通常 role_id 在 config 层是 unique (一个 role 对应一个 honor),
        取首个命中。多对多不预期。
        """
        for d in self._cfg_definitions(guild_id):
            if d.role_id == role_id:
                return d
        return None

    # --- autocomplete ---

    async def honor_uuid_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """为所有荣誉UUID参数提供自动补全选项。"""
        all_defs = self.data_manager.get_all_honor_definitions(interaction.guild_id)

        choices = []
        for honor_def in all_defs:
            if honor_def.is_archived:
                continue
            choice_name = f"{honor_def.name} ({str(honor_def.uuid)[:8]})"
            if not current or current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(honor_def.uuid)))

        return choices[:25]

    async def role_id_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """列出 role_sync_honor=true 的 honor 对应的 Discord 身份组 ID（按身份组名匹配）。"""
        choices: List[app_commands.Choice[str]] = []
        guild = interaction.guild
        if guild is None:
            return choices
        # role_sync_honor 是配置层状态 → 走 cfg.definitions 而非 ORM
        for defn in self._cfg_definitions(guild.id):
            if not defn.role_sync_honor or not defn.role_id:
                continue
            role = guild.get_role(defn.role_id)
            if role is None:
                continue
            choice_name = f"[{role.name}] {defn.name} ({str(defn.uuid)[:8]})"
            if not current or current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(role.id)))
        return choices[:25]

    async def role_sync_honor_uuid_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """只列出 role_sync_honor=true 的 honor UUID。"""
        choices: List[app_commands.Choice[str]] = []
        guild_id = interaction.guild_id
        if guild_id is None:
            return choices
        # role_sync_honor 是配置层状态 → 走 cfg.definitions 而非 ORM
        for defn in self._cfg_definitions(guild_id):
            if not defn.role_sync_honor:
                continue
            choice_name = f"{defn.name} ({str(defn.uuid)[:8]})"
            if not current or current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(defn.uuid)))
        return choices[:25]

    # --- admin 命令（@admin_group.command 标准 pattern，自动注册到 `/荣誉头衔丨管理`）---

    @admin_group.command(
        name="管理持有者",
        description="查看并移除特定荣誉的持有者。",
    )
    @app_commands.describe(honor_uuid="选择要管理的荣誉头衔")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_holders(self, interaction: discord.Interaction, honor_uuid: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send("❌ guild 未就绪", ephemeral=True)
            return

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(
                f"❌ 找不到 UUID `{honor_uuid}` 的荣誉定义。", ephemeral=True,
            )
            return

        view = HonorHoldersManageView(self, guild, honor_def)  # type: ignore[arg-type]
        await view.start(interaction, ephemeral=True)

    @admin_group.command(
        name="同步角色荣誉",
        description="批量 sync role_sync_honor honor 的 role 持有者。三个入口互斥：all_sync / role_id / honor_uuid；执行前需二次确认。",
    )
    @app_commands.describe(
        all_sync="同步全部 role_sync_honor=true 的 honor",
        role_id="按 Discord 身份组 ID 同步（自动补全）",
        honor_uuid="按 honor UUID 同步（自动补全）",
    )
    @app_commands.autocomplete(role_id=role_id_autocomplete)
    @app_commands.autocomplete(honor_uuid=role_sync_honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def cmd_sync_role_honors(
        self,
        interaction: discord.Interaction,
        all_sync: bool = False,
        # role_id 声明为 str 是有意的: Discord 身份组 snowflake 64-bit 常超过
        # 2^53-1, 但 INTEGER 参数的 autocomplete choice 会被 Discord API 做 int53 校验
        # → 即便 Choice.value=str(...) 也照样报 50035。改 STRING 参数绕过此校验,
        # 内部 int() 转换; honor_uuid 本就是 str 同样的原因。
        role_id: Optional[str] = None,
        honor_uuid: Optional[str] = None,
    ):
        """手动批量 sync：遍历 guild 所有成员，给持有 role_sync_honor=true honor role 的人 grant_honor。

        三个入口互斥，必须恰好指定一个：
        - all_sync=True：同步全部 role_sync_honor=true 的 honor
        - role_id=<id>：按 Discord 身份组 ID 找对应 honor
        - honor_uuid=<uuid>：按 honor UUID 找

        执行前发送 ephemeral 二次确认 view。
        """
        # 1. 互斥校验
        specified = sum([all_sync, role_id is not None, honor_uuid is not None])
        if specified != 1:
            await interaction.response.send_message(
                "❌ 必须恰好指定一个参数：all_sync / role_id / honor_uuid",
                ephemeral=True,
            )
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ guild 未就绪", ephemeral=True)
            return

        # 2. 解析 scope + 找 target_honor_uuids
        target_honor_uuids: List[str] = []
        scope_desc = ""

        if all_sync:
            # role_sync_honor 是配置层状态 → cfg.definitions 而不是 ORM
            # config 是真理源 + 没有 is_archived 概念 (归档靠从 toml 删除)
            target_honor_uuids = [
                str(d.uuid) for d in self._cfg_definitions(guild.id)
                if d.role_sync_honor and d.role_id
            ]
            scope_desc = "🌐 全部 role_sync_honor=true honor"
        elif role_id is not None:
            # role_id 是 str（绕过 Discord int53 校验）→ 校验 + 转 int
            if not role_id.isdigit():
                await interaction.response.send_message(
                    f"❌ 身份组 ID `{role_id}` 不是合法的整数",
                    ephemeral=True,
                )
                return
            role_id_int = int(role_id)
            # role_sync_honor 是配置层状态 → cfg 查; 不需要拉 ORM
            cfg_def = self._find_cfg_def_by_role_id(guild.id, role_id_int)
            if cfg_def is None or not cfg_def.role_sync_honor:
                await interaction.response.send_message(
                    f"❌ 身份组 ID {role_id} 没有对应 role_sync_honor 的 honor",
                    ephemeral=True,
                )
                return
            target_honor_uuids = [cfg_def.uuid]
            role = guild.get_role(role_id_int)
            role_name = role.name if role else f"ID:{role_id}"
            scope_desc = f"🎭 [身份组] {role_name}"
        else:  # honor_uuid
            honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
            if honor_def is None:
                await interaction.response.send_message(
                    f"❌ honor '{honor_uuid[:8]}' 不存在",
                    ephemeral=True,
                )
                return
            # role_sync_honor 是配置层状态 → cfg 查
            cfg_def = self._find_cfg_def_by_uuid(guild.id, honor_uuid)
            if cfg_def is None or not cfg_def.role_sync_honor or not cfg_def.role_id:
                await interaction.response.send_message(
                    f"❌ honor '{honor_uuid[:8]}' 无效或非 role_sync_honor 类型",
                    ephemeral=True,
                )
                return
            target_honor_uuids = [honor_uuid]
            scope_desc = f"🏆 [Honor] {honor_def.name}"

        if not target_honor_uuids:
            await interaction.response.send_message(
                "❌ 没有可同步的 honor（toml 没配或都已归档）",
                ephemeral=True,
            )
            return

        # 3. 二次确认 embed + view
        # ★ 按 role 入手计算 sync_pairs（不遍历 guild.members）
        if all_sync:
            scope = "all"
            target_value = None
        elif role_id is not None:
            scope = "role"
            target_value = role_id
        else:
            scope = "honor"
            target_value = honor_uuid

        sync_pairs, skip_reasons = self._collect_sync_pairs(guild, scope, target_value)

        # role/honor scope：skip_reasons 非空 → 直接报错（admin 输入无效）
        if scope in ("role", "honor") and skip_reasons:
            await interaction.response.send_message(
                "\n".join(skip_reasons),
                ephemeral=True,
            )
            return

        estimated_members = sum(
            len([m for m in role.members if not m.bot])
            for _, role in sync_pairs
        )

        confirm_embed = discord.Embed(
            title="⚠️ 确认同步角色荣誉",
            description=(
                f"**Scope**: {scope_desc}\n"
                f"**目标 honor 数**: {len(sync_pairs)}\n"
                f"**预估影响成员**: {estimated_members} 名\n\n"
                f"按 role 入口扫描（不走全 guild）。\n"
                f"此操作**不可撤销**（honor 是永久记录）。"
            ),
            color=discord.Color.orange(),
        )

        # all scope：skip_reasons 展示在二次确认 embed（admin 二次确认时能看到）
        if scope == "all" and skip_reasons:
            shown = "\n".join(skip_reasons[:10])
            extra = f"\n... 共 {len(skip_reasons)} 项" if len(skip_reasons) > 10 else ""
            confirm_embed.add_field(
                name="⚠️ 将被 skip 的项（不阻断）",
                value=shown + extra,
                inline=False,
            )

        view = ConfirmSyncView(
            on_confirm=lambda: self._do_sync_role_honors(
                interaction, scope, target_value, skip_reasons,
            ),
        )
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)

    # --- helpers ---

    def _collect_sync_pairs(
        self,
        guild: discord.Guild,
        scope: str,
        target: Optional[str | int],
    ) -> Tuple[List[Tuple[str, discord.Role]], List[str]]:
        """收集 (sync_pairs, skip_reasons)。

        - all_sync scope：找不到的项记 skip_reason（不阻断，由 caller 决定如何展示）
        - role/honor scope：找不到 → skip_reason 非空，caller 应直接报错
        """
        sync_pairs: List[Tuple[str, discord.Role]] = []
        skip_reasons: List[str] = []

        if scope == "all":
            # role_sync_honor 是配置层状态 → cfg 查, 不用 ORM (ORM 无该字段)
            for cfg_def in self._cfg_definitions(guild.id):
                if not cfg_def.role_sync_honor or not cfg_def.role_id:
                    continue
                role = guild.get_role(cfg_def.role_id)
                if role is None:
                    skip_reasons.append(
                        f"⚠️ honor `{cfg_def.name}` ({str(cfg_def.uuid)[:8]}) 的 "
                        f"role_id {cfg_def.role_id} 在 Discord 中不存在"
                    )
                    continue
                sync_pairs.append((cfg_def.uuid, role))
        elif scope == "role":
            # role_sync_honor 是配置层状态 → cfg 查
            cfg_def = self._find_cfg_def_by_role_id(guild.id, int(target))  # type: ignore[arg-type]
            if cfg_def is None or not cfg_def.role_sync_honor:
                skip_reasons.append(f"❌ 身份组 ID {target} 没有对应 role_sync_honor 的 honor")
            else:
                role = guild.get_role(cfg_def.role_id)
                if role is None:
                    skip_reasons.append(f"❌ 身份组 ID {target} 在 Discord 中不存在")
                else:
                    sync_pairs.append((cfg_def.uuid, role))
        else:  # scope == "honor"
            # role_sync_honor 是配置层状态 → cfg 查
            cfg_def = self._find_cfg_def_by_uuid(guild.id, str(target))  # type: ignore[arg-type]
            if cfg_def is None or not cfg_def.role_sync_honor or not cfg_def.role_id:
                skip_reasons.append(f"❌ honor `{str(target)[:8]}` 无效或非 role_sync_honor 类型")
            else:
                role = guild.get_role(cfg_def.role_id)
                if role is None:
                    skip_reasons.append(
                        f"❌ honor `{cfg_def.name}` 的 role_id {cfg_def.role_id} 在 Discord 中不存在"
                    )
                else:
                    sync_pairs.append((str(target), role))

        return sync_pairs, skip_reasons

    async def _do_sync_role_honors(
        self,
        interaction: discord.Interaction,
        scope: str,
        target: Optional[str | int],
        skip_reasons: List[str],
    ) -> None:
        """按 role 入口扫描 sync（避免遍历整个 guild）。被 ConfirmSyncView.confirm 触发。"""
        guild = interaction.guild
        if guild is None:
            return

        sync_pairs, _ = self._collect_sync_pairs(guild, scope, target)
        total_pairs = len(sync_pairs)

        def _progress_text(processed_pairs: int, granted: int) -> str:
            if total_pairs == 0:
                return (
                    "无需处理（没有可同步的 honor/role 配置）\n"
                    f"已新增 {granted} 条 honor 记录"
                )
            percent = int(processed_pairs / total_pairs * 100)
            bar_width = 10
            filled = "█" * (percent * bar_width // 100)
            empty = "░" * (bar_width - len(filled))
            return (
                f"[{filled}{empty}] {percent}% ({processed_pairs}/{total_pairs})\n"
                f"已扫描 {processed_pairs} 个 honor/role 组合\n"
                f"已新增 {granted} 条 honor 记录"
            )

        progress_embed = discord.Embed(
            title="🔄 同步角色荣誉中...",
            description=_progress_text(0, 0),
            color=discord.Color.blue(),
        )
        progress_msg = await interaction.followup.send(embed=progress_embed)

        granted_count = 0
        processed_pairs = 0
        update_every = max(1, total_pairs // 20)

        for honor_uuid, role in sync_pairs:
            # ★ role.members 直接拿持有该 role 的成员（不遍历 guild.members）
            members = [m for m in role.members if not m.bot]
            for member in members:
                if self.data_manager.grant_honor(member.id, honor_uuid):
                    granted_count += 1

            processed_pairs += 1
            if processed_pairs % update_every == 0 or processed_pairs == total_pairs:
                progress_embed.description = _progress_text(processed_pairs, granted_count)
                try:
                    await progress_msg.edit(embed=progress_embed)
                except discord.NotFound:
                    pass

        done_embed = discord.Embed(
            title="✅ 同步角色荣誉完成",
            description=_progress_text(processed_pairs, granted_count),
            color=discord.Color.green(),
        )

        # ★ 完成时汇报 skip 的项（admin 知道具体哪些 role/honor 配置有问题）
        if skip_reasons:
            shown = "\n".join(skip_reasons[:10])
            extra = f"\n... 共 {len(skip_reasons)} 项" if len(skip_reasons) > 10 else ""
            done_embed.add_field(
                name="⚠️ 被 skip 的项（建议检查 toml / Discord role_id）",
                value=shown + extra,
                inline=False,
            )

        await progress_msg.edit(embed=done_embed)

    # --- 授予 / 批量授予 / 设置最终持有者（2026-08-31 从 cup_honor 提升到全局） ---

    @admin_group.command(
        name="授予",
        description="为用户手动授予一个荣誉及其身份组（任何 honor 都可）。",
    )
    @app_commands.describe(member="要授予荣誉的成员", honor_uuid="要授予的荣誉")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def grant_honor_to_member(
        self, interaction: discord.Interaction, member: discord.Member, honor_uuid: str,
    ):
        await interaction.response.defer(ephemeral=True)

        # 1. 验证 honor 是否存在（任何 honor——普通 + cup_honor 都可）
        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(
                f"❌ **操作失败**：找不到 UUID 为 `{honor_uuid}` 的荣誉定义。", ephemeral=True,
            )
            return

        # 2. 尝试授予 honor（grant_honor 返回定义对象=新授予，返回 None=已存在）
        granted_def = self.data_manager.grant_honor(member.id, honor_uuid)
        response_lines: List[str] = []

        if not granted_def:
            response_lines.append(f"☑️ {member.mention} 已拥有荣誉 **{honor_def.name}**。")
            response_lines.append("ℹ️ 未进行身份组操作，因为用户已持有该荣誉。")
            await interaction.followup.send(
                "\n".join(response_lines), ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        response_lines.append(f"🏅 已为 {member.mention} 授予荣誉 **{honor_def.name}**。")

        # 3. 尝试 add role（如果 honor 有 role_id）—— 仅在 honor 是新授予时执行
        if not honor_def.role_id:
            response_lines.append("⚠️ **提示**：此荣誉未关联任何身份组，无需佩戴。")
        else:
            role = interaction.guild.get_role(honor_def.role_id) if interaction.guild else None
            if role is None:
                response_lines.append(
                    f"❌ **警告**：荣誉已授予，但在服务器中未找到对应的身份组 "
                    f"(ID: {honor_def.role_id})。请联系管理员检查配置。"
                )
            elif role in member.roles:
                response_lines.append(f"☑️ 用户已佩戴身份组 {role.mention}。")
            else:
                try:
                    await member.add_roles(role, reason=f"由 {interaction.user} 手动授予荣誉")
                    response_lines.append(f"✅ 已为用户佩戴身份组 {role.mention}。")
                except discord.Forbidden:
                    response_lines.append(
                        f"❌ **权限不足**：荣誉已授予，但我无法为用户添加身份组 {role.mention}。"
                    )
                except Exception as e:
                    self.logger.error(f"为用户 {member} 添加身份组 {role.name} 时出错: {e}", exc_info=True)
                    response_lines.append("❌ **未知错误**：荣誉已授予，但添加身份组时发生错误。")

        await interaction.followup.send(
            "\n".join(response_lines), ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @admin_group.command(
        name="批量授予",
        description="批量授予一个荣誉给多个用户（从 ID 列表或消息链接解析目标）。",
    )
    @app_commands.describe(
        honor_uuid="要授予的荣誉。",
        user_ids="【模式一】要授予的用户的 ID，用英文逗号分隔。",
        message_link="【模式二】包含目标用户的消息链接，将授予所有被提及的用户。",
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def bulk_grant_honor(
        self,
        interaction: discord.Interaction,
        honor_uuid: str,
        user_ids: Optional[str] = None,
        message_link: Optional[str] = None,
    ):
        """批量授予 honor，支持从 ID 列表或消息链接中解析用户。

        通用机制——任何 honor（普通 + cup_honor）都可批量授予。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send("❌ guild 未就绪", ephemeral=True)
            return

        # 1. 输入验证
        if not user_ids and not message_link:
            await interaction.followup.send(
                "❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True,
            )
            return
        if user_ids and message_link:
            await interaction.followup.send(
                "❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True,
            )
            return

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(
                f"❌ **错误**：找不到 UUID 为 `{honor_uuid}` 的荣誉定义。", ephemeral=True,
            )
            return

        # 2. 收集用户
        try:
            members_to_process, error_logs = await self._parse_members_from_input(
                guild, user_ids, message_link,
            )
        except (ValueError, IOError) as e:
            await interaction.followup.send(f"❌ **操作失败**：{e}", ephemeral=True)
            return

        if not members_to_process:
            final_message = "🤷 **操作终止**：未找到任何有效的、非机器人的用户进行操作。"
            if error_logs:
                final_message += "\n\n**解析遇到的问题：**\n" + "\n".join(error_logs)
            await interaction.followup.send(final_message, ephemeral=True)
            return

        # 3. 二次确认
        member_mentions = " ".join([m.mention for m in members_to_process])
        if len(member_mentions) > 1000:
            member_mentions = f"共 {len(members_to_process)} 人，列表过长已省略。"

        embed = discord.Embed(
            title="⚠️ 批量授予确认",
            description=(
                f"你即将为以下 **{len(members_to_process)}** 位成员授予荣誉：\n"
                f"**{honor_def.name}**"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="目标成员", value=member_mentions, inline=False)
        embed.set_footer(text="请确认操作。此操作将在后台进行。")

        view = ConfirmationView(author=interaction.user)
        view.message = await interaction.followup.send(
            embed=embed, view=view, ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await view.wait()

        if view.value is None:  # 超时
            return
        if not view.value:
            await interaction.edit_original_response(
                content="操作已取消。", embed=None, view=None,
            )
            return
        await interaction.edit_original_response(
            content="⚙️ 正在处理，请稍候...", embed=None, view=None,
        )

        # 4. 执行
        newly_granted: List[discord.Member] = []
        already_had: List[discord.Member] = []
        role_added: List[discord.Member] = []
        role_failed: List[discord.Member] = []
        role = guild.get_role(honor_def.role_id) if honor_def.role_id else None

        for member in members_to_process:
            if self.data_manager.grant_honor(member.id, honor_uuid):
                newly_granted.append(member)
            else:
                already_had.append(member)
            if role and role not in member.roles:
                try:
                    await member.add_roles(
                        role, reason=f"由 {interaction.user} 批量授予荣誉",
                    )
                    role_added.append(member)
                except discord.Forbidden:
                    role_failed.append(member)
                except Exception:
                    role_failed.append(member)

        # 5. 报告
        final_embed = discord.Embed(
            title="✅ 批量授予完成",
            description=f"已完成对 **{honor_def.name}** 荣誉的批量授予操作。",
            color=discord.Color.green(),
        )
        final_embed.add_field(
            name="总处理人数", value=f"`{len(members_to_process)}` 人", inline=False,
        )
        final_embed.add_field(
            name="新授予荣誉", value=f"`{len(newly_granted)}` 人", inline=True,
        )
        final_embed.add_field(
            name="本已拥有", value=f"`{len(already_had)}` 人", inline=True,
        )
        role_status_parts: List[str] = []
        if role:
            role_status_parts.append(f"新佩戴: `{len(role_added)}`")
            if role_failed:
                role_status_parts.append(f"失败: `{len(role_failed)}`")
            role_status = " | ".join(role_status_parts)
        else:
            role_status = "未关联身份组"
        final_embed.add_field(name="身份组状态", value=role_status, inline=True)
        if error_logs:
            final_embed.add_field(
                name="解析警告", value="\n".join(error_logs[:5]), inline=False,
            )
        await interaction.edit_original_response(content="", embed=final_embed)

    @admin_group.command(
        name="设置最终持有者-危险操作-仅必要时",
        description="【底层危险命令】设置 honor 的最终持有者名单：grant + add role 给名单成员，remove role 给名单外。",
    )
    @app_commands.describe(
        honor_uuid="要操作的 honor。",
        user_ids="【模式一】最终持有者的 ID，用英文逗号分隔。",
        message_link="【模式二】包含最终持有者的消息链接。",
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def set_final_holders(
        self,
        interaction: discord.Interaction,
        honor_uuid: str,
        user_ids: Optional[str] = None,
        message_link: Optional[str] = None,
    ):
        """【底层危险命令】将提供的用户列表设置为 honor 的唯一持有者。

        行为：
        1. 给名单内成员 grant_honor + add_role（如果 honor 有 role_id）
        2. **remove_role** 给名单外但持有该 role 的成员

        ⚠️ 此命令违反 unix 哲学默认（bot 不调 remove_roles）——仅在管理员**已确认**需要批量清理时使用。
        保留为"底层危险命令"按用户拍板（2026-08-31）。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send("❌ guild 未就绪", ephemeral=True)
            return

        # 1. 输入验证
        if not user_ids and not message_link:
            await interaction.followup.send(
                "❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True,
            )
            return
        if user_ids and message_link:
            await interaction.followup.send(
                "❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True,
            )
            return

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            await interaction.followup.send(
                f"❌ **错误**：此荣誉未定义或未关联身份组，无法执行同步操作。", ephemeral=True,
            )
            return

        role = guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(
                f"❌ **错误**：在服务器中找不到与荣誉关联的身份组 (ID: {honor_def.role_id})。",
                ephemeral=True,
            )
            return

        try:
            definitive_members, error_logs = await self._parse_members_from_input(
                guild, user_ids, message_link,
            )
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

        # 3. 二次确认
        embed = discord.Embed(
            title="‼️ 高危操作确认：设置最终持有者",
            description=(
                f"你即将同步荣誉 **{honor_def.name}** 及其身份组 {role.mention}。\n"
                f"**提供的名单将被视为唯一合法的持有者名单。**\n\n"
                f"⚠️ 此命令会 **remove role 给名单外成员**——违反默认 unix 哲学，谨慎使用。"
            ),
            color=discord.Color.red(),
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
        view.message = await interaction.followup.send(
            embed=embed, view=view, ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await view.wait()

        if view.value is None:
            return
        if view.value is False:
            await interaction.edit_original_response(
                content="操作已取消。", embed=None, view=None,
            )
            return
        await interaction.edit_original_response(
            content="⚙️ **正在执行同步...** 这可能需要一些时间。", embed=None, view=None,
        )

        # 4. 执行
        newly_granted = 0
        role_added_ok = 0
        role_removed_ok = 0
        role_add_failed: List[str] = []
        role_remove_failed: List[str] = []

        for member in definitive_members:
            if self.data_manager.grant_honor(member.id, honor_uuid):
                newly_granted += 1
        for member in members_to_add:
            try:
                await member.add_roles(role, reason=f"由 {interaction.user} 执行「设置持有者」操作")
                role_added_ok += 1
            except Exception:
                role_add_failed.append(member.mention)
        for member in members_to_remove:
            try:
                await member.remove_roles(role, reason=f"由 {interaction.user} 执行「设置持有者」操作")
                role_removed_ok += 1
            except Exception:
                role_remove_failed.append(member.mention)

        # 5. 报告
        final_embed = discord.Embed(
            title="✅ 同步操作完成",
            description=(
                f"已根据你的名单，完成对荣誉 **{honor_def.name}** ({role.mention}) 的持有者设置。"
            ),
            color=discord.Color.green(),
        )
        final_embed.add_field(
            name="最终持有者总数", value=f"`{len(definitive_members)}` 人", inline=False,
        )
        final_embed.add_field(
            name="新授予荣誉记录", value=f"`{newly_granted}` 人", inline=True,
        )
        final_embed.add_field(
            name="新佩戴身份组", value=f"`{role_added_ok}` 人", inline=True,
        )
        final_embed.add_field(
            name="被移除身份组", value=f"`{role_removed_ok}` 人", inline=True,
        )
        if role_add_failed or role_remove_failed:
            error_details = ""
            if role_add_failed:
                error_details += (
                    f"**添加失败 ({len(role_add_failed)}人):** "
                    f"{' '.join(role_add_failed)}\n"
                )
            if role_remove_failed:
                error_details += (
                    f"**移除失败 ({len(role_remove_failed)}人):** "
                    f"{' '.join(role_remove_failed)}"
                )
            final_embed.add_field(
                name="⚠️ 操作失败详情 (通常为权限问题)", value=error_details, inline=False,
            )
        await interaction.edit_original_response(content="", embed=final_embed)

    @staticmethod
    async def _parse_members_from_input(
        guild: discord.Guild,
        user_ids: Optional[str] = None,
        message_link: Optional[str] = None,
    ) -> Tuple[Set[discord.Member], List[str]]:
        """通用 helper：从用户 ID 列表或消息链接中解析成员。

        返回 (members_set, error_logs)。两个输入互斥，调用方校验。
        """
        members_to_process: Set[discord.Member] = set()
        error_logs: List[str] = []

        if user_ids:
            id_list = {uid.strip() for uid in user_ids.split(",")}
            for uid_str in id_list:
                if not uid_str.isdigit():
                    error_logs.append(f"无效ID格式: `{uid_str}`")
                    continue
                try:
                    member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                    if not member.bot:
                        members_to_process.add(member)
                except discord.NotFound:
                    error_logs.append(f"未找到用户: `{uid_str}`")

        elif message_link:
            match = re.search(r"discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)", message_link)
            if not match or int(match.group(1)) != guild.id:
                raise ValueError("无效的消息链接，或链接不属于本服务器。")

            channel_id, message_id = int(match.group(2)), int(match.group(3))
            try:
                channel = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
                message = await channel.fetch_message(message_id)

                all_mentioned_members = set(message.mentions)
                content_to_scan = message.content
                for embed in message.embeds:
                    if embed.description:
                        content_to_scan += "\n" + embed.description
                    for field in embed.fields:
                        content_to_scan += f"\n{field.name}\n{field.value}"

                mentioned_ids = re.findall(r"<@!?(\d+)>", content_to_scan)
                for uid_str in set(mentioned_ids):
                    try:
                        member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                        if member:
                            all_mentioned_members.add(member)
                    except discord.NotFound:
                        error_logs.append(f"消息中提及的用户 `{uid_str}` 未找到。")

                for member in all_mentioned_members:
                    if not member.bot:
                        members_to_process.add(member)

            except (discord.NotFound, discord.Forbidden) as e:
                raise IOError(f"找不到指定的消息/频道，或我没有权限访问它: {e}")

        return members_to_process, error_logs

    @admin_group.command(
        name="重置通知状态",
        description="【维护】重置一个 honor 的「已通知」状态，使其可以再次触发到期提醒。",
    )
    @app_commands.describe(honor_uuid="选择要重置通知状态的 honor（普通 + cup_honor 都可）。")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def reset_notification_state(
        self, interaction: discord.Interaction, honor_uuid: str,
    ):
        """重置 honor 的「已通知」状态——通用工具（2026-08-31 从 cup_honor 提升）。

        通用机制：HonorExpirationCog 推过期提醒后会调 `notification_manager.add_notified(uuid)`
        防止重复推。如果 admin 误操作（删了 role 还要再发一次等），可用此命令重置状态。
        """
        await interaction.response.defer(ephemeral=True)

        # 验证 honor 是否存在（任何 honor——普通 + cup_honor 都可）
        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(
                f"❌ **操作失败**：找不到 UUID 为 `{honor_uuid}` 的荣誉定义。", ephemeral=True,
            )
            return

        was_removed = await self.notification_manager.remove_notified(honor_uuid)

        if was_removed:
            embed = discord.Embed(
                title="✅ 通知状态已重置",
                description=(
                    f"荣誉 **{honor_def.name}** 的「已通知」标记已被移除。\n"
                    f"在下一次到期检查时，如果它仍然符合过期条件，将会**重新发送通知**。"
                ),
                color=discord.Color.green(),
            )
            embed.set_footer(text=f"UUID: {honor_uuid}")
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(
                title="ℹ️ 无需操作",
                description=(
                    f"荣誉 **{honor_def.name}** 本来就**不**在已通知列表中。\n"
                    f"无需进行重置。"
                ),
                color=discord.Color.blue(),
            )
            embed.set_footer(text=f"UUID: {honor_uuid}")
            await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: "RoleBot") -> None:
    """Cog 入口点。cog_map 中排在 HonorCog 之后。"""
    await bot.add_cog(HonorAdminCog(bot))


__all__ = [
    "ConfirmSyncView",
    "HonorAdminCog",
    "setup",
]
