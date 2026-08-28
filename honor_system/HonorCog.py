# honor_system/cog.py
from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import cast, Optional, Tuple, TYPE_CHECKING, Dict, List

import discord
from discord import ui, Color, app_commands

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
from honor_system.cup_honor.cup_honor_json_manager import CupHonorJsonManager
from honor_system.honor_config_manager import HonorConfigManager
from utility.feature_cog import FeatureCog, PanelEntry
from honor_system.module.common_models import BaseHonorDefinition
from honor_system.getCogs import getHonorAnniversaryModuleCog, getRoleClaimHonorModuleCog
from honor_system.data_manager.honor_data_manager import HonorDataManager
from .honor_def_models import HonorDefinition
from .HonorManageView import HonorHoldersManageView, HonorManageView

if TYPE_CHECKING:
    from main import RoleBot


# --- 主Cog ---
class HonorCog(FeatureCog, name="Honor"):
    """管理荣誉系统"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)  # 调用父类 (FeatureCog) 的构造函数
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.cup_honor_manager = CupHonorJsonManager.get_instance(logger=self.logger)
        # honor toml 配置（每个 guild 一份 data/honor_{guild_id}.toml）—— 单例 + cache
        self.honor_config = HonorConfigManager.get_instance()
        self.running_backfill_tasks: Dict[int, asyncio.Task] = {}
        # 安全缓存，用于存储此模块管理的所有身份组ID
        self.safe_honor_role_ids: set[int] = set()

        self.bot.loop.create_task(self.synchronize_all_honor_definitions())

    def get_guide_embed(self, guild_id: int) -> discord.Embed:
        """按 guild_id 取荣誉活动指引 embed——走 embed_guides_{guild_id}.toml。

        ``EmbedGuidesConfigManager.get(guild_id).honor_celebrate_guide`` 始终返回
        有效 ``EmbedGuideSection``（默认或配置），无 None 检查必要。
        """
        return EmbedGuidesConfigManager.get_instance().get(guild_id).honor_celebrate_guide.to_embed()

    # 注：guide_url 已删除——toml 是 source of truth，不再有 Discord 跳转 URL。

    # --- FeatureCog 接口实现 ---
    async def update_safe_roles_cache(self):
        """
        [接口实现] 从荣誉定义中更新此模块管理的安全身份组缓存。
        """
        self.logger.info(f"模块 '{self.qualified_name}' 开始更新安全身份组缓存...")

        new_cache = set()

        # 从数据库中获取所有荣誉定义
        all_honor_defs = []
        with self.data_manager.get_db() as db:
            all_honor_defs = db.query(HonorDefinition).filter(HonorDefinition.is_archived == False).all()

        if not all_honor_defs:
            self.logger.info(f"模块 '{self.qualified_name}' 没有找到任何荣誉定义。")
            self.safe_honor_role_ids = new_cache
            return

        for honor_def in all_honor_defs:
            if honor_def.role_id:
                new_cache.add(honor_def.role_id)

        self.safe_honor_role_ids = new_cache
        self.logger.info(f"模块 '{self.qualified_name}' 安全缓存更新完毕，共加载 {len(self.safe_honor_role_ids)} 个身份组。")

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """
        [接口实现] 返回一个用于主面板的 "我的荣誉墙" 按钮。
        """

        async def honor_panel_callback(interaction: discord.Interaction):
            # 这是原 /荣誉面板 命令的所有逻辑
            await interaction.response.defer(ephemeral=True)
            member = cast(discord.Member, interaction.user)
            guild = cast(discord.Guild, interaction.guild)

            # --- 调用子模块进行检查 ---
            anniversary_cog = getHonorAnniversaryModuleCog(self)
            if anniversary_cog:
                # 调用子模块执行其独立的检查逻辑
                await anniversary_cog.check_and_grant_anniversary_honor(member, guild)
            else:
                self.logger.warning("无法找到 HonorAnniversaryModule 来检查周年荣誉。")

            # 调用新模块，检查基于身份组的荣誉
            role_claim_cog = getRoleClaimHonorModuleCog(self)
            if role_claim_cog:
                await role_claim_cog.check_and_grant_role_sync_honor(member, guild)
            else:
                self.logger.warning("无法找到 RoleClaimHonorModule 来检查基于身份组的荣誉。")

            view = HonorManageView(self, member, guild)

            await view.start(interaction, ephemeral=True)

        honor_button = ui.Button(
            label="我的荣誉墙",
            style=discord.ButtonStyle.secondary,
            emoji="🏆",
            custom_id="honor_cog:show_honor_panel"
        )
        honor_button.callback = honor_panel_callback

        return [
            PanelEntry(
                button=honor_button,
                description="管理/查看你的荣誉，__包括杯赛荣誉__。"
            )
        ]

    def _iter_configured_guild_ids(self) -> list[int]:
        """返回所有已配置 toml 的 guild_id（按 data/honor_*.toml 遍历）。"""
        data_dir = Path("data")
        if not data_dir.exists():
            return []
        guild_ids: list[int] = []
        for toml_path in data_dir.glob("honor_*.toml"):
            m = re.match(r"honor_(\d+)\.toml", toml_path.name)
            if m:
                guild_ids.append(int(m.group(1)))
        return guild_ids

    def get_all_config_honor_definitions(self) -> list[BaseHonorDefinition]:
        """
        获取所有配置源（honor_{guild_id}.toml + cup_honors.json）中的荣誉定义，
        并以统一的 BaseHonorDefinition 模型对象列表返回。
        """
        all_definitions: list[BaseHonorDefinition] = []

        # 1. 从 honor_{guild_id}.toml 加载普通荣誉（按 data/ 目录下的 toml 文件遍历）
        for guild_id in self._iter_configured_guild_ids():
            try:
                cfg = self.honor_config.get(guild_id)
            except Exception as e:
                self.logger.error(
                    "加载 honor toml 失败 guild %s: %s", guild_id, e,
                )
                continue
            if cfg is None:
                # _iter 已筛过 toml 存在的 guild，这里只是防御
                continue
            for item in cfg.definitions:
                all_definitions.append(BaseHonorDefinition.model_validate(item.model_dump()))

        # 2. 从 JSON文件 加载杯赛荣誉
        self.cup_honor_manager.load_data()  # 确保加载最新数据
        all_cup_honors = self.cup_honor_manager.get_all_cup_honors()
        # CupHonorDefinition 已经是 BaseHonorDefinition 的子类，可以直接添加
        all_definitions.extend(all_cup_honors)

        return all_definitions

    async def synchronize_all_honor_definitions(self):
        await self.bot.wait_until_ready()
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")

        all_config_definitions = self.get_all_config_honor_definitions()
        all_legitimate_uuids = {str(d.uuid) for d in all_config_definitions}

        # 2. 遍历配置，处理创建和更新
        with self.data_manager.get_db() as db:
            for guild_id in self._iter_configured_guild_ids():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                try:
                    cfg = self.honor_config.get(guild_id)
                except Exception as e:
                    self.logger.error(
                        "加载 honor toml 失败 guild %s: %s", guild_id, e,
                    )
                    continue
                if cfg is None:
                    # _iter 已筛过 toml 存在的 guild，这里只是防御
                    continue
                for config_def in cfg.definitions:
                    config_dict = config_def.model_dump()
                    # 查找当前配置项对应的数据库记录 (通过 UUID)
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_dict['uuid']).one_or_none()

                    if db_def:
                        # 记录存在，更新它
                        db_def.name = config_dict['name']
                        db_def.description = config_dict['description']
                        db_def.role_id = config_dict.get('role_id', None)
                        db_def.icon_url = config_dict.get('icon_url', None)
                        db_def.guild_id = guild_id
                        db_def.hidden_until_earned = config_dict.get('hidden_until_earned', True)  # 确保有默认值
                        db_def.is_archived = False  # 确保它处于激活状态
                    else:
                        # 记录不存在，创建它
                        new_def = HonorDefinition(
                            uuid=config_dict['uuid'],
                            guild_id=guild_id,
                            name=config_dict['name'],
                            description=config_dict['description'],
                            role_id=config_dict.get('role_id', None),
                            icon_url=config_dict.get('icon_url', None),
                            hidden_until_earned=config_dict.get('hidden_until_earned', True),  # 确保有默认值
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_dict['name']}")

            # 5. 归档操作：只归档那些既不在config也不在cup_honor.json中的荣誉
            db_uuids_to_check = db.query(HonorDefinition.uuid).filter(HonorDefinition.is_archived == False).all()
            db_uuids_set = {uuid_tuple[0] for uuid_tuple in db_uuids_to_check}

            uuids_to_archive = db_uuids_set - all_legitimate_uuids

            if uuids_to_archive:
                self.logger.warning(f"发现 {len(uuids_to_archive)} 个需要归档的荣誉...")
                # 使用 in_ 操作批量更新
                db.query(HonorDefinition).filter(HonorDefinition.uuid.in_(uuids_to_archive)).update({"is_archived": True}, synchronize_session=False)

            # 最终提交所有更改
            db.commit()

        self.logger.info("HonorCog: 荣誉定义同步完成。")

    # --- 管理员指令组 ---
    honor_admin_group = app_commands.Group(
        name="荣誉头衔丨核心",
        description="管理荣誉头衔",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

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

            choice_name = f"{honor_def.name} ({honor_def.uuid[:8]})"
            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=honor_def.uuid))

        return choices[:25]

    @honor_admin_group.command(name="管理持有者", description="查看并移除特定荣誉的持有者。")
    @app_commands.describe(honor_uuid="选择要管理的荣誉头衔")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_holders(self, interaction: discord.Interaction, honor_uuid: str):
        """启动一个视图，用于管理特定荣誉的持有者。"""
        await interaction.response.defer(ephemeral=True)
        guild = cast(discord.Guild, interaction.guild)

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ 找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        view = HonorHoldersManageView(self, guild, honor_def)
        await view.start(interaction, ephemeral=True)

    # === 同步角色荣誉（手动批量 sync） ===

    async def role_id_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[int]]:
        """列出 role_sync_honor=true 的 honor 对应的 Discord 身份组 ID（按身份组名匹配）。"""
        guild = interaction.guild
        if guild is None:
            return []

        all_defs = self.data_manager.get_all_honor_definitions(guild.id)
        choices: List[app_commands.Choice[int]] = []

        for honor_def in all_defs:
            if honor_def.is_archived:
                continue
            if not honor_def.role_sync_honor or not honor_def.role_id:
                continue

            role = guild.get_role(honor_def.role_id)
            if not role:
                continue

            role_name = role.name
            if not current or current.lower() in role_name.lower():
                choices.append(app_commands.Choice(
                    name=f"🎭 {role_name} ({role.id})"[:100],
                    value=role.id,
                ))

        return choices[:25]

    async def role_sync_honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """只列出 role_sync_honor=true 的 honor UUID。"""
        all_defs = self.data_manager.get_all_honor_definitions(interaction.guild_id)
        choices: List[app_commands.Choice[str]] = []

        for honor_def in all_defs:
            if honor_def.is_archived:
                continue
            if not honor_def.role_sync_honor:
                continue

            choice_name = f"{honor_def.name} ({str(honor_def.uuid)[:8]})"
            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(honor_def.uuid)))

        return choices[:25]

    @honor_admin_group.command(
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
            role_id: Optional[int] = None,
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

        guild = cast(discord.Guild, interaction.guild)
        if guild is None:
            await interaction.response.send_message("❌ guild 未就绪", ephemeral=True)
            return

        # 2. 解析 scope + 找 target_honor_uuids
        target_honor_uuids: List[str] = []
        scope_desc = ""

        if all_sync:
            all_defs = self.data_manager.get_all_honor_definitions(guild.id)
            target_honor_uuids = [
                str(d.uuid) for d in all_defs
                if d.role_sync_honor and d.role_id and not d.is_archived
            ]
            scope_desc = "🌐 全部 role_sync_honor=true honor"
        elif role_id is not None:
            with self.data_manager.get_db() as db:
                honor_def = db.query(HonorDefinition).filter_by(
                    role_id=role_id, is_archived=False,
                ).first()
            if honor_def is None or not honor_def.role_sync_honor:
                await interaction.response.send_message(
                    f"❌ 身份组 ID {role_id} 没有对应 role_sync_honor 的 honor",
                    ephemeral=True,
                )
                return
            target_honor_uuids = [str(honor_def.uuid)]
            role = guild.get_role(role_id)
            role_name = role.name if role else f"ID:{role_id}"
            scope_desc = f"🎭 [身份组] {role_name}"
        else:  # honor_uuid
            with self.data_manager.get_db() as db:
                honor_def = db.query(HonorDefinition).filter_by(
                    uuid=honor_uuid, is_archived=False,
                ).first()
            if honor_def is None or not honor_def.role_sync_honor or not honor_def.role_id:
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
            all_defs = self.data_manager.get_all_honor_definitions(guild.id)
            for honor_def in all_defs:
                if not honor_def.role_sync_honor or not honor_def.role_id or honor_def.is_archived:
                    continue
                role = guild.get_role(honor_def.role_id)
                if role is None:
                    skip_reasons.append(
                        f"⚠️ honor `{honor_def.name}` ({str(honor_def.uuid)[:8]}) 的 "
                        f"role_id {honor_def.role_id} 在 Discord 中不存在"
                    )
                    continue
                sync_pairs.append((str(honor_def.uuid), role))
        elif scope == "role":
            with self.data_manager.get_db() as db:
                honor_def = db.query(HonorDefinition).filter_by(
                    role_id=target, is_archived=False,
                ).first()
            if honor_def is None or not honor_def.role_sync_honor:
                skip_reasons.append(f"❌ 身份组 ID {target} 没有对应 role_sync_honor 的 honor")
            else:
                role = guild.get_role(honor_def.role_id)
                if role is None:
                    skip_reasons.append(f"❌ 身份组 ID {target} 在 Discord 中不存在")
                else:
                    sync_pairs.append((str(honor_def.uuid), role))
        else:  # scope == "honor"
            honor_def = self.data_manager.get_honor_definition_by_uuid(target)
            if honor_def is None or not honor_def.role_sync_honor or not honor_def.role_id:
                skip_reasons.append(f"❌ honor `{target[:8]}` 无效或非 role_sync_honor 类型")
            else:
                role = guild.get_role(honor_def.role_id)
                if role is None:
                    skip_reasons.append(
                        f"❌ honor `{honor_def.name}` 的 role_id {honor_def.role_id} 在 Discord 中不存在"
                    )
                else:
                    sync_pairs.append((target, role))

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


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
