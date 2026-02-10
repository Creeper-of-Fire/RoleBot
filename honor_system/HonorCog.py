# honor_system/cog.py
from __future__ import annotations

import asyncio
from typing import cast, Optional, TYPE_CHECKING, Dict, List

import discord
from discord import ui, Color, app_commands

import config
import config_data
from core.embed_link.embed_manager import EmbedLinkManager
from honor_system.cup_honor.cup_honor_json_manager import CupHonorJsonManager
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
        self.running_backfill_tasks: Dict[int, asyncio.Task] = {}
        # 安全缓存，用于存储此模块管理的所有身份组ID
        self.safe_honor_role_ids: set[int] = set()

        self.bot.loop.create_task(self.synchronize_all_honor_definitions())

        self.guide_manager = EmbedLinkManager.get_or_create(
            key="honor_celebrate_guide",
            bot=self.bot,
            default_embed=discord.Embed(
                title="🎊 当前进行中的荣誉获取活动",
                description="管理员尚未配置，或正在加载中。",
                color=Color.orange()
            )
        )

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

    def get_all_config_honor_definitions(self) -> list[BaseHonorDefinition]:
        """
        获取所有配置源（config.py, cup_honors.json）中的荣誉定义，
        并以统一的 BaseHonorDefinition 模型对象列表返回。
        """
        all_definitions: list[BaseHonorDefinition] = []

        # 1. 从 config_data.py 加载普通荣誉
        for guild_id, guild_config in config_data.HONOR_CONFIG.items():
            for honor_dict in guild_config.get("definitions", []):
                all_definitions.append(BaseHonorDefinition.model_validate(honor_dict))

        # 2. 从 JSON文件 加载杯赛荣誉
        self.cup_honor_manager.load_data()  # 确保加载最新数据
        all_cup_honors = self.cup_honor_manager.get_all_cup_honors()
        # CupHonorDefinition 已经是 BaseHonorDefinition 的子类，可以直接添加
        all_definitions.extend(all_cup_honors)

        # 3. 合并所有合法的、不应被归档的荣誉UUID
        return all_definitions

    async def synchronize_all_honor_definitions(self):
        await self.bot.wait_until_ready()
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")

        all_config_definitions = self.get_all_config_honor_definitions()
        all_legitimate_uuids = {str(d.uuid) for d in all_config_definitions}

        # 2. 遍历配置，处理创建和更新
        with self.data_manager.get_db() as db:
            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                for config_def in guild_config.get("definitions", []):
                    # 查找当前配置项对应的数据库记录 (通过 UUID)
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_def['uuid']).one_or_none()

                    if db_def:
                        # 记录存在，更新它
                        db_def.name = config_def['name']
                        db_def.description = config_def['description']
                        db_def.role_id = config_def.get('role_id', None)
                        db_def.icon_url = config_def.get('icon_url', None)
                        db_def.guild_id = guild_id
                        db_def.hidden_until_earned = config_def.get('hidden_until_earned', True)  # 确保有默认值
                        db_def.is_archived = False  # 确保它处于激活状态
                    else:
                        # 记录不存在，创建它
                        new_def = HonorDefinition(
                            uuid=config_def['uuid'],
                            guild_id=guild_id,
                            name=config_def['name'],
                            description=config_def['description'],
                            role_id=config_def.get('role_id', None),
                            icon_url=config_def.get('icon_url', None),
                            hidden_until_earned=config_def.get('hidden_until_earned', True),  # 确保有默认值
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_def['name']}")

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


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
