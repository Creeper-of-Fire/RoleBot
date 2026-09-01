# honor_system/cog.py
from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import cast, Optional, Tuple, TYPE_CHECKING, Dict, List

import discord
from discord import ui, Color

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
from honor_system.cup_honor.cup_honor_json_manager import CupHonorJsonManager
from honor_system.honor_config_manager import HonorConfigManager
from utility.feature_cog import FeatureCog, PanelEntry
from utility.scheduled_loop import scheduled_loop
from honor_system.module.common_models import BaseHonorDefinition
from honor_system.getCogs import getHonorAnniversaryModuleCog, getRoleClaimHonorModuleCog
from honor_system.data_manager.honor_data_manager import HonorDataManager
from .honor_def_models import HonorDefinition
from .HonorManageView import HonorManageView

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
        # synchronize_all_honor_definitions 改用 @scheduled_loop，启动移至 cog_load

    async def cog_load(self) -> None:
        """Cog 加载时：注册到 CoreCog + 启动 scheduled_loop 任务。"""
        # super().cog_load() 等 1 秒然后注册——保留这个行为
        await super().cog_load()
        # 启动 @scheduled_loop 装饰的启动期一次性任务
        # 等 ready 后会跑一次（run_on_startup=True）
        self.synchronize_all_honor_definitions.start()

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

    @scheduled_loop(count=1, run_on_startup=True)
    async def synchronize_all_honor_definitions(self):
        """启动期一次性同步 honor toml → SQLite。

        重活（toml 读 + SQLAlchemy session + db.commit()）放后台线程池跑——
        防 sync SQLAlchemy 阻塞事件循环造成 404。

        装饰器自动注入 wait_until_ready；run_on_startup=True 表示 ready 后立即跑一次；
        count=1 表示跑 1 次后停止（这是启动期一次性任务，不是周期任务）。
        """
        # 装饰器会自动注入 await self.bot.wait_until_ready()
        # 整个 body 扔到后台线程池
        await asyncio.to_thread(self._blocking_sync_worker)

    def _blocking_sync_worker(self):
        """[同步/线程池] synchronize_all_honor_definitions 的重活版本——跑在后台线程。

        所有同步操作（Path.glob + toml 解析 + SQLAlchemy session + db.commit()）都搬到这里。
        原 async 方法保留 async 接口 + scheduled_loop 调度。
        """
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


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
