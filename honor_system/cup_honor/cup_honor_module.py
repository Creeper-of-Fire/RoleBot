# honor_system/cup_honor_module.py
from __future__ import annotations

import datetime
import re
import typing
from pathlib import Path
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import discord
from discord import app_commands, ui
from discord.ext import commands

from shared.ui.views import ConfirmationView
from .cup_honor_json_manager import CupHonorJsonManager
from .cup_honor_models import CupHonorDefinition
from honor_system.data_manager.honor_data_manager import HonorDataManager
from honor_system.honor_config_manager import HonorConfigManager
from honor_system.honor_def_models import UserHonor, HonorDefinition
from .cup_honor_module_view import CupHonorManageView

if typing.TYPE_CHECKING:
    from main import RoleBot


class CupHonorModuleCog(commands.Cog, name="CupHonorModule"):
    """【荣誉子模块】管理手动的、有时效性的杯赛头衔。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger
        self.honor_data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.cup_honor_manager = CupHonorJsonManager.get_instance(logger=self.logger)
        # honor toml 元配置（每个 guild 一份 data/honor_{guild_id}.toml）—— cup_honor.notification
        self.honor_config = HonorConfigManager.get_instance()
        # 注：NotificationStateManager 已搬到 honor_system/ 顶层（2026-08-31）—— HonorExpirationCog 用同一个单例。



    # --- 数据库同步辅助函数 ---
    async def sync_cup_honor_to_db(self, guild_id: int, honor_def: CupHonorDefinition, original_uuid_str: Optional[str] = None):
        """将Pydantic模型的数据同步（插入或更新）到SQLAlchemy数据库。"""
        with self.honor_data_manager.get_db() as db:
            # 如果UUID改变了，需要将旧的记录归档
            if original_uuid_str and original_uuid_str != str(honor_def.uuid):
                old_db_def = db.query(HonorDefinition).filter_by(uuid=original_uuid_str).one_or_none()
                if old_db_def:
                    self.logger.warning(f"杯赛荣誉UUID从 {original_uuid_str} 变更为 {honor_def.uuid}，正在归档旧记录...")
                    old_db_def.is_archived = True
                    db.add(old_db_def)

            # 查找或创建新的数据库记录
            db_def = db.query(HonorDefinition).filter_by(uuid=str(honor_def.uuid)).one_or_none()
            if not db_def:
                db_def = HonorDefinition(uuid=str(honor_def.uuid), guild_id=guild_id)
                self.logger.info(f"为杯赛荣誉 '{honor_def.name}' 创建新的数据库记录。")

            # 更新数据
            db_def.name = honor_def.name
            db_def.description = honor_def.description
            db_def.role_id = honor_def.role_id
            db_def.hidden_until_earned = honor_def.hidden_until_earned
            db_def.is_archived = False  # 确保是激活状态

            db.add(db_def)
            db.commit()

    async def archive_honor_in_db(self, honor_uuid: str):
        """在数据库中归档一个荣誉定义。"""
        with self.honor_data_manager.get_db() as db:
            db_def = db.query(HonorDefinition).filter_by(uuid=honor_uuid).one_or_none()
            if db_def:
                db_def.is_archived = True
                db.add(db_def)
                db.commit()
                self.logger.info(f"已在数据库中归档荣誉 {honor_uuid}。")



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
        """
               为杯赛荣誉UUID参数提供自动补全选项。
               选项会按过期时间降序排列，并在结果过多时提示用户。
               """
        # 1. 获取所有杯赛荣誉
        all_cup_honors = self.cup_honor_manager.get_all_cup_honors()
        if not all_cup_honors:
            return []

        # 2. 按过期时间降序排序
        #    这样最新、最晚到期的荣誉会优先显示在列表顶部
        sorted_honors = sorted(
            all_cup_honors,
            key=lambda h: h.cup_honor.expiration_date,
            reverse=True
        )

        # 3. 根据用户输入进行筛选
        choices = []
        for honor_def in sorted_honors:
            # 为了更好的用户体验，我们可以在名称中也加入过期日期
            expiration_str = honor_def.cup_honor.expiration_date.strftime('%Y-%m-%d')
            choice_name = f"{honor_def.name} (至{expiration_str}) ({str(honor_def.uuid)[:8]})"

            # 模糊匹配用户输入
            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(honor_def.uuid)))

        # 4. 处理Discord的25个选项上限
        if len(choices) > 25:
            # 如果筛选出的结果超过25个，只返回前24个，并附带一条提示信息
            final_choices = choices[:24]
            final_choices.append(
                app_commands.Choice(
                    name="⚠️ 结果过多，请输入更精确的关键词进行搜索...",
                    # 这个value可以是任何不会被正常解析的字符串，防止用户意外选中
                    value="too_many_results_to_show"
                )
            )
            return final_choices
        else:
            # 如果结果在25个以内，直接返回
            return choices

    @cup_honor_group.command(name="管理", description="通过JSON编辑器管理所有杯赛头衔。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_cup_honors(self, interaction: discord.Interaction):
        """启动一个视图，用于管理所有杯赛荣誉。"""
        await interaction.response.defer(ephemeral=True)
        view = CupHonorManageView(self)
        await view.start(interaction)

    # 注：`授予` / `批量授予` / `设置最终持有者` 已删除（2026-08-31）——
    # 通用机制已在 HonorAdminCog 实现（任何 honor 都可手动授予，包括普通 honor + cup_honor）。
    # admin 现在用 `/荣誉头衔丨管理 授予` / `批量授予` / `设置最终持有者-危险操作-仅必要时`（HonorAdminCog 提供）。

    # 注：`从身份组同步` / `全部从身份组同步` 已删除（2026-08-31）——
    # 通用机制已在 HonorAdminCog.cmd_sync_role_honors 实现（基于 role_sync_honor 字段过滤）。
    # admin 现在用 `/荣誉头衔丨管理 同步角色荣誉`（HonorAdminCog 提供）。
    # `_process_role_sync` 仍保留——cup_honor 的 ExpiredHonorNoticeView.fix_records_button 在用。

    # 注：`重置通知状态` 已删除（2026-08-31）——通用机制已搬到 HonorAdminCog.reset_notification_state
    # （cup_honor 自己不再有专属 reset 命令）。NotificationStateManager 单例由 HonorExpirationCog + HonorAdminCog 共用。


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CupHonorModuleCog(bot))
