# honor_system/role_sync_honor_module.py
from __future__ import annotations

import typing
from typing import Optional, List

import discord

from utility.feature_cog import FeatureCog
from .getCogs import getHonorCog
from .honor_data_manager import HonorDataManager

if typing.TYPE_CHECKING:
    from main import RoleBot


class RoleClaimHonorModuleCog(FeatureCog, name="RoleClaimHonorModule"):
    """
    【荣誉子模块】处理基于身份组的荣誉自动认领。

    此模块的核心功能是：当用户访问其荣誉墙时，系统会自动检测该用户是否拥有
    某些被标记为“可通过身份组认领”的荣誉所对应的身份组。如果用户拥有身份组，
    但荣誉记录尚未在数据库中，系统会自动为用户授予该荣誉。

    这使得由管理员或其他机器人手动授予的身份组，能够无缝地被本荣誉系统接管，
    用户之后便可以通过荣誉墙自由佩戴或卸下这些身份组。
    """

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        pass

    async def update_safe_roles_cache(self):
        pass

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        self.honor_data_manager = HonorDataManager.getDataManager(logger=bot.logger)

    async def check_and_grant_role_sync_honor(self, member: discord.Member, guild: discord.Guild):
        """
        【按需检查】检查用户是否因持有特定身份组而符合荣誉授予条件。
        此函数在用户与荣誉系统交互（如打开荣誉墙）时被调用。

        配置示例 (在 config_data.py 的 honor definition 中):
        {
            "uuid": "some-uuid-for-legacy-role",
            "name": "元老成员",
            "description": "通过持有旧的元老身份组自动认领。",
            "role_id": 123456789012345678,
            "role_sync_honor": True  // <-- 新增的标记字段
        }
        """
        honor_cog = getHonorCog(self)
        all_definitions = honor_cog.get_all_definitions_in_config()

        member_role_ids = {role.id for role in member.roles}

        # 2. 遍历所有荣誉定义，查找标记为 role_sync_honor 的荣誉
        for honor_def_config in all_definitions:
            # 检查是否是“角色认领”类型的荣誉
            if not honor_def_config.get("role_sync_honor"):
                continue

            # 检查配置是否完整
            honor_uuid = honor_def_config.get("uuid")
            role_id = honor_def_config.get("role_id")
            if not honor_uuid or not role_id:
                self.logger.warning(f"一个 role_sync_honor 的定义缺少 uuid 或 role_id: {honor_def_config}")
                continue

            # 3. 如果用户拥有对应的身份组，则尝试授予荣誉
            if role_id in member_role_ids:
                # grant_honor 方法是幂等的，如果用户已拥有则不会重复操作
                granted_def = self.honor_data_manager.grant_honor(member.id, honor_uuid)

                if granted_def:
                    self.logger.info(
                        f"[身份组认领] 用户 {member} ({member.id}) 因持有身份组 ID {role_id} "
                        f"而自动认领了荣誉 '{granted_def.name}'"
                    )


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(RoleClaimHonorModuleCog(bot))
