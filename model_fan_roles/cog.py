"""
model_roles/cog.py
模型身份组功能的 Cog。
"""
from __future__ import annotations

import datetime
import typing
from typing import Dict, List, Optional, Any

import discord
from discord import ui

from model_fan_roles.model_config import MODEL_ROLES_CONFIG
from model_fan_roles.view import ModelRolesView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import safe_defer, try_get_member

if typing.TYPE_CHECKING:
    from main import RoleBot

# 统计缓存过期时间（分钟）
STATS_CACHE_TIMEOUT_MINUTES = 1

class ModelFanRolesCog(FeatureCog, name="ModelFanRoles"):
    """管理大语言模型相关身份组的功能模块。"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        # 缓存：{ guild_id: [ {role_id, name, emoji}, ... ] }
        # 只存储经过验证存在的、非危险的身份组配置
        self.safe_model_config_cache: Dict[int, List[Dict[str, Any]]] = {}

        # 统计数据缓存: { guild_id: { role_id: member_count } }
        self.stats_cache: Dict[int, Dict[int, int]] = {}
        # 上次更新统计的时间: { guild_id: datetime }
        self.stats_last_updated: Dict[int, datetime.datetime] = {}

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        """【接口方法】返回显示在主面板上的入口按钮。"""
        return [ModelFanPanelButton(self)]

    async def update_safe_roles_cache(self):
        """【接口方法】从配置中加载并验证身份组安全性。"""
        self.logger.info("ModelFanRolesCog: 开始更新模型身份组缓存...")

        core_cog = self.bot.get_cog("Core")
        if not core_cog:
            self.logger.warning("ModelFanRolesCog: Core Cog 未加载，跳过缓存更新。")
            return

        new_cache = {}

        for guild_id, models_list in MODEL_ROLES_CONFIG.items():
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            safe_models_list = []

            for model_data in models_list:
                role_id = model_data["role_id"]
                role = guild.get_role(role_id)

                if role:
                    # 向 Core 注册名称缓存，方便日志记录
                    core_cog.role_name_cache[role_id] = role.name

                    # 安全检查
                    if is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的模型身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                    else:
                        # 验证通过，加入缓存
                        safe_models_list.append(model_data)
                else:
                    self.logger.warning(f"服务器 '{guild.name}' 中未找到配置的角色 ID: {role_id}")

            if safe_models_list:
                new_cache[guild_id] = safe_models_list

        self.safe_model_config_cache = new_cache
        self.logger.info(f"ModelFanRolesCog: 缓存更新完毕，共加载 {len(new_cache)} 个服务器的配置。")

    async def get_ranked_model_data(self, guild: discord.Guild) -> tuple[List[Dict[str, Any]], datetime.datetime]:
        """
        获取经过排序（按人数降序）的模型数据列表。
        如果缓存过期，会重新计算人数。

        Returns:
            (sorted_data_list, last_updated_time)
        """
        guild_id = guild.id
        base_configs = self.safe_model_config_cache.get(guild_id, [])
        if not base_configs:
            return [], datetime.datetime.now()

        now = datetime.datetime.now()
        last_update = self.stats_last_updated.get(guild_id)

        # 检查是否需要刷新统计
        if not last_update or (now - last_update) > datetime.timedelta(minutes=STATS_CACHE_TIMEOUT_MINUTES):
            self.logger.info(f"刷新服务器 {guild.name} 的模型身份组统计数据...")
            new_stats = {}
            for config in base_configs:
                role_id = config["role_id"]
                role = guild.get_role(role_id)
                # 如果 role 没了，计数为 -1，沉底
                count = len(role.members) if role else -1
                new_stats[role_id] = count

            self.stats_cache[guild_id] = new_stats
            self.stats_last_updated[guild_id] = now
            last_update = now

        stats = self.stats_cache.get(guild_id, {})

        # 根据统计数据排序：人数多的排前面 (强者羞辱弱者 logic)
        sorted_configs = sorted(
            base_configs,
            key=lambda x: stats.get(x["role_id"], 0),
            reverse=True
        )

        return sorted_configs, last_update


class ModelFanPanelButton(ui.Button):
    """主面板上的入口按钮：'模型粉丝领取'"""

    def __init__(self, cog: ModelFanRolesCog):
        super().__init__(
            label="模型粉丝领取",
            style=discord.ButtonStyle.primary,
            custom_id="open_model_fan_panel",
            emoji="🧬"
        )
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """点击后弹出模型选择视图"""
        await safe_defer(interaction, thinking=True)

        member = interaction.user
        if isinstance(member, discord.User):
            member = await try_get_member(interaction.guild, member.id)

        if not member:
            await interaction.followup.send("错误：无法获取成员信息。", ephemeral=True)
            return

        # 检查当前服务器是否有配置
        if interaction.guild.id not in self.cog.safe_model_config_cache:
            await interaction.followup.send("本服务器尚未配置模型身份组。", ephemeral=True)
            return

        view = ModelRolesView(self.cog, member)
        await view.start(interaction, ephemeral=True)


async def setup(bot: 'RoleBot'):
    await bot.add_cog(ModelFanRolesCog(bot))