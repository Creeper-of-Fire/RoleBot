"""
model_roles/cog.py
模型身份组功能的 Cog。
"""
from __future__ import annotations

import asyncio
import datetime
import typing
from typing import Dict, List, Optional

import discord
from discord import ui

import config
from role_system.model_fan_roles.model_fan_roles_config_manager import (
    ModelFanRolesConfigManager,
)
from role_system.model_fan_roles.model_fan_roles_config_models import ModelRoleConfig
from role_system.model_fan_roles.view import ModelRolesView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog, PanelEntry
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
        self.safe_model_config_cache: Dict[int, List[ModelRoleConfig]] = {}

        # 统计数据缓存: { guild_id: { role_id: member_count } }
        self.stats_cache: Dict[int, Dict[int, int]] = {}
        # 上次更新统计的时间: { guild_id: datetime }
        self.stats_last_updated: Dict[int, datetime.datetime] = {}

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """【接口方法】返回显示在主面板上的入口按钮。"""
        return [
            PanelEntry(
                button=ModelFanPanelButton(self),
                description="获取专属大模型粉丝身份组！"
            )
        ]

    async def update_safe_roles_cache(self):
        """【接口方法】从配置中加载并验证身份组安全性。

        数据源：``ModelFanRolesConfigManager`` 读 ``data/model_fan_roles_{guild_id}.toml``。
        遍历 ``config.GUILD_IDS``（顶层硬编码常量）覆盖到所有已知 guild。无 toml 的 guild
        跳过——与原 ``MODEL_ROLES_CONFIG`` 字典不存在的语义一致。
        """
        self.logger.info("ModelFanRolesCog: 开始更新模型身份组缓存...")

        core_cog = self.bot.get_cog("Core")
        if not core_cog:
            self.logger.warning("ModelFanRolesCog: Core Cog 未加载，跳过缓存更新。")
            return

        manager = ModelFanRolesConfigManager.get_instance()

        new_cache = {}

        for guild_id in config.GUILD_IDS:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            models_cfg = manager.get(guild_id)
            if models_cfg is None:
                # 该服没 toml——视作"未启用模型阵营"
                continue

            safe_models_list = []

            for model_data in models_cfg.models:
                role_id = model_data.role_id
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

        # 主动触发一次 stats 后台刷新（fire-and-forget，不阻塞）
        # ——让首次访问 view 的用户不看到"0 人"空 cache。
        for guild_id in new_cache:
            self.bot.loop.create_task(self._refresh_stats_background(guild_id))

    async def get_ranked_model_data(self, guild: discord.Guild) -> tuple[List[ModelRoleConfig], datetime.datetime]:
        """
        获取经过排序（按人数降序）的模型数据列表。

        设计要点（避免 view callback 阻塞事件循环触发 404）：
        - 永远**立刻用 cache 返回**排序结果（最多过期 1 分钟）。
        - 如果 cache 过期 / 为空，fire-and-forget 后台刷新——不 await，
          调用方零等待。
        - ``Role.members`` 在 discord.py 2.x 里是 ``[m for m in guild._members.values() if m._roles.has(role_id)]``
          ——纯内存 list comprehension，**不触发网络请求**；但 guild 有 10000+ 成员、K 个 model role 时
          O(K·N) 同步遍历能跑 1+ 秒，所以实际统计放 ``asyncio.to_thread`` 的线程池跑。

        Returns:
            (sorted_data_list, last_updated_time)
        """
        guild_id = guild.id
        base_configs = self.safe_model_config_cache.get(guild_id, [])
        if not base_configs:
            return [], datetime.datetime.now()

        now = datetime.datetime.now()
        last_update = self.stats_last_updated.get(guild_id)

        # cache 过期 / 为空 → 后台 fire-and-forget 刷新（**不 await**）
        needs_refresh = (
            not last_update
            or (now - last_update) > datetime.timedelta(minutes=STATS_CACHE_TIMEOUT_MINUTES)
        )
        if needs_refresh:
            self.bot.loop.create_task(self._refresh_stats_background(guild_id))

        # 立刻用 cache 排序返回（哪怕过期 1 分钟——对人气榜展示完全可接受）
        stats = self.stats_cache.get(guild_id, {})
        sorted_configs = sorted(
            base_configs,
            key=lambda x: stats.get(x.role_id, 0),
            reverse=True,
        )

        return sorted_configs, last_update or now

    async def _refresh_stats_background(self, guild_id: int):
        """
        后台异步刷新 stats——纯 CPU 工作移到线程池，不阻塞事件循环。

        配合 ``get_ranked_model_data`` 的"立刻返回 cache"模式使用：
        view callback 永远不被 stats 计算阻塞。
        """
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return
        base_configs = self.safe_model_config_cache.get(guild_id, [])
        if not base_configs:
            return

        self.logger.info(f"后台刷新服务器 {guild.name} 的模型身份组统计数据...")
        new_stats = await asyncio.to_thread(
            self._blocking_count_role_members, base_configs, guild,
        )
        self.stats_cache[guild_id] = new_stats
        self.stats_last_updated[guild_id] = datetime.datetime.now()

    @staticmethod
    def _blocking_count_role_members(
        base_configs: List[ModelRoleConfig],
        guild: discord.Guild,
    ) -> Dict[int, int]:
        """
        [同步/线程池] 遍历 base_configs，对每个 model role 调 ``len(role.members)``。

        Role.members 在 discord.py 2.x 里是纯内存 list comprehension，
        不触发网络请求；但 K 个 model role × N 个 members = O(K·N) 同步遍历
        在 guild 有 10000+ 成员时能跑 1+ 秒——必须放线程池，不能阻塞事件循环。
        """
        new_stats: Dict[int, int] = {}
        for config in base_configs:
            role_id = config.role_id
            role = guild.get_role(role_id)
            # role 没了，计数为 -1，沉底
            new_stats[role_id] = len(role.members) if role else -1
        return new_stats


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
