from __future__ import annotations

import typing
from typing import Optional, List, Dict

import discord
from discord import ui, Color

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
from role_system.fashion.fashion_config_manager import FashionConfigManager
from role_system.fashion.fashion_view import FashionManageView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog, PanelEntry
from utility.helpers import safe_defer, try_get_member
from utility.scheduled_loop import scheduled_loop

if typing.TYPE_CHECKING:
    from main import RoleBot
    from core.CoreCog import CoreCog


class FashionCog(FeatureCog, name="Fashion"):
    """管理所有幻化身份组相关的功能。"""

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        return [
            PanelEntry(
                description="基于你的基础身份组，获得幻化能力。",
                button=FashionPanelButton(self)
            ),
        ]

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        self.safe_fashion_map_cache: Dict[int, Dict[int, List[int]]] = {}
        # check_fashion_role_validity_task.start() 挪到 cog_load，
        # 避免 __init__ 期间 self.bot.loop 尚未就绪导致 RuntimeError。

    async def cog_load(self) -> None:
        """Cog 加载时启动后台调度任务。"""
        await super().cog_load()
        self.check_fashion_role_validity_task.start()

    def cog_unload(self):
        self.check_fashion_role_validity_task.cancel()

    def get_guide_embed(self, guild_id: int) -> discord.Embed:
        """按 guild_id 取幻化身份组入门指引 embed——走 embed_guides_{guild_id}.toml。

        ``EmbedGuidesConfigManager.get(guild_id).fashion_guide`` 始终返回有效
        ``EmbedGuideSection``（默认或配置），无 None 检查必要。
        """
        return EmbedGuidesConfigManager.get_instance().get(guild_id).fashion_guide.to_embed()

    # 注：guide_url 已删除——toml 是 source of truth，不再有 Discord 跳转 URL。

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。

        数据源：``FashionConfigManager`` 读 ``data/fashion_{guild_id}.toml``。
        遍历 ``config.GUILD_IDS``（顶层硬编码常量，2026-08 迁 toml 后不再依赖
        config_data 字典的 keys）覆盖到所有已知 guild。无 toml 的 guild 跳过——
        与原 ``FASHION_CONFIG`` 字典不存在的语义一致。
        """
        self.logger.info("FashionCog: 开始更新安全幻化身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        manager = FashionConfigManager.get_instance()

        for guild_id in config.GUILD_IDS:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            fashion_cfg = manager.get(guild_id)
            if fashion_cfg is None:
                # 该服没 toml——视作"未配置幻化系统"
                self.safe_fashion_map_cache.pop(guild_id, None)
                continue

            configured_fashion_map = fashion_cfg.fashion_map
            current_safe_fashion_map = {}
            for entry in configured_fashion_map:
                base_role_id = entry.base_role_id
                base_role = guild.get_role(base_role_id)
                if base_role: core_cog.role_name_cache[base_role_id] = base_role.name

                safe_fashions_for_base = []
                for fashion_role_id in entry.fashion_role_ids:
                    fashion_role = guild.get_role(fashion_role_id)
                    if fashion_role:
                        core_cog.role_name_cache[fashion_role_id] = fashion_role.name
                        if is_role_dangerous(fashion_role):
                            self.logger.warning(f"服务器 '{guild.name}' 的幻化身份组 '{fashion_role.name}'(ID:{fashion_role_id}) 含敏感权限，已排除。")
                        else:
                            safe_fashions_for_base.append(fashion_role_id)

                if safe_fashions_for_base:
                    current_safe_fashion_map[base_role_id] = safe_fashions_for_base

            self.safe_fashion_map_cache[guild_id] = current_safe_fashion_map
        self.logger.info("FashionCog: 安全幻化身份组缓存更新完毕。")

    @scheduled_loop(hours=24, run_on_startup=False, run_in_background=False)
    async def check_fashion_role_validity_task(self):
        """
        每日检查所有用户的幻化身份组是否仍然合法。
        此方法现在使用 role.members，确保检查所有持有者，而不再错误地依赖 timed_roles 数据。
        """
        pass
        # self.logger.info("开始检查幻化身份组合法性...")
        # processed_count = 0
        #
        # for guild_id, safe_fashion_map in self.safe_fashion_map_cache.items():
        #     guild = self.bot.get_guild(guild_id)
        #     if not guild or not safe_fashion_map:
        #         continue
        #
        #     # 创建一个 {fashion_id: base_id} 的反向查找表，方便快速查找
        #     fashion_to_base_map = {
        #         fashion_id: base_id
        #         for base_id, fashion_ids in safe_fashion_map.items()
        #         for fashion_id in fashion_ids
        #     }
        #
        #     # 遍历缓存中所有已知的安全幻化身份组
        #     for fashion_id, base_id in fashion_to_base_map.items():
        #         fashion_role = guild.get_role(fashion_id)
        #         if not fashion_role:
        #             continue
        #
        #         # 正确做法：遍历持有该幻化身份组的所有成员
        #         for member in fashion_role.members:
        #             # 检查该成员是否拥有对应的基础身份组
        #             has_base_role = any(r.id == base_id for r in member.roles)
        #
        #             if not has_base_role:
        #                 try:
        #                     # 如果没有基础组，则移除幻化组
        #                     await member.remove_roles(fashion_role, reason="幻化基础身份组已丢失，自动移除")
        #                     self.logger.info(
        #                         f"用户 {member.display_name} ({member.id}) 在服务器 {guild.name} 失去了幻化组 '{fashion_role.name}' 的基础组，已移除幻化。")
        #                     # 尝试私信用户
        #                     await member.send(f"你在服务器 **{guild.name}** 的幻化身份组 `{fashion_role.name}` 已被移除，因为你不再拥有其对应的基础身份组。")
        #                 except discord.Forbidden:
        #                     # 无法私信或移除角色（可能机器人权限低于用户）
        #                     self.logger.warning(f"无法为用户 {member.display_name} 移除不合格的幻化身份组 '{fashion_role.name}'，权限不足。")
        #                 except discord.HTTPException as e:
        #                     self.logger.error(f"移除用户 {member.display_name} 的幻化身份组时发生HTTP错误: {e}")
        #
        #             # 添加延迟以避免 API 限速
        #             processed_count += 1
        #             if processed_count % 10 == 0:
        #                 await asyncio.sleep(1)
        #
        # self.logger.info("幻化身份组合法性检查完成。")

    @check_fashion_role_validity_task.before_loop
    async def before_fashion_task(self):
        await self.bot.wait_until_ready()


class FashionPanelButton(ui.Button):
    """打开幻化衣橱的按钮。"""

    def __init__(self, cog: FashionCog):
        super().__init__(label="幻化衣橱", style=discord.ButtonStyle.success, custom_id="open_fashion_panel", emoji="👗")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户创建并发送一个幻化衣橱面板。"""
        await safe_defer(interaction, thinking=True)
        if not self.cog.safe_fashion_map_cache.get(interaction.guild_id):
            await interaction.followup.send("❌ 此服务器尚未配置或未启用幻化系统。", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        view = FashionManageView(self.cog, member)
        await view.start(interaction, ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(FashionCog(bot))
