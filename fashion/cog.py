from __future__ import annotations

import asyncio
import typing
from typing import Optional, List, Dict

import discord
from discord import ui
from discord.ext import tasks, commands

import config_data
from fashion.fashion_view import FashionManageView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import safe_defer, try_get_member

if typing.TYPE_CHECKING:
    from main import RoleBot
    from core.cog import CoreCog


class FashionCog(FeatureCog, name="Fashion"):
    """管理所有幻化身份组相关的功能。"""

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [FashionPanelButton(self)]

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.safe_fashion_map_cache: Dict[int, Dict[int, List[int]]] = {}
        self.check_fashion_role_validity_task.start()

    def cog_unload(self):
        self.check_fashion_role_validity_task.cancel()

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("FashionCog: 开始更新安全幻化身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        for guild_id, fashion_cfg in config_data.FASHION_CONFIG.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            configured_fashion_map = fashion_cfg.get("fashion_map", {})
            current_safe_fashion_map = {}
            for base_role_id, fashion_role_ids_list in configured_fashion_map.items():
                base_role = guild.get_role(base_role_id)
                if base_role: core_cog.role_name_cache[base_role_id] = base_role.name

                safe_fashions_for_base = []
                for fashion_role_id in fashion_role_ids_list:
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

    @tasks.loop(hours=24)
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
        await view._rebuild_view()
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(FashionCog(bot))
