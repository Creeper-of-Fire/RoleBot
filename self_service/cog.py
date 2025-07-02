from __future__ import annotations

import typing
from typing import Dict, List, Optional

import discord
from discord import ui
from discord.ext import commands

import config
from utility.helpers import safe_defer, try_get_member
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from self_service.self_service_view import SelfServiceManageView

if typing.TYPE_CHECKING:
    from core.cog import CoreCog
    from main import RoleBot


class SelfServiceCog(FeatureCog, name="SelfService"):
    """管理所有自助身份组相关的功能。"""

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.safe_self_service_role_ids_cache: Dict[int, List[int]] = {}

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [SelfServicePanelButton(self)]

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("SelfServiceCog: 开始更新安全自助身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        for guild_id, guild_cfg in config.GUILD_CONFIGS.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            configured_ss_ids = guild_cfg.get("self_service_roles", [])
            current_safe_ss_ids = []
            for role_id in configured_ss_ids:
                role = guild.get_role(role_id)
                if role:
                    core_cog.role_name_cache[role_id] = role.name
                    if is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的自助身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                    else:
                        current_safe_ss_ids.append(role_id)
            self.safe_self_service_role_ids_cache[guild_id] = current_safe_ss_ids
        self.logger.info("SelfServiceCog: 安全自助身份组缓存更新完毕。")

class SelfServicePanelButton(ui.Button):
    """打开自助身份组管理面板的按钮。"""

    def __init__(self, cog: SelfServiceCog):
        super().__init__(label="自助身份组", style=discord.ButtonStyle.primary, custom_id="open_self_service_panel", emoji="🛠️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户创建并发送一个自助身份组管理面板。"""
        await safe_defer(interaction, thinking=True)
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        view = SelfServiceManageView(self.cog, member)
        await view._rebuild_view()
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=True)

async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(SelfServiceCog(bot))
