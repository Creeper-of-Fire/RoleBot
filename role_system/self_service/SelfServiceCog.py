from __future__ import annotations

import typing
from typing import Dict, List, Optional

import discord
from discord import ui

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
from role_system.self_service.self_service_view import SelfServiceManageView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog, PanelEntry
from utility.helpers import safe_defer, try_get_member

if typing.TYPE_CHECKING:
    from core.CoreCog import CoreCog
    from main import RoleBot


class SelfServiceCog(FeatureCog, name="SelfService"):
    """管理所有自助身份组相关的功能。"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        self.safe_self_service_role_ids_cache: Dict[int, List[int]] = {}

    def get_guide_embed(self, guild_id: int) -> discord.Embed:
        """按 guild_id 取自助身份组指引 embed——走 embed_guides_{guild_id}.toml。

        ``EmbedGuidesConfigManager.get(guild_id).self_service_guide`` 始终返回有效
        ``EmbedGuideSection``（默认或配置），无 None 检查必要。
        """
        return EmbedGuidesConfigManager.get_instance().get(guild_id).self_service_guide.to_embed()

    # 注：guide_url 已删除——toml 是 source of truth，不再有 Discord 跳转 URL。

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        return [
            PanelEntry(
                button=SelfServicePanelButton(self),
                description="为避免频繁`@全体成员`，大部分通知需要您领取这些身份组以订阅。"
            )
        ]

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
        super().__init__(label="通知身份组", style=discord.ButtonStyle.primary, custom_id="open_self_service_panel", emoji="🔔")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户创建并发送一个自助身份组管理面板。"""
        await safe_defer(interaction, thinking=True)
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        view = SelfServiceManageView(self.cog, member)
        await view.start(interaction, ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(SelfServiceCog(bot))
