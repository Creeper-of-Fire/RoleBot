from __future__ import annotations

import typing
from typing import Dict, List, Optional

import discord
from discord import ui, Color
from discord.ext import commands, tasks

import config
from self_service.self_service_view import SelfServiceManageView
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import safe_defer, try_get_member

if typing.TYPE_CHECKING:
    from core.cog import CoreCog
    from main import RoleBot

SELF_SERVICE_GUIDE_POST = {
    "guild_id": 1134557553011998840,  # 指引帖子所在的服务器ID
    "channel_id": 1392167349951398008,  # 指引帖子所在的频道ID
    "post_id": 1392167360261001226,  # 指引帖子的消息ID
}


class SelfServiceCog(FeatureCog, name="SelfService"):
    """管理所有自助身份组相关的功能。"""

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.safe_self_service_role_ids_cache: Dict[int, List[int]] = {}
        self.update_guide_embed_task.start()
        self.guide_url: Optional[str] = None
        self._guide_embed: Optional[discord.Embed] = None

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

    @property
    def guide_embed(self) -> discord.Embed:
        if self._guide_embed is not None:
            return self._guide_embed
        else:
            return discord.Embed(
                title="🛠️ 自助身份组身份入门指引",
                description="管理员尚未配置入门指引，或指引正在加载中。请联系管理员/稍后重试。",
                color=Color.orange()
            )

    def cog_unload(self):
        self.update_guide_embed_task.cancel()

    @tasks.loop(minutes=15)
    async def update_guide_embed_task(self):
        """每15分钟运行一次，获取并缓存自助身份组指引的Embed。"""
        guide_info = SELF_SERVICE_GUIDE_POST
        if not all(guide_info.get(k) for k in ["guild_id", "channel_id", "post_id"]):
            # 如果配置不完整，则不执行任务
            if self.update_guide_embed_task.current_loop > 0:  # 首次不提示
                self.logger.warning("自助身份组指引配置不完整，无法更新缓存Embed。")
            return

        try:
            guild = self.bot.get_guild(guide_info["guild_id"])
            if not guild:
                self.logger.error(f"无法找到指引所在的服务器 (ID: {guide_info['guild_id']})。")
                return

            channel = await guild.fetch_channel(guide_info["channel_id"])
            message = await channel.fetch_message(guide_info["post_id"])

            new_embed = None
            if message.embeds:
                new_embed = message.embeds[0].copy()
            elif message.content:
                new_embed = discord.Embed(title=channel.name, description=message.content, color=discord.Color.green())

            if new_embed:
                self._guide_embed = new_embed
                self.logger.info("成功缓存了自助身份组指引 Embed。")
            else:
                self.logger.warning("指引消息为空，无法缓存 Embed。")

            self.guide_url = message.jump_url
            self.logger.info(f"成功缓存了自助身份组指引 URL: {self.guide_url}")

        except (discord.NotFound, discord.Forbidden, ValueError) as e:
            self.logger.error(f"更新自助身份组指引 Embed 失败: {e}")
            self._guide_embed = None
            self.guide_url = None

    @update_guide_embed_task.before_loop
    async def before_self_service_task(self):
        await self.bot.wait_until_ready()


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
        await view._rebuild_view()
        await interaction.followup.send(embed=view.embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(SelfServiceCog(bot))
