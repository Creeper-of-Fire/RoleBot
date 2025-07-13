from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import ui, Color

import config
from utility.auth import is_role_dangerous
from utility.helpers import try_get_member, safe_defer
from utility.paginated_view import PaginatedView
from utility.role_service import update_member_roles

if TYPE_CHECKING:
    from self_service.cog import SelfServiceCog

SELF_SERVICE_ROLES_PER_PAGE = 10


class SelfServiceManageView(PaginatedView):
    """用户私有的自助身份组管理视图。"""

    def __init__(self, cog: 'SelfServiceCog', user: discord.Member):
        self.cog = cog
        self.user = user
        self.guild = user.guild

        all_self_service_role_ids = self.cog.safe_self_service_role_ids_cache.get(self.guild.id, [])
        if not all_self_service_role_ids:
            self.cog.logger.info(f"服务器 {self.guild.id} 没有可供用户 {self.user.id} 管理的安全自助身份组。")

        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        # [改动] 调用父类构造函数，只传递数据
        get_all_self_service_role_ids= lambda: all_self_service_role_ids
        super().__init__(
            all_items_provider=get_all_self_service_role_ids,
            items_per_page=SELF_SERVICE_ROLES_PER_PAGE,
            timeout=timeout_minutes * 60
        )

    # [改动] 实现新的抽象方法 _rebuild_view
    async def _rebuild_view(self):
        self.clear_items()

        member = self.guild.get_member(self.user.id)
        if member is None:
            self.embed = discord.Embed(title="错误", description="无法加载您的信息，您可能已离开服务器。", color=Color.red())
            self.add_item(ui.Button(label="错误", style=discord.ButtonStyle.danger, disabled=True))
            self.stop()
            return

        # --- 以下是原来 _rebuild_view 的逻辑 ---
        member_role_ids = {role.id for role in member.roles}

        page_ss_role_ids = self.get_page_items()

        for i, role_id in enumerate(page_ss_role_ids):
            role = self.guild.get_role(role_id)
            if role:
                # 计算按钮所在的行 (0 或 1)
                row_index = i // 5
                self.add_item(SelfServiceRoleButton(
                    self.cog, role, role.id in member_role_ids, row=row_index
                ))

        if not self.all_items and config.GUILD_CONFIGS.get(self.guild.id, {}).get("self_service_roles"):
            self.add_item(ui.Button(label="无可用自助组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=0))

        # [改动] 从基类添加分页按钮
        self._add_pagination_buttons(row=3)

        self.embed = self.cog.guide_embed.copy()  # 使用 .copy() 避免修改缓存中的原始 embed
        if not self.all_items:
            self.embed.description = "此服务器没有可供您管理的自助身份组。"
        self.embed.set_footer(text=f"面板将在 {config.ROLE_MANAGER_CONFIG.get('private_panel_timeout_minutes', 3)} 分钟后失效。")

        if self.cog.guide_url:
            self.add_item(ui.Button(
                label=f"跳转到 “{self.cog.guide_embed.title}”",
                style=discord.ButtonStyle.link,
                url=self.cog.guide_url,
                row=4
            ))


class SelfServiceRoleButton(ui.Button):
    """自助身份组的切换按钮，用户点击可以领取或移除对应的身份组。"""

    def __init__(self, cog: 'SelfServiceCog', role: discord.Role, is_selected: bool, row: int | None = None):
        self.cog = cog
        self.role = role
        super().__init__(label=role.name, style=discord.ButtonStyle.success if is_selected else discord.ButtonStyle.secondary,
                         custom_id=f"toggle_self_service_role:{role.id}", row=row)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member = interaction.user
        if not (self.role in member.roles):
            if is_role_dangerous(self.role):
                await interaction.followup.send(f"❌ 操作失败：身份组 **{self.role.name}** 包含敏感权限。", ephemeral=True)
                refreshed_member = await try_get_member(interaction.guild, member.id)
                if refreshed_member:
                    new_view = SelfServiceManageView(self.cog, refreshed_member)
                    await new_view.update_view(interaction)
                return
        roles_to_add = []
        roles_to_remove = []
        if self.role in member.roles:
            roles_to_remove.append(self.role)
        else:
            roles_to_add.append(self.role)

        await update_member_roles(
            cog=self.cog,
            member=member,
            to_add_ids={r.id for r in roles_to_add},
            to_remove_ids={r.id for r in roles_to_remove},
            reason="自助身份组操作"
        )

        if isinstance(self.view, PaginatedView):
            await self.view.update_view(interaction)
