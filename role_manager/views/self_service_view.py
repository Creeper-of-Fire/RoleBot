from __future__ import annotations
from typing import TYPE_CHECKING

import discord
from discord import ui, Color

import config
from role_manager.helpers.auth import is_role_dangerous
from role_manager.helpers.helpers import try_get_member, safe_defer
from role_manager.services.role_service import update_member_roles
from role_manager.views.share import PaginatedView

if TYPE_CHECKING:
    from role_manager.cog import RoleManagerCog

SELF_SERVICE_ROLES_PER_PAGE = 10


class SelfServiceManageView(PaginatedView):
    """用户私有的自助身份组管理视图。"""

    def __init__(self, cog: RoleManagerCog, user: discord.Member):
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(cog, user, items_per_page=SELF_SERVICE_ROLES_PER_PAGE, timeout=timeout_minutes * 60)

        all_self_service_role_ids = self.cog.safe_self_service_role_ids_cache.get(self.guild.id, [])
        self._update_page_info(all_self_service_role_ids)

        if not self.all_items:
            self.cog.logger.info(f"服务器 {self.guild.id} 没有可供用户 {self.user.id} 管理的安全自助身份组。")

    async def _rebuild_view(self):
        self.clear_items()
        member = self.guild.get_member(self.user.id)
        if not member:
            self.cog.logger.warning(f"无法在 _rebuild_view 中找到用户 {self.user.id}。")
            self.embed = discord.Embed(title="错误", description="无法加载您的信息，您可能已离开服务器。", color=Color.red())
            self.add_item(ui.Button(label="错误", style=discord.ButtonStyle.danger, disabled=True))
            self.stop()
            return

        current_self_service_ids = {role.id for role in member.roles}

        start_index, end_index = self.page * self.items_per_page, (self.page + 1) * self.items_per_page
        page_ss_role_ids = self.all_items[start_index:end_index]

        for row_offset in range(2):
            current_processing_row = row_offset
            if current_processing_row > 4: break
            start_index_in_page = row_offset * 5
            for i in range(5):
                index_in_page = start_index_in_page + i
                if index_in_page < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[index_in_page]
                    role = self.guild.get_role(role_id)
                    if role: self.add_item(
                        SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))

        if not self.all_items and config.GUILD_CONFIGS.get(self.guild.id, {}).get("self_service_roles"): self.add_item(
            ui.Button(label="无可用自助组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=0))

        self._add_pagination_buttons(row=2)

        self.embed = discord.Embed(title=f"🛠️ {self.user.display_name} 的自助身份组", color=Color.gold())
        if not self.all_items:
            self.embed.description = "此服务器没有可供您管理的自助身份组。"
        self.embed.set_footer(text=f"面板将在 {config.ROLE_MANAGER_CONFIG.get('private_panel_timeout_minutes', 3)} 分钟后失效。")


class SelfServiceRoleButton(ui.Button):
    """自助身份组的切换按钮，用户点击可以领取或移除对应的身份组。"""

    def __init__(self, cog: RoleManagerCog, role: discord.Role, is_selected: bool, row: int | None = None):
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
                    await new_view._rebuild_view()
                    await interaction.edit_original_response(embed=new_view.embed, view=new_view)
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

        # Refresh the view
        refreshed_member = await try_get_member(interaction.guild, member.id)
        if refreshed_member:
            new_view = SelfServiceManageView(self.cog, refreshed_member)
            await new_view._rebuild_view()
            await interaction.edit_original_response(embed=new_view.embed, view=new_view)
        else:
            # Failsafe if member left
            await interaction.edit_original_response(content="操作完成。", view=None, embed=None)