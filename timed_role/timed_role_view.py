from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import ui, Color

import config
from timed_role import timer
from timed_role.timer import get_daily_limit_seconds
from utility.auth import is_role_dangerous
from utility.helpers import safe_defer, format_duration_hms
from utility.paginated_view import PaginatedView
from utility.role_service import update_member_roles

if TYPE_CHECKING:
    from timed_role.cog import TimedRolesCog

TIMED_ROLES_PER_PAGE = 25


class TimedRoleManageView(PaginatedView):
    """用户私有的限时身份组管理视图。"""

    def __init__(self, cog: TimedRolesCog, user: discord.Member):
        self.cog = cog
        self.user = user
        self.guild = user.guild

        all_timed_role_ids = self.cog.safe_timed_role_ids_cache.get(self.guild.id, [])
        if not all_timed_role_ids:
            self.cog.logger.info(f"服务器 {self.guild.id} 没有可供用户 {self.user.id} 管理的安全限时身份组。")

        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        # [改动] 调用父类构造函数，只传递数据
        get_all_timed_role_ids = lambda: all_timed_role_ids
        super().__init__(
            all_items_provider=get_all_timed_role_ids,
            items_per_page=TIMED_ROLES_PER_PAGE,
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
        user_guild_data = self.cog.timed_role_data_manager._get_guild_user_data(self.user.id, self.guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))

        page_timed_role_ids = self.get_page_items()

        self.add_item(PrivateTimedRoleSelect(self.cog, self.guild.id, page_timed_role_ids, current_timed_role_ids,
                                             page_num=self.page, total_pages=self.total_pages, row=0))

        self.add_item(ReturnTimedRoleButton(self.cog, row=1))

        # [改动] 从基类添加分页按钮
        self._add_pagination_buttons(row=2)

        self.embed = discord.Embed(title=f"⏳ {self.user.display_name} 的限时身份组", color=Color.blurple())

        remaining_seconds = self.cog.timed_role_data_manager.get_remaining_seconds(member.id, self.guild.id)
        daily_limit_seconds = get_daily_limit_seconds(self.guild.id)
        used_seconds = daily_limit_seconds - remaining_seconds

        self.embed.add_field(name="😺 今日总时长", value=format_duration_hms(daily_limit_seconds), inline=True)
        self.embed.add_field(name="🙀 今日剩余时长", value=format_duration_hms(remaining_seconds), inline=True)
        self.embed.add_field(name="😼 今日已用时长", value=format_duration_hms(used_seconds), inline=True)

        if current_timed_role_ids:
            roles_text = " ".join([f"<@&{rid}>" for rid in current_timed_role_ids if self.guild.get_role(rid)])
            self.embed.add_field(name="当前持有：", value=roles_text if roles_text else "无", inline=False)
        else:
            self.embed.add_field(name="当前持有：", value="你当前未持有任何限时身份组。", inline=False)

        reset_hour = config.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
        if not self.all_items:
            self.embed.description = "此服务器没有可供您管理的限时身份组。"
        self.embed.set_footer(
            text=f"每日UTC+8 {reset_hour}点重置时长 | 面板将在 {config.ROLE_MANAGER_CONFIG.get('private_panel_timeout_minutes', 3)} 分钟后失效。")


class PrivateTimedRoleSelect(ui.Select):
    """用户私有的限时身份组选择菜单。"""

    def __init__(self, cog: TimedRolesCog, guild_id: int, page_role_ids: list[int], current_selection_ids: set[int],
                 page_num: int, total_pages: int, row: int = 0):
        self.cog = cog
        options = [discord.SelectOption(label=cog.role_name_cache.get(rid, f"未知(ID:{rid})"), value=str(rid),
                                        default=(rid in current_selection_ids)) for rid in page_role_ids if
                   cog.role_name_cache.get(rid)]
        placeholder = "选择你的限时高亮身份组..."
        if total_pages > 1: placeholder = f"限时高亮组 (第 {page_num + 1}/{total_pages} 页)..."
        if not page_role_ids and config.GUILD_CONFIGS.get(guild_id, {}).get("timed_roles"):
            placeholder = "无安全限时组可选"
        elif not options and not page_role_ids:
            placeholder = "本服未配置限时身份组"
        elif not options and page_role_ids:
            placeholder = "限时组名称加载中..."
        super().__init__(placeholder=placeholder, min_values=0, max_values=len(options) if options else 1,
                         options=options if options else [discord.SelectOption(label="无可用选项", value="_placeholder", default=False)],
                         custom_id="private_timed_role_select", disabled=not options, row=row)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild

        # 1. 计算新的身份组选择
        current_data = self.cog.timed_role_data_manager._get_guild_user_data(member.id, guild.id)
        all_current_selection_set = set(current_data.get("current_timed_roles", []))
        new_selection_in_page = {int(v) for v in self.values if v != "_placeholder"}
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}
        selections_not_in_this_page = all_current_selection_set - options_in_this_page_ids
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)

        # 2. 识别危险和有效的身份组
        roles_to_add_ids, dangerous_attempted_names = set(), []
        for role_id in (final_new_selection_set - all_current_selection_set):
            role = guild.get_role(role_id)
            if role and is_role_dangerous(role):
                dangerous_attempted_names.append(role.name)
            elif role:
                roles_to_add_ids.add(role_id)

        await interaction.edit_original_response(content="# ✅ 正在尝试变更身份……")
        if dangerous_attempted_names:
            await interaction.followup.send(f"❌ 操作失败：尝试获取的身份组 '{', '.join(dangerous_attempted_names)}' 包含敏感权限。", ephemeral=True)
            await self._refresh_view(interaction)
            return

        # 3. 检查用户时长
        is_permanent_guild = timer.is_guild_permanent(guild.id)
        if roles_to_add_ids and not is_permanent_guild and self.cog.timed_role_data_manager.get_remaining_seconds(member.id, guild.id) <= 0:
            await interaction.followup.send("❌ 你今天的限时身份组使用时长已用尽，无法选择新的身份组。", ephemeral=True)
            await self._refresh_view(interaction)
            return

        # 4. 更新身份组并处理数据
        roles_to_remove_ids = all_current_selection_set - final_new_selection_set
        await update_member_roles(self.cog, member, roles_to_add_ids, roles_to_remove_ids, "自助操作限时组")

        if not all_current_selection_set and final_new_selection_set:
            await self.cog.timed_role_data_manager.claim_timed_roles(member.id, list(final_new_selection_set), guild.id)
        elif all_current_selection_set and not final_new_selection_set:
            await self.cog.timed_role_data_manager.return_timed_roles(member.id, guild.id)
        elif all_current_selection_set != final_new_selection_set:
            await self.cog.timed_role_data_manager.claim_timed_roles(member.id, list(final_new_selection_set), guild.id)

        await self._refresh_view(interaction)

    async def _refresh_view(self, interaction: discord.Interaction):
        if isinstance(self.view, PaginatedView):
            await self.view.update_view(interaction)


class ReturnTimedRoleButton(ui.Button):
    """一键归还所有限时身份组的按钮。"""

    def __init__(self, cog: TimedRolesCog, *, row=None):
        super().__init__(label="一键归还限时组", style=discord.ButtonStyle.red, custom_id="return_timed_role_button", emoji="↩️", row=row)
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """响应按钮点击，为用户移除所有限时身份组并结算使用时间。"""
        await safe_defer(interaction, thinking=True)
        member, guild = interaction.user, interaction.guild
        user_guild_data = self.cog.timed_role_data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        if not current_role_ids:
            await interaction.followup.send(f"你在 **{guild.name}** 当前没有可归还的限时身份组。", ephemeral=True)
            await self._refresh_view(interaction, member)
            return
        roles_to_remove = [role for role in member.roles if role.id in current_role_ids]
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason="用户一键归还限时身份组")

        used_seconds = await self.cog.timed_role_data_manager.return_timed_roles(member.id, guild.id)
        remaining_seconds = self.cog.timed_role_data_manager.get_remaining_seconds(member.id, guild.id)
        roles_text = ", ".join([f"**{r.name}**" for r in roles_to_remove]) if roles_to_remove else "已归还的身份组"
        await interaction.followup.send(
            f"✅ 你已归还服务器 **{guild.name}** 的限时组: {roles_text}。\n本次使用 {format_duration_hms(int(used_seconds))}。\n今天在本服剩余可用时间：{format_duration_hms(remaining_seconds)}。",
            ephemeral=True)

        await self._refresh_view(interaction, member)

    async def _refresh_view(self, interaction: discord.Interaction, member: discord.Member):
        if isinstance(self.view, PaginatedView):
            await self.view.update_view(interaction)
