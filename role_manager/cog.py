# src/role_manager/cog.py
from __future__ import annotations

import math
from typing import TYPE_CHECKING

import discord
from discord import app_commands, ui, Color
from discord.ext import commands, tasks

import config
import config_data
from .data_manager import DataManager, DAILY_LIMIT_SECONDS

if TYPE_CHECKING:
    from ..bot import RoleBot

# ===================================================================
# 分页常量
# ===================================================================
TIMED_ROLES_PER_PAGE = 25
# 【改动】减少每页自助身份组数量，为翻页控件留出空间
# 2行按钮 (每行5个) = 10个，然后一行翻页控件
SELF_SERVICE_ROLES_PER_PAGE = 10


# ===================================================================
# 核心辅助函数 (无改动)
# ===================================================================
async def safe_defer(interaction: discord.Interaction, *, thinking: bool = False):
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True, thinking=thinking)


async def try_get_member(guild: discord.Guild, member_id: int) -> discord.Member | None:
    member = guild.get_member(member_id)
    if member:
        return member
    try:
        return await guild.fetch_member(member_id)
    except discord.NotFound:
        return None


def format_duration_hms(total_seconds: int) -> str:
    if total_seconds <= 0: return "`0` 秒"
    seconds, hours, minutes = int(total_seconds), 0, 0
    if seconds >= 3600: hours, seconds = divmod(seconds, 3600)
    if seconds >= 60: minutes, seconds = divmod(seconds, 60)
    parts = []
    if hours > 0: parts.append(f"`{hours}` 小时")
    if minutes > 0: parts.append(f"`{minutes}` 分钟")
    if seconds > 0 or not parts: parts.append(f"`{seconds}` 秒")
    return " ".join(parts)


# ===================================================================
# 主 Cog 类
# ===================================================================
class RoleManagerCog(commands.Cog, name="RoleManager"):
    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        self.data_manager = DataManager()
        self.role_name_cache = {}
        self.daily_reset_task.start()
        self.check_expired_roles_task.start()
        self._update_role_cache_task.start()

    def cog_unload(self):
        self.daily_reset_task.cancel()
        self.check_expired_roles_task.cancel()
        self._update_role_cache_task.cancel()

    async def _create_private_manage_panel(self, user: discord.Member) -> tuple[discord.Embed, ui.View]:
        guild = user.guild
        remaining_seconds = self.data_manager.get_remaining_seconds(user.id, guild.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_guild_data = self.data_manager._get_guild_user_data(user.id, guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))

        guild_config = config.GUILD_CONFIGS.get(guild.id, {})
        managed_self_service_ids = set(guild_config.get("self_service_roles", []))

        # 获取最新的成员信息以获得当前角色
        member = guild.get_member(user.id) or await try_get_member(guild, user.id)
        if not member:  # 如果找不到成员，可能已离开服务器
            embed = discord.Embed(title="错误", description="无法获取您的成员信息。", color=Color.red())
            return embed, ui.View()  # 返回空视图

        current_self_service_ids = {role.id for role in member.roles if role.id in managed_self_service_ids}

        timed_roles_text = "\n".join(
            f"• {role.mention}" for role in sorted([r for r in guild.roles if r.id in current_timed_role_ids], key=lambda r: r.name)) or "无"
        self_service_roles_text = "\n".join(
            f"• {role.mention}" for role in sorted([r for r in member.roles if r.id in current_self_service_ids], key=lambda r: r.name)) or "无"

        embed = discord.Embed(title=f"⚙️ {user.display_name} 在「{guild.name}」的身份组管理面板",
                              description="在这里管理你的身份组。你的选择会自动保存并刷新此面板。", color=Color.green())
        embed.add_field(name="⏱️ 本服限时组时间", value=f"已用: {format_duration_hms(used_seconds)}\n剩余: {format_duration_hms(remaining_seconds)}",
                        inline=False)
        embed.add_field(name="🎨 当前限时高亮组", value=timed_roles_text, inline=True)
        embed.add_field(name="🔧 当前自助身份组", value=self_service_roles_text, inline=True)
        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        embed.set_footer(text=f"此面板将在{timeout_minutes}分钟后失效。")

        view = UserManageView(self, member)  # 传递 member 对象
        return embed, view

    # --- 后台任务 (无改动) ---
    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        if await self.data_manager.daily_reset():
            self.logger.info(f"每日计时器已在 UTC+8 {config_data.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        self.logger.debug("正在检查过期限时身份组...")
        for user_id, guild_id, role_ids in self.data_manager.get_users_with_active_timed_role():
            if self.data_manager.get_remaining_seconds(user_id, guild_id) <= 0:
                self.logger.info(f"用户 {user_id} 在服务器 {guild_id} 的限时身份组已过期，正在移除...")
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)
                    continue
                member = await try_get_member(guild, user_id)
                if not member:
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)
                    continue
                roles_to_remove = [role for role in guild.roles if role.id in role_ids and role in member.roles]
                if roles_to_remove:
                    try:
                        await member.remove_roles(*roles_to_remove, reason="限时身份组过期自动移除")
                        self.logger.info(f"成功为用户 {user_id} 移除了 {len(roles_to_remove)} 个身份组。")
                        await self.data_manager.force_return_timed_roles(user_id, guild_id)
                        try:
                            await member.send(f"你在服务器 **{guild.name}** 的限时身份组因使用时长已耗尽，已自动移除。")
                        except discord.Forbidden:
                            pass
                    except Exception as e:
                        self.logger.error(f"自动移除用户 {user_id} 的身份组失败: {e}")
                else:
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)

    @tasks.loop(hours=1)
    async def _update_role_cache_task(self):
        self.logger.info("正在更新身份组名称缓存...")
        updated_count = 0
        for guild_id, guild_cfg in config.GUILD_CONFIGS.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue
            all_role_ids = guild_cfg.get("timed_roles", []) + guild_cfg.get("self_service_roles", [])
            for role_id in all_role_ids:
                role = guild.get_role(role_id)
                if role and self.role_name_cache.get(role_id) != role.name:
                    self.role_name_cache[role_id] = role.name
                    updated_count += 1
        self.logger.info(f"身份组名称缓存更新完毕，共更新/加载了 {updated_count} 个名称。")

    @daily_reset_task.before_loop
    @check_expired_roles_task.before_loop
    @_update_role_cache_task.before_loop
    async def before_all_tasks(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(MainPanelView(self))  # 主面板的持久化视图
        # UserManageView 是临时的，不需要在这里 add_view
        self.logger.info("身份组管理模块已就绪，持久化视图已注册。")

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_guild=True)
    async def send_panel(self, interaction: discord.Interaction):
        if interaction.guild_id not in config.GUILD_IDS:
            await interaction.response.send_message("❌ 此服务器未配置身份组机器人。", ephemeral=True)
            return
        embed = discord.Embed(title="✨ 身份组自助中心 ✨", description="欢迎来到身份组自助中心！\n\n点击下方的按钮来管理你的身份组或查询状态。",
                              color=discord.Color.blurple())
        embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")
        view = MainPanelView(self)
        await interaction.response.send_message(embed=embed, view=view)


# ===================================================================
# 交互组件定义 (带翻页功能)
# ===================================================================

class UserManageView(ui.View):
    def __init__(self, cog: RoleManagerCog, user: discord.Member):  # user 现在是 discord.Member
        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(timeout=timeout_minutes * 60)
        self.cog = cog
        self.user = user  # 保存为 discord.Member 对象
        self.guild = user.guild

        self.timed_role_page = 0
        self.self_service_page = 0

        guild_config = config.GUILD_CONFIGS.get(self.guild.id, {})
        self.all_timed_role_ids = guild_config.get("timed_roles", [])
        self.all_self_service_role_ids = guild_config.get("self_service_roles", [])

        self._rebuild_view()

    def _rebuild_view(self):
        self.clear_items()
        user_guild_data = self.cog.data_manager._get_guild_user_data(self.user.id, self.guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))

        # 获取最新的成员对象以确保角色信息是最新的
        # self.user 可能因为缓存不是最新的，但 id 是可靠的
        member = self.guild.get_member(self.user.id)
        if not member:  # 如果找不到成员，可能已离开服务器，视图应该停止或显示错误
            self.cog.logger.warning(f"无法在 _rebuild_view 中找到用户 {self.user.id}，可能已离开服务器。")
            # 可以添加一个错误提示组件，或者让视图超时
            error_label = ui.Label("无法加载您的信息，您可能已离开服务器。")
            self.add_item(error_label)
            self.stop()  # 停止视图交互
            return

        current_self_service_ids = {role.id for role in member.roles}

        # --- 1. 限时身份组部分 ---
        total_timed_pages = math.ceil(len(self.all_timed_role_ids) / TIMED_ROLES_PER_PAGE)
        # 行 0: 限时身份组下拉菜单
        if self.all_timed_role_ids:
            start_tr = self.timed_role_page * TIMED_ROLES_PER_PAGE
            end_tr = start_tr + TIMED_ROLES_PER_PAGE
            page_timed_role_ids = self.all_timed_role_ids[start_tr:end_tr]
            self.add_item(PrivateTimedRoleSelect(
                self.cog, self.guild.id, page_timed_role_ids, current_timed_role_ids,
                page_num=self.timed_role_page, total_pages=total_timed_pages, row=0
            ))

        ss_start_row = 1  # 自助身份组部分的起始行号
        # 行 1: 限时身份组翻页 (如果需要)
        if total_timed_pages > 1:
            self.add_item(PaginationButton(label="◀️ 限时组", custom_id="page_timed_prev", disabled=self.timed_role_page == 0, row=1))
            self.add_item(PaginationButton(label="限时组 ▶️", custom_id="page_timed_next", disabled=self.timed_role_page >= total_timed_pages - 1, row=1))
            ss_start_row = 2  # 如果有限时组翻页，自助组从第2行开始

        # --- 2. 自助身份组部分 (使用 SELF_SERVICE_ROLES_PER_PAGE = 10) ---
        page_ss_role_ids = []
        if self.all_self_service_role_ids:
            start_ss = self.self_service_page * SELF_SERVICE_ROLES_PER_PAGE
            end_ss = start_ss + SELF_SERVICE_ROLES_PER_PAGE
            page_ss_role_ids = self.all_self_service_role_ids[start_ss:end_ss]

        # 行 `ss_start_row`: 自助身份组按钮 1-5
        current_processing_row = ss_start_row
        if current_processing_row <= 4:  # 确保不超过最大行数
            for i in range(5):
                if i < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[i]
                    role = self.guild.get_role(role_id)
                    if role:
                        self.add_item(SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))

        # 行 `ss_start_row + 1`: 自助身份组按钮 6-10
        current_processing_row = ss_start_row + 1
        if current_processing_row <= 4:  # 确保不超过最大行数
            for i in range(5, 10):  # 索引 5 到 9 对应列表中的第 6 到 10 个元素
                if i < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[i]
                    role = self.guild.get_role(role_id)
                    if role:
                        self.add_item(SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))

        # 行 `ss_start_row + 2`: 自助身份组翻页
        current_processing_row = ss_start_row + 2
        total_self_service_pages = math.ceil(len(self.all_self_service_role_ids) / SELF_SERVICE_ROLES_PER_PAGE)
        if total_self_service_pages > 1:
            if current_processing_row <= 4:  # 确保不超过最大行数
                self.add_item(PaginationButton(label="◀️ 自助", custom_id="page_ss_prev", disabled=self.self_service_page == 0, row=current_processing_row))
                self.add_item(
                    PaginationButton(label=f"{self.self_service_page + 1}/{total_self_service_pages}", style=discord.ButtonStyle.secondary, disabled=True,
                                     row=current_processing_row))
                self.add_item(PaginationButton(label="自助 ▶️", custom_id="page_ss_next", disabled=self.self_service_page >= total_self_service_pages - 1,
                                               row=current_processing_row))
            else:
                self.cog.logger.warning(f"无法为服务器 {self.guild.id} 的自助身份组添加翻页控件：行数不足。")

    async def pagination_callback(self, interaction: discord.Interaction):
        custom_id = interaction.data['custom_id']

        if custom_id == "page_timed_prev":
            self.timed_role_page -= 1
        elif custom_id == "page_timed_next":
            self.timed_role_page += 1
        elif custom_id == "page_ss_prev":
            self.self_service_page -= 1
        elif custom_id == "page_ss_next":
            self.self_service_page += 1

        self._rebuild_view()
        # 检查视图是否已停止（例如，如果 _rebuild_view 中找不到成员）
        if self.is_finished():
            await interaction.response.edit_message(content="操作已完成或出现错误。", view=None, embed=None)
        else:
            await interaction.response.edit_message(view=self)


class PaginationButton(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def callback(self, interaction: discord.Interaction):
        view: UserManageView = self.view
        await view.pagination_callback(interaction)


class PrivateTimedRoleSelect(ui.Select):
    def __init__(self, cog: RoleManagerCog, guild_id: int, page_role_ids: list[int], current_selection_ids: set[int], page_num: int, total_pages: int,
                 row: int = 0):
        self.cog = cog

        options = [
            discord.SelectOption(
                label=cog.role_name_cache.get(rid, f"未知(ID:{rid})"),
                value=str(rid),
                default=(rid in current_selection_ids)
            ) for rid in page_role_ids if cog.role_name_cache.get(rid)  # 确保有名字才显示
        ]

        placeholder = "选择你的限时高亮身份组..."
        if total_pages > 1:
            placeholder = f"限时高亮组 (第 {page_num + 1}/{total_pages} 页)..."

        if not options and not page_role_ids:  # 如果配置了但是缓存没拿到名字
            placeholder = "本服未配置限时身份组或名称加载中"
        elif not options and page_role_ids:
            placeholder = "限时组名称加载中..."

        super().__init__(
            placeholder=placeholder,
            min_values=0,
            max_values=len(options) if options else 1,  # 如果options为空，max_values为1不会出错，但不会有任何选项
            options=options if options else [discord.SelectOption(label="无可用选项", value="_placeholder", default=False)],  # 防止空选项列表报错
            custom_id="private_timed_role_select",
            disabled=not options,  # 如果没有有效选项则禁用
            row=row
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild  # interaction.user 是 discord.Member

        current_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        all_current_selection_set = set(current_data.get("current_timed_roles", []))

        # self.values 包含的是当前提交的选项（value是role_id的字符串）
        # 如果用户取消了所有选择，self.values 会是空列表
        new_selection_in_page_str = set(self.values)
        new_selection_in_page = {int(v) for v in new_selection_in_page_str if v != "_placeholder"}

        # 确定本页原有的选项ID，以便计算仅在本页发生的变化
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}

        # 保留不在当前页的旧选择
        selections_not_in_this_page = all_current_selection_set - options_in_this_page_ids

        # 最终的新选择是：(不在本页的旧选择) U (在本页的新选择)
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)
        new_selection_ids = list(final_new_selection_set)

        if final_new_selection_set and self.cog.data_manager.get_remaining_seconds(member.id, guild.id) <= 0:
            if not final_new_selection_set.issubset(all_current_selection_set):  # 即，尝试添加新的，而不是仅仅移除
                await interaction.followup.send("❌ 你今天的限时身份组使用时长已用尽，无法选择新的身份组。", ephemeral=True)
                # 刷新面板以重置用户的错误选择
                refreshed_member = await try_get_member(guild, member.id)
                if refreshed_member:
                    new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
                    await interaction.edit_original_response(embed=new_embed, view=new_view)
                else:  # 成员找不到了
                    await interaction.edit_original_response(content="错误：无法刷新面板。", embed=None, view=None)
                return

        roles_to_add_ids = final_new_selection_set - all_current_selection_set
        roles_to_remove_ids = all_current_selection_set - final_new_selection_set

        if roles_to_add_ids: await member.add_roles(*[r for r in guild.roles if r.id in roles_to_add_ids], reason="自助领取限时组")
        if roles_to_remove_ids: await member.remove_roles(*[r for r in guild.roles if r.id in roles_to_remove_ids], reason="自助移除限时组")

        if all_current_selection_set or final_new_selection_set:  # 仅当选择发生变化时更新数据
            if not all_current_selection_set and final_new_selection_set:
                await self.cog.data_manager.claim_timed_roles(member.id, new_selection_ids, guild.id)
            elif all_current_selection_set and not final_new_selection_set:
                await self.cog.data_manager.return_timed_roles(member.id, guild.id)
            elif all_current_selection_set != final_new_selection_set:  # 集合内容有变化
                await self.cog.data_manager.claim_timed_roles(member.id, new_selection_ids, guild.id)

        refreshed_member = await try_get_member(guild, member.id)
        if refreshed_member:
            new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
            await interaction.edit_original_response(embed=new_embed, view=new_view)
        else:
            await interaction.edit_original_response(content="错误：无法刷新面板。", embed=None, view=None)


class SelfServiceRoleButton(ui.Button):
    def __init__(self, cog: RoleManagerCog, role: discord.Role, is_selected: bool, row: int | None = None):  # 接受 row
        self.cog = cog
        self.role = role
        super().__init__(
            label=role.name,
            style=discord.ButtonStyle.success if is_selected else discord.ButtonStyle.secondary,
            custom_id=f"toggle_self_service_role:{role.id}",
            row=row  # 应用 row
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction,thinking=True)
        member = interaction.user  # interaction.user 是 discord.Member 对象
        if self.role in member.roles:
            await member.remove_roles(self.role, reason="自助移除身份组")
        else:
            await member.add_roles(self.role, reason="自助领取身份组")

        refreshed_member = await try_get_member(interaction.guild, member.id)
        if refreshed_member:
            new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
            await interaction.edit_original_response(embed=new_embed, view=new_view)
        else:
            await interaction.edit_original_response(content="错误：无法刷新面板。", embed=None, view=None)


class MainPanelView(ui.View):  # 主面板视图
    def __init__(self, cog: RoleManagerCog):
        super().__init__(timeout=None)  # 持久化视图
        self.cog = cog
        self.add_item(OpenManagePanelButton(cog))
        self.add_item(ReturnTimedRoleButton(cog))
        self.add_item(QueryTimeButton(cog))


class OpenManagePanelButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="管理我的身份组", style=discord.ButtonStyle.primary, custom_id="open_manage_panel", emoji="⚙️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction,thinking=True)
        # 确保 interaction.user 是 Member 对象
        if not isinstance(interaction.user, discord.Member):
            member = await try_get_member(interaction.guild, interaction.user.id)
            if not member:
                await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
                return
        else:
            member = interaction.user

        embed, view = await self.cog._create_private_manage_panel(member)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class QueryTimeButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="查询我的时间", style=discord.ButtonStyle.secondary, custom_id="query_time_button", emoji="⏱️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction,thinking=True)
        member, guild = interaction.user, interaction.guild  # interaction.user 是 Member
        remaining_seconds = self.cog.data_manager.get_remaining_seconds(member.id, guild.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_guild_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        embed = discord.Embed(title=f"⏱️ 你在「{guild.name}」的时间使用情况", color=discord.Color.blue())
        embed.add_field(name="今日已用时长", value=format_duration_hms(used_seconds), inline=False)
        embed.add_field(name="今日剩余时长", value=format_duration_hms(remaining_seconds), inline=False)
        if current_role_ids:
            roles_text = ", ".join([f"**{guild.get_role(rid).name}**" for rid in current_role_ids if guild.get_role(rid)])
            embed.add_field(name="当前持有", value=f"你当前正在使用 {roles_text}，计时进行中。", inline=False)
        else:
            embed.add_field(name="当前持有", value="你当前未持有任何限时身份组。", inline=False)
        reset_hour = config_data.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
        embed.set_footer(text=f"每日下午{reset_hour}点重置时长。")
        await interaction.followup.send(embed=embed, ephemeral=True)


class ReturnTimedRoleButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="一键归还限时组", style=discord.ButtonStyle.red, custom_id="return_timed_role_button", emoji="↩️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild  # interaction.user 是 Member
        user_guild_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        if not current_role_ids:
            await interaction.followup.send(f"你在 **{guild.name}** 当前没有可归还的限时身份组。", ephemeral=True)
            return
        roles_to_remove = [role for role in member.roles if role.id in current_role_ids]
        if roles_to_remove:
            await member.remove_roles(*roles_to_remove, reason="用户一键归还限时身份组")
        used_seconds = await self.cog.data_manager.return_timed_roles(member.id, guild.id)
        remaining_seconds = self.cog.data_manager.get_remaining_seconds(member.id, guild.id)
        roles_text = ", ".join([f"**{r.name}**" for r in roles_to_remove]) if roles_to_remove else "已归还的身份组"
        await interaction.followup.send(
            f"✅ 你已归还服务器 **{guild.name}** 的限时组: {roles_text}。\n"
            f"本次使用 {format_duration_hms(int(used_seconds))}。\n"
            f"今天在本服剩余可用时间：{format_duration_hms(remaining_seconds)}。",
            ephemeral=True
        )


# ===================================================================
# setup 函数 (无改动)
# ===================================================================
async def setup(bot: commands.Bot):
    await bot.add_cog(RoleManagerCog(bot))