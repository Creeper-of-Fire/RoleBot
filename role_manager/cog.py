# src/role_manager/cog.py
from __future__ import annotations

import math
from typing import TYPE_CHECKING

import discord
from discord import app_commands, ui, Color
from discord.ext import commands, tasks

import config  # 导入整个 config 模块
import config_data
from .data_manager import DataManager, DAILY_LIMIT_SECONDS

if TYPE_CHECKING:
    from ..bot import RoleBot

# 分页常量
TIMED_ROLES_PER_PAGE = 25
SELF_SERVICE_ROLES_PER_PAGE = 10


# 核心辅助函数 (无改动)
async def safe_defer(interaction: discord.Interaction, *, thinking: bool = False):
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True, thinking=thinking)


async def try_get_member(guild: discord.Guild, member_id: int) -> discord.Member | None:
    member = guild.get_member(member_id)
    if member: return member
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


class RoleManagerCog(commands.Cog, name="RoleManager"):
    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        self.data_manager = DataManager()
        self.role_name_cache = {}
        # 【新增】缓存安全身份组ID，分服务器存储
        self.safe_timed_role_ids_cache: dict[int, list[int]] = {}
        self.safe_self_service_role_ids_cache: dict[int, list[int]] = {}

        self.daily_reset_task.start()
        self.check_expired_roles_task.start()
        self._update_role_cache_task.start()

    def cog_unload(self):
        self.daily_reset_task.cancel()
        self.check_expired_roles_task.cancel()
        self._update_role_cache_task.cancel()

    # =========================
    # 权限检查相关
    # =========================
    def _is_role_dangerous(self, role: discord.Role) -> bool:
        """检查身份组是否包含危险权限。"""
        if role.permissions.administrator:  # 管理员权限始终危险
            return True
        for perm_name, has_perm in role.permissions:
            if has_perm and perm_name in config.DANGEROUS_PERMISSIONS:
                return True
        return False

    async def _filter_and_cache_safe_roles(self):
        """
        过滤配置文件中的身份组，移除包含危险权限的身份组，并缓存安全的身份组ID。
        此函数应在机器人准备好后，或当GUILD_CONFIGS可能发生变化时调用。
        """
        self.logger.info("开始过滤并缓存安全的身份组...")
        self.safe_timed_role_ids_cache.clear()
        self.safe_self_service_role_ids_cache.clear()
        changed_count = 0

        for guild_id, guild_cfg in config.GUILD_CONFIGS.items():
            guild = self.bot.get_guild(guild_id)
            if not guild:
                self.logger.warning(f"无法找到服务器 {guild_id}，跳过其安全身份组缓存。")
                continue

            # 处理限时身份组
            configured_timed_ids = guild_cfg.get("timed_roles", [])
            current_safe_timed_ids = []
            for role_id in configured_timed_ids:
                role = guild.get_role(role_id)
                if role:
                    if self.role_name_cache.get(role_id) != role.name:  # 顺便更新名称缓存
                        self.role_name_cache[role_id] = role.name
                    if self._is_role_dangerous(role):
                        self.logger.warning(
                            f"服务器 '{guild.name}' (ID: {guild_id}) 的限时身份组 '{role.name}' (ID: {role_id}) "
                            f"包含敏感权限，将从自助服务中排除。"
                        )
                        changed_count += 1
                    else:
                        current_safe_timed_ids.append(role_id)
                else:
                    self.logger.warning(f"在服务器 {guild_id} 中未找到配置的限时身份组ID: {role_id}。")
            self.safe_timed_role_ids_cache[guild_id] = current_safe_timed_ids

            # 处理自助身份组
            configured_ss_ids = guild_cfg.get("self_service_roles", [])
            current_safe_ss_ids = []
            for role_id in configured_ss_ids:
                role = guild.get_role(role_id)
                if role:
                    if self.role_name_cache.get(role_id) != role.name:  # 顺便更新名称缓存
                        self.role_name_cache[role_id] = role.name
                    if self._is_role_dangerous(role):
                        self.logger.warning(
                            f"服务器 '{guild.name}' (ID: {guild_id}) 的自助身份组 '{role.name}' (ID: {role_id}) "
                            f"包含敏感权限，将从自助服务中排除。"
                        )
                        changed_count += 1
                    else:
                        current_safe_ss_ids.append(role_id)
                else:
                    self.logger.warning(f"在服务器 {guild_id} 中未找到配置的自助身份组ID: {role_id}。")
            self.safe_self_service_role_ids_cache[guild_id] = current_safe_ss_ids

        if changed_count > 0:
            self.logger.info(f"安全身份组缓存构建完成，{changed_count} 个身份组因权限问题被排除。")
        else:
            self.logger.info("安全身份组缓存构建完成，所有已配置身份组均安全。")

    async def _create_private_manage_panel(self, user: discord.Member) -> tuple[discord.Embed, ui.View]:
        guild = user.guild
        # 确保安全身份组缓存已为该服务器初始化
        if guild.id not in self.safe_timed_role_ids_cache or guild.id not in self.safe_self_service_role_ids_cache:
            self.logger.info(f"服务器 {guild.id} 的安全身份组缓存未就绪，将立即构建。")
            await self._filter_and_cache_safe_roles()  # 如果还没有，立即构建一次

        remaining_seconds = self.data_manager.get_remaining_seconds(user.id, guild.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_guild_data = self.data_manager._get_guild_user_data(user.id, guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))

        # 使用安全缓存中的身份组ID
        # managed_self_service_ids = set(config.GUILD_CONFIGS.get(guild.id, {}).get("self_service_roles", []))
        managed_self_service_ids = set(self.safe_self_service_role_ids_cache.get(guild.id, []))

        member = guild.get_member(user.id) or await try_get_member(guild, user.id)
        if not member:
            embed = discord.Embed(title="错误", description="无法获取您的成员信息。", color=Color.red())
            return embed, ui.View()

        current_self_service_ids = {role.id for role in member.roles if role.id in managed_self_service_ids}

        timed_roles_text = "\n".join(
            f"• {role.mention}" for role in sorted([r for r in guild.roles if r.id in current_timed_role_ids], key=lambda r: r.name)) or "无"
        self_service_roles_text = "\n".join(
            f"• {role.mention}" for role in sorted([r for r in member.roles if r.id in current_self_service_ids], key=lambda r: r.name)) or "无"

        embed = discord.Embed(title=f"⚙️ {user.display_name} 在「{guild.name}」的身份组管理面板",
                              description="在这里管理你的身份组。你的选择会自动保存并刷新此面板。", color=Color.green())
        embed.add_field(name="⏱️ 本服限时组时间", value=
        f"已用: {format_duration_hms(used_seconds)}\n"
        f"剩余: {format_duration_hms(remaining_seconds)}\n"
        f"每天 UTC+8  {config_data.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。",
                        inline=False)
        embed.add_field(name="🎨 当前限时高亮组", value=timed_roles_text, inline=True)
        embed.add_field(name="🔧 当前自助身份组", value=self_service_roles_text, inline=True)
        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        embed.set_footer(text=f"此面板将在{timeout_minutes}分钟后失效。")

        view = UserManageView(self, member)
        return embed, view

    # --- 后台任务 ---
    @tasks.loop(minutes=1)
    async def daily_reset_task(self):  # (无改动)
        if await self.data_manager.daily_reset():
            self.logger.info(f"每日计时器已在 UTC+8 {config_data.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):  # (无改动)
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
        """更新身份组名称缓存，并重新构建安全身份组缓存。"""
        self.logger.info("开始执行每小时的身份组缓存和安全列表更新...")
        # 1. 更新名称缓存 (基本逻辑不变，但 _filter_and_cache_safe_roles 也会更新)
        # 2. 重新构建安全身份组缓存
        await self._filter_and_cache_safe_roles()  # 此函数现在也包含名称缓存和危险性日志记录
        self.logger.info("每小时身份组缓存和安全列表更新完毕。")

    @daily_reset_task.before_loop
    @check_expired_roles_task.before_loop
    @_update_role_cache_task.before_loop
    async def before_all_tasks(self):
        await self.bot.wait_until_ready()
        # 机器人就绪后，立即构建一次安全缓存
        await self._filter_and_cache_safe_roles()

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(MainPanelView(self))
        self.logger.info("身份组管理模块已就绪，持久化视图已注册。")
        # 安全缓存的构建移至 before_all_tasks 中，确保 bot 准备好

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")  # (无改动)
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


class UserManageView(ui.View):
    def __init__(self, cog: RoleManagerCog, user: discord.Member):
        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(timeout=timeout_minutes * 60)
        self.cog = cog
        self.user = user
        self.guild = user.guild

        self.timed_role_page = 0
        self.self_service_page = 0

        # 【改动】使用 Cog 中缓存的安全身份组ID列表
        self.all_timed_role_ids = self.cog.safe_timed_role_ids_cache.get(self.guild.id, [])
        self.all_self_service_role_ids = self.cog.safe_self_service_role_ids_cache.get(self.guild.id, [])

        if not self.all_timed_role_ids and not self.all_self_service_role_ids:
            self.cog.logger.info(f"服务器 {self.guild.id} 没有可供用户 {self.user.id} 管理的安全身份组。")

        self._rebuild_view()

    def _rebuild_view(self):  # (内部逻辑不变，但依赖的 self.all_..._ids 已经过安全过滤)
        self.clear_items()
        user_guild_data = self.cog.data_manager._get_guild_user_data(self.user.id, self.guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))
        member = self.guild.get_member(self.user.id)
        if not member:
            self.cog.logger.warning(f"无法在 _rebuild_view 中找到用户 {self.user.id}，可能已离开服务器。")
            error_label = ui.Label("无法加载您的信息，您可能已离开服务器。")
            self.add_item(error_label)
            self.stop()
            return
        current_self_service_ids = {role.id for role in member.roles}

        total_timed_pages = math.ceil(len(self.all_timed_role_ids) / TIMED_ROLES_PER_PAGE)
        ss_start_row = 1
        if self.all_timed_role_ids:  # 只有当有安全的限时组时才添加
            start_tr = self.timed_role_page * TIMED_ROLES_PER_PAGE
            end_tr = start_tr + TIMED_ROLES_PER_PAGE
            page_timed_role_ids = self.all_timed_role_ids[start_tr:end_tr]
            self.add_item(PrivateTimedRoleSelect(
                self.cog, self.guild.id, page_timed_role_ids, current_timed_role_ids,
                page_num=self.timed_role_page, total_pages=total_timed_pages, row=0
            ))
        elif config.GUILD_CONFIGS.get(self.guild.id, {}).get("timed_roles"):  # 如果配置了但都被过滤了
            self.add_item(ui.Button(label="无可用限时组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=0))

        if total_timed_pages > 1:
            self.add_item(PaginationButton(label="◀️ 限时组", custom_id="page_timed_prev", disabled=self.timed_role_page == 0, row=1))
            self.add_item(PaginationButton(label="限时组 ▶️", custom_id="page_timed_next", disabled=self.timed_role_page >= total_timed_pages - 1, row=1))
            ss_start_row = 2

        page_ss_role_ids = []
        if self.all_self_service_role_ids:  # 只有当有安全的自助组时才继续
            start_ss = self.self_service_page * SELF_SERVICE_ROLES_PER_PAGE
            end_ss = start_ss + SELF_SERVICE_ROLES_PER_PAGE
            page_ss_role_ids = self.all_self_service_role_ids[start_ss:end_ss]

        current_processing_row = ss_start_row
        if current_processing_row <= 4 and page_ss_role_ids:
            for i in range(5):
                if i < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[i]
                    role = self.guild.get_role(role_id)
                    if role: self.add_item(SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))
        elif not self.all_self_service_role_ids and config.GUILD_CONFIGS.get(self.guild.id, {}).get("self_service_roles") and current_processing_row <= 4:
            self.add_item(ui.Button(label="无可用自助组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=current_processing_row))

        current_processing_row = ss_start_row + 1
        if current_processing_row <= 4 and page_ss_role_ids:
            for i in range(5, 10):
                if i < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[i]
                    role = self.guild.get_role(role_id)
                    if role: self.add_item(SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))

        current_processing_row = ss_start_row + 2
        total_self_service_pages = math.ceil(len(self.all_self_service_role_ids) / SELF_SERVICE_ROLES_PER_PAGE)
        if total_self_service_pages > 1:
            if current_processing_row <= 4:
                self.add_item(PaginationButton(label="◀️ 自助", custom_id="page_ss_prev", disabled=self.self_service_page == 0, row=current_processing_row))
                self.add_item(
                    PaginationButton(label=f"{self.self_service_page + 1}/{total_self_service_pages}", style=discord.ButtonStyle.secondary, disabled=True,
                                     row=current_processing_row))
                self.add_item(PaginationButton(label="自助 ▶️", custom_id="page_ss_next", disabled=self.self_service_page >= total_self_service_pages - 1,
                                               row=current_processing_row))
            else:
                self.cog.logger.warning(f"无法为服务器 {self.guild.id} 的自助身份组添加翻页控件：行数不足。")

    async def pagination_callback(self, interaction: discord.Interaction):  # (无改动)
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
        if self.is_finished():
            await interaction.response.edit_message(content="操作已完成或出现错误。", view=None, embed=None)
        else:
            await interaction.response.edit_message(view=self)


class PaginationButton(ui.Button):  # (无改动)
    def __init__(self, **kwargs): super().__init__(**kwargs)

    async def callback(self, interaction: discord.Interaction):
        view: UserManageView = self.view
        await view.pagination_callback(interaction)


class PrivateTimedRoleSelect(ui.Select):
    # page_role_ids 现在传入的是已经过安全过滤的身份组ID
    def __init__(self, cog: RoleManagerCog, guild_id: int, page_role_ids: list[int], current_selection_ids: set[int], page_num: int, total_pages: int,
                 row: int = 0):
        # (构造函数基本不变，依赖 page_role_ids 已被过滤)
        self.cog = cog
        options = [
            discord.SelectOption(
                label=cog.role_name_cache.get(rid, f"未知(ID:{rid})"),
                value=str(rid), default=(rid in current_selection_ids)
            ) for rid in page_role_ids if cog.role_name_cache.get(rid)
        ]
        placeholder = "选择你的限时高亮身份组..."
        if total_pages > 1: placeholder = f"限时高亮组 (第 {page_num + 1}/{total_pages} 页)..."
        if not page_role_ids and config.GUILD_CONFIGS.get(guild_id, {}).get("timed_roles"):  # 配置了但全被过滤
            placeholder = "无安全限时组可选"
        elif not options and not page_role_ids:
            placeholder = "本服未配置限时身份组"  # 完全没配置
        elif not options and page_role_ids:
            placeholder = "限时组名称加载中..."

        super().__init__(
            placeholder=placeholder, min_values=0, max_values=len(options) if options else 1,
            options=options if options else [discord.SelectOption(label="无可用选项", value="_placeholder", default=False)],
            custom_id="private_timed_role_select", disabled=not options, row=row
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild
        current_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        all_current_selection_set = set(current_data.get("current_timed_roles", []))
        new_selection_in_page_str = set(self.values)
        new_selection_in_page = {int(v) for v in new_selection_in_page_str if v != "_placeholder"}
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}
        selections_not_in_this_page = all_current_selection_set - options_in_this_page_ids
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)

        # 【新增】后端安全检查
        roles_to_actually_add_ids = set()
        dangerous_attempted_names = []
        for role_id_to_add in (final_new_selection_set - all_current_selection_set):
            role_obj = guild.get_role(role_id_to_add)
            if role_obj and self.cog._is_role_dangerous(role_obj):
                dangerous_attempted_names.append(role_obj.name)
            elif role_obj:  # 安全或未找到（不太可能，因为是从安全列表来的）
                roles_to_actually_add_ids.add(role_id_to_add)

        if dangerous_attempted_names:
            await interaction.followup.send(
                f"❌ 操作失败：尝试获取的身份组 '{', '.join(dangerous_attempted_names)}' 包含敏感权限。它们已被UI过滤，不应能被选择。",
                ephemeral=True
            )
            # 刷新面板以显示正确状态，不进行任何角色更改
            refreshed_member = await try_get_member(guild, member.id)
            if refreshed_member:
                new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
                await interaction.edit_original_response(embed=new_embed, view=new_view)
            return

        # 更新 new_selection_ids 以反映实际安全的选择
        new_selection_ids = list((all_current_selection_set - (all_current_selection_set - final_new_selection_set)) | roles_to_actually_add_ids)
        final_new_selection_set = set(new_selection_ids)  # 更新集合

        # --- 后续逻辑使用 roles_to_actually_add_ids ---
        roles_to_remove_ids = all_current_selection_set - final_new_selection_set

        if final_new_selection_set and self.cog.data_manager.get_remaining_seconds(member.id, guild.id) <= 0:
            if not final_new_selection_set.issubset(all_current_selection_set):
                await interaction.followup.send("❌ 你今天的限时身份组使用时长已用尽，无法选择新的身份组。", ephemeral=True)
                refreshed_member = await try_get_member(guild, member.id)
                if refreshed_member:
                    new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
                    await interaction.edit_original_response(embed=new_embed, view=new_view)
                return

        if roles_to_actually_add_ids: await member.add_roles(*[r for r in guild.roles if r.id in roles_to_actually_add_ids], reason="自助领取限时组")
        if roles_to_remove_ids: await member.remove_roles(*[r for r in guild.roles if r.id in roles_to_remove_ids], reason="自助移除限时组")

        if all_current_selection_set or final_new_selection_set:
            if not all_current_selection_set and final_new_selection_set:
                await self.cog.data_manager.claim_timed_roles(member.id, new_selection_ids, guild.id)
            elif all_current_selection_set and not final_new_selection_set:
                await self.cog.data_manager.return_timed_roles(member.id, guild.id)
            elif all_current_selection_set != final_new_selection_set:
                await self.cog.data_manager.claim_timed_roles(member.id, new_selection_ids, guild.id)

        refreshed_member = await try_get_member(guild, member.id)
        if refreshed_member:
            new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
            await interaction.edit_original_response(embed=new_embed, view=new_view)


class SelfServiceRoleButton(ui.Button):
    # role 对象传入时，应已通过安全过滤
    def __init__(self, cog: RoleManagerCog, role: discord.Role, is_selected: bool, row: int | None = None):
        # (构造函数无改动)
        self.cog = cog
        self.role = role
        super().__init__(label=role.name, style=discord.ButtonStyle.success if is_selected else discord.ButtonStyle.secondary,
                         custom_id=f"toggle_self_service_role:{role.id}", row=row)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=True)
        member = interaction.user

        # 【新增】后端安全检查 (主要用于添加时)
        if not (self.role in member.roles):  # 如果要添加
            if self.cog._is_role_dangerous(self.role):
                await interaction.followup.send(
                    f"❌ 操作失败：身份组 **{self.role.name}** 包含敏感权限。它已被UI过滤，不应能被选择。",
                    ephemeral=True
                )
                # 刷新面板以显示正确状态
                refreshed_member = await try_get_member(interaction.guild, member.id)
                if refreshed_member:
                    new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
                    await interaction.edit_original_response(embed=new_embed, view=new_view)
                return

        if self.role in member.roles:
            await member.remove_roles(self.role, reason="自助移除身份组")
        else:
            await member.add_roles(self.role, reason="自助领取身份组")

        refreshed_member = await try_get_member(interaction.guild, member.id)
        if refreshed_member:
            new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
            await interaction.edit_original_response(embed=new_embed, view=new_view)


# --- MainPanel 和其他按钮 (OpenManagePanelButton, QueryTimeButton, ReturnTimedRoleButton) 无权限相关改动 ---
class MainPanelView(ui.View):  # (无改动)
    def __init__(self, cog: RoleManagerCog):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(OpenManagePanelButton(cog))
        self.add_item(ReturnTimedRoleButton(cog))
        self.add_item(QueryTimeButton(cog))


class OpenManagePanelButton(ui.Button):  # (无改动)
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="管理我的身份组", style=discord.ButtonStyle.primary, custom_id="open_manage_panel", emoji="⚙️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=True)
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        embed, view = await self.cog._create_private_manage_panel(member)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class QueryTimeButton(ui.Button):  # (无改动)
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="查询我的时间", style=discord.ButtonStyle.secondary, custom_id="query_time_button", emoji="⏱️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=True)
        member, guild = interaction.user, interaction.guild
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


class ReturnTimedRoleButton(ui.Button):  # (无改动)
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="一键归还限时组", style=discord.ButtonStyle.red, custom_id="return_timed_role_button", emoji="↩️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild
        user_guild_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        current_role_ids = user_guild_data.get("current_timed_roles", [])
        if not current_role_ids:
            await interaction.followup.send(f"你在 **{guild.name}** 当前没有可归还的限时身份组。", ephemeral=True)
            return
        roles_to_remove = [role for role in member.roles if role.id in current_role_ids]
        if roles_to_remove: await member.remove_roles(*roles_to_remove, reason="用户一键归还限时身份组")
        used_seconds = await self.cog.data_manager.return_timed_roles(member.id, guild.id)
        remaining_seconds = self.cog.data_manager.get_remaining_seconds(member.id, guild.id)
        roles_text = ", ".join([f"**{r.name}**" for r in roles_to_remove]) if roles_to_remove else "已归还的身份组"
        await interaction.followup.send(
            f"✅ 你已归还服务器 **{guild.name}** 的限时组: {roles_text}。\n"
            f"本次使用 {format_duration_hms(int(used_seconds))}。\n"
            f"今天在本服剩余可用时间：{format_duration_hms(remaining_seconds)}。",
            ephemeral=True
        )


async def setup(bot: commands.Bot):  # (无改动)
    await bot.add_cog(RoleManagerCog(bot))
