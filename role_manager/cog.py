# src/role_manager/cog.py
from __future__ import annotations

import asyncio
import math
from typing import TYPE_CHECKING, List, Dict

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
FASHION_ROLES_PER_PAGE = 25


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
        self.safe_timed_role_ids_cache: dict[int, list[int]] = {}
        self.safe_self_service_role_ids_cache: dict[int, list[int]] = {}
        # 【改动】幻化缓存类型变为 dict[int, list[int]]
        self.safe_fashion_map_cache: dict[int, Dict[int, List[int]]] = {}

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
        if role.permissions.administrator: return True
        for perm_name, has_perm in role.permissions:
            if has_perm and perm_name in config.DANGEROUS_PERMISSIONS:
                return True
        return False

    async def _filter_and_cache_safe_roles(self):
        """【改动】过滤逻辑升级以支持一对多幻化映射。"""
        self.logger.info("开始过滤并缓存安全的身份组...")
        self.safe_timed_role_ids_cache.clear()
        self.safe_self_service_role_ids_cache.clear()
        self.safe_fashion_map_cache.clear()
        changed_count = 0
        all_guild_ids = set(config.GUILD_CONFIGS.keys()) | set(config_data.FASHION_CONFIG.keys())

        for guild_id in all_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                self.logger.warning(f"无法找到服务器 {guild_id}，跳过其安全身份组缓存。")
                continue

            guild_cfg = config.GUILD_CONFIGS.get(guild_id, {})
            fashion_cfg = config_data.FASHION_CONFIG.get(guild_id, {})

            # 处理限时和自助身份组 (逻辑不变)
            # ...
            configured_timed_ids = guild_cfg.get("timed_roles", [])
            current_safe_timed_ids = []
            for role_id in configured_timed_ids:
                role = guild.get_role(role_id)
                if role:
                    if self.role_name_cache.get(role_id) != role.name: self.role_name_cache[role_id] = role.name
                    if self._is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的限时身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                        changed_count += 1
                    else:
                        current_safe_timed_ids.append(role_id)
                else:
                    self.logger.warning(f"在服务器 {guild_id} 中未找到配置的限时身份组ID: {role_id}。")
            self.safe_timed_role_ids_cache[guild_id] = current_safe_timed_ids

            configured_ss_ids = guild_cfg.get("self_service_roles", [])
            current_safe_ss_ids = []
            for role_id in configured_ss_ids:
                role = guild.get_role(role_id)
                if role:
                    if self.role_name_cache.get(role_id) != role.name: self.role_name_cache[role_id] = role.name
                    if self._is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的自助身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                        changed_count += 1
                    else:
                        current_safe_ss_ids.append(role_id)
                else:
                    self.logger.warning(f"在服务器 {guild_id} 中未找到配置的自助身份组ID: {role_id}。")
            self.safe_self_service_role_ids_cache[guild_id] = current_safe_ss_ids
            # ...

            # 【改动】处理一对多幻化身份组
            configured_fashion_map = fashion_cfg.get("fashion_map", {})
            current_safe_fashion_map = {}
            for base_role_id, fashion_role_ids_list in configured_fashion_map.items():
                base_role = guild.get_role(base_role_id)
                if base_role and self.role_name_cache.get(base_role_id) != base_role.name:
                    self.role_name_cache[base_role_id] = base_role.name

                safe_fashions_for_base = []
                for fashion_role_id in fashion_role_ids_list:
                    fashion_role = guild.get_role(fashion_role_id)
                    if fashion_role:
                        if self.role_name_cache.get(fashion_role_id) != fashion_role.name: self.role_name_cache[fashion_role_id] = fashion_role.name
                        if self._is_role_dangerous(fashion_role):
                            self.logger.warning(f"服务器 '{guild.name}' 的幻化身份组 '{fashion_role.name}'(ID:{fashion_role_id}) 含敏感权限，已从幻化系统排除。")
                            changed_count += 1
                        else:
                            safe_fashions_for_base.append(fashion_role_id)
                    else:
                        self.logger.warning(f"在服务器 {guild_id} 中未找到配置的幻化身份组ID: {fashion_role_id}。")

                if safe_fashions_for_base:  # 只有当这个基础身份组至少有一个安全幻化时才加入缓存
                    current_safe_fashion_map[base_role_id] = safe_fashions_for_base

            self.safe_fashion_map_cache[guild_id] = current_safe_fashion_map

        if changed_count > 0:
            self.logger.info(f"安全身份组缓存构建完成，{changed_count} 个身份组因权限问题被排除。")
        else:
            self.logger.info("安全身份组缓存构建完成，所有已配置身份组均安全。")

    # _create_private_manage_panel (无改动)
    async def _create_private_manage_panel(self, user: discord.Member) -> tuple[discord.Embed, ui.View]:
        guild = user.guild
        if guild.id not in self.safe_timed_role_ids_cache or guild.id not in self.safe_self_service_role_ids_cache:
            self.logger.info(f"服务器 {guild.id} 的安全身份组缓存未就绪，将立即构建。")
            await self._filter_and_cache_safe_roles()
        remaining_seconds = self.data_manager.get_remaining_seconds(user.id, guild.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_guild_data = self.data_manager._get_guild_user_data(user.id, guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))
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
        embed.add_field(name="⏱️ 本服限时组时间",
                        value=f"已用: {format_duration_hms(used_seconds)}\n剩余: {format_duration_hms(remaining_seconds)}\n每天 UTC+8  {config.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。",
                        inline=False)
        embed.add_field(name="🎨 当前限时高亮组", value=timed_roles_text, inline=True)
        embed.add_field(name="🔧 当前自助身份组", value=self_service_roles_text, inline=True)
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        embed.set_footer(text=f"此面板将在{timeout_minutes}分钟后失效。")
        view = UserManageView(self, member)
        return embed, view

    # 【改动】_create_fashion_panel 逻辑升级
    async def _create_fashion_panel(self, user: discord.Member) -> tuple[discord.Embed, ui.View]:
        guild = user.guild
        if guild.id not in self.safe_fashion_map_cache:
            self.logger.info(f"服务器 {guild.id} 的幻化缓存未就绪，将立即构建。")
            await self._filter_and_cache_safe_roles()

        safe_fashion_map = self.safe_fashion_map_cache.get(guild.id, {})
        # 从一对多映射中提取所有幻化ID
        all_fashion_role_ids = {fid for fid_list in safe_fashion_map.values() for fid in fid_list}

        member = guild.get_member(user.id) or await try_get_member(guild, user.id)
        if not member:
            embed = discord.Embed(title="错误", description="无法获取您的成员信息。", color=Color.red())
            return embed, ui.View()

        current_worn_fashion_ids = {role.id for role in member.roles if role.id in all_fashion_role_ids}
        worn_fashion_text = "\n".join(
            f"• {role.mention}" for role in sorted([r for r in guild.roles if r.id in current_worn_fashion_ids], key=lambda r: r.name)) or "无"

        embed = discord.Embed(title=f"👗 {user.display_name} 的幻化面板",
                              description="在这里，你可以为你拥有的基础身份组生成“幻化”，以覆盖你的其他的基础身份组。\n只有当你拥有某个基础身份组时，对应的幻化选项才会出现在下面的菜单中。",
                              color=Color.from_rgb(255, 105, 180))
        embed.add_field(name="当前佩戴的幻化", value=worn_fashion_text, inline=False)
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        embed.set_footer(text=f"此面板将在{timeout_minutes}分钟后失效。")
        view = FashionManageView(self, member)
        return embed, view

    # 其他后台任务、监听器、命令 (无改动)
    # ...
    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        if await self.data_manager.daily_reset(): self.logger.info(f"每日计时器已在 UTC+8 {config.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        self.logger.debug("正在检查过期限时身份组...")
        # 获取所有活跃用户，这里不涉及API
        users_to_check = self.data_manager.get_users_with_active_timed_role()

        # 引入一个计数器和更长的延迟间隔
        processed_count = 0
        for user_id, guild_id, role_ids in users_to_check:
            # 这里的 get_remaining_seconds 内部可能调用 _get_guild_user_data，不涉及API
            if self.data_manager.get_remaining_seconds(user_id, guild_id) <= 0:
                self.logger.info(f"用户 {user_id} 在服务器 {guild_id} 的限时身份组已过期，正在移除...")
                guild, member = self.bot.get_guild(guild_id), None
                if guild:
                    # try_get_member 可能会触发 API
                    member = await try_get_member(guild, user_id)

                if not guild or not member:
                    # 无法获取成员或服务器，强制清除本地状态
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)
                    continue

                roles_to_remove = [role for role in guild.roles if role.id in role_ids and role in member.roles]
                if roles_to_remove:
                    try:
                        # remove_roles 会触发 API
                        await member.remove_roles(*roles_to_remove, reason="限时身份组过期自动移除")
                        self.logger.info(f"成功为用户 {user_id} 移除了 {len(roles_to_remove)} 个身份组。")
                        await self.data_manager.force_return_timed_roles(user_id, guild_id)
                        # try:
                        #     # member.send 也会触发 API
                        #     await member.send(f"你在服务器 **{guild.name}** 的限时身份组因使用时长已耗尽，已自动移除。")
                        # except discord.Forbidden:
                        #     pass
                    except Exception as e:
                        self.logger.error(f"自动移除用户 {user_id} 的身份组失败: {e}")
                else:
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)

                # 【新增】处理完一个用户后暂停，根据实际情况调整延迟
                # 例如：每5个用户延迟1秒，或者每个用户延迟0.2秒
                processed_count += 1
                if processed_count % 5 == 0:  # 每处理5个用户，暂停一小会儿
                    await asyncio.sleep(1)  # 暂停1秒
                elif processed_count % 1 == 0:  # 如果用户少，可以每个用户都暂停短时间
                    await asyncio.sleep(0.1)  # 暂停0.1秒

    # 【新增任务】每日检查幻化身份组的合法性
    @tasks.loop(hours=24)  # 每天运行一次，这个频率是合理的
    async def check_fashion_role_validity_task(self):
        if not config.CHECK_FASHION_ROLE_VALIDITY:
            return

        self.logger.info("开始检查幻化身份组合法性...")

        processed_count = 0
        for user_id_str, guilds_data in self.data_manager._data["users"].items():
            user_id = int(user_id_str)

            for guild_id_str, user_guild_data in guilds_data.items():
                guild_id = int(guild_id_str)
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    self.logger.warning(f"无法找到服务器 {guild_id}，跳过其幻化合法性检查。")
                    continue

                # try_get_member 可能会触发 API
                member = await try_get_member(guild, user_id)
                if not member:
                    continue  # 用户不在服务器或无法获取，无需检查其幻化合法性

                # 获取该服务器所有安全的幻化映射
                safe_fashion_map = self.safe_fashion_map_cache.get(guild_id, {})
                # 建立一个 {fashion_id: base_id} 的反向查找表
                fashion_to_base_map = {fid: bid for bid, fids in safe_fashion_map.items() for fid in fids}

                roles_to_remove = []
                for role in member.roles:
                    if role.id in fashion_to_base_map:  # 如果这是一个幻化身份组
                        base_role_id = fashion_to_base_map[role.id]
                        # 检查用户是否持有对应的基础身份组
                        if not any(r.id == base_role_id for r in member.roles):
                            roles_to_remove.append(role)
                            self.logger.info(
                                f"用户 {user_id} 在服务器 {guild_id} 失去了幻化组 {role.name} (ID:{role.id}) 的基础组 {self.role_name_cache.get(base_role_id, f'ID:{base_role_id}')}，将移除幻化。")

                if roles_to_remove:
                    try:
                        await member.remove_roles(*roles_to_remove, reason="幻化基础身份组已丢失")
                        self.logger.info(f"成功为用户 {user_id} 移除了 {len(roles_to_remove)} 个不合格的幻化身份组。")
                        try:
                            # 尝试私信用户
                            removed_names = ", ".join([r.name for r in roles_to_remove])
                            await member.send(f"你在服务器 **{guild.name}** 的幻化身份组 `{removed_names}` 已被移除，因为你不再拥有其对应的基础身份组。")
                        except discord.Forbidden:
                            pass  # 无法私信
                    except Exception as e:
                        self.logger.error(f"移除用户 {user_id} 的幻化身份组失败: {e}")

                # 【新增】在处理每个用户后都进行延迟
                processed_count += 1
                if processed_count % 10 == 0:  # 例如，每处理10个用户，暂停3秒
                    await asyncio.sleep(3)
                else:  # 或者每个用户都暂停短暂时间
                    await asyncio.sleep(0.2)  # 暂停0.2秒
        self.logger.info("幻化身份组合法性检查完成。")

    @tasks.loop(hours=1)
    async def _update_role_cache_task(self):
        self.logger.info("开始执行每小时的身份组缓存和安全列表更新...")
        await self._filter_and_cache_safe_roles()
        self.logger.info("每小时身份组缓存和安全列表更新完毕。")

    @daily_reset_task.before_loop
    @check_expired_roles_task.before_loop
    @_update_role_cache_task.before_loop
    @check_fashion_role_validity_task.before_loop
    async def before_all_tasks(self):
        await self.bot.wait_until_ready()
        await self._filter_and_cache_safe_roles()

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(MainPanelView(self))
        self.logger.info("身份组管理模块已就绪，持久化视图已注册。")

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS or config_data.FASHION_CONFIG.keys()])
    @app_commands.default_permissions(manage_guild=True)
    async def send_panel(self, interaction: discord.Interaction):
        all_configured_guilds = set(config.GUILD_CONFIGS.keys()) | set(config_data.FASHION_CONFIG.keys())
        if interaction.guild_id not in all_configured_guilds:
            await interaction.response.send_message("❌ 此服务器未配置身份组机器人。", ephemeral=True)
            return
        embed = discord.Embed(title="✨ 身份组自助中心 ✨", description="欢迎来到身份组自助中心！\n\n点击下方的按钮来管理你的身份组或查询状态。",
                              color=discord.Color.blurple())
        embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")
        view = MainPanelView(self)
        await interaction.response.send_message(embed=embed, view=view)
    # ...


# UserManageView, PaginationButton, PrivateTimedRoleSelect, SelfServiceRoleButton (无改动)
# ...
class UserManageView(ui.View):
    def __init__(self, cog: RoleManagerCog, user: discord.Member):
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(timeout=timeout_minutes * 60)
        self.cog, self.user, self.guild = cog, user, user.guild
        self.timed_role_page, self.self_service_page = 0, 0
        self.all_timed_role_ids = self.cog.safe_timed_role_ids_cache.get(self.guild.id, [])
        self.all_self_service_role_ids = self.cog.safe_self_service_role_ids_cache.get(self.guild.id, [])
        if not self.all_timed_role_ids and not self.all_self_service_role_ids: self.cog.logger.info(
            f"服务器 {self.guild.id} 没有可供用户 {self.user.id} 管理的安全身份组。")
        self._rebuild_view()

    def _rebuild_view(self):
        self.clear_items()
        user_guild_data = self.cog.data_manager._get_guild_user_data(self.user.id, self.guild.id)
        current_timed_role_ids = set(user_guild_data.get("current_timed_roles", []))
        member = self.guild.get_member(self.user.id)
        if not member:
            self.cog.logger.warning(f"无法在 _rebuild_view 中找到用户 {self.user.id}。")
            self.add_item(ui.Label("无法加载您的信息，您可能已离开服务器。"))
            self.stop()
            return
        current_self_service_ids = {role.id for role in member.roles}
        total_timed_pages = math.ceil(len(self.all_timed_role_ids) / TIMED_ROLES_PER_PAGE)
        ss_start_row = 1
        if self.all_timed_role_ids:
            start_tr, end_tr = self.timed_role_page * TIMED_ROLES_PER_PAGE, (self.timed_role_page + 1) * TIMED_ROLES_PER_PAGE
            page_timed_role_ids = self.all_timed_role_ids[start_tr:end_tr]
            self.add_item(PrivateTimedRoleSelect(self.cog, self.guild.id, page_timed_role_ids, current_timed_role_ids, page_num=self.timed_role_page,
                                                 total_pages=total_timed_pages, row=0))
        elif config.GUILD_CONFIGS.get(self.guild.id, {}).get("timed_roles"):
            self.add_item(ui.Button(label="无可用限时组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=0))
        if total_timed_pages > 1:
            self.add_item(PaginationButton(label="◀️ 限时组", custom_id="page_timed_prev", disabled=self.timed_role_page == 0, row=1))
            self.add_item(PaginationButton(label="限时组 ▶️", custom_id="page_timed_next", disabled=self.timed_role_page >= total_timed_pages - 1, row=1))
            ss_start_row = 2
        page_ss_role_ids = []
        if self.all_self_service_role_ids:
            start_ss, end_ss = self.self_service_page * SELF_SERVICE_ROLES_PER_PAGE, (self.self_service_page + 1) * SELF_SERVICE_ROLES_PER_PAGE
            page_ss_role_ids = self.all_self_service_role_ids[start_ss:end_ss]
        for row_offset in range(2):
            current_processing_row = ss_start_row + row_offset
            if current_processing_row > 4: break
            start_index_in_page = row_offset * 5
            for i in range(5):
                index_in_page = start_index_in_page + i
                if index_in_page < len(page_ss_role_ids):
                    role_id = page_ss_role_ids[index_in_page]
                    role = self.guild.get_role(role_id)
                    if role: self.add_item(SelfServiceRoleButton(self.cog, role, role.id in current_self_service_ids, row=current_processing_row))
        if not self.all_self_service_role_ids and config.GUILD_CONFIGS.get(self.guild.id, {}).get("self_service_roles") and ss_start_row <= 4: self.add_item(
            ui.Button(label="无可用自助组 (权限原因)", style=discord.ButtonStyle.secondary, disabled=True, row=ss_start_row))
        total_self_service_pages = math.ceil(len(self.all_self_service_role_ids) / SELF_SERVICE_ROLES_PER_PAGE)
        if total_self_service_pages > 1:
            pagination_row = ss_start_row + 2
            if pagination_row <= 4:
                self.add_item(PaginationButton(label="◀️ 自助", custom_id="page_ss_prev", disabled=self.self_service_page == 0, row=pagination_row))
                self.add_item(ui.Button(label=f"{self.self_service_page + 1}/{total_self_service_pages}", style=discord.ButtonStyle.secondary, disabled=True,
                                        row=pagination_row))
                self.add_item(PaginationButton(label="自助 ▶️", custom_id="page_ss_next", disabled=self.self_service_page >= total_self_service_pages - 1,
                                               row=pagination_row))
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
        if self.is_finished():
            await interaction.response.edit_message(content="操作已完成或出现错误。", view=None, embed=None)
        else:
            await interaction.response.edit_message(view=self)


# ...

# 【改动】FashionManageView 升级
class FashionManageView(ui.View):
    def __init__(self, cog: RoleManagerCog, user: discord.Member | None):
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(timeout=timeout_minutes * 60)
        self.cog = cog

        self.user = user
        self.guild = user.guild
        self.fashion_page = 0

        # 【核心改动】获取所有配置的幻化，而不仅仅是用户可用的
        safe_fashion_map = self.cog.safe_fashion_map_cache.get(self.guild.id, {})

        # self.all_fashion_options 包含所有可能的幻化 (fashion_id, base_id)
        self.all_fashion_options: List[tuple[int, int]] = []
        # self.fashion_to_base_map 用于在 callback 中快速校验权限 {fashion_id: base_id}
        self.fashion_to_base_map: Dict[int, int] = {}

        for base_id, fashion_ids_list in safe_fashion_map.items():
            for fashion_id in fashion_ids_list:
                self.all_fashion_options.append((fashion_id, base_id))
                self.fashion_to_base_map[fashion_id] = base_id

        # 按幻化名称排序，保证显示顺序稳定
        self.all_fashion_options.sort(key=lambda x: self.cog.role_name_cache.get(x[0], ''))

        if not self.all_fashion_options:
            self.cog.logger.info(f"服务器 {self.guild.id} 未配置幻化系统。")

        self._rebuild_view()

    def _rebuild_view(self):
        self.clear_items()

        member = self.guild.get_member(self.user.id)
        if not member:
            self.cog.logger.warning(f"无法在 FashionManageView._rebuild_view 中找到用户 {self.user.id}。")
            self.add_item(ui.Label("无法加载您的信息，您可能已离开服务器。"))
            self.stop()
            return

        current_worn_fashion_ids = {role.id for role in member.roles}
        total_pages = math.ceil(len(self.all_fashion_options) / FASHION_ROLES_PER_PAGE)

        start_index = self.fashion_page * FASHION_ROLES_PER_PAGE
        end_index = start_index + FASHION_ROLES_PER_PAGE
        page_fashion_options = self.all_fashion_options[start_index:end_index]

        # 【修复】在这里获取用户身份组ID，并将其传递给 FashionRoleSelect
        user_role_ids = {r.id for r in self.user.roles}

        self.add_item(FashionRoleSelect(
            self.cog, self.guild.id, page_fashion_options, current_worn_fashion_ids,
            user_role_ids,  # 将 user_role_ids 作为参数传入
            page_num=self.fashion_page, total_pages=total_pages
        ))

        if total_pages > 1:
            self.add_item(PaginationButton(label="◀️ 上一页", custom_id="page_fashion_prev", disabled=self.fashion_page == 0, row=1))
            self.add_item(ui.Button(label=f"第 {self.fashion_page + 1}/{total_pages} 页", style=discord.ButtonStyle.secondary, disabled=True, row=1))
            self.add_item(PaginationButton(label="下一页 ▶️", custom_id="page_fashion_next", disabled=self.fashion_page >= total_pages - 1, row=1))

    async def pagination_callback(self, interaction: discord.Interaction):
        custom_id = interaction.data['custom_id']
        if custom_id == "page_fashion_prev":
            self.fashion_page -= 1
        elif custom_id == "page_fashion_next":
            self.fashion_page += 1
        self._rebuild_view()
        if self.is_finished():
            await interaction.response.edit_message(content="操作已完成或出现错误。", view=None, embed=None)
        else:
            await interaction.response.edit_message(view=self)


# 【改动】FashionRoleSelect 升级，以支持显示所有（包括锁定的）选项
class FashionRoleSelect(ui.Select):
    # 【修复】__init__ 签名增加了 user_role_ids 参数
    def __init__(self, cog: RoleManagerCog, guild_id: int, page_options_data: List[tuple[int, int]],
                 current_selection_ids: set[int], user_role_ids: set[int], page_num: int, total_pages: int):
        self.cog = cog
        self.guild_id = guild_id

        # 【修复】现在直接使用传入的 user_role_ids 参数，而不是 self.view.user.roles
        options = []
        for fashion_id, base_id in page_options_data:
            fashion_name = cog.role_name_cache.get(fashion_id, f"未知(ID:{fashion_id})")
            base_name = cog.role_name_cache.get(base_id, "未知基础组")

            if fashion_name and base_name:
                is_unlocked = base_id in user_role_ids
                label_prefix = "✅ " if is_unlocked else "🔒 "
                description_text = f"由「{base_name}」解锁" if is_unlocked else f"需要拥有「{base_name}」"

                options.append(
                    discord.SelectOption(
                        label=f"{label_prefix}{fashion_name}",
                        value=str(fashion_id),
                        description=description_text,
                        default=(fashion_id in current_selection_ids)
                    )
                )

        placeholder = "选择你的幻化（✅=可佩戴, 🔒=未解锁）..."
        if total_pages > 1: placeholder = f"幻化 (第 {page_num + 1}/{total_pages} 页, ✅=可佩戴, 🔒=未解锁)..."

        safe_fashion_map = self.cog.safe_fashion_map_cache.get(guild_id, {})
        if not page_options_data and not safe_fashion_map:
            placeholder = "本服未配置幻化系统"
        elif not page_options_data and safe_fashion_map and not any(base_id in user_role_ids for _, base_id in page_options_data):
            placeholder = "你没有可幻化的基础身份组"
        elif not options and page_options_data:
            placeholder = "幻化名称加载中..."

        super().__init__(
            placeholder=placeholder, min_values=0, max_values=len(options) if options else 1,
            options=options if options else [discord.SelectOption(label="无可用选项", value="_placeholder", default=False)],
            custom_id="private_fashion_role_select", disabled=not options, row=0
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild

        # 【重要】在 callback 中，self.view 是可用的
        fashion_to_base_map = self.view.fashion_to_base_map
        all_fashion_role_ids = set(fashion_to_base_map.keys())

        member_role_ids = {r.id for r in member.roles}
        old_selection_set = member_role_ids.intersection(all_fashion_role_ids)

        new_selection_in_page = {int(v) for v in self.values if v != "_placeholder"}
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}
        selections_not_in_this_page = old_selection_set - options_in_this_page_ids
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)

        roles_to_add_ids = final_new_selection_set - old_selection_set
        roles_to_remove_ids = old_selection_set - final_new_selection_set

        roles_to_actually_add, roles_to_actually_remove = [], []
        failed_attempts = []

        for role_id in roles_to_add_ids:
            required_base_id = fashion_to_base_map.get(role_id)
            if required_base_id and required_base_id in member_role_ids:
                role_obj = guild.get_role(role_id)
                if role_obj and not self.cog._is_role_dangerous(role_obj):
                    roles_to_actually_add.append(role_obj)
                else:
                    self.cog.logger.warning(f"用户 {member.id} 尝试获取危险/不存在的幻化 {role_id}，已阻止。")
            else:
                role_name = self.cog.role_name_cache.get(role_id, f"ID:{role_id}")
                base_name = self.cog.role_name_cache.get(required_base_id, f"ID:{required_base_id}")
                failed_attempts.append(f"**{role_name}** (需要 **{base_name}**)")

        for role_id in roles_to_remove_ids:
            role_obj = guild.get_role(role_id)
            if role_obj: roles_to_actually_remove.append(role_obj)

        if roles_to_actually_add: await member.add_roles(*roles_to_actually_add, reason="自助幻化")
        if roles_to_actually_remove: await member.remove_roles(*roles_to_actually_remove, reason="自助卸下幻化")

        if failed_attempts:
            await interaction.followup.send(
                f"❌ 操作部分成功。\n你无法佩戴以下幻化，因为你缺少必需的基础身份组：\n- " + "\n- ".join(failed_attempts),
                ephemeral=True
            )

        refreshed_member = await try_get_member(guild, member.id)
        if refreshed_member:
            new_embed, new_view = await self.cog._create_fashion_panel(refreshed_member)
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=new_embed, view=new_view)
            else:
                await interaction.followup.send(embed=new_embed, view=new_view, ephemeral=True)


class PaginationButton(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        # 根据 custom_id 判断是哪个面板的翻页
        if "fashion" in self.custom_id:
            await view.pagination_callback(interaction)
        elif "timed" in self.custom_id or "ss" in self.custom_id:
            await view.pagination_callback(interaction)
        else:  # 默认或未知，交给 view 处理
            await view.pagination_callback(interaction)


class PrivateTimedRoleSelect(ui.Select):
    # (此类无改动)
    def __init__(self, cog: RoleManagerCog, guild_id: int, page_role_ids: list[int], current_selection_ids: set[int], page_num: int, total_pages: int,
                 row: int = 0):
        self.cog = cog
        options = [discord.SelectOption(label=cog.role_name_cache.get(rid, f"未知(ID:{rid})"), value=str(rid), default=(rid in current_selection_ids)) for rid
                   in page_role_ids if cog.role_name_cache.get(rid)]
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
        current_data = self.cog.data_manager._get_guild_user_data(member.id, guild.id)
        all_current_selection_set = set(current_data.get("current_timed_roles", []))
        new_selection_in_page = {int(v) for v in self.values if v != "_placeholder"}
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}
        selections_not_in_this_page = all_current_selection_set - options_in_this_page_ids
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)
        roles_to_actually_add_ids, dangerous_attempted_names = set(), []
        for role_id_to_add in (final_new_selection_set - all_current_selection_set):
            role_obj = guild.get_role(role_id_to_add)
            if role_obj and self.cog._is_role_dangerous(role_obj):
                dangerous_attempted_names.append(role_obj.name)
            elif role_obj:
                roles_to_actually_add_ids.add(role_id_to_add)
        if dangerous_attempted_names:
            await interaction.followup.send(f"❌ 操作失败：尝试获取的身份组 '{', '.join(dangerous_attempted_names)}' 包含敏感权限。", ephemeral=True)
            refreshed_member = await try_get_member(guild, member.id)
            if refreshed_member:
                new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
                await interaction.edit_original_response(embed=new_embed, view=new_view)
            return
        new_selection_ids = list((all_current_selection_set - (all_current_selection_set - final_new_selection_set)) | roles_to_actually_add_ids)
        final_new_selection_set = set(new_selection_ids)
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
    # (此类无改动)
    def __init__(self, cog: RoleManagerCog, role: discord.Role, is_selected: bool, row: int | None = None):
        self.cog = cog
        self.role = role
        super().__init__(label=role.name, style=discord.ButtonStyle.success if is_selected else discord.ButtonStyle.secondary,
                         custom_id=f"toggle_self_service_role:{role.id}", row=row)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member = interaction.user
        if not (self.role in member.roles):
            if self.cog._is_role_dangerous(self.role):
                await interaction.followup.send(f"❌ 操作失败：身份组 **{self.role.name}** 包含敏感权限。", ephemeral=True)
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

# --- MainPanel, Buttons and setup --- (无改动, 但FashionPanelButton的回调现在依赖于新的缓存结构)
# ...
class MainPanelView(ui.View):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(OpenManagePanelButton(cog))
        self.add_item(FashionPanelButton(cog))
        self.add_item(ReturnTimedRoleButton(cog))
        self.add_item(QueryTimeButton(cog))


class OpenManagePanelButton(ui.Button):
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


class FashionPanelButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="幻化衣橱", style=discord.ButtonStyle.success, custom_id="open_fashion_panel", emoji="👗")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=True)
        if not self.cog.safe_fashion_map_cache.get(interaction.guild_id):
            await interaction.followup.send("❌ 此服务器尚未配置或未启用幻化系统。", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else await try_get_member(interaction.guild, interaction.user.id)
        if not member:
            await interaction.followup.send("错误：无法获取您的服务器成员信息。", ephemeral=True)
            return
        embed, view = await self.cog._create_fashion_panel(member)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


# ... etc (QueryTimeButton, ReturnTimedRoleButton, setup) 它们都不受影响
class QueryTimeButton(ui.Button):
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
        reset_hour = config.ROLE_MANAGER_CONFIG.get("reset_hour_utc8", 16)
        embed.set_footer(text=f"每日UTC+8 {reset_hour}点重置时长。")
        await interaction.followup.send(embed=embed, ephemeral=True)


class ReturnTimedRoleButton(ui.Button):
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
            f"✅ 你已归还服务器 **{guild.name}** 的限时组: {roles_text}。\n本次使用 {format_duration_hms(int(used_seconds))}。\n今天在本服剩余可用时间：{format_duration_hms(remaining_seconds)}。",
            ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleManagerCog(bot))