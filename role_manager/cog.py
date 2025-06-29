# src/role_manager/cog.py
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, List, Dict

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
import config_data
from .data_manager import DataManager
from .helpers.helpers import try_get_member
from .helpers.auth import is_role_dangerous
from .views.views import MainPanelView

if TYPE_CHECKING:
    from ..main import RoleBot

# 分页常量
TIMED_ROLES_PER_PAGE = 25
SELF_SERVICE_ROLES_PER_PAGE = 10
FASHION_ROLES_PER_PAGE = 25



class RoleManagerCog(commands.Cog, name="RoleManager"):
    """管理服务器中的限时、自助和幻化身份组。"""
    def __init__(self, bot: RoleBot):
        """初始化 RoleManagerCog。"""
        self.bot = bot
        self.logger = bot.logger
        self.data_manager = DataManager()
        self.role_name_cache = {}
        self.safe_timed_role_ids_cache: dict[int, list[int]] = {}
        self.safe_self_service_role_ids_cache: dict[int, list[int]] = {}
        self.safe_fashion_map_cache: dict[int, Dict[int, List[int]]] = {}  # 幻化映射缓存

        self.daily_reset_task.start()
        self.check_expired_roles_task.start()
        self._update_role_cache_task.start()

    def cog_unload(self):
        """当 Cog 被卸载时，取消所有后台任务。"""
        self.daily_reset_task.cancel()
        self.check_expired_roles_task.cancel()
        self._update_role_cache_task.cancel()



    async def _filter_and_cache_safe_roles(self):
        """过滤并缓存所有服务器中配置的安全身份组。

        此函数会遍历所有已配置的服务器，检查其中的限时、自助和幻化身份组，
        排除掉包含危险权限的身份组后，将安全的身份组ID存入相应的缓存中。
        """
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

            # 处理限时和自助身份组
            configured_timed_ids = guild_cfg.get("timed_roles", [])
            current_safe_timed_ids = []
            for role_id in configured_timed_ids:
                role = guild.get_role(role_id)
                if role:
                    if self.role_name_cache.get(role_id) != role.name: self.role_name_cache[role_id] = role.name
                    if is_role_dangerous(role):
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
                    if is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的自助身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                        changed_count += 1
                    else:
                        current_safe_ss_ids.append(role_id)
                else:
                    self.logger.warning(f"在服务器 {guild_id} 中未找到配置的自助身份组ID: {role_id}。")
            self.safe_self_service_role_ids_cache[guild_id] = current_safe_ss_ids

            # 处理幻化身份组
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
                        if is_role_dangerous(fashion_role):
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


    # 其他后台任务、监听器、命令
    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        """每日定时任务，用于重置用户的限时身份组使用时间。"""
        if await self.data_manager.daily_reset(): self.logger.info(f"每日计时器已在 UTC+8 {config.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        """每分钟检查并移除所有用户已过期的限时身份组。"""
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
                        # 调用归还函数来结算使用时长
                        used_seconds_this_session = await self.data_manager.return_timed_roles(user_id, guild_id)
                        self.logger.info(f"用户 {user_id} 在服务器 {guild_id} 的本次会话结算了 {used_seconds_this_session:.2f} 秒。")
                        # try:
                        #     # member.send 也会触发 API
                        #     await member.send(f"你在服务器 **{guild.name}** 的限时身份组因使用时长已耗尽，已自动移除。")
                        # except discord.Forbidden:
                        #     pass
                    except Exception as e:
                        self.logger.error(f"自动移除用户 {user_id} 的身份组失败: {e}")
                else:
                    await self.data_manager.force_return_timed_roles(user_id, guild_id)

                # 处理完一个用户后暂停，根据实际情况调整延迟
                # 例如：每5个用户延迟1秒，或者每个用户延迟0.2秒
                processed_count += 1
                if processed_count % 5 == 0:  # 每处理5个用户，暂停一小会儿
                    await asyncio.sleep(1)  # 暂停1秒
                elif processed_count % 1 == 0:  # 如果用户少，可以每个用户都暂停短时间
                    await asyncio.sleep(0.1)  # 暂停0.1秒

    @tasks.loop(hours=24)
    async def check_fashion_role_validity_task(self):
        """每日检查所有用户的幻化身份组是否仍然合法（即是否还拥有基础身份组）。"""
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

                processed_count += 1
                if processed_count % 10 == 0:  # 例如，每处理10个用户，暂停3秒
                    await asyncio.sleep(3)
                else:  # 或者每个用户都暂停短暂时间
                    await asyncio.sleep(0.2)  # 暂停0.2秒
        self.logger.info("幻化身份组合法性检查完成。")

    @tasks.loop(hours=1)
    async def _update_role_cache_task(self):
        """每小时更新一次服务器的身份组缓存和安全列表。"""
        self.logger.info("开始执行每小时的身份组缓存和安全列表更新...")
        await self._filter_and_cache_safe_roles()
        self.logger.info("每小时身份组缓存和安全列表更新完毕。")

    @daily_reset_task.before_loop
    @check_expired_roles_task.before_loop
    @_update_role_cache_task.before_loop
    @check_fashion_role_validity_task.before_loop
    async def before_all_tasks(self):
        """在所有后台任务开始前，等待机器人就绪并初始化身份组缓存。"""
        await self.bot.wait_until_ready()
        await self._filter_and_cache_safe_roles()

    @commands.Cog.listener()
    async def on_ready(self):
        """当 Cog 准备就绪时调用的事件监听器，用于注册持久化视图。"""
        self.bot.add_view(MainPanelView(self))
        self.logger.info("身份组管理模块已就绪，持久化视图已注册。")

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS or config_data.FASHION_CONFIG.keys()])
    @app_commands.default_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        """一个斜杠命令，用于在当前频道发送一个公共的身份组管理入口面板。"""
        all_configured_guilds = set(config.GUILD_CONFIGS.keys()) | set(config_data.FASHION_CONFIG.keys())
        if interaction.guild_id not in all_configured_guilds:
            await interaction.response.send_message("❌ 此服务器未配置身份组机器人。", ephemeral=True)
            return
        embed = discord.Embed(title="✨ 身份组自助中心 ✨", description="欢迎来到身份组自助中心！\n\n点击下方的按钮来管理你的身份组或查询状态。",
                              color=discord.Color.blurple())
        embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")
        view = MainPanelView(self)
        await interaction.response.send_message(embed=embed, view=view)




async def setup(bot: commands.Bot):
    """Cog的入口点，用于加载RoleManagerCog。"""
    await bot.add_cog(RoleManagerCog(bot))