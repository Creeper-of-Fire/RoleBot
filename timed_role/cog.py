from __future__ import annotations

import asyncio
import typing
from datetime import datetime
from typing import Optional, List, Dict

import discord
from discord import app_commands
from discord.ext import tasks, commands

import config
from timed_role import timer
from timed_role.buttons import TimedRolePanelButton
from timed_role.timed_role_data_manager import TimedRoleDataManager
from timed_role.timer import UTC8
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import try_get_member

if typing.TYPE_CHECKING:
    from core.cog import CoreCog
    from main import RoleBot


class TimedRolesCog(FeatureCog, name="TimedRoles"):
    """
    管理所有限时身份组相关的功能。
    - 拥有自己的 DataManager 和计时器逻辑。
    - 运行自己的过期检查和每日重置任务。
    - 实现 update_safe_roles_cache 方法供 CoreCog 调用。
    """

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [TimedRolePanelButton(self)]

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.timed_role_data_manager = TimedRoleDataManager()
        self.safe_timed_role_ids_cache: Dict[int, List[int]] = {}

        self.daily_reset_task.start()
        self.check_expired_roles_task.start()

    def cog_unload(self):
        self.daily_reset_task.cancel()
        self.check_expired_roles_task.cancel()

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("TimedRolesCog: 开始更新安全限时身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        for guild_id, guild_cfg in config.GUILD_CONFIGS.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            configured_timed_ids = guild_cfg.get("timed_roles", [])
            current_safe_timed_ids = []
            for role_id in configured_timed_ids:
                role = guild.get_role(role_id)
                if role:
                    core_cog.role_name_cache[role_id] = role.name
                    if is_role_dangerous(role):
                        self.logger.warning(f"服务器 '{guild.name}' 的限时身份组 '{role.name}'(ID:{role_id}) 含敏感权限，已排除。")
                    else:
                        current_safe_timed_ids.append(role_id)
            self.safe_timed_role_ids_cache[guild_id] = current_safe_timed_ids
        self.logger.info("TimedRolesCog: 安全限时身份组缓存更新完毕。")

    @app_commands.command(name="强制触发限时身份组每日重置", description="在当前服务器强制触发限时身份组每日重置")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def force_reset_timed_roles_command(self, interaction: discord.Interaction):
        """【管理员专属】强制触发所有服务器的限时身份组每日重置。"""
        self.logger.info(f"管理员 {interaction.user} 正在强制触发限时身份组每日重置...")
        await interaction.response.send_message("正在强制触发每日重置...", ephemeral=True)
        if timer.is_guild_permanent(interaction.guild_id):
            await interaction.followup.send(f"❌ 服务器{interaction.guild.name}为永久服务器，不支持强制重置。", ephemeral=True)
            return
        await self.timed_role_data_manager.daily_reset(self, [interaction.guild])

        await interaction.followup.send(f"✅ 服务器{interaction.guild.name}强制重置成功。", ephemeral=True)
        self.logger.info("管理员强制重置成功。")

    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        """每日定时任务，用于重置用户的限时身份组使用时间。"""
        now = datetime.now(UTC8)
        reset_hour = config.ROLE_MANAGER_CONFIG['reset_hour_utc8']

        # 检查是否到达每日重置时间
        # 为了防止重复触发，我们需要检查上一次重置的时间戳
        last_reset = await self.timed_role_data_manager.get_last_reset_time()
        today_reset_time = now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)

        if now >= today_reset_time > last_reset:
            self.logger.info(f"已到达每日重置时间 (UTC+8 {reset_hour}点)，正在启动重置...")
            # Cog 负责筛选需要重置的服务器
            guilds_to_reset = [
                g for g in self.bot.guilds
                if g.id in config.GUILD_CONFIGS and not timer.is_guild_permanent(g.id)
            ]

            if not guilds_to_reset:
                self.logger.info("每日重置：没有需要处理的服务器，跳过。")
                # 即使没有服务器要重置，也要更新时间戳以防重复触发
                await self.timed_role_data_manager.update_last_reset_time()
                return

            await self.timed_role_data_manager.daily_reset(self, guilds_to_reset)

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        """每分钟检查并移除所有用户已过期的限时身份组。"""
        self.logger.debug("正在检查过期限时身份组...")
        # 获取所有活跃用户，这里不涉及API
        users_to_check = self.timed_role_data_manager.get_users_with_active_timed_role()

        # 引入一个计数器和更长的延迟间隔
        processed_count = 0
        for user_id, guild_id, role_ids in users_to_check:
            if timer.is_guild_permanent(guild_id):
                # --- 这是为永久服务器新增的“自愈”逻辑 ---
                # 检查该用户的已用时间是否大于0，如果是，则说明数据存在异常（例如从非永久切换而来）
                # 我们需要将其重置，以确保其“永久”状态。
                await self.timed_role_data_manager.reset_user_used_seconds(user_id, guild_id)
                continue
            # 这里的 get_remaining_seconds 内部可能调用 _get_guild_user_data，不涉及API
            if self.timed_role_data_manager.get_remaining_seconds(user_id, guild_id) <= 0:
                self.logger.info(f"用户 {user_id} 在服务器 {guild_id} 的限时身份组已过期，正在移除...")
                guild, member = self.bot.get_guild(guild_id), None
                if guild:
                    # try_get_member 可能会触发 API
                    member = await try_get_member(guild, user_id)

                if not guild or not member:
                    # 无法获取成员或服务器，强制清除本地状态
                    await self.timed_role_data_manager.force_return_timed_roles(user_id, guild_id)
                    continue

                roles_to_remove = [role for role in guild.roles if role.id in role_ids and role in member.roles]
                if roles_to_remove:
                    try:
                        # remove_roles 会触发 API
                        await member.remove_roles(*roles_to_remove, reason="限时身份组过期自动移除")
                        self.logger.info(f"成功为用户 {user_id} 移除了 {len(roles_to_remove)} 个身份组。")
                        # 调用归还函数来结算使用时长
                        used_seconds_this_session = await self.timed_role_data_manager.return_timed_roles(user_id, guild_id)
                        self.logger.info(f"用户 {user_id} 在服务器 {guild_id} 的本次会话结算了 {used_seconds_this_session:.2f} 秒。")
                        # try:
                        #     # member.send 也会触发 API
                        #     await member.send(f"你在服务器 **{guild.name}** 的限时身份组因使用时长已耗尽，已自动移除。")
                        # except discord.Forbidden:
                        #     pass
                    except Exception as e:
                        self.logger.error(f"自动移除用户 {user_id} 的身份组失败: {e}")
                else:
                    await self.timed_role_data_manager.force_return_timed_roles(user_id, guild_id)

                # 处理完一个用户后暂停，根据实际情况调整延迟
                # 例如：每5个用户延迟1秒，或者每个用户延迟0.2秒
                processed_count += 1
                if processed_count % 5 == 0:  # 每处理5个用户，暂停一小会儿
                    await asyncio.sleep(1)  # 暂停1秒
                elif processed_count % 1 == 0:  # 如果用户少，可以每个用户都暂停短时间
                    await asyncio.sleep(0.1)  # 暂停0.1秒

    @daily_reset_task.before_loop
    @check_expired_roles_task.before_loop
    async def before_timed_roles_tasks(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(TimedRolesCog(bot))
