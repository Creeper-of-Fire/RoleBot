# src/role_manager/cog.py
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, List, Dict, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
import config_data
from .helpers.auth import is_role_dangerous
from .helpers.helpers import try_get_member
from .timed_role_data_manager import TimedRoleDataManager
from .views.share import FeatureCog
from .views.views import MainPanelView, TimedRolePanelButton, QueryTimeButton, ReturnTimedRoleButton, SelfServicePanelButton, FashionPanelButton

if TYPE_CHECKING:
    from ..main import RoleBot


# ===================================================================
# 1. 核心协调 Cog (CoreCog)
# ===================================================================

class CoreCog(commands.Cog, name="Core"):
    """
    核心协调Cog。
    - 管理全局的 role_name_cache。
    - 提供主面板入口命令。
    - 周期性地触发所有功能模块的安全缓存更新。
    - 对其他模块的具体实现和配置保持无知。
    """

    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        self.role_name_cache: Dict[int, str] = {}
        self.feature_cogs: List[FeatureCog] = []
        self._update_all_caches_task.start()

    def cog_unload(self):
        self._update_all_caches_task.cancel()

    @tasks.loop(hours=1)
    async def _update_all_caches_task(self):
        """每小时调用所有已注册功能模块的缓存更新方法。"""
        self.logger.info("开始执行每小时的全局安全缓存更新...")
        if not self.feature_cogs:
            self.logger.warning("没有功能模块注册到 CoreCog，缓存更新任务跳过。")
            return

        # 使用 ayncio.gather 并行执行所有模块的更新
        results = await asyncio.gather(
            *[cog.update_safe_roles_cache() for cog in self.feature_cogs],
            return_exceptions=True
        )

        for cog, result in zip(self.feature_cogs, results):
            if isinstance(result, Exception):
                self.logger.error(f"模块 {cog.qualified_name} 在更新缓存时发生错误: {result}", exc_info=result)

        self.logger.info("每小时全局安全缓存更新完毕。")

    @_update_all_caches_task.before_loop
    async def before_cache_update_task(self):
        """在任务开始前，等待机器人就绪并执行一次初始缓存。"""
        await self.bot.wait_until_ready()
        # 确保在第一次循环前，所有 feature_cogs 都已注册
        # setup_hook 是更稳妥的地方，但这里延迟一下也能工作
        await asyncio.sleep(5)
        self.logger.info("CoreCog 已就绪，准备执行首次缓存更新...")

    def register_feature_cog(self, cog: FeatureCog):
        """允许其他功能模块向核心Cog注册自己。"""
        if asyncio.iscoroutinefunction(cog.update_safe_roles_cache):
            self.feature_cogs.append(cog)
            self.logger.info(f"功能模块 {cog.qualified_name} 已成功注册到 CoreCog。")
        else:
            self.logger.error(f"尝试注册的模块 {cog.qualified_name} 未实现 'update_safe_roles_cache' 异步方法，注册失败。")

    @commands.Cog.listener()
    async def on_ready(self):
        """当 Cog 准备就绪时，注册持久化视图。"""
        # 注意：MainPanelView 的构造函数需要一个 cog 实例，
        # 尽管它现在大部分功能都分散了，但为了向后兼容和简单性，
        # 我们可以暂时传入 CoreCog 自身或任一其他 Cog。
        # 更好的做法是重构 MainPanelView，使其不依赖任何特定的 feature cog。
        # 这里我们暂时传入 CoreCog。
        self.bot.add_view(MainPanelView(self))  # MainPanelView 现在由 CoreCog 负责
        self.logger.info("核心模块已就绪，主控制面板持久化视图已注册。")

    @app_commands.command(name="打开身份组自助中心面板", description="发送身份组管理面板到当前频道")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    async def send_panel(self, interaction: discord.Interaction):
        """发送一个公共的身份组管理入口面板。"""
        # 此命令现在不关心任何具体配置，只是发送面板
        embed = discord.Embed(title="✨ 身份组自助中心 ✨", description="欢迎来到身份组自助中心！\n\n点击下方的按钮来管理你的身份组或查询状态。",
                              color=discord.Color.blurple())
        embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")

        # MainPanelView 的 __init__ 需要修改，以动态地从 bot 获取 cogs
        view = MainPanelView(self)
        await interaction.response.send_message(embed=embed, view=view)


# ===================================================================
# 2. 限时身份组 Cog (TimedRolesCog)
# ===================================================================

class TimedRolesCog(FeatureCog, name="TimedRoles"):
    """
    管理所有限时身份组相关的功能。
    - 拥有自己的 DataManager 和计时器逻辑。
    - 运行自己的过期检查和每日重置任务。
    - 实现 update_safe_roles_cache 方法供 CoreCog 调用。
    """

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [TimedRolePanelButton(self), QueryTimeButton(self), ReturnTimedRoleButton(self)]

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

    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        """每日定时任务，用于重置用户的限时身份组使用时间。"""
        if await self.timed_role_data_manager.daily_reset():
            self.logger.info(f"每日计时器已在 UTC+8 {config.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        """每分钟检查并移除所有用户已过期的限时身份组。"""
        self.logger.debug("正在检查过期限时身份组...")
        # 获取所有活跃用户，这里不涉及API
        users_to_check = self.timed_role_data_manager.get_users_with_active_timed_role()

        # 引入一个计数器和更长的延迟间隔
        processed_count = 0
        for user_id, guild_id, role_ids in users_to_check:
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


# ===================================================================
# 3. 自助身份组 Cog (SelfServiceCog)
# ===================================================================

class SelfServiceCog(FeatureCog, name="SelfService"):
    """管理所有自助身份组相关的功能。"""

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.safe_self_service_role_ids_cache: Dict[int, List[int]] = {}

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


# ===================================================================
# 4. 幻化身份组 Cog (FashionCog)
# ===================================================================

class FashionCog(FeatureCog, name="Fashion"):
    """管理所有幻化身份组相关的功能。"""

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        return [FashionPanelButton(self)]

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        self.safe_fashion_map_cache: Dict[int, Dict[int, List[int]]] = {}
        self.check_fashion_role_validity_task.start()

    def cog_unload(self):
        self.check_fashion_role_validity_task.cancel()

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("FashionCog: 开始更新安全幻化身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        for guild_id, fashion_cfg in config_data.FASHION_CONFIG.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            configured_fashion_map = fashion_cfg.get("fashion_map", {})
            current_safe_fashion_map = {}
            for base_role_id, fashion_role_ids_list in configured_fashion_map.items():
                base_role = guild.get_role(base_role_id)
                if base_role: core_cog.role_name_cache[base_role_id] = base_role.name

                safe_fashions_for_base = []
                for fashion_role_id in fashion_role_ids_list:
                    fashion_role = guild.get_role(fashion_role_id)
                    if fashion_role:
                        core_cog.role_name_cache[fashion_role_id] = fashion_role.name
                        if is_role_dangerous(fashion_role):
                            self.logger.warning(f"服务器 '{guild.name}' 的幻化身份组 '{fashion_role.name}'(ID:{fashion_role_id}) 含敏感权限，已排除。")
                        else:
                            safe_fashions_for_base.append(fashion_role_id)

                if safe_fashions_for_base:
                    current_safe_fashion_map[base_role_id] = safe_fashions_for_base

            self.safe_fashion_map_cache[guild_id] = current_safe_fashion_map
        self.logger.info("FashionCog: 安全幻化身份组缓存更新完毕。")

    @tasks.loop(hours=24)
    async def check_fashion_role_validity_task(self):
        """
        每日检查所有用户的幻化身份组是否仍然合法。
        此方法现在使用 role.members，确保检查所有持有者，而不再错误地依赖 timed_roles 数据。
        """
        self.logger.info("开始检查幻化身份组合法性...")
        processed_count = 0

        for guild_id, safe_fashion_map in self.safe_fashion_map_cache.items():
            guild = self.bot.get_guild(guild_id)
            if not guild or not safe_fashion_map:
                continue

            # 创建一个 {fashion_id: base_id} 的反向查找表，方便快速查找
            fashion_to_base_map = {
                fashion_id: base_id
                for base_id, fashion_ids in safe_fashion_map.items()
                for fashion_id in fashion_ids
            }

            # 遍历缓存中所有已知的安全幻化身份组
            for fashion_id, base_id in fashion_to_base_map.items():
                fashion_role = guild.get_role(fashion_id)
                if not fashion_role:
                    continue

                # 正确做法：遍历持有该幻化身份组的所有成员
                for member in fashion_role.members:
                    # 检查该成员是否拥有对应的基础身份组
                    has_base_role = any(r.id == base_id for r in member.roles)

                    if not has_base_role:
                        try:
                            # 如果没有基础组，则移除幻化组
                            await member.remove_roles(fashion_role, reason="幻化基础身份组已丢失，自动移除")
                            self.logger.info(
                                f"用户 {member.display_name} ({member.id}) 在服务器 {guild.name} 失去了幻化组 '{fashion_role.name}' 的基础组，已移除幻化。")
                            # 尝试私信用户
                            await member.send(f"你在服务器 **{guild.name}** 的幻化身份组 `{fashion_role.name}` 已被移除，因为你不再拥有其对应的基础身份组。")
                        except discord.Forbidden:
                            # 无法私信或移除角色（可能机器人权限低于用户）
                            self.logger.warning(f"无法为用户 {member.display_name} 移除不合格的幻化身份组 '{fashion_role.name}'，权限不足。")
                        except discord.HTTPException as e:
                            self.logger.error(f"移除用户 {member.display_name} 的幻化身份组时发生HTTP错误: {e}")

                    # 添加延迟以避免 API 限速
                    processed_count += 1
                    if processed_count % 10 == 0:
                        await asyncio.sleep(1)

        self.logger.info("幻化身份组合法性检查完成。")

    @check_fashion_role_validity_task.before_loop
    async def before_fashion_task(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    """Cog的入口点，用于加载RoleManagerCog。"""
    await bot.add_cog(CoreCog(bot))
    await bot.add_cog(TimedRolesCog(bot))
    await bot.add_cog(SelfServiceCog(bot))
    await bot.add_cog(FashionCog(bot))
