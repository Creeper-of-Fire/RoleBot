# src/role_manager/cog.py
from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import app_commands, ui, Color
from discord.ext import commands, tasks

import config
import config_data
from .data_manager import DataManager, DAILY_LIMIT_SECONDS

if TYPE_CHECKING:
    from ..bot import RoleBot  # 假设你的bot主类叫RoleBot


# ===================================================================
# 核心辅助函数
# ===================================================================
async def safe_defer(interaction: discord.Interaction):
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True, thinking=False)


async def try_get_member(guild: discord.Guild, member_id: int) -> discord.Member:
    return guild.get_member(member_id) or await guild.fetch_member(member_id)


def format_duration_hms(total_seconds: int) -> str:
    """
    将总秒数格式化为 'X 小时 Y 分钟 Z 秒' 的可读字符串。
    - 如果超过1小时，为了简洁，默认不显示秒。
    - 智能地组合小时、分钟和秒。
    """
    if total_seconds <= 0:
        return "`0` 秒"

    seconds = int(total_seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if hours > 0:
        parts.append(f"`{hours}` 小时")
    if minutes > 0:
        parts.append(f"`{minutes}` 分钟")
    if secs > 0:
        parts.append(f"`{secs}` 秒")

    return " ".join(parts) if parts else "`0` 秒"


# ===================================================================
# 主 Cog 类 - 它现在是所有逻辑的中心和发起者
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

    # --- 核心UI生成器 ---

    async def _create_private_manage_panel(self, user: discord.Member) -> tuple[discord.Embed, ui.View]:
        """根据用户当前状态，生成一个个性化的、私有的管理面板。"""
        remaining_seconds = self.data_manager.get_remaining_seconds(user.id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_data = self.data_manager._get_user(user.id)
        current_timed_role_id = user_data.get("current_timed_role")

        guild_config = config_data.GUILD_CONFIGS.get(user.guild.id, {})
        managed_self_service_ids = set(guild_config.get("self_service_roles", []))
        current_self_service_ids = {role.id for role in user.roles if role.id in managed_self_service_ids}

        timed_role_text = "无"
        if current_timed_role_id:
            role = user.guild.get_role(current_timed_role_id)
            if role: timed_role_text = role.mention

        current_self_service_roles = sorted([r for r in user.roles if r.id in current_self_service_ids], key=lambda r: r.name)
        self_service_roles_text = "\n".join(f"• {role.mention}" for role in current_self_service_roles) or "无"

        embed = discord.Embed(title=f"⚙️ {user.display_name}的身份组管理面板", description="在这里管理你的身份组。你的选择会自动保存并刷新此面板。",
                              color=Color.green())

        # 【优化】使用新的时间格式化函数
        used_text = format_duration_hms(used_seconds)
        remaining_text = format_duration_hms(remaining_seconds)
        embed.add_field(name="⏱️ 限时组时间", value=f"已用: {used_text}\n剩余: {remaining_text}", inline=False)

        embed.add_field(name="🎨 当前限时高亮组", value=timed_role_text, inline=True)
        embed.add_field(name="🔧 当前自助身份组", value=self_service_roles_text, inline=True)

        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        embed.set_footer(text=f"此面板将在{timeout_minutes}分钟后失效。")

        view = UserManageView(self, user, current_timed_role_id, current_self_service_ids)
        return embed, view

    # --- 后台任务 (无改动) ---

    @tasks.loop(minutes=1)
    async def daily_reset_task(self):
        if await self.data_manager.daily_reset():
            self.logger.info(f"每日计时器已在 UTC+8 {config_data.ROLE_MANAGER_CONFIG.get('reset_hour_utc8', 16)} 点重置。")

    @tasks.loop(minutes=1)
    async def check_expired_roles_task(self):
        self.logger.debug("正在检查过期限时身份组...")
        for user_id, role_id, guild_id in self.data_manager.get_users_with_active_timed_role():
            if self.data_manager.get_remaining_seconds(user_id) <= 0:
                self.logger.info(f"用户 {user_id} 的限时身份组 {role_id} 已过期，正在移除...")
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    await self.data_manager.force_return_timed_role(user_id)
                    continue
                member, role = guild.get_member(user_id), guild.get_role(role_id)
                if member and role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="限时身份组过期自动移除")
                        self.logger.info(f"成功为用户 {user_id} 移除了身份组 {role_id}。")
                        await self.data_manager.force_return_timed_role(user_id)
                        try:
                            await member.send(f"你的限时身份组 **{role.name}** 因使用时长已耗尽，已自动移除。")
                        except discord.Forbidden:
                            pass
                    except Exception as e:
                        self.logger.error(f"自动移除用户 {user_id} 的身份组 {role_id} 失败: {e}")
                else:
                    await self.data_manager.force_return_timed_role(user_id)

    @tasks.loop(hours=1)
    async def _update_role_cache_task(self):
        self.logger.info("正在更新身份组名称缓存...")
        updated_count = 0
        for guild_id, guild_cfg in config_data.GUILD_CONFIGS.items():
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

    # --- 事件监听器和应用命令 (无改动) ---

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(MainPanelView(self))
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
# 交互组件定义 (无结构改动，仅修改文本显示)
# ===================================================================

# --- 1. 私有管理视图 ---

class UserManageView(ui.View):
    """【私有】用户专属的管理视图，现在动态添加按钮"""

    def __init__(self, cog: RoleManagerCog, user: discord.Member, current_timed_role_id: int | None, current_self_service_ids: set[int]):
        timeout_minutes = config_data.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(timeout=timeout_minutes * 60)
        self.cog = cog

        # 添加限时身份组选择菜单 (保持不变)
        self.add_item(PrivateTimedRoleSelect(cog, user.guild.id, current_timed_role_id))

        # 【核心改动】动态添加自助身份组的 "开关按钮"
        guild_config = config_data.GUILD_CONFIGS.get(user.guild.id, {})
        self_service_role_ids = guild_config.get("self_service_roles", [])

        for role_id in self_service_role_ids:
            role = user.guild.get_role(role_id)
            if role:
                is_selected = role.id in current_self_service_ids
                self.add_item(SelfServiceRoleButton(cog, role, is_selected))


class PrivateTimedRoleSelect(ui.Select):
    def __init__(self, cog: RoleManagerCog, guild_id: int, current_selection_id: int | None):
        self.cog = cog
        guild_config = config_data.GUILD_CONFIGS.get(guild_id, {})
        role_ids = guild_config.get("timed_roles", [])
        options = [discord.SelectOption(label=cog.role_name_cache.get(rid, f"未知(ID:{rid})"), value=str(rid), default=(rid == current_selection_id)) for rid in
                   role_ids]
        super().__init__(placeholder="选择一个限时高亮身份组..." if options else "本服未配置限时身份组", min_values=0, max_values=1, options=options,
                         custom_id="private_timed_role_select", disabled=not options)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild
        current_role_id = self.cog.data_manager._get_user(member.id).get("current_timed_role")

        if self.values:
            selected_role_id = int(self.values[0])
            if selected_role_id != current_role_id:
                if current_role_id: await self.cog.data_manager.return_timed_role(member.id)
                if self.cog.data_manager.get_remaining_seconds(member.id) <= 0:
                    await interaction.followup.send("❌ 你今天的限时身份组使用时长已用尽。", ephemeral=True)
                    return
                role_to_add = guild.get_role(selected_role_id)
                if role_to_add: await member.add_roles(role_to_add); await self.cog.data_manager.claim_timed_role(member.id, selected_role_id, guild.id)
        elif current_role_id:
            role_to_remove = guild.get_role(current_role_id)
            if role_to_remove: await member.remove_roles(role_to_remove)
            await self.cog.data_manager.return_timed_role(member.id)

        refreshed_member = await try_get_member(interaction.guild, member.id)
        new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
        await interaction.edit_original_response(embed=new_embed, view=new_view)


class SelfServiceRoleButton(ui.Button):
    """【全新】单个自助身份组的开关按钮"""

    def __init__(self, cog: RoleManagerCog, role: discord.Role, is_selected: bool):
        self.cog = cog
        self.role = role

        super().__init__(
            label=role.name,
            style=discord.ButtonStyle.success if is_selected else discord.ButtonStyle.secondary,
            custom_id=f"toggle_self_service_role:{role.id}"  # 使用 custom_id 传递角色信息
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member = interaction.user

        # 一键切换逻辑
        if self.role in member.roles:
            await member.remove_roles(self.role, reason="自助移除身份组")
        else:
            await member.add_roles(self.role, reason="自助领取身份组")

        refreshed_member = await try_get_member(interaction.guild, member.id)
        new_embed, new_view = await self.cog._create_private_manage_panel(refreshed_member)
        await interaction.edit_original_response(embed=new_embed, view=new_view)


# --- 2. 公共面板视图 ---

class MainPanelView(ui.View):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(OpenManagePanelButton(cog))
        self.add_item(ReturnTimedRoleButton(cog))
        self.add_item(QueryTimeButton(cog))


class OpenManagePanelButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="管理我的身份组", style=discord.ButtonStyle.primary, custom_id="open_manage_panel", emoji="⚙️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        embed, view = await self.cog._create_private_manage_panel(interaction.user)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class QueryTimeButton(ui.Button):
    def __init__(self, cog: RoleManagerCog):
        super().__init__(label="查询我的时间", style=discord.ButtonStyle.secondary, custom_id="query_time_button", emoji="⏱️")
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        user_id = interaction.user.id
        remaining_seconds = self.cog.data_manager.get_remaining_seconds(user_id)
        used_seconds = DAILY_LIMIT_SECONDS - remaining_seconds
        user_data = self.cog.data_manager._get_user(user_id)
        current_role_id = user_data.get("current_timed_role")

        embed = discord.Embed(title="⏱️ 你的限时身份组时间使用情况", color=discord.Color.blue())

        # 【优化】使用新的时间格式化函数
        embed.add_field(name="今日已用时长", value=format_duration_hms(used_seconds), inline=False)
        embed.add_field(name="今日剩余时长", value=format_duration_hms(remaining_seconds), inline=False)

        if current_role_id:
            guild = self.cog.bot.get_guild(user_data.get("current_timed_role_guild_id"))
            role = guild.get_role(current_role_id) if guild else None
            role_name = f"**{role.name}**" if role else f"未知身份组(ID:{current_role_id})"
            embed.add_field(name="当前持有", value=f"你当前正在使用 {role_name}，计时进行中。", inline=False)
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
        member = interaction.user
        user_data = self.cog.data_manager._get_user(member.id)
        current_role_id = user_data.get("current_timed_role")
        if not current_role_id:
            await interaction.followup.send("你当前没有可归还的限时身份组。", ephemeral=True)
            return

        guild_id = user_data.get("current_timed_role_guild_id")
        guild = self.cog.bot.get_guild(guild_id)
        role_to_remove = guild.get_role(current_role_id) if guild else None

        if role_to_remove and role_to_remove in member.roles:
            await member.remove_roles(role_to_remove, reason="用户一键归还限时身份组")

        used_seconds = await self.cog.data_manager.return_timed_role(member.id)
        remaining_seconds = self.cog.data_manager.get_remaining_seconds(member.id)

        # 【优化】使用新的时间格式化函数
        used_text = format_duration_hms(used_seconds)
        remaining_text = format_duration_hms(remaining_seconds)
        await interaction.followup.send(
            f"✅ 你已归还身份组 **{role_to_remove.name if role_to_remove else f'ID:{current_role_id}'}**。\n"
            f"本次使用 {used_text}。\n"
            f"今天剩余可用时间：{remaining_text}。",
            ephemeral=True
        )


# ===================================================================
# setup 函数
# ===================================================================
async def setup(bot: commands.Bot):
    await bot.add_cog(RoleManagerCog(bot))
