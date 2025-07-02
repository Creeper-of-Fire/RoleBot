# role_sync/cog.py

from __future__ import annotations

import asyncio
import typing
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config
import config_data
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import safe_defer

if typing.TYPE_CHECKING:
    from main import RoleBot
    from core.cog import CoreCog


class RoleSyncCog(FeatureCog, name="RoleSync"):
    """
    管理所有身份组同步相关的逻辑。
    - 实时同步：当用户获得身份组A时，自动授予身份组B。
    - 每日同步：每日检查拥有身份组C的用户，并授予他们身份组D。
    - 手动同步：通过命令为所有拥有身份组E的用户授予身份组F。
    """

    def __init__(self, bot: RoleBot):
        super().__init__(bot)
        # 缓存安全的同步规则
        # {'guild_id': {source_id: target_id}}
        self.safe_direct_sync_map_cache: Dict[int, Dict[int, int]] = {}
        # {'guild_id': [{'source': source_id, 'target': target_id}]}
        self.safe_daily_sync_pairs_cache: Dict[int, List[Dict[str, int]]] = {}

        self.daily_sync_task.start()

    def cog_unload(self):
        self.daily_sync_task.cancel()

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        """此模块没有面向普通用户的前端面板按钮。"""
        return None

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("RoleSyncCog: 开始更新安全同步身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        # 清空旧缓存
        self.safe_direct_sync_map_cache.clear()
        self.safe_daily_sync_pairs_cache.clear()

        for guild_id, sync_cfg in config_data.ROLE_SYNC_CONFIG.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            # 1. 处理直接同步 (A -> B)
            direct_sync_map = sync_cfg.get("direct_sync_map", {})
            safe_direct_map = {}
            for source_id, target_id in direct_sync_map.items():
                target_role = guild.get_role(target_id)
                if target_role:
                    core_cog.role_name_cache[target_id] = target_role.name
                    if is_role_dangerous(target_role):
                        self.logger.warning(
                            f"服务器 '{guild.name}' 的直接同步目标组 '{target_role.name}'(ID:{target_id}) 含敏感权限，已排除。")
                    else:
                        safe_direct_map[source_id] = target_id
            if safe_direct_map:
                self.safe_direct_sync_map_cache[guild_id] = safe_direct_map

            # 2. 处理每日同步 (C -> D)
            daily_sync_pairs = sync_cfg.get("daily_sync_pairs", [])
            safe_daily_pairs = []
            for pair in daily_sync_pairs:
                target_id = pair.get("target")
                target_role = guild.get_role(target_id)
                if target_role:
                    core_cog.role_name_cache[target_id] = target_role.name
                    if is_role_dangerous(target_role):
                        self.logger.warning(
                            f"服务器 '{guild.name}' 的每日同步目标组 '{target_role.name}'(ID:{target_id}) 含敏感权限，已排除。")
                    else:
                        safe_daily_pairs.append(pair)
            if safe_daily_pairs:
                self.safe_daily_sync_pairs_cache[guild_id] = safe_daily_pairs

        self.logger.info("RoleSyncCog: 安全同步身份组缓存更新完毕。")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """监听成员身份组变化，实现 A -> B 的实时同步。"""
        if before.roles == after.roles:
            return  # 身份组未变化

        guild_id = after.guild.id
        sync_map = self.safe_direct_sync_map_cache.get(guild_id)
        if not sync_map:
            return  # 该服务器无配置

        added_roles = set(after.roles) - set(before.roles)
        if not added_roles:
            return  # 没有新增身份组

        for added_role in added_roles:
            if added_role.id in sync_map:
                target_role_id = sync_map[added_role.id]
                target_role = after.guild.get_role(target_role_id)
                # 检查用户是否已拥有目标身份组
                if target_role and target_role not in after.roles:
                    try:
                        await after.add_roles(target_role, reason=f"自动同步：因获得 '{added_role.name}'")
                        self.logger.info(f"用户 {after.display_name} 因获得 '{added_role.name}'，已自动同步身份组 '{target_role.name}'。")
                    except discord.Forbidden:
                        self.logger.warning(f"无法为 {after.display_name} 同步身份组 '{target_role.name}'，权限不足。")
                    except discord.HTTPException as e:
                        self.logger.error(f"为 {after.display_name} 同步身份组时发生HTTP错误: {e}")

    @tasks.loop(hours=24)
    async def daily_sync_task(self):
        """每日任务：检查拥有身份组C的用户，并授予他们身份组D。"""
        self.logger.info("开始执行每日身份组同步任务...")
        processed_count = 0

        for guild_id, sync_pairs in self.safe_daily_sync_pairs_cache.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            for pair in sync_pairs:
                source_role = guild.get_role(pair["source"])
                target_role = guild.get_role(pair["target"])

                if not source_role or not target_role:
                    continue

                self.logger.info(f"正在为服务器 '{guild.name}' 处理每日同步：'{source_role.name}' -> '{target_role.name}'")

                # 遍历所有拥有源身份组的成员
                for member in source_role.members:
                    if target_role not in member.roles:
                        try:
                            await member.add_roles(target_role, reason=f"每日自动同步，因拥有 '{source_role.name}'")
                            self.logger.info(f"已为用户 {member.display_name} 每日同步身份组 '{target_role.name}'。")

                            try:
                                await member.send(
                                    f"你好！在服务器 **{guild.name}** 中，因为你拥有身份组 `{source_role.name}`，我们已自动为你同步了身份组 `{target_role.name}`。"
                                )
                            except discord.Forbidden:
                                self.logger.warning(f"无法私信用户 {member.display_name} ({member.id})，他们可能关闭了私信。")
                            except discord.HTTPException as e:
                                self.logger.error(f"私信用户 {member.display_name} 时发生HTTP错误: {e}")

                            processed_count += 1
                            if processed_count % 10 == 0:
                                await asyncio.sleep(1)  # API限速
                        except discord.Forbidden:
                            self.logger.warning(f"无法为 {member.display_name} 每日同步身份组 '{target_role.name}'，权限不足。")
                        except discord.HTTPException as e:
                            self.logger.error(f"为 {member.display_name} 每日同步身份组时发生HTTP错误: {e}")

        self.logger.info("每日身份组同步任务完成。")

    @daily_sync_task.before_loop
    async def before_daily_sync_task(self):
        await self.bot.wait_until_ready()

    def _create_progress_bar(self, current: int, total: int, bar_length: int = 20) -> str:
        """创建一个文本格式的进度条。"""
        if total == 0:
            return f"[{'░' * bar_length}] 0.0%"
        fraction = current / total
        filled_length = int(bar_length * fraction)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        return f"[{bar}] {fraction:.1%}"

    @app_commands.command(name="刷新成员缓存", description="【非常耗时！注意！】手动拉取服务器所有成员信息到机器人缓存中（带进度条）。")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config.GUILD_IDS])
    @app_commands.default_permissions(manage_roles=True)
    async def refresh_member_cache(self, interaction: discord.Interaction):
        """
        手动触发从 Discord API 拉取服务器所有成员，并显示实时进度条。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)
        guild = interaction.guild
        if not guild:
            await interaction.edit_original_response(content="❌ 无法获取服务器信息。")
            return

        total_members = guild.member_count
        if total_members == 0:
            await interaction.edit_original_response(content="✅ 服务器中没有成员。")
            return

        self.logger.info(f"服务器 '{guild.name}' (ID: {guild.id}) 由 {interaction.user} 手动触发了成员缓存刷新。")

        # 初始进度条消息
        embed = discord.Embed(
            title="⏳ 正在刷新成员缓存...",
            description=f"正在从服务器拉取 **{total_members}** 名成员的信息...",
            color=discord.Color.blue()
        )
        embed.add_field(name="进度", value=self._create_progress_bar(0, total_members), inline=False)
        await interaction.edit_original_response(embed=embed)

        fetched_count = 0
        last_update_count = 0

        # 使用异步迭代器逐个获取成员
        try:
            async for member in guild.fetch_members(limit=None):
                fetched_count += 1
                # 为了避免过于频繁地编辑消息（API限速），我们每获取一定数量的成员或进度变化超过5%时才更新
                if fetched_count - last_update_count >= 100 or fetched_count == total_members:
                    last_update_count = fetched_count

                    embed.description = f"正在处理成员: **{fetched_count} / {total_members}**"
                    embed.set_field_at(
                        index=0,  # 更新第一个字段
                        name="进度",
                        value=self._create_progress_bar(fetched_count, total_members),
                        inline=False
                    )
                    await interaction.edit_original_response(embed=embed)
                    # 稍微暂停一下，给API一点喘息空间
                    await asyncio.sleep(0.1)

        except Exception as e:
            self.logger.error(f"刷新成员缓存时发生错误: {e}", exc_info=True)
            error_embed = discord.Embed(
                title="❌ 刷新中断",
                description=f"在处理过程中发生错误。\n`{e}`",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(embed=error_embed)
            return

        # 任务完成后的最终消息
        final_embed = discord.Embed(
            title="✅ 成员缓存刷新完成",
            description=f"成功将 **{fetched_count}** 名（共 {total_members} 名）成员的信息同步到了机器人缓存中。",
            color=discord.Color.green()
        )
        final_embed.set_footer(text=f"当前缓存成员数: {len(guild.members)}")
        await interaction.edit_original_response(embed=final_embed)

    @app_commands.command(name="手动触发每日同步", description="立即执行一次每日身份组同步检查任务。")
    @app_commands.guilds(*[discord.Object(id=gid) for gid in config_data.ROLE_SYNC_CONFIG.keys()])
    @app_commands.default_permissions(manage_roles=True)
    async def manual_daily_sync(self, interaction: discord.Interaction):
        """手动触发 daily_sync_task 任务。"""
        await interaction.response.send_message("▶️ 已手动触发每日身份组同步任务...", ephemeral=True)
        self.logger.info(f"每日同步任务由 {interaction.user} ({interaction.user.id}) 手动触发。")

        # 使用 create_task 在后台运行，防止阻塞交互响应
        # 这样可以立即回复用户，而任务在后台执行
        self.bot.loop.create_task(self.daily_sync_task())

        await interaction.edit_original_response(content="✅ 每日身份组同步任务已在后台启动。请查看机器人日志了解进度和结果。")

    @app_commands.command(name="同步身份组", description="为所有拥有指定源身份组的成员，批量添加目标身份组。")
    @app_commands.describe(source_role="需要检查的源身份组", target_role="需要授予的目标身份组")
    @app_commands.default_permissions(manage_roles=True)
    async def sync_roles_command(self, interaction: discord.Interaction, source_role: discord.Role, target_role: discord.Role):
        """命令：为所有拥有E的用户授予F。"""
        await safe_defer(interaction, thinking=True)

        # 安全性检查
        if source_role == target_role:
            await interaction.followup.send("❌ 源身份组和目标身份组不能是同一个。", ephemeral=True)
            return
        if is_role_dangerous(target_role):
            await interaction.followup.send(f"❌ 目标身份组 '{target_role.name}' 包含危险权限，操作已中止。", ephemeral=True)
            return
        if interaction.guild.me.top_role <= target_role:
            await interaction.followup.send(f"❌ 我的身份组权限低于目标身份组 '{target_role.name}'，无法进行操作。", ephemeral=True)
            return

        members_to_add = [m for m in source_role.members if target_role not in m.roles]

        if not members_to_add:
            await interaction.followup.send(f"✅ 所有拥有 '{source_role.name}' 的成员均已拥有 '{target_role.name}'，无需操作。", ephemeral=True)
            return

        await interaction.followup.send(f"⏳ 正在为 {len(members_to_add)} 名拥有 '{source_role.name}' 的成员添加 '{target_role.name}'...", ephemeral=True)

        success_count = 0
        fail_count = 0
        for member in members_to_add:
            try:
                await member.add_roles(target_role, reason=f"由 {interaction.user} 手动触发同步")
                success_count += 1
                await asyncio.sleep(0.2)  # 避免速率限制
            except discord.Forbidden:
                fail_count += 1
            except discord.HTTPException:
                fail_count += 1

        await interaction.followup.send(f"✅ 同步完成！\n- 成功为 {success_count} 人添加了身份组。\n- 失败 {fail_count} 人（可能因为权限不足）。", ephemeral=True)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(RoleSyncCog(bot))
