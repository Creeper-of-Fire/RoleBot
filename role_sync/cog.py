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
from role_sync.role_sync_data_manager import RoleSyncDataManager
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog

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
        self.data_manager = RoleSyncDataManager()
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

    async def sync_rule_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """当用户输入rule参数时，动态生成同步规则列表。"""
        choices = []
        guild_id = interaction.guild_id
        sync_map = self.safe_direct_sync_map_cache.get(guild_id, {})
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        role_name_cache = core_cog.role_name_cache if core_cog else {}

        if not sync_map:
            return [app_commands.Choice(name="此服务器无A->B同步规则", value="disabled")]

        for source_id, target_id in sync_map.items():
            source_name = role_name_cache.get(source_id, f"未知身份组(ID:{source_id})")
            target_name = role_name_cache.get(target_id, f"未知身份组(ID:{target_id})")
            choice_name = f"{source_name} -> {target_name}"

            # 如果用户正在输入，进行模糊匹配
            if current.lower() in choice_name.lower():
                # Choice 的 value 必须是 string, 我们用 source_id 作为唯一标识
                choices.append(app_commands.Choice(name=choice_name, value=str(source_id)))

        # Discord 限制最多返回 25 个选项
        return choices[:25]

    @app_commands.command(name="同步未记录成员", description="扫描成员，为符合特定A->B规则但未被记录的人执行同步。")
    @app_commands.describe(rule="要扫描的特定同步规则")
    @app_commands.autocomplete(rule=sync_rule_autocomplete)  # 绑定自动补全
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_roles=True)
    async def sync_unlogged_members(self, interaction: discord.Interaction, rule: str):
        """
        手动扫描服务器，处理特定的一条 A->B 同步规则，并显示实时进度。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)

        guild = interaction.guild
        if rule == "disabled":
            await interaction.followup.send("❌ 操作已取消，因为此服务器没有配置 A->B 同步规则。", ephemeral=True)
            return

        try:
            source_role_id = int(rule)
        except ValueError:
            await interaction.followup.send("❌ 无效的规则选择，请从列表中选择。", ephemeral=True)
            return

        sync_map = self.safe_direct_sync_map_cache.get(guild.id, {})
        target_role_id = sync_map.get(source_role_id)

        if not target_role_id:
            await interaction.followup.send("❌ 所选规则已不存在或已失效。", ephemeral=True)
            return

        source_role = guild.get_role(source_role_id)
        target_role = guild.get_role(target_role_id)

        if not source_role or not target_role:
            await interaction.followup.send("❌ 规则中的一个或多个身份组已不存在。", ephemeral=True)
            return

        self.logger.info(f"管理员 {interaction.user} 触发了对规则 '{source_role.name} -> {target_role.name}' 的同步扫描。")

        members_to_scan = guild.members
        total_members = len(members_to_scan)
        processed_members, synced_count, logged_count, failed_count = 0, 0, 0, 0

        # 初始化 Embed
        embed = discord.Embed(
            title=f"⏳ 正在扫描规则: {source_role.name} -> {target_role.name}",
            description=f"基于当前缓存扫描 **{total_members}** 名成员。",
            color=discord.Color.blue()
        )
        embed.add_field(name="扫描进度", value=self._create_progress_bar(0, total_members), inline=False)
        embed.add_field(name="✅ 新增同步", value="`0` 人", inline=True)
        embed.add_field(name="✍️ 补录记录", value="`0` 人", inline=True)
        embed.add_field(name="❌ 同步失败", value="`0` 人", inline=True)
        await interaction.edit_original_response(embed=embed)

        # 开始扫描
        for member in members_to_scan:
            processed_members += 1
            if member.bot: continue

            if source_role in member.roles and not self.data_manager.is_synced(guild.id, source_role_id, member.id):
                if target_role in member.roles:
                    await self.data_manager.mark_as_synced(guild.id, source_role_id, member.id)
                    logged_count += 1
                else:
                    try:
                        await member.add_roles(target_role, reason=f"手动全量同步: {source_role.name}->{target_role.name}")
                        await self.data_manager.mark_as_synced(guild.id, source_role_id, member.id)
                        synced_count += 1
                        await asyncio.sleep(0.1)
                    except (discord.Forbidden, discord.HTTPException):
                        failed_count += 1

            if processed_members % 25 == 0 or processed_members == total_members:
                embed.set_field_at(0, name="扫描进度", value=self._create_progress_bar(processed_members, total_members))
                embed.set_field_at(1, name="✅ 新增同步", value=f"`{synced_count}` 人")
                embed.set_field_at(2, name="✍️ 补录记录", value=f"`{logged_count}` 人")
                embed.set_field_at(3, name="❌ 同步失败", value=f"`{failed_count}` 人")
                await interaction.edit_original_response(embed=embed)
                await asyncio.sleep(0.2)

        # 完成后的 Embed
        final_embed = discord.Embed(
            title=f"✅ 规则扫描完成: {source_role.name} -> {target_role.name}",
            description=f"已基于 **当前缓存** 扫描了 **{total_members}** 名成员。",
            color=discord.Color.green()
        )
        final_embed.add_field(name="新增同步", value=f"`{synced_count}` 人", inline=True)
        final_embed.add_field(name="补录记录", value=f"`{logged_count}` 人", inline=True)
        final_embed.add_field(name="同步失败", value=f"`{failed_count}` 人", inline=True)
        await interaction.edit_original_response(embed=final_embed)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(RoleSyncCog(bot))
