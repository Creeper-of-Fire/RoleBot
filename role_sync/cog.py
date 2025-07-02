# role_sync/cog.py

from __future__ import annotations

import asyncio
import io
import typing
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config_data
from role_sync.role_sync_data_manager import RoleSyncDataManager, create_rule_key, DATA_FILE
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog
from utility.helpers import create_progress_bar

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
            source_id = added_role.id
            target_id = sync_map.get(source_id)
            if not target_id: continue

            if self.data_manager.is_synced(guild_id, source_id, target_id, after.id): continue

            target_role = after.guild.get_role(target_id)
            if not target_role: continue

            if target_role in after.roles:
                await self.data_manager.mark_as_synced(guild_id, source_id, target_id, after.id)
                continue

            try:
                await after.add_roles(target_role, reason=f"自动同步: {added_role.name}")
                await self.data_manager.mark_as_synced(guild_id, source_id, target_id, after.id)
            except Exception as e:
                self.logger.error(f"为 {after.display_name} 同步时出错: {e}")

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

    async def sync_rule_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """当用户输入rule参数时，动态生成同步规则列表。"""
        choices = []
        guild_id = interaction.guild_id
        sync_map = self.safe_direct_sync_map_cache.get(guild_id, {})
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        role_name_cache = core_cog.role_name_cache if core_cog else {}

        # 新增一个“所有规则”的选项
        all_rules_choice = app_commands.Choice(name="[扫描所有规则]", value="all")
        if not current or "所有" in all_rules_choice.name:
            choices.append(all_rules_choice)

        for source_id, target_id in sync_map.items():
            source_name = role_name_cache.get(source_id, f"ID:{source_id}")
            target_name = role_name_cache.get(target_id, f"ID:{target_id}")
            choice_name = f"{source_name} -> {target_name}"
            rule_key = create_rule_key(source_id, target_id)

            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=rule_key))
        return choices[:25]

    @app_commands.command(name="同步未记录成员", description="扫描缓存中的成员，为符合规则但未被记录的人执行同步。")
    @app_commands.describe(rule="[可选] 选择要扫描的特定规则，不选则扫描所有规则。")
    @app_commands.autocomplete(rule=sync_rule_autocomplete)
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_roles=True)
    async def sync_unlogged_members(self, interaction: discord.Interaction, rule: Optional[str] = "all"):
        """
        手动扫描服务器，处理特定的一条 A->B 同步规则，并显示实时进度。
        """
        await interaction.response.defer(ephemeral=False, thinking=True)
        guild = interaction.guild
        sync_map = self.safe_direct_sync_map_cache.get(guild.id, {})

        if not sync_map:
            await interaction.followup.send("❌ 此服务器没有配置任何 A->B 实时同步规则。", ephemeral=True)
            return

        rules_to_scan = {}
        if rule == "all":
            rules_to_scan = sync_map
            scan_title = "扫描所有规则"
        else:
            try:
                source_id_str, target_id_str = rule.split('-')
                source_id, target_id = int(source_id_str), int(target_id_str)
                rules_to_scan[source_id] = target_id
                source_role = guild.get_role(source_id)
                target_role = guild.get_role(target_id)
                if not source_role or not target_role:
                    await interaction.followup.send("❌ 规则中的身份组已不存在。", ephemeral=True)
                    return
                scan_title = f"扫描规则: {source_role.name} -> {target_role.name}"
            except (ValueError, KeyError):
                await interaction.followup.send("❌ 无效的规则选择，请从列表中选择。", ephemeral=True)
                return

        # --- 扫描主逻辑 ---
        embed = discord.Embed(title=f"⏳ {scan_title}", description="正在初始化扫描...", color=discord.Color.blue())
        await interaction.edit_original_response(embed=embed)

        total_synced, total_logged, total_failed = 0, 0, 0
        total_members_to_scan = 0

        # 先计算总人数
        for source_id in rules_to_scan.keys():
            source_role = guild.get_role(source_id)
            if source_role:
                total_members_to_scan += len(source_role.members)

        if total_members_to_scan == 0:
            await interaction.followup.send("✅ 所有相关源身份组下都没有成员，无需扫描。", ephemeral=True)
            return

        embed.description = f"准备扫描 **{len(rules_to_scan)}** 条规则，共涉及 **{total_members_to_scan}** 名成员（去重前）。"
        embed.add_field(name="扫描进度", value=create_progress_bar(0, total_members_to_scan), inline=False)
        embed.add_field(name="✅ 同步", value="0", inline=True)
        embed.add_field(name="✍️ 补录", value="0", inline=True)
        embed.add_field(name="❌ 失败", value="0", inline=True)
        await interaction.edit_original_response(embed=embed)

        processed_members_count = 0
        for source_id, target_id in rules_to_scan.items():
            source_role = guild.get_role(source_id)
            target_role = guild.get_role(target_id)
            if not source_role or not target_role: continue

            # 【核心优化】只扫描拥有源身份组的成员
            for member in source_role.members:
                processed_members_count += 1
                if member.bot: continue

                if not self.data_manager.is_synced(guild.id, source_id, target_id, member.id):
                    if target_role in member.roles:
                        await self.data_manager.mark_as_synced(guild.id, source_id, target_id, member.id)
                        total_logged += 1
                    else:
                        try:
                            await member.add_roles(target_role, reason="手动全量同步")
                            await self.data_manager.mark_as_synced(guild.id, source_id, target_id, member.id)
                            total_synced += 1
                        except (discord.Forbidden, discord.HTTPException):
                            total_failed += 1

                if processed_members_count % 10 == 0:
                    embed.set_field_at(0, name="扫描进度", value=create_progress_bar(processed_members_count, total_members_to_scan))
                    embed.set_field_at(1, name="✅ 同步", value=f"`{total_synced}`")
                    embed.set_field_at(2, name="✍️ 补录", value=f"`{total_logged}`")
                    embed.set_field_at(3, name="❌ 失败", value=f"`{total_failed}`")
                    await interaction.edit_original_response(embed=embed)
                    await asyncio.sleep(0.2)

        final_embed = discord.Embed(title=f"✅ {scan_title} 完成", color=discord.Color.green())
        final_embed.description = f"扫描了 **{processed_members_count}** 名成员。"
        final_embed.add_field(name="新增同步", value=f"`{total_synced}`人", inline=True)
        final_embed.add_field(name="补录记录", value=f"`{total_logged}`人", inline=True)
        final_embed.add_field(name="同步失败", value=f"`{total_failed}`人", inline=True)
        await interaction.edit_original_response(embed=final_embed)


    @app_commands.command(name="管理同步日志", description="管理A->B同步规则的日志记录。")
    @app_commands.describe(
        action="要执行的操作：清除特定规则日志，清除所有日志，或导出日志。",
        rule="[仅清除特定规则时需要] 选择要清除日志的规则。"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="清除特定规则的日志", value="clear_rule"),
        app_commands.Choice(name="导出日志文件", value="export_log"),
        app_commands.Choice(name="清除所有日志（删除文件）", value="clear_all"),
    ])
    @app_commands.autocomplete(rule=sync_rule_autocomplete)
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def manage_sync_log(self, interaction: discord.Interaction, action: str, rule: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)

        if action == "clear_rule":
            if not rule or rule == 'all':
                await interaction.followup.send("❌ 请使用 `rule` 参数选择一个**具体**的规则来清除。", ephemeral=True)
                return
            try:
                source_id_str, target_id_str = rule.split('-')
                source_id, target_id = int(source_id_str), int(target_id_str)
                success = await self.data_manager.clear_rule_log(interaction.guild_id, source_id, target_id)
                if success:
                    await interaction.followup.send(f"✅ 已成功清除规则 `{rule}` 的同步日志。", ephemeral=True)
                else:
                    await interaction.followup.send(f"ℹ️ 未找到规则 `{rule}` 的日志，无需操作。", ephemeral=True)
            except ValueError:
                await interaction.followup.send("❌ 无效的规则格式。", ephemeral=True)

        elif action == "export_log":
            try:
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    log_content = f.read()
                log_file = discord.File(io.StringIO(log_content), filename="role_sync_log.json")
                await interaction.followup.send("📄 这是当前的同步日志文件：", file=log_file, ephemeral=True)
            except FileNotFoundError:
                await interaction.followup.send("ℹ️ 日志文件不存在，无需导出。", ephemeral=True)

        elif action == "clear_all":
            success = await self.data_manager.clear_all_logs()
            if success:
                await interaction.followup.send("🗑️ 已成功删除所有同步日志文件。", ephemeral=True)
            else:
                await interaction.followup.send("ℹ️ 日志文件不存在，无需操作。", ephemeral=True)


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(RoleSyncCog(bot))
