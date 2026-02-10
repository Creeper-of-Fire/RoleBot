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
from role_sync.role_sync_data_manager import RoleSyncDataManager, create_rule_key
from utility.auth import is_role_dangerous
from utility.feature_cog import FeatureCog, PanelEntry
from utility.helpers import create_progress_bar
from utility.permison import is_super_admin
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from main import RoleBot
    from core.CoreCog import CoreCog


class RoleSyncCog(FeatureCog, name="RoleSync"):
    """
    管理所有身份组同步相关的逻辑。
    - 实时同步：当用户获得身份组A时，自动授予身份组B。
    - 每日同步：每日检查拥有身份组C的用户，并授予他们身份组D。
    - 手动同步：通过命令为所有拥有身份组E的用户授予身份组F。
    """

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        self.data_manager = RoleSyncDataManager.get_instance(logger=self.logger)
        # 缓存安全的同步规则
        # {'guild_id': [{'source': source_id, 'target': target_id}]}
        self.safe_direct_sync_pairs_cache: Dict[int, List[Dict[str, int]]] = {}
        # {'guild_id': [{'source': source_id, 'target': target_id}]}
        self.safe_daily_sync_pairs_cache: Dict[int, List[Dict[str, int]]] = {}

        self.daily_sync_task.start()

    def cog_unload(self):
        self.daily_sync_task.cancel()

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """此模块没有面向普通用户的前端面板按钮。"""
        pass

    async def update_safe_roles_cache(self):
        """【接口方法】更新本模块的安全身份组缓存。"""
        self.logger.info("RoleSyncCog: 开始更新安全同步身份组缓存...")
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        if not core_cog: return

        # 清空旧缓存
        self.safe_direct_sync_pairs_cache.clear()
        self.safe_daily_sync_pairs_cache.clear()

        for guild_id, sync_cfg in config_data.ROLE_SYNC_CONFIG.items():
            guild = self.bot.get_guild(guild_id)
            if not guild: continue

            # 1. 处理直接同步 (A -> B)
            direct_sync_pairs = sync_cfg.get("direct_sync_pairs", [])
            safe_direct_pairs = []
            for pair in direct_sync_pairs:
                source_id = pair.get("source")
                target_id = pair.get("target")
                if not source_id or not target_id:
                    continue

                target_role = guild.get_role(target_id)
                source_role = guild.get_role(source_id)
                if target_role:
                    core_cog.role_name_cache[target_id] = target_role.name
                    if is_role_dangerous(target_role):
                        self.logger.warning(
                            f"服务器 '{guild.name}' 的直接同步目标组 '{target_role.name}'(ID:{target_id}) 含敏感权限，已排除。")
                    else:
                        self.logger.info(
                            f"服务器 '{guild.name}' 已添加同步规则 '{source_role.name}->{target_role.name}'。")
                        safe_direct_pairs.append(pair)
            if safe_direct_pairs:
                self.safe_direct_sync_pairs_cache[guild_id] = safe_direct_pairs

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
        sync_pairs = self.safe_direct_sync_pairs_cache.get(guild_id)
        if not sync_pairs:
            return  # 该服务器无配置

        added_roles = set(after.roles) - set(before.roles)
        if not added_roles:
            return  # 没有新增身份组

        # 遍历所有新增的身份组，并为每个身份组检查所有可能的同步规则
        for added_role in added_roles:
            added_role_id = added_role.id
            for pair in sync_pairs:
                source_id = pair["source"]
                target_id = pair["target"]

                # 如果新增的身份组是某个规则的源
                if added_role_id == source_id:
                    if self.data_manager.is_synced(guild_id, source_id, target_id, after.id):
                        continue

                    target_role = after.guild.get_role(target_id)
                    if not target_role:
                        continue

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

    sync_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨装饰丨同步", description="用户身份组同步相关指令",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @sync_group.command(name="手动触发每日同步", description="立即执行一次每日身份组C->D同步检查任务。")
    @app_commands.checks.has_permissions(manage_roles=True)
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
        # 获取同步规则列表
        sync_pairs = self.safe_direct_sync_pairs_cache.get(guild_id, [])
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        role_name_cache = core_cog.role_name_cache if core_cog else {}

        # 新增一个“所有规则”的选项
        all_rules_choice = app_commands.Choice(name="[扫描所有规则]", value="all")
        if not current or "所有" in all_rules_choice.name:
            choices.append(all_rules_choice)

        # 【修改】遍历规则列表
        for pair in sync_pairs:
            source_id = pair["source"]
            target_id = pair["target"]
            source_name = role_name_cache.get(source_id, f"ID:{source_id}")
            target_name = role_name_cache.get(target_id, f"ID:{target_id}")
            choice_name = f"{source_name} -> {target_name}"
            rule_key = create_rule_key(source_id, target_id)

            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=rule_key))
        return choices[:25]

    @sync_group.command(name="同步未记录成员", description="扫描缓存中的成员，为符合A->B规则但未被记录的人执行同步（支持超时回退）。")
    @app_commands.describe(rule="[可选] 选择要扫描的特定规则，不选则扫描所有规则。")
    @app_commands.autocomplete(rule=sync_rule_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def sync_unlogged_members(self, interaction: discord.Interaction, rule: Optional[str] = "all"):
        await interaction.response.defer(ephemeral=False)
        guild = interaction.guild

        # 【新增】启动时立即保存用户信息
        user_id = interaction.user.id
        user_mention = interaction.user.mention

        # --- 1. 【修改】解析规则 ---
        sync_pairs = self.safe_direct_sync_pairs_cache.get(guild.id, [])

        if not sync_pairs:
            await interaction.followup.send("❌ 此服务器没有配置任何 A->B 实时同步规则。", ephemeral=True)
            return

        rules_to_scan = []  # 【修改】待扫描的规则现在是一个列表
        scan_title = ""
        if rule == "all":
            rules_to_scan = sync_pairs
            scan_title = "扫描所有规则"
        else:
            # 【修改】从列表中查找匹配的规则
            found_pair = None
            for pair in sync_pairs:
                if create_rule_key(pair["source"], pair["target"]) == rule:
                    found_pair = pair
                    break

            if not found_pair:
                await interaction.followup.send("❌ 所选规则已不存在或已失效。", ephemeral=True)
                return

            rules_to_scan.append(found_pair)
            source_role = guild.get_role(found_pair["source"])
            target_role = guild.get_role(found_pair["target"])
            if not source_role or not target_role:
                await interaction.followup.send("❌ 规则中的身份组已不存在。", ephemeral=True)
                return
            scan_title = f"扫描规则: {source_role.name} -> {target_role.name}"

        # --- 2. 初始化扫描 ---
        embed = discord.Embed(title=f"⏳ {scan_title}", description="正在初始化扫描...", color=discord.Color.blue())
        try:
            await interaction.followup.send(embed=embed, ephemeral=True)
        except discord.NotFound:  # 如果 defer 后用户立刻关闭窗口，followup也可能失败
            self.logger.warning(f"用户 {user_id} 在发送初始进度条前关闭了交互。")
            return

        progress_message = await interaction.original_response()
        fallback_triggered = False

        total_synced, total_logged, total_failed = 0, 0, 0

        # 【修改】计算总扫描人数
        total_members_to_scan = 0
        all_source_members = set()
        for pair in rules_to_scan:
            source_id = pair["source"]
            source_role = guild.get_role(source_id)
            if source_role:
                # 使用集合来自动去重
                all_source_members.update(source_role.members)
        total_members_to_scan = len(all_source_members)

        if total_members_to_scan == 0:
            await progress_message.edit(content="✅ 所有相关源身份组下都没有成员，无需扫描。", embed=None, view=None)
            return

        embed.description = f"准备扫描 **{len(rules_to_scan)}** 条规则，共涉及 **{total_members_to_scan}** 名独立成员。"
        embed.add_field(name="扫描进度", value=create_progress_bar(0, total_members_to_scan), inline=False)
        embed.add_field(name="✅ 同步", value="`0`", inline=True)
        embed.add_field(name="✍️ 补录", value="`0`", inline=True)
        embed.add_field(name="❌ 失败", value="`0`", inline=True)
        await progress_message.edit(embed=embed)

        processed_members_count = 0

        # 用于节流和解决竞态条件的状态变量
        last_update_time = asyncio.get_event_loop().time()
        last_edit_task: asyncio.Task | None = None

        # 遍历去重后的成员集合
        for member in all_source_members:
            processed_members_count += 1
            if member.bot:
                continue

            # 检查此成员符合哪些待扫描的规则
            member_role_ids = {r.id for r in member.roles}
            # 【修改】遍历待扫描的规则列表
            for pair in rules_to_scan:
                source_id = pair["source"]
                target_id = pair["target"]
                if source_id in member_role_ids:
                    if not self.data_manager.is_synced(guild.id, source_id, target_id, member.id):
                        target_role = guild.get_role(target_id)
                        if not target_role: continue

                        if target_id in member_role_ids:
                            await self.data_manager.mark_as_synced(guild.id, source_id, target_id, member.id)
                            total_logged += 1
                        else:
                            try:
                                await member.add_roles(target_role, reason="手动全量同步")
                                await self.data_manager.mark_as_synced(guild.id, source_id, target_id, member.id)
                                total_synced += 1
                            except (discord.Forbidden, discord.HTTPException):
                                total_failed += 1

            # --- 3. 非阻塞节流带回退的进度更新 ---
            current_time = asyncio.get_event_loop().time()
            # 条件：1. 处理了一定数量，且距离上次更新超过一定时间；2. 或者扫描已完成
            if ((processed_members_count % 5 == 0) and (current_time - last_update_time > 0.2)) or (processed_members_count == total_members_to_scan):
                last_update_time = current_time  # 更新时间戳

                embed_copy = embed.copy()
                embed_copy.set_field_at(0, name="扫描进度", value=create_progress_bar(processed_members_count, total_members_to_scan), inline=False)
                embed_copy.set_field_at(1, name="✅ 同步", value=f"`{total_synced}`", inline=True)
                embed_copy.set_field_at(2, name="✍️ 补录", value=f"`{total_logged}`", inline=True)
                embed_copy.set_field_at(3, name="❌ 失败", value=f"`{total_failed}`", inline=True)

                try:
                    # 启动后台任务，并保存对它的引用
                    last_edit_task = asyncio.create_task(progress_message.edit(embed=embed_copy))
                except discord.NotFound:
                    if not fallback_triggered:
                        fallback_triggered = True
                        await interaction.channel.send(f"⏳ {user_mention}，交互已超时...", allowed_mentions=discord.AllowedMentions(users=True))
                        progress_message = await interaction.channel.send(embed=embed_copy)
                    else:
                        last_edit_task = asyncio.create_task(progress_message.edit(embed=embed_copy))

        # --- 4. 【核心优化】发送最终结果，并解决竞态条件 ---
        # 在发送最终结果前，等待最后一个进度更新任务完成
        if last_edit_task:
            try:
                await last_edit_task
            except Exception as e:
                # 即使最后一个任务失败也无所谓，最终结果会覆盖它
                self.logger.warning(f"等待最后一个进度更新任务时发生错误: {e}")

        final_embed = discord.Embed(title=f"✅ {scan_title} 完成", color=discord.Color.green())
        final_embed.description = f"扫描了 **{processed_members_count}** 名独立成员。"
        final_embed.add_field(name="新增同步", value=f"`{total_synced}`人", inline=True)
        final_embed.add_field(name="补录记录", value=f"`{total_logged}`人", inline=True)
        final_embed.add_field(name="同步失败", value=f"`{total_failed}`人", inline=True)
        final_embed.set_footer(text="任务已全部完成。")

        try:
            await progress_message.edit(content=None, embed=final_embed, view=None)
        except discord.NotFound:
            # 如果连最后一次编辑都失败了，就再发一条全新的公开消息
            final_embed.description += "\n(原始进度条消息已失效)"
            await interaction.channel.send(content=f"{user_mention} 你的扫描任务已完成！", embed=final_embed)

    @sync_group.command(name="清理同步记录", description="清理A->B同步规则的记录。")
    @app_commands.describe(
        action="要执行的操作：清除特定规则记录，清除所有记录。",
        rule="[仅清除特定规则时需要] 选择要清除记录的规则。"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="清除特定规则的记录", value="clear_rule"),
        app_commands.Choice(name="清除所有记录（删除文件）", value="clear_all"),
    ])
    @is_super_admin()
    @app_commands.autocomplete(rule=sync_rule_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_sync_record(self, interaction: discord.Interaction, action: str, rule: Optional[str] = None):
        # --- 所有删除操作都需要确认 ---
        # 1. 准备确认消息和视图
        view = ConfirmationView(author=interaction.user)
        confirm_message = ""

        if action == "clear_rule":
            if not rule or rule == 'all':
                await interaction.response.send_message("❌ 请使用 `rule` 参数选择一个**具体**的规则来清除。", ephemeral=True)
                return
            confirm_message = f"你确定要清除规则 `{rule}` 的同步日志吗？\n\n**这将导致该规则下的所有成员在下次扫描时被重新同步。** 此操作不可撤销。"

        elif action == "clear_all":
            confirm_message = "你确定要**清除所有同步记录**吗？\n\n**这将删除 `role_sync_log.json` 文件，所有规则都将重置为初始状态。** 此操作不可撤销！"

        # 2. 发送确认请求
        await interaction.response.send_message(confirm_message, view=view, ephemeral=True)
        view.message = await interaction.original_response()  # 存储消息以便超时后编辑

        # 3. 等待用户响应
        await view.wait()

        # 4. 根据用户的选择执行操作
        if view.value is None:  # 超时
            await interaction.followup.send("⏰ 操作已超时，已自动取消。", ephemeral=True)
        elif view.value:  # 用户点击了确认
            if action == "clear_rule":
                try:
                    source_id_str, target_id_str = rule.split('-')
                    source_id, target_id = int(source_id_str), int(target_id_str)
                    success = await self.data_manager.clear_rule_log(interaction.guild_id, source_id, target_id)
                    if success:
                        await interaction.followup.send(f"✅ 已成功清除规则 `{rule}` 的同步记录。", ephemeral=True)
                    else:
                        await interaction.followup.send(f"ℹ️ 未找到规则 `{rule}` 的记录，无需操作。", ephemeral=True)
                except ValueError:
                    await interaction.followup.send("❌ 无效的规则格式。", ephemeral=True)

            elif action == "clear_all":
                await self.data_manager.clear_all_data()
                await interaction.followup.send("🗑️ 已成功删除所有同步记录文件。", ephemeral=True)
        else:  # 用户点击了取消
            await interaction.followup.send("❌ 操作已取消。", ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(RoleSyncCog(bot))
