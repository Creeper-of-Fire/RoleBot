# honor_system/cog.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import cast, Optional, TYPE_CHECKING, Dict, Literal, List

import discord
from discord import ui, Color, app_commands

import config
import config_data
from core.embed_link.embed_manager import EmbedLinkManager
from utility.feature_cog import FeatureCog
from utility.paginated_view import PaginatedView
from .anniversary_module import HonorAnniversaryModuleCog
from .cup_honor_json_manager import CupHonorJsonManager
from .honor_data_manager import HonorDataManager
from .models import HonorDefinition, UserHonor
from .role_sync_honor_module import RoleClaimHonorModuleCog

if TYPE_CHECKING:
    from main import RoleBot

ShownMode = Literal["equipped", "unequipped_owned", "pure_achievement", "unearned", "unearned_do_not_shown"]


@dataclass
class HonorShownData:
    data: HonorDefinition
    shown_mode: ShownMode


# --- 管理荣誉持有者 ---
class HonorHoldersManageView(PaginatedView):
    def __init__(self, cog: 'HonorCog', guild: discord.Guild, honor_def: HonorDefinition):
        self.cog = cog
        self.guild = guild
        self.honor_def = honor_def

        # 数据提供者：获取所有持有该荣誉的用户记录
        data_provider = lambda: self.cog.data_manager.get_honor_holders(self.honor_def.uuid)

        super().__init__(all_items_provider=data_provider, items_per_page=25, timeout=300)

    async def on_selection_submit(self, interaction: discord.Interaction):
        """处理管理员提交的选择，移除未被选中的成员的荣誉。"""
        await interaction.response.defer(ephemeral=True, thinking=True)

        # 1. 获取本页所有成员的ID
        current_page_holders = self.get_page_items()
        original_ids_on_page = {holder.user_id for holder in current_page_holders}

        # 2. 获取管理员希望保留的成员ID
        kept_user_ids = {int(uid_str) for uid_str in interaction.data.get("values", [])}

        # 3. 计算需要移除荣誉的成员ID
        ids_to_revoke = original_ids_on_page - kept_user_ids
        if not ids_to_revoke:
            await interaction.followup.send("☑️ 在当前页面没有需要移除的成员。", ephemeral=True)
            return

        # 4. 执行移除操作
        # 4.1 从数据库移除
        revoked_db_count = self.cog.data_manager.revoke_honor_from_users(list(ids_to_revoke), self.honor_def.uuid)

        # 4.2 移除身份组
        revoked_role_members = []
        if self.honor_def.role_id:
            role = self.guild.get_role(self.honor_def.role_id)
            if role:
                for user_id in ids_to_revoke:
                    member = self.guild.get_member(user_id)
                    if member and role in member.roles:
                        try:
                            await member.remove_roles(role, reason=f"管理员 {interaction.user} 移除荣誉")
                            revoked_role_members.append(member)
                        except discord.Forbidden:
                            self.cog.logger.warning(f"无法移除成员 {member.display_name} 的身份组 {role.name}，权限不足。")
                        except Exception as e:
                            self.cog.logger.error(f"移除成员 {member.display_name} 身份组时出错: {e}")

        # 5. 发送操作报告
        embed = discord.Embed(
            title=f"荣誉移除操作完成",
            description=f"已处理对荣誉 **{self.honor_def.name}** 持有者的更改。",
            color=Color.green()
        )
        embed.add_field(name="数据库记录移除数量", value=f"`{revoked_db_count}` 条", inline=False)
        if revoked_role_members:
            mentions = [m.mention for m in revoked_role_members]
            embed.add_field(name="成功移除身份组的成员", value=" ".join(mentions), inline=False)
        else:
            embed.add_field(name="身份组移除情况", value="无或操作失败。", inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

        # 6. 更新视图以反映最新状态
        await self.update_view(interaction)

    async def _rebuild_view(self):
        """构建或重建视图界面。"""
        self.clear_items()

        # 获取当前页的持有者记录
        current_page_holders: List[UserHonor] = self.get_page_items()

        # 获取对应的成员对象，并过滤掉已离开服务器的
        current_members = []
        for holder in current_page_holders:
            member = self.guild.get_member(holder.user_id)
            if member:
                current_members.append(member)

        # --- 创建Embed ---
        embed = discord.Embed(
            title=f"管理荣誉【{self.honor_def.name}】的持有者",
            color=Color.blue()
        )
        description = (
            f"**总持有者**: `{len(self.all_items)}` 人\n\n"
            "下方列表显示了 **当前页** 的成员。取消勾选并点击选择框外部，即可移除他们的此项荣誉及其关联身份组。\n"
            "**注意：此操作不可逆！每次提交仅处理当前页面的成员。**"
        )
        if not current_members:
            description += "\n\n*本页无成员显示（可能成员已离开服务器）。*"

        embed.description = description
        embed.set_footer(text=f"第 {self.page + 1}/{self.total_pages} 页")
        self.embed = embed

        # --- 创建Select Menu ---
        if current_members:
            options = [
                discord.SelectOption(
                    label=f"{member.name} ({member.id})",
                    description=f"Display Name: {member.display_name}",
                    value=str(member.id),
                    default=True  # 默认全部勾选
                )
                for member in current_members
            ]

            select_menu = ui.Select(
                placeholder="选择要保留此荣誉的成员（默认全选）...",
                min_values=0,
                max_values=len(options),
                options=options,
                custom_id="honor_holder_select",
                row=0
            )
            select_menu.callback = self.on_selection_submit
            self.add_item(select_menu)

        # 添加翻页按钮
        self._add_pagination_buttons(row=1)


# --- 视图定义 ---
class HonorManageView(PaginatedView):
    def __init__(self, cog: 'HonorCog', member: discord.Member, guild: discord.Guild):
        self.cog = cog
        self.member = member
        self.guild = guild
        data_provider = lambda: self.create_honor_shown_list()
        super().__init__(
            all_items_provider=data_provider,
            items_per_page=10,
            timeout=180
        )
        self.message: Optional[discord.Message] = None

    # --- 下拉菜单的交互逻辑 (带详细日志的调试版) ---
    async def on_honor_select(self, interaction: discord.Interaction):
        """
        处理与分页同步的多选荣誉下拉框的交互。
        此版本增加了详细的调试日志，以追踪状态计算过程。
        """
        await interaction.response.defer(ephemeral=True)

        # --- 0. 开始调试 ---
        self.cog.logger.debug("--- [荣誉选择] Debug Start ---")

        # --- 1. 获取所有必要的数据 ---
        selections_on_this_page = set(interaction.data.get("values", []))
        self.cog.logger.debug(f"{'1a. 用户本次提交的选择 (selections_on_this_page):':<50} {selections_on_this_page}")

        all_wearable_honors = [
            uh.definition for uh in self.cog.data_manager.get_user_honors(self.member.id)
            if uh.definition.role_id is not None
        ]
        if not all_wearable_honors:
            await interaction.followup.send("你当前没有可佩戴的荣誉。", ephemeral=True)
            self.cog.logger.debug("--- [荣誉选择] Debug End: 用户无荣誉 ---")
            return

        wearable_honor_map = {h.uuid: h for h in all_wearable_honors}

        member_role_ids = {role.id for role in self.member.roles}
        currently_equipped_uuids = {
            h.uuid for h in all_wearable_honors if h.role_id in member_role_ids
        }
        currently_equipped_role_ids = {
            wearable_honor_map[uuid].role_id for uuid in currently_equipped_uuids
        }
        self.cog.logger.debug(f"{'1b. 当前实际佩戴的荣誉UUID (currently_equipped_uuids):':<50} {currently_equipped_uuids}")
        self.cog.logger.debug(f"{'1c. 当前实际佩戴的角色ID (currently_equipped_role_ids):':<50} {currently_equipped_role_ids}")

        # --- 2. 采用更稳健的方式构建最终的“期望状态” ---

        # a. 获取当前页面上所有可操作荣誉的UUID
        page_items = self.get_page_items()
        # 额外日志：看看 get_page_items() 到底返回了什么
        self.cog.logger.debug(f"{'2a. 原始页面项目 (get_page_items):':<50} {[item.__class__.__name__ for item in page_items]}")

        uuids_on_this_page = {
            item.data.uuid for item in page_items if hasattr(item, 'data') and hasattr(item.data, 'uuid')
        }
        self.cog.logger.debug(f"{'2b. 计算出的本页荣誉UUID (uuids_on_this_page):':<50} {uuids_on_this_page}")

        # b. 从当前已佩戴的荣誉中，排除掉本次页面可以操作的荣誉
        equipped_uuids_preserved = currently_equipped_uuids - uuids_on_this_page
        self.cog.logger.debug(f"{'2c. 需要保留的非本页荣誉 (equipped_uuids_preserved):':<50} {equipped_uuids_preserved}")

        # c. 将保留下来的其他页面的荣誉，与当前页面的新选择合并
        final_desired_uuids = equipped_uuids_preserved.union(selections_on_this_page)
        self.cog.logger.debug(f"{'2d. 最终期望佩戴的荣誉UUID (final_desired_uuids):':<50} {final_desired_uuids}")

        # --- 3. 计算需要添加和移除的角色 ---
        final_desired_role_ids = {
            wearable_honor_map[uuid].role_id
            for uuid in final_desired_uuids if uuid in wearable_honor_map
        }
        self.cog.logger.debug(f"{'3a. 最终期望佩戴的角色ID (final_desired_role_ids):':<50} {final_desired_role_ids}")

        roles_to_add_ids = final_desired_role_ids - currently_equipped_role_ids
        roles_to_remove_ids = currently_equipped_role_ids - final_desired_role_ids

        self.cog.logger.debug(f"{'3b. 需要添加的角色ID (roles_to_add_ids):':<50} {roles_to_add_ids}")
        self.cog.logger.debug(f"{'3c. 需要移除的角色ID (roles_to_remove_ids):':<50} {roles_to_remove_ids}")

        roles_to_add = [self.guild.get_role(rid) for rid in roles_to_add_ids if rid]
        roles_to_remove = [self.guild.get_role(rid) for rid in roles_to_remove_ids if rid]

        roles_to_add = [r for r in roles_to_add if r]
        roles_to_remove = [r for r in roles_to_remove if r]

        if not roles_to_add and not roles_to_remove:
            await interaction.followup.send("☑️ 你的荣誉佩戴状态没有变化。", ephemeral=True)
            self.cog.logger.debug("--- [荣誉选择] Debug End: 状态无变化 ---")
            return

        # --- 4. 执行操作并发送反馈 ---
        try:
            self.cog.logger.debug(f"准备添加角色: {[r.name for r in roles_to_add]}")
            self.cog.logger.debug(f"准备移除角色: {[r.name for r in roles_to_remove]}")
            if roles_to_add:
                await self.member.add_roles(*roles_to_add, reason="用户佩戴荣誉")
            if roles_to_remove:
                await self.member.remove_roles(*roles_to_remove, reason="用户卸下荣誉")

            response_lines = ["✅ **荣誉身份组已更新！**"]
            if roles_to_add:
                response_lines.append(f"**新增佩戴**: {', '.join([r.mention for r in roles_to_add])}")
            if roles_to_remove:
                response_lines.append(f"**卸下荣誉**: {', '.join([r.mention for r in roles_to_remove])}")

            await interaction.followup.send("\n".join(response_lines), ephemeral=True)

        except discord.Forbidden:
            self.cog.logger.error("权限不足，无法修改角色。")
            await interaction.followup.send(
                "❌ **操作失败！**\n我没有足够的权限来为你添加/移除身份组。请确保我的机器人角色在身份组列表中的位置高于所有荣誉身份组。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"批量佩戴/卸下荣誉时发生未知错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)

        self.cog.logger.debug("--- [荣誉选择] Debug End: 操作完成 ---")

        # --- 5. 更新视图以反映最新状态 ---
        fresh_member = self.guild.get_member(self.member.id) or await self.guild.fetch_member(self.member.id)
        if fresh_member:
            self.member = fresh_member
        await self.update_view(interaction)

    # --- 视图重建逻辑 ---
    async def _rebuild_view(self):
        self.clear_items()

        current_page_honor_data = self.get_page_items()
        main_honor_embed = self.create_honor_embed(self.member, current_page_honor_data)
        self.embed = [main_honor_embed, self.cog.guide_manager.embed]

        self._add_pagination_buttons(row=2)

        if self.cog.guide_manager.url:
            self.add_item(ui.Button(
                label=f"跳转到 “{self.cog.guide_manager.embed.title}”",
                style=discord.ButtonStyle.link,
                url=self.cog.guide_manager.url,
                row=1
            ))

        # --- Select Menu 构建逻辑 ---
        options = []
        # 只遍历当前页面的项目来生成选项
        for honor_data in current_page_honor_data:
            # 只为可佩戴的荣誉（已佩戴或未佩戴但拥有）创建选项
            if honor_data.shown_mode in ["equipped", "unequipped_owned"]:
                honor_def = honor_data.data
                is_equipped_now = honor_data.shown_mode == "equipped"

                options.append(discord.SelectOption(
                    label=honor_def.name,
                    description=honor_def.description[:90],
                    value=honor_def.uuid,
                    emoji="✅" if is_equipped_now else "⬜",
                    default=is_equipped_now  # 关键：设置默认选中状态
                ))

        if not options:
            return  # 如果当前页没有任何可佩戴的荣誉，则不显示下拉框

        honor_select = ui.Select(
            placeholder="选择你想佩戴的荣誉身份组...",
            min_values=0,
            max_values=len(options),
            options=options,
            custom_id="honor_select",
            row=0
        )
        honor_select.callback = self.on_honor_select
        self.add_item(honor_select)

    def create_honor_shown_list(self) -> List[HonorShownData]:
        guild = self.guild
        member = self.member
        honor_shown_list: List[HonorShownData] = []
        guild_config = config_data.HONOR_CONFIG.get(guild.id, {})
        all_config_definitions_raw = guild_config.get("definitions", [])
        config_uuid_order_map = {
            definition['uuid']: index
            for index, definition in enumerate(all_config_definitions_raw)
        }
        all_definitions_from_db = self.cog.data_manager.get_all_honor_definitions(guild.id)
        user_honor_instances = self.cog.data_manager.get_user_honors(member.id)
        member_role_ids = {role.id for role in member.roles}
        owned_honor_definitions_map = {uh.honor_uuid: uh.definition for uh in user_honor_instances}

        for definition in all_definitions_from_db:
            if definition.uuid in owned_honor_definitions_map:
                # 1. 首先，最直接地检查用户是否已佩戴该身份组。
                #    这个判断同时隐式地确认了 role_id 存在且有效。
                if definition.role_id and definition.role_id in member_role_ids:
                    honor_shown_list.append(HonorShownData(definition, "equipped"))

                # 2. 如果用户没有佩戴，我们再检查这个身份组是否还存在于服务器上，
                #    以判断它是否是一个“可佩戴”的荣誉。
                elif definition.role_id and guild.get_role(definition.role_id):
                    honor_shown_list.append(HonorShownData(definition, "unequipped_owned"))

                # 3. 如果以上条件都不满足（即荣誉没有关联 role_id，或者关联的 role_id 已失效），
                #    那么它就是一个纯粹的成就。
                else:
                    honor_shown_list.append(HonorShownData(definition, "pure_achievement"))
            else:
                if not definition.hidden_until_earned:
                    honor_shown_list.append(HonorShownData(definition, "unearned"))

        def sort_key(honor_data: HonorShownData):
            order = {
                "equipped": 0,
                "unequipped_owned": 1,
                "pure_achievement": 2,
                "unearned": 3,
            }
            original_order_index = config_uuid_order_map.get(honor_data.data.uuid, 999)
            return order.get(honor_data.shown_mode, 99), original_order_index

        honor_shown_list.sort(key=sort_key)
        return honor_shown_list

    def create_honor_embed(self, member: discord.Member, current_page_honor_data: List[HonorShownData]) -> discord.Embed:
        embed = discord.Embed(title=f"{member.display_name}的荣誉墙", color=member.color)
        if member.display_avatar:
            embed.set_thumbnail(url=member.display_avatar.url)

        equipped_honors_lines, unequipped_owned_honors_lines = [], []
        pure_achievement_honors_lines, unearned_honors_lines = [], []

        for honor_data in current_page_honor_data:
            definition = honor_data.data
            # 根据荣誉的分类 (shown_mode) 来决定如何显示文本，而不是直接检查 role_id
            if honor_data.shown_mode in ["equipped", "unequipped_owned"]:
                # 只有当它被正确分类为可佩戴时，才显示身份组提及
                honor_line_text = f"<@&{definition.role_id}>\n*└ {definition.description}*"
            else:
                # 其他情况（纯粹成就、未获得）都只显示名称
                honor_line_text = f"**{definition.name}**\n*└ {definition.description}*"

            if honor_data.shown_mode == "equipped":
                equipped_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "unequipped_owned":
                unequipped_owned_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "pure_achievement":
                pure_achievement_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "unearned":
                unearned_honors_lines.append(honor_line_text)

        user_honor_count = sum(1 for item in self.all_items if item.shown_mode != "unearned")
        all_visible_honors_count = len(self.all_items)
        public_unearned_honors_count = all_visible_honors_count - user_honor_count

        if not user_honor_count and not public_unearned_honors_count:
            embed.description = "目前没有可用的荣誉定义。请联系管理员添加。"
        elif not user_honor_count and public_unearned_honors_count:
            embed.description = "你还没有获得任何荣誉哦！查看下方待解锁荣誉，多多参与社区活动吧！"
        elif user_honor_count == all_visible_honors_count:
            embed.description = "🎉 你已经解锁了所有可用的（或可见的）荣誉！恭喜你！"
        else:
            embed.description = "你已获得部分荣誉。请查看下方已佩戴、未佩戴的荣誉，或探索待解锁的更多荣誉。"

        if equipped_honors_lines:
            embed.add_field(name="✅ 已佩戴荣誉", value="\n\n".join(equipped_honors_lines), inline=False)
        if unequipped_owned_honors_lines:
            embed.add_field(name="☑️ 未佩戴荣誉 (可佩戴身份组)", value="\n\n".join(unequipped_owned_honors_lines), inline=False)
        if pure_achievement_honors_lines:
            embed.add_field(name="✨ 纯粹成就荣誉 (无身份组)", value="\n\n".join(pure_achievement_honors_lines), inline=False)
        if unearned_honors_lines:
            embed.add_field(name="💡 待解锁荣誉", value="\n\n".join(unearned_honors_lines), inline=False)

        if not (equipped_honors_lines or unequipped_owned_honors_lines or pure_achievement_honors_lines or unearned_honors_lines):
            embed.add_field(name="\u200b", value="*本页暂无荣誉显示。*", inline=False)

        embed.set_footer(text=f"第 {self.page + 1}/{self.total_pages} 页 | 使用下方选择器佩戴/卸下本页显示的荣誉。")
        return embed


# --- 主Cog ---
class HonorCog(FeatureCog, name="Honor"):
    """管理荣誉系统"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)  # 调用父类 (FeatureCog) 的构造函数
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.cup_honor_manager = CupHonorJsonManager.get_instance(logger=self.logger)
        self.running_backfill_tasks: Dict[int, asyncio.Task] = {}
        # 安全缓存，用于存储此模块管理的所有身份组ID
        self.safe_honor_role_ids: set[int] = set()

        self.bot.loop.create_task(self.synchronize_all_honor_definitions())

        self.guide_manager = EmbedLinkManager.get_or_create(
            key="honor_celebrate_guide",
            bot=self.bot,
            default_embed=discord.Embed(
                title="🎊 当前进行中的荣誉获取活动",
                description="管理员尚未配置，或正在加载中。",
                color=Color.orange()
            )
        )

    # --- FeatureCog 接口实现 ---
    async def update_safe_roles_cache(self):
        """
        [接口实现] 从荣誉定义中更新此模块管理的安全身份组缓存。
        """
        self.logger.info(f"模块 '{self.qualified_name}' 开始更新安全身份组缓存...")

        new_cache = set()

        # 从数据库中获取所有荣誉定义
        all_honor_defs = []
        with self.data_manager.get_db() as db:
            all_honor_defs = db.query(HonorDefinition).filter(HonorDefinition.is_archived == False).all()

        if not all_honor_defs:
            self.logger.info(f"模块 '{self.qualified_name}' 没有找到任何荣誉定义。")
            self.safe_honor_role_ids = new_cache
            return

        for honor_def in all_honor_defs:
            if honor_def.role_id:
                new_cache.add(honor_def.role_id)

        self.safe_honor_role_ids = new_cache
        self.logger.info(f"模块 '{self.qualified_name}' 安全缓存更新完毕，共加载 {len(self.safe_honor_role_ids)} 个身份组。")

    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        """
        [接口实现] 返回一个用于主面板的 "我的荣誉墙" 按钮。
        """

        async def honor_panel_callback(interaction: discord.Interaction):
            # 这是原 /荣誉面板 命令的所有逻辑
            await interaction.response.defer(ephemeral=True)
            member = cast(discord.Member, interaction.user)
            guild = cast(discord.Guild, interaction.guild)

            # --- 调用子模块进行检查 ---
            anniversary_cog: Optional[HonorAnniversaryModuleCog] = self.bot.get_cog("HonorAnniversaryModule")
            if anniversary_cog:
                # 调用子模块执行其独立的检查逻辑
                await anniversary_cog.check_and_grant_anniversary_honor(member, guild)
            else:
                self.logger.warning("无法找到 HonorAnniversaryModule 来检查周年荣誉。")

            # 调用新模块，检查基于身份组的荣誉
            role_claim_cog: Optional[RoleClaimHonorModuleCog] = self.bot.get_cog("RoleClaimHonorModule")
            if role_claim_cog:
                await role_claim_cog.check_and_grant_role_sync_honor(member, guild)
            else:
                self.logger.warning("无法找到 RoleClaimHonorModule 来检查基于身份组的荣誉。")

            view = HonorManageView(self, member, guild)

            await view.start(interaction, ephemeral=True)

        honor_button = ui.Button(
            label="我的荣誉墙",
            style=discord.ButtonStyle.secondary,
            emoji="🏆",
            custom_id="honor_cog:show_honor_panel"
        )
        honor_button.callback = honor_panel_callback

        return [honor_button]

    async def synchronize_all_honor_definitions(self):
        await self.bot.wait_until_ready()
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")

        # 1. 从配置文件收集普通荣誉UUID
        all_config_uuids = set()
        for guild_id, guild_config in config_data.HONOR_CONFIG.items():
            for honor_def in guild_config.get("definitions", []):
                all_config_uuids.add(honor_def['uuid'])

        # 2. 从JSON文件收集杯赛荣誉UUID
        self.cup_honor_manager.load_data()  # 确保加载最新数据
        all_cup_honor_uuids = {str(honor.uuid) for honor in self.cup_honor_manager.get_all_cup_honors()}

        # 3. 合并所有合法的、不应被归档的荣誉UUID
        all_legitimate_uuids = all_config_uuids.union(all_cup_honor_uuids)

        # 2. 遍历配置，处理创建和更新
        with self.data_manager.get_db() as db:
            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                for config_def in guild_config.get("definitions", []):
                    # --- 冲突处理逻辑 ---
                    # 查找是否存在名称相同但 UUID 不同的旧定义
                    conflicting_old_def = db.query(HonorDefinition).filter(
                        HonorDefinition.guild_id == guild_id,
                        HonorDefinition.name == config_def['name'],
                        HonorDefinition.uuid != config_def['uuid']
                    ).one_or_none()

                    if conflicting_old_def:
                        # 发现冲突，归档旧定义
                        self.logger.warning(
                            f"发现名称冲突: 荣誉 '{config_def['name']}' 已存在 (UUID: {conflicting_old_def.uuid})，"
                            f"但新配置使用 UUID: {config_def['uuid']}。将归档旧定义。"
                        )
                        conflicting_old_def.is_archived = True
                        # 可选：重命名以彻底解决 UNIQUE 约束，即使在归档状态下
                        conflicting_old_def.name = f"{conflicting_old_def.name}_archived_{int(time.time())}"
                        db.add(conflicting_old_def)
                        db.flush()  # 立即将更改写入会话，以便后续操作不会再次冲突

                    # --- 原有的同步逻辑 ---
                    # 查找当前配置项对应的数据库记录 (通过 UUID)
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_def['uuid']).one_or_none()

                    if db_def:
                        # 记录存在，更新它
                        db_def.name = config_def['name']
                        db_def.description = config_def['description']
                        db_def.role_id = config_def.get('role_id', None)
                        db_def.icon_url = config_def.get('icon_url', None)
                        db_def.guild_id = guild_id
                        db_def.hidden_until_earned = config_def.get('hidden_until_earned', True)  # 确保有默认值
                        db_def.is_archived = False  # 确保它处于激活状态
                    else:
                        # 记录不存在，创建它
                        new_def = HonorDefinition(
                            uuid=config_def['uuid'],
                            guild_id=guild_id,
                            name=config_def['name'],
                            description=config_def['description'],
                            role_id=config_def.get('role_id', None),
                            icon_url=config_def.get('icon_url', None),
                            hidden_until_earned=config_def.get('hidden_until_earned', True),  # 确保有默认值
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_def['name']}")

            # 5. 归档操作：只归档那些既不在config也不在cup_honor.json中的荣誉
            db_uuids_to_check = db.query(HonorDefinition.uuid).filter(HonorDefinition.is_archived == False).all()
            db_uuids_set = {uuid_tuple[0] for uuid_tuple in db_uuids_to_check}

            uuids_to_archive = db_uuids_set - all_legitimate_uuids

            if uuids_to_archive:
                self.logger.warning(f"发现 {len(uuids_to_archive)} 个需要归档的荣誉...")
                # 使用 in_ 操作批量更新
                db.query(HonorDefinition).filter(HonorDefinition.uuid.in_(uuids_to_archive)).update({"is_archived": True}, synchronize_session=False)

            # 最终提交所有更改
            db.commit()

        self.logger.info("HonorCog: 荣誉定义同步完成。")

    # --- 管理员指令组 ---
    honor_admin_group = app_commands.Group(
        name="荣誉头衔丨核心",
        description="管理荣誉头衔",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    async def honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """为所有荣誉UUID参数提供自动补全选项。"""
        all_defs = self.data_manager.get_all_honor_definitions(interaction.guild_id)

        choices = []
        for honor_def in all_defs:
            if honor_def.is_archived:
                continue

            choice_name = f"{honor_def.name} ({honor_def.uuid[:8]})"
            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=honor_def.uuid))

        return choices[:25]

    @honor_admin_group.command(name="管理持有者", description="查看并移除特定荣誉的持有者。")
    @app_commands.describe(honor_uuid="选择要管理的荣誉头衔")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_holders(self, interaction: discord.Interaction, honor_uuid: str):
        """启动一个视图，用于管理特定荣誉的持有者。"""
        await interaction.response.defer(ephemeral=True)
        guild = cast(discord.Guild, interaction.guild)

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ 找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        view = HonorHoldersManageView(self, guild, honor_def)
        await view.start(interaction, ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
