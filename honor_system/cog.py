# honor_system/cog.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import cast, Optional, TYPE_CHECKING, Dict, Literal, List

import discord
from discord import ui, Color
from discord.ext import commands

import config_data
from core.embed_link.embed_manager import EmbedLinkManager
from utility.feature_cog import FeatureCog
from utility.paginated_view import PaginatedView
from .anniversary_module import HonorAnniversaryModuleCog
from .data_manager import HonorDataManager
from .models import HonorDefinition

if TYPE_CHECKING:
    from main import RoleBot

ShownMode = Literal["equipped", "unequipped_owned", "pure_achievement", "unearned", "unearned_do_not_shown"]

@dataclass
class HonorShownData:
    data: HonorDefinition
    shown_mode: ShownMode


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

    async def on_honor_select(self, interaction: discord.Interaction):
        """
        处理多选荣誉下拉框的交互。
        通过比较用户提交的“期望状态”和当前的“实际状态”，来计算需要添加和移除的角色。
        """
        await interaction.response.defer(ephemeral=True)

        # 1. 获取用户提交的“期望状态”（即所有被选中的荣誉UUID）
        desired_honor_uuids = set(interaction.data.get("values", []))

        # 2. 获取当前用户所有可佩戴的荣誉和其实际佩戴的荣誉
        all_wearable_honors = [
            uh.definition for uh in self.cog.data_manager.get_user_honors(self.member.id)
            if uh.definition.role_id is not None
        ]

        if not all_wearable_honors:
            await interaction.followup.send("你当前没有可佩戴的荣誉。", ephemeral=True)
            return

        wearable_honor_map = {h.uuid: h for h in all_wearable_honors}

        member_role_ids = {role.id for role in self.member.roles}
        # 计算出当前实际佩戴的、且由本系统管理的荣誉角色ID
        current_role_ids = {
            h.role_id for h in all_wearable_honors if h.role_id in member_role_ids
        }

        # 3. 计算出用户期望佩戴的荣誉角色ID
        desired_role_ids = {
            wearable_honor_map[uuid].role_id
            for uuid in desired_honor_uuids if uuid in wearable_honor_map and wearable_honor_map[uuid].role_id is not None
        }

        # 4. 通过集合运算，计算出需要添加和移除的角色
        roles_to_add_ids = desired_role_ids - current_role_ids
        roles_to_remove_ids = current_role_ids - desired_role_ids

        roles_to_add = [self.guild.get_role(rid) for rid in roles_to_add_ids]
        roles_to_remove = [self.guild.get_role(rid) for rid in roles_to_remove_ids]

        # 过滤掉已不存在的角色
        roles_to_add = [r for r in roles_to_add if r is not None]
        roles_to_remove = [r for r in roles_to_remove if r is not None]

        if not roles_to_add and not roles_to_remove:
            await interaction.followup.send("☑️ 你的荣誉佩戴状态没有变化。", ephemeral=True)
            return

        # 5. 执行操作并发送反馈
        try:
            if roles_to_add:
                await self.member.add_roles(*roles_to_add, reason="用户佩戴荣誉")
            if roles_to_remove:
                await self.member.remove_roles(*roles_to_remove, reason="用户卸下荣誉")

            # 构建详细的反馈消息
            response_lines = ["✅ **荣誉身份组已更新！**"]
            if roles_to_add:
                response_lines.append(f"**新增佩戴**: {', '.join([r.mention for r in roles_to_add])}")
            if roles_to_remove:
                response_lines.append(f"**卸下荣誉**: {', '.join([r.mention for r in roles_to_remove])}")

            await interaction.followup.send("\n".join(response_lines), ephemeral=True)

        except discord.Forbidden:
            await interaction.followup.send(
                "❌ **操作失败！**\n我没有足够的权限来为你添加/移除身份组。请确保我的机器人角色在身份组列表中的位置高于所有荣誉身份组。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"批量佩戴/卸下荣誉时发生错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)

        # 6. 更新视图以反映最新状态
        fresh_member = self.guild.get_member(self.member.id) or await self.guild.fetch_member(self.member.id)
        if fresh_member:
            self.member = fresh_member
        await self.update_view(interaction)

    async def _rebuild_view(self):
        self.clear_items()

        current_page_honor_data = self.get_page_items()
        main_honor_embed = self.create_honor_embed(self.member, current_page_honor_data)
        self.embed = [main_honor_embed, self.cog.guide_manager.embed]

        self._add_pagination_buttons(row=2)  # 将翻页按钮下移一行，给选择器和指南按钮留出空间

        if self.cog.guide_manager.url:
            self.add_item(ui.Button(
                label=f"跳转到 “{self.cog.guide_manager.embed.title}”",
                style=discord.ButtonStyle.link,
                url=self.cog.guide_manager.url,
                row=1
            ))

        # --- Select Menu 构建逻辑 ---
        user_honors_earned = self.cog.data_manager.get_user_honors(self.member.id)
        wearable_honors = [uh for uh in user_honors_earned if uh.definition.role_id is not None]

        if not wearable_honors:
            return  # 如果没有任何可佩戴的荣誉，则不显示下拉框

        member_role_ids = {role.id for role in self.member.roles}
        options = []
        for uh_instance in wearable_honors:
            honor_def = uh_instance.definition
            is_equipped_now = honor_def.role_id in member_role_ids

            options.append(discord.SelectOption(
                label=honor_def.name,
                description=honor_def.description[:90],  # 描述可以长一点
                value=honor_def.uuid,
                emoji="✅" if is_equipped_now else "⬜",
                default=is_equipped_now  # <-- 关键：设置默认选中状态
            ))

        if not options:
            return

        honor_select = ui.Select(
            placeholder="选择你想佩戴的荣誉身份组...",
            min_values=0,  # 允许用户取消所有选择
            max_values=len(options),  # 最多可选所有项
            options=options,
            custom_id="honor_select",  # 最好用新的custom_id以避免冲突
            row=0
        )
        honor_select.callback = self.on_honor_select
        self.add_item(honor_select)

    def create_honor_shown_list(self) -> List[HonorShownData]:
        guild = self.guild
        member = self.member
        honor_shown_list: List[HonorShownData] = []

        # --- 获取有序的荣誉定义列表 ---
        # data_manager 返回的列表顺序依赖于数据库查询结果，不一定是我们想要的。
        # 我们需要从 config_data 直接获取原始定义的顺序。
        guild_config = config_data.HONOR_CONFIG.get(guild.id, {})
        all_config_definitions_raw = guild_config.get("definitions", [])

        # 为了能快速查找，创建一个 UUID 到原始顺序索引的映射
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
                if definition.role_id is not None:
                    if definition.role_id in member_role_ids:
                        honor_shown_list.append(HonorShownData(definition, "equipped"))
                    else:
                        honor_shown_list.append(HonorShownData(definition, "unequipped_owned"))
                else:
                    honor_shown_list.append(HonorShownData(definition, "pure_achievement"))
            else:
                if not definition.hidden_until_earned:
                    honor_shown_list.append(HonorShownData(definition, "unearned"))

        # 【排序逻辑】对列表进行排序，确保显示顺序一致
        def sort_key(honor_data: HonorShownData):
            """定义排序的规则。"""
            # 1. 定义显示模式的优先级顺序
            order = {
                "equipped": 0,
                "unequipped_owned": 1,
                "pure_achievement": 2,
                "unearned": 3,
            }

            # --- 第二排序标准 ---
            # 从我们创建的映射中获取该荣誉在配置文件中的原始索引。
            # 如果万一找不到（理论上不应该发生），给一个很大的默认值，让它排在最后。
            original_order_index = config_uuid_order_map.get(honor_data.data.uuid, 999)

            # 2. 返回一个元组，Python 会依次比较元组中的元素
            #    首先按荣誉类型（已佩戴 > 未佩戴 > ...）排序
            #    如果类型相同，则按其在配置文件中的原始顺序排序
            return order.get(honor_data.shown_mode, 99), original_order_index

        honor_shown_list.sort(key=sort_key)

        return honor_shown_list

    # --- 荣誉展示与管理 ---
    def create_honor_embed(self, member: discord.Member, current_page_honor_data: List[HonorShownData]) -> discord.Embed:
        """
        根据当前页面需要显示的 HonorShownData 列表，创建并返回一个 Embed。
        """
        embed = discord.Embed(title=f"{member.display_name}的荣誉墙", color=member.color)
        if member.display_avatar:
            embed.set_thumbnail(url=member.display_avatar.url)

        # 分类当前页数据
        equipped_honors_lines, unequipped_owned_honors_lines = [], []
        pure_achievement_honors_lines, unearned_honors_lines = [], []

        for honor_data in current_page_honor_data:
            definition = honor_data.data
            honor_line_text = f"**{definition.name}**\n*└ {definition.description}*"
            if definition.role_id is not None:
                honor_line_text = f"<@&{definition.role_id}>\n*└ {definition.description}*"

            if honor_data.shown_mode == "equipped":
                equipped_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "unequipped_owned":
                unequipped_owned_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "pure_achievement":
                pure_achievement_honors_lines.append(honor_line_text)
            elif honor_data.shown_mode == "unearned":
                unearned_honors_lines.append(honor_line_text)

        # 总体描述逻辑
        # self.all_items 此时已是最新数据，可以直接使用
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

        # 添加字段
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

        embed.set_footer(text=f"第 {self.page + 1}/{self.total_pages} 页 | 佩戴/卸下荣誉需使用下方的下拉选择器进行操作。")
        return embed


# --- 主Cog ---
class HonorCog(FeatureCog, name="Honor"):
    """管理荣誉系统"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)  # 调用父类 (FeatureCog) 的构造函数
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
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

            view = HonorManageView(self, member, guild)

            await view.start(interaction, ephemeral=True)

        honor_button = ui.Button(
            label="我的荣誉墙（临时测试）",
            style=discord.ButtonStyle.secondary,
            emoji="🏆",
            custom_id="honor_cog:show_honor_panel"
        )
        honor_button.callback = honor_panel_callback

        return [honor_button]

    async def synchronize_all_honor_definitions(self):
        await self.bot.wait_until_ready()
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")
        all_config_uuids = set()
        for guild_id, guild_config in config_data.HONOR_CONFIG.items():
            for honor_def in guild_config.get("definitions", []):
                all_config_uuids.add(honor_def['uuid'])
        with self.data_manager.get_db() as db:
            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                for config_def in guild_config.get("definitions", []):
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_def['uuid']).one_or_none()
                    if db_def:
                        db_def.name = config_def['name']
                        db_def.description = config_def['description']
                        db_def.role_id = config_def.get('role_id')
                        db_def.icon_url = config_def.get('icon_url')
                        db_def.guild_id = guild_id
                        db_def.hidden_until_earned = config_def.get('hidden_until_earned')
                        db_def.is_archived = False
                    else:
                        new_def = HonorDefinition(
                            uuid=config_def['uuid'],
                            guild_id=guild_id,
                            name=config_def['name'],
                            description=config_def['description'],
                            role_id=config_def.get('role_id'),
                            icon_url=config_def.get('icon_url'),
                            hidden_until_earned=config_def.get('hidden_until_earned'),
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_def['name']}")
            db_uuids_to_check = db.query(HonorDefinition.uuid).filter(HonorDefinition.is_archived == False).all()
            db_uuids_set = {uuid_tuple[0] for uuid_tuple in db_uuids_to_check}
            uuids_to_archive = db_uuids_set - all_config_uuids
            if uuids_to_archive:
                self.logger.warning(f"发现 {len(uuids_to_archive)} 个需要归档的荣誉...")
                db.query(HonorDefinition).filter(HonorDefinition.uuid.in_(uuids_to_archive)).update({"is_archived": True})
            db.commit()
        self.logger.info("HonorCog: 荣誉定义同步完成。")


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
