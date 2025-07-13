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
        await interaction.response.defer(ephemeral=True)
        selected_honor_uuid = interaction.data["values"][0]

        selected_honor_def = next(
            (hd for hd in self.cog.data_manager.get_all_honor_definitions(self.guild.id)
             if hd.uuid == selected_honor_uuid),
            None
        )

        if not selected_honor_def or selected_honor_def.role_id is None:
            await interaction.followup.send("❌ 选择的荣誉无效或未关联身份组。", ephemeral=True)
            await self.update_view(interaction)
            return

        role_id_int: int = cast(int, selected_honor_def.role_id)
        target_role = self.guild.get_role(role_id_int)
        if not target_role:
            await interaction.followup.send(f"⚠️ 荣誉 **{selected_honor_def.name}** 关联的身份组(ID:{selected_honor_def.role_id})已不存在。", ephemeral=True)
            await self.update_view(interaction)
            return

        member_has_role = target_role in self.member.roles
        try:
            if member_has_role:
                await self.member.remove_roles(target_role, reason=f"用户卸下荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"☑️ 已卸下荣誉 **{selected_honor_def.name}** 并移除身份组。", ephemeral=True)
            else:
                await self.member.add_roles(target_role, reason=f"用户佩戴荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"✅ 已佩戴荣誉 **{selected_honor_def.name}** 并获得身份组！", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ 操作失败！我没有足够的权限来为你添加/移除身份组。请确保我的角色高于此荣誉的身份组。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"佩戴/卸下荣誉身份组时发生错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)

        fresh_member = self.guild.get_member(self.member.id)
        if fresh_member is None:  # 如果不在缓存中，从API获取
            try:
                fresh_member = await self.guild.fetch_member(self.member.id)
            except discord.NotFound:
                await interaction.followup.send("❌ 无法获取您的成员信息，操作失败。", ephemeral=True)
                return

        # 更新视图内部的成员引用，确保后续 _rebuild_view 使用最新数据
        self.member = fresh_member

        await self.update_view(interaction)

    async def _rebuild_view(self):
        self.clear_items()

        current_page_honor_data = self.get_page_items()
        main_honor_embed = self.create_honor_embed(self.member, current_page_honor_data)
        self.embed = [main_honor_embed, self.cog.guide_manager.embed]

        self._add_pagination_buttons(row=1)

        if self.cog.guide_manager.url:
            self.add_item(ui.Button(
                label=f"跳转到 “{self.cog.guide_manager.embed.title}”",
                style=discord.ButtonStyle.link,
                url=self.cog.guide_manager.url,
                row=2
            ))

        user_honors_earned = self.cog.data_manager.get_user_honors(self.member.id)
        if not user_honors_earned:
            return

        member_role_ids = {role.id for role in self.member.roles}
        options = []
        for uh_instance in user_honors_earned:
            honor_def = uh_instance.definition
            if honor_def.role_id is None:
                continue

            is_equipped_now = honor_def.role_id in member_role_ids
            equip_emoji = "✅" if is_equipped_now else "🔘"

            options.append(discord.SelectOption(
                label=f"{equip_emoji} {honor_def.name}",
                description=honor_def.description[:80],
                value=honor_def.uuid
            ))

        if not options:
            return

        honor_select = ui.Select(
            placeholder="选择一个荣誉来佩戴或卸下身份组...",
            min_values=1,
            max_values=1,
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
        all_definitions = self.cog.data_manager.get_all_honor_definitions(guild.id)
        user_honor_instances = self.cog.data_manager.get_user_honors(member.id)
        member_role_ids = {role.id for role in member.roles}
        owned_honor_definitions_map = {uh.honor_uuid: uh.definition for uh in user_honor_instances}

        for definition in all_definitions:
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
            # 2. 返回一个元组，Python 会依次比较元组中的元素
            #    首先按荣誉类型（已佩戴 > 未佩戴 > ...）排序
            #    如果类型相同，则按荣誉名称的字母顺序排序（不区分大小写）
            return order.get(honor_data.shown_mode, 99), honor_data.data.name.lower()

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

    def __init__(self, bot: RoleBot):
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


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    import os
    if not os.path.exists('data'):
        os.makedirs('data')
    await bot.add_cog(HonorCog(bot))
