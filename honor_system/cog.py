# honor_system/cog.py
from __future__ import annotations

import datetime
import typing
from zoneinfo import ZoneInfo

import discord
from discord import app_commands, ui
from discord.ext import commands

import config_data
from .data_manager import HonorDataManager
from .models import HonorDefinition

if typing.TYPE_CHECKING:
    from main import RoleBot


# --- 视图定义 ---
class HonorManageView(ui.View):
    def __init__(self, cog: 'HonorCog', member: discord.Member, guild: discord.Guild):  # 新增 guild 参数
        super().__init__(timeout=180)
        self.cog = cog
        self.member = member
        self.guild = guild  # 保存 guild 引用
        self.message: typing.Optional[discord.Message] = None
        self.build_view()

    def build_view(self):
        """动态构建或重建视图"""
        self.clear_items()  # 清空旧按钮和选择器

        # 获取用户已获得的荣誉列表
        user_honors_earned = self.cog.data_manager.get_user_honors(self.member.id)
        if not user_honors_earned:
            return  # 如果用户没有任何获得的荣誉，就不显示选择器了

        # 获取用户当前实际拥有的身份组ID集合
        member_role_ids = {role.id for role in self.member.roles}

        options = []
        for uh_instance in user_honors_earned:
            honor_def = uh_instance.definition  # 获取荣誉定义

            # 只有当荣誉关联了身份组时，才能被佩戴/卸下
            if honor_def.role_id is None:
                continue  # 如果没有关联身份组，跳过，不显示在选择器中

            # 判断当前是否佩戴
            is_equipped_now = honor_def.role_id in member_role_ids
            equip_emoji = "✅" if is_equipped_now else "🔘"  # 佩戴用勾，未佩戴用圆点

            options.append(discord.SelectOption(
                label=f"{equip_emoji} {honor_def.name}",
                description=honor_def.description[:80],  # 描述限制长度
                value=honor_def.uuid  # 使用荣誉的UUID作为值，因为我们直接操作 HonorDefinition
            ))

        # 如果没有可供操作（有身份组关联）的荣誉，也就不显示选择器
        if not options:
            return

        honor_select = ui.Select(
            placeholder="选择一个荣誉来佩戴或卸下身份组...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="honor_select"
        )
        honor_select.callback = self.on_honor_select
        self.add_item(honor_select)

    async def on_honor_select(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)  # 保持为悄悄话

        selected_honor_uuid = interaction.data["values"][0]

        # 1. 查找对应的 HonorDefinition
        selected_honor_def = next(
            (hd for hd in self.cog.data_manager.get_all_honor_definitions(self.guild.id)
             if hd.uuid == selected_honor_uuid),
            None
        )

        if not selected_honor_def or selected_honor_def.role_id is None:
            await interaction.followup.send("❌ 选择的荣誉无效或未关联身份组。", ephemeral=True)
            await self.update_display(interaction)
            return

        role_id_int: int = typing.cast(int, selected_honor_def.role_id)

        target_role = self.guild.get_role(role_id_int)
        if not target_role:
            await interaction.followup.send(f"⚠️ 荣誉 **{selected_honor_def.name}** 关联的身份组(ID:{selected_honor_def.role_id})已不存在。", ephemeral=True)
            await self.update_display(interaction)
            return

        # 2. 判断当前用户是否拥有该身份组
        member_has_role = target_role in self.member.roles

        try:
            if member_has_role:
                # 用户有身份组，则移除
                await self.member.remove_roles(target_role, reason=f"用户卸下荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"☑️ 已卸下荣誉 **{selected_honor_def.name}** 并移除身份组。", ephemeral=True)
            else:
                # 用户没有身份组，则添加
                await self.member.add_roles(target_role, reason=f"用户佩戴荣誉: {selected_honor_def.name}")
                await interaction.followup.send(f"✅ 已佩戴荣誉 **{selected_honor_def.name}** 并获得身份组！", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ 操作失败！我没有足够的权限来为你添加/移除身份组。请确保我的角色高于此荣誉的身份组。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"佩戴/卸下荣誉身份组时发生错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)

        # 无论操作成功与否，都更新面板以显示最新状态
        # 刷新 self.member 的角色缓存，确保 update_display 拿到最新数据
        self.member = await self.guild.fetch_member(self.member.id)
        await self.update_display(interaction)

    async def update_display(self, interaction: discord.Interaction):
        """更新交互消息的 Embed 和 View"""
        # 重新构建视图，它会基于 member.roles 刷新状态
        self.build_view()
        # 重新创建 Embed，它也会基于 member.roles 刷新状态
        embed = self.cog.create_honor_embed(self.member, self.guild)  # 传递 guild
        await interaction.edit_original_response(embed=embed, view=self)

    async def on_timeout(self):
        if self.message:
            # timeout 时，select 菜单会被禁用，但会保留在消息中
            # 可以选择清空所有 item 或者禁用它们
            for item in self.children:
                item.disabled = True  # 禁用所有按钮/选择器
            await self.message.edit(content="*这个荣誉面板已超时，请重新使用 `/荣誉面板` 命令。*", view=self)


# --- 主Cog ---
class HonorCog(commands.Cog, name="Honor"):
    """管理荣誉系统"""

    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        self.data_manager = HonorDataManager()

        # 在机器人准备就绪后执行同步任务
        self.bot.loop.create_task(self.synchronize_all_honor_definitions())

    async def synchronize_all_honor_definitions(self):
        """
        [核心] 在机器人启动时运行。
        将 config_data.py 中的荣誉定义同步到数据库。
        - 如果配置中的荣誉在数据库中不存在，则创建。
        - 如果已存在，则更新其名称、描述等信息。
        - 如果数据库中的荣誉在配置中已不存在，则将其标记为“已归档”。
        """
        await self.bot.wait_until_ready()  # 确保机器人已连接
        self.logger.info("HonorCog: 开始同步所有服务器的荣誉定义...")

        # 1. 从配置中获取所有应存在的荣誉UUID
        all_config_uuids = set()
        for guild_id, guild_config in config_data.HONOR_CONFIG.items():
            for honor_def in guild_config.get("definitions", []):
                all_config_uuids.add(honor_def['uuid'])

        with self.data_manager.get_db() as db:
            # 2. 同步每个服务器的荣誉
            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                self.logger.info(f"同步服务器 {guild_id} 的荣誉...")
                for config_def in guild_config.get("definitions", []):
                    # 尝试从数据库获取现有的定义
                    db_def = db.query(HonorDefinition).filter_by(uuid=config_def['uuid']).one_or_none()

                    if db_def:
                        # 更新现有荣誉
                        db_def.name = config_def['name']
                        db_def.description = config_def['description']
                        db_def.role_id = config_def.get('role_id')
                        db_def.icon_url = config_def.get('icon_url')
                        db_def.guild_id = guild_id
                        db_def.is_archived = False  # 确保它不是归档状态
                    else:
                        # 创建新荣誉
                        new_def = HonorDefinition(
                            uuid=config_def['uuid'],
                            guild_id=guild_id,
                            name=config_def['name'],
                            description=config_def['description'],
                            role_id=config_def.get('role_id'),
                            icon_url=config_def.get('icon_url'),
                        )
                        db.add(new_def)
                        self.logger.info(f"  -> 已创建新荣誉: {config_def['name']}")

            # 3. 归档处理：找出数据库中存在但在配置中已删除的荣誉
            db_uuids_to_check = db.query(HonorDefinition.uuid).filter(HonorDefinition.is_archived == False).all()
            db_uuids_set = {uuid_tuple[0] for uuid_tuple in db_uuids_to_check}

            uuids_to_archive = db_uuids_set - all_config_uuids

            if uuids_to_archive:
                self.logger.warning(f"发现 {len(uuids_to_archive)} 个需要归档的荣誉...")
                db.query(HonorDefinition).filter(HonorDefinition.uuid.in_(uuids_to_archive)).update({"is_archived": True})

            db.commit()

        self.logger.info("HonorCog: 荣誉定义同步完成。")

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        """监听帖子创建事件，用于荣誉授予"""
        if not isinstance(thread.parent, discord.ForumChannel):
            return

        author = thread.owner
        if not author or author.bot:
            return

        # 1. 处理基础活动荣誉
        event_cfg = config_data.HONOR_CONFIG.get("event_honor", {})
        if event_cfg.get("enabled") and thread.parent.id in event_cfg.get("target_forum_ids", []):
            tz = ZoneInfo("Asia/Shanghai")  # UTC+8
            now = datetime.datetime.now(tz)
            start_time = datetime.datetime.fromisoformat(event_cfg["start_time"]).replace(tzinfo=tz)
            end_time = datetime.datetime.fromisoformat(event_cfg["end_time"]).replace(tzinfo=tz)

            if start_time <= now <= end_time:
                honor_uuid_to_grant = event_cfg.get("honor_uuid")
                if honor_uuid_to_grant:
                    granted_honor_def = self.data_manager.grant_honor(author.id, honor_uuid_to_grant)
                    if granted_honor_def:
                        honor_name = granted_honor_def.name
                        self.logger.info(f"用户 {author} ({author.id}) 因参与活动获得了荣誉 '{honor_name}'")
                        # try:
                        #     await author.send(
                        #         f"🎉 恭喜！因在活动期间于 **{thread.parent.name}** 发布了新帖子，你已获得荣誉：**{honor_name}**！\n你可以使用 `/honor` 命令查看和佩戴。")
                        # except discord.Forbidden:
                        #     pass  # 用户关闭了私信

        # 2. 处理高级里程碑荣誉
        milestone_cfg = config_data.HONOR_CONFIG.get("milestone_honor", {})
        if milestone_cfg.get("enabled") and thread.parent.id in milestone_cfg.get("target_forum_ids", []):
            # a. 记录帖子
            self.data_manager.add_tracked_post(thread.id, author.id, thread.parent.id)

            # b. 检查里程碑
            post_count = self.data_manager.get_user_post_count(author.id)
            milestones = milestone_cfg.get("milestones", {})

            # 倒序检查，这样即使一次达到多个里程碑，也能正确处理
            for count_req, honor_uuid in sorted(milestones.items(), key=lambda item: item[0], reverse=True):
                if post_count >= count_req:
                    granted_honor_def = self.data_manager.grant_honor(author.id, honor_uuid)
                    if granted_honor_def:
                        honor_name = granted_honor_def.name
                        self.logger.info(f"用户 {author} ({author.id}) 发帖数达到 {count_req}，获得了荣誉 '{honor_name}'")
                        # try:
                        #     await author.send(f"🏆 里程碑达成！你的累计发帖数已达到 **{count_req}**，特此授予你荣誉：**{honor_name}**！\n继续努力，解锁更高成就吧！")
                        # except discord.Forbidden:
                        #     pass
                    # 找到第一个达成的里程碑并授予后就停止，防止重复授予低级荣誉
                    break

    def create_honor_embed(self, member: discord.Member, guild: discord.Guild) -> discord.Embed:
        """为用户创建荣誉展示 Embed，显示已拥有、纯粹成就和待解锁的荣誉"""
        all_definitions = self.data_manager.get_all_honor_definitions(guild.id)
        user_honor_instances = self.data_manager.get_user_honors(member.id)

        member_role_ids = {role.id for role in member.roles}

        # 构建已拥有荣誉的字典，方便快速查找其定义
        owned_honor_definitions_map = {uh.honor_uuid: uh.definition for uh in user_honor_instances}

        equipped_honors_lines = []  # 用户拥有，且佩戴了身份组
        unequipped_owned_honors_lines = []  # 用户拥有，有身份组但未佩戴
        pure_achievement_honors_lines = []  # 用户拥有，但无身份组关联（纯粹成就）
        unearned_honors_lines = []  # 用户未拥有

        for definition in all_definitions:
            honor_line_text = f"**{definition.name}**\n*└ {definition.description}*"
            if definition.role_id is not None:
                honor_line_text = f"<@&{definition.role_id}>\n*└ {definition.description}*"

            if definition.uuid in owned_honor_definitions_map:
                # 用户拥有这个荣誉
                if definition.role_id is not None:
                    # 荣誉关联了身份组，判断是否佩戴
                    if definition.role_id in member_role_ids:
                        equipped_honors_lines.append(honor_line_text)
                    else:
                        unequipped_owned_honors_lines.append(honor_line_text)
                else:
                    # 荣誉没有关联身份组，是纯粹的成就
                    pure_achievement_honors_lines.append(honor_line_text)
            else:
                # 用户未拥有这个荣誉
                unearned_honors_lines.append(honor_line_text)

        embed = discord.Embed(
            title=f"{member.display_name}的荣誉墙",
            color=member.color
        )
        if member.display_avatar:
            embed.set_thumbnail(url=member.display_avatar.url)

        # 添加描述性文字
        if not user_honor_instances and not all_definitions:
            embed.description = "目前没有可用的荣誉定义。请联系管理员添加。"
        elif not user_honor_instances:
            embed.description = "你还没有获得任何荣誉哦！查看下方待解锁荣誉，多多参与社区活动吧！"
        elif all_definitions and len(user_honor_instances) == len(all_definitions) and not unearned_honors_lines:
            # 确保 unearned_honors_lines 为空，即所有荣誉都已被获得
            embed.description = "🎉 你已经解锁了所有可用的荣誉！恭喜你！"
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

        embed.set_footer(text="佩戴/卸下荣誉需使用下方的下拉选择器进行操作。")
        return embed

    @app_commands.command(name="荣誉面板", description="查看和管理你的荣誉。")
    @app_commands.guild_only()
    async def show_honor_panel(self, interaction: discord.Interaction):
        """显示用户的荣誉管理面板"""
        await interaction.response.defer(ephemeral=True)
        member = typing.cast(discord.Member, interaction.user)
        guild = typing.cast(discord.Guild, interaction.guild)

        embed = self.create_honor_embed(member, interaction.guild)
        view = HonorManageView(self, member, guild)

        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    # 确保 data 目录存在
    import os
    if not os.path.exists('data'):
        os.makedirs('data')

    await bot.add_cog(HonorCog(bot))
