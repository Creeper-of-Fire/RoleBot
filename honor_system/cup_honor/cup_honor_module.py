# honor_system/cup_honor_module.py
from __future__ import annotations

import datetime
import re
import typing
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from zoneinfo import ZoneInfo

import discord
from discord import app_commands, ui
from discord.ext import commands, tasks

import config_data
from utility.views import ConfirmationView
from .cup_honor_json_manager import CupHonorJsonManager
from .cup_honor_models import CupHonorDefinition
from honor_system.data_manager.honor_data_manager import HonorDataManager
from honor_system.honor_def_models import UserHonor, HonorDefinition
from .cup_honor_module_notification_state_data_manager import NotificationStateManager
from .cup_honor_module_view import CupHonorManageView

if typing.TYPE_CHECKING:
    from main import RoleBot

class ExpiredHonorNoticeView(ui.View):
    """
    一个自包含的视图，用于处理杯赛头衔到期的通知。
    它在初始化时执行数据健康检查，生成报告，并提供一键修复功能。
    """

    def __init__(
            self,
            cog: 'CupHonorModuleCog',
            guild: discord.Guild,
            honor_def: HonorDefinition,
            admin_role_id: int
    ):
        super().__init__(timeout=86400)  # 按钮有效期24小时
        self.cog = cog
        self.guild = guild
        self.honor_def = honor_def
        self.admin_role_id = admin_role_id

        # 将所有数据处理和状态计算都放在初始化函数中
        self._perform_data_check()
        self._configure_components()

    def _perform_data_check(self):
        """执行数据交叉验证，并将结果存储为实例属性。"""
        self.role = self.guild.get_role(self.honor_def.role_id)
        if not self.role:
            # 如果身份组不存在，设置空状态
            self.role_holders = set()
            self.db_honor_holders = set()
        else:
            self.role_holders = set(self.role.members)

        with self.cog.honor_data_manager.get_db() as db:
            user_honor_records = db.query(UserHonor).filter(UserHonor.honor_uuid == str(self.honor_def.uuid)).all()
            self.db_honor_holders = {
                member for user_id in [r.user_id for r in user_honor_records]
                if (member := self.guild.get_member(user_id))
            }

        # 计算差异
        self.members_to_fix = list(self.role_holders - self.db_honor_holders)
        self.members_ok = list(self.role_holders.intersection(self.db_honor_holders))
        self.members_record_only = list(self.db_honor_holders - self.role_holders)

    def _configure_components(self):
        """根据数据检查的结果，配置视图中的按钮等组件。"""
        # 获取按钮引用
        fix_button: discord.ui.Button = self.children[0]

        if not self.members_to_fix:
            fix_button.disabled = True
            fix_button.label = "无需补发"
            fix_button.style = discord.ButtonStyle.secondary
        else:
            fix_button.disabled = False
            fix_button.label = f"为 {len(self.members_to_fix)} 人补发荣誉记录"
            fix_button.style = discord.ButtonStyle.primary

    def create_initial_embed(self) -> discord.Embed:
        """根据实例的状态创建初始的Embed消息。"""
        color = discord.Color.orange() if self.members_to_fix else discord.Color.blue()
        role_mention = self.role.mention if self.role else f"`ID: {self.honor_def.role_id}` (已删除)"

        embed = discord.Embed(
            title="🏆 杯赛头衔到期与数据检查",
            description=f"荣誉 **{self.honor_def.name}** ({role_mention}) 已到期。",
            color=color
        )
        embed.set_footer(text=f"荣誉UUID: {self.honor_def.uuid}")

        if self.members_to_fix:
            mentions = " ".join([m.mention for m in self.members_to_fix])
            if len(mentions) > 1000: mentions = f"共 {len(self.members_to_fix)} 人，列表过长已省略。"
            embed.add_field(
                name="🚨 **危险：数据不一致**",
                value=f"以下 **{len(self.members_to_fix)}** 人拥有身份组但**无**荣誉记录！\n"
                      f"**请点击下方按钮为他们补发记录，否则荣誉将丢失！**\n{mentions}",
                inline=False
            )

        if self.members_ok:
            mentions = " ".join([m.mention for m in self.members_ok])
            if len(mentions) > 1000: mentions = f"共 {len(self.members_ok)} 人，列表过长已省略。"
            embed.add_field(
                name="✅ **状态正常：请移除身份组**",
                value=f"以下 **{len(self.members_ok)}** 人数据记录正常，请手动移除身份组。\n{mentions}",
                inline=False
            )

        if not self.members_to_fix and not self.members_ok:
            embed.add_field(name="ℹ️ 无需操作", value="当前没有成员佩戴此身份组。", inline=False)

        if self.members_record_only:
            embed.add_field(name="ℹ️ 备注", value=f"另有 **{len(self.members_record_only)}** 人有记录但无身份组，无需处理。", inline=False)

        return embed

    @ui.button(label="补发荣誉记录", style=discord.ButtonStyle.primary, custom_id="fix_cup_honor_records")
    async def fix_records_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 权限检查
        if not any(role.id == self.admin_role_id for role in interaction.user.roles):
            await interaction.response.send_message("❌ 你没有权限执行此操作。", ephemeral=True)
            return

        await interaction.response.defer()  # 使用defer来表示正在处理，避免交互超时

        # --- 调用健壮的、复用的同步逻辑 ---
        res = await self.cog._process_role_sync(self.guild, str(self.honor_def.uuid))

        # --- 处理结果 ---
        if not res["success"]:
            # 如果同步过程出错，通知管理员
            error_embed = discord.Embed(
                title="❌ 荣誉记录补发失败",
                description=f"在尝试同步荣誉 **{self.honor_def.name}** 时发生错误。\n"
                            f"错误信息: `{res['error_msg']}`",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(embed=error_embed, view=None)
            return

        self.cog.logger.info(f"管理员 {interaction.user} 通过到期通知修复了荣誉 {self.honor_def.uuid}。"
                             f"新授予: {res['newly_granted']}, 已拥有: {res['already_had']}.")

        # 禁用所有按钮
        for item in self.children:
            item.disabled = True

        # 创建成功的Embed
        success_embed = discord.Embed(
            title="✅ 荣誉记录补发完成",
            description=f"已为所有拥有 {res['role_mention']} 身份组的成员检查并补发了荣誉 **{self.honor_def.name}**。",
            color=discord.Color.green()
        )
        success_embed.add_field(name="新授予荣誉", value=f"`{res['newly_granted']}` 人", inline=True)
        success_embed.add_field(name="本就拥有荣誉", value=f"`{res['already_had']}` 人", inline=True)
        success_embed.add_field(name="下一步操作", value=f"现在可以安全地手动移除身份组了。", inline=False)
        success_embed.set_footer(text=f"荣誉UUID: {self.honor_def.uuid}")

        # 编辑原始消息
        await interaction.edit_original_response(embed=success_embed, view=self)


class CupHonorModuleCog(commands.Cog, name="CupHonorModule"):
    """【荣誉子模块】管理手动的、有时效性的杯赛头衔。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger
        self.honor_data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.cup_honor_manager = CupHonorJsonManager.get_instance(logger=self.logger)
        # 用于存储已发送过通知的荣誉UUID，防止重复提醒
        self.notification_manager = NotificationStateManager.get_instance(logger=self.logger)
        self.expiration_check_loop.start()

    def cog_unload(self):
        """当Cog被卸载时，取消后台任务。"""
        self.expiration_check_loop.cancel()

    # --- 后台任务：检查过期的杯赛头衔 ---
    async def _perform_expiration_check(self):
        """
        执行一次完整的杯赛头衔到期检查。
        此方法被启动任务和定时循环共同调用。
        """
        self.logger.info("正在执行杯赛头衔到期检查...")
        try:
            now_aware = datetime.datetime.now(ZoneInfo("Asia/Shanghai"))

            for guild_id, guild_config in config_data.HONOR_CONFIG.items():
                cup_cfg = guild_config.get("cup_honor", {})
                if not cup_cfg.get("enabled"):
                    continue

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    self.logger.warning(f"无法找到服务器 {guild_id}，跳过杯赛头衔检查。")
                    continue

                await self._check_guild_for_expired_titles(guild, cup_cfg, now_aware)
        except Exception as e:
            self.logger.error(f"杯赛头衔到期检查任务发生未知错误: {e}", exc_info=True)

    @tasks.loop(hours=24)
    async def expiration_check_loop(self):
        """每天运行一次，检查是否有杯赛头衔到期，并通知管理员。"""
        await self._perform_expiration_check()

    # --- before_loop，在启动时也调用辅助方法 ---
    @expiration_check_loop.before_loop
    async def before_expiration_check(self):
        """在任务开始前，等待机器人完全准备好，并立即执行一次检查。"""
        await self.bot.wait_until_ready()
        self.logger.info("机器人已就绪。正在执行启动时的杯赛头衔到期检查...")
        await self._perform_expiration_check()

    # --- 数据库同步辅助函数 ---
    async def sync_cup_honor_to_db(self, guild_id: int, honor_def: CupHonorDefinition, original_uuid_str: Optional[str] = None):
        """将Pydantic模型的数据同步（插入或更新）到SQLAlchemy数据库。"""
        with self.honor_data_manager.get_db() as db:
            # 如果UUID改变了，需要将旧的记录归档
            if original_uuid_str and original_uuid_str != str(honor_def.uuid):
                old_db_def = db.query(HonorDefinition).filter_by(uuid=original_uuid_str).one_or_none()
                if old_db_def:
                    self.logger.warning(f"杯赛荣誉UUID从 {original_uuid_str} 变更为 {honor_def.uuid}，正在归档旧记录...")
                    old_db_def.is_archived = True
                    db.add(old_db_def)

            # 查找或创建新的数据库记录
            db_def = db.query(HonorDefinition).filter_by(uuid=str(honor_def.uuid)).one_or_none()
            if not db_def:
                db_def = HonorDefinition(uuid=str(honor_def.uuid), guild_id=guild_id)
                self.logger.info(f"为杯赛荣誉 '{honor_def.name}' 创建新的数据库记录。")

            # 更新数据
            db_def.name = honor_def.name
            db_def.description = honor_def.description
            db_def.role_id = honor_def.role_id
            db_def.hidden_until_earned = honor_def.hidden_until_earned
            db_def.is_archived = False  # 确保是激活状态

            db.add(db_def)
            db.commit()

    async def archive_honor_in_db(self, honor_uuid: str):
        """在数据库中归档一个荣誉定义。"""
        with self.honor_data_manager.get_db() as db:
            db_def = db.query(HonorDefinition).filter_by(uuid=honor_uuid).one_or_none()
            if db_def:
                db_def.is_archived = True
                db.add(db_def)
                db.commit()
                self.logger.info(f"已在数据库中归档荣誉 {honor_uuid}。")

    async def _check_guild_for_expired_titles(self, guild: discord.Guild, cup_cfg: dict, now: datetime.datetime):
        """处理单个服务器的过期检查逻辑。"""
        titles = self.cup_honor_manager.get_all_cup_honors()
        notification_cfg = cup_cfg.get("notification", {})

        if not titles or not notification_cfg.get("channel_id") or not notification_cfg.get("admin_role_id"):
            self.logger.warning(f"服务器 {guild.name} 的杯赛头衔配置不完整，跳过。")
            return

        for honor_def in titles:
            honor_uuid = str(honor_def.uuid)
            if self.notification_manager.has_been_notified(honor_uuid):
                continue  # 已处理过，跳过

            expiration_date = honor_def.cup_honor.expiration_date
            if now >= expiration_date:
                self.logger.info(f"荣誉 {honor_uuid} 在服务器 {guild.name} 已过期，开始检查用户...")
                await self._notify_admin_for_expired_honor(guild, honor_uuid, expiration_date, notification_cfg)
                await self.notification_manager.add_notified(honor_uuid)

    async def _notify_admin_for_expired_honor(self, guild: discord.Guild, honor_uuid: str, exp_date: datetime.datetime,
                                              notify_cfg: dict):
        """为单个过期的荣誉构建并发送包含数据健康检查的智能通知。"""
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            self.logger.warning(f"荣誉 {honor_uuid} 定义无效或未关联身份组，无法发送到期通知。")
            return

        notification_channel = guild.get_channel(notify_cfg["channel_id"])
        admin_role = guild.get_role(notify_cfg["admin_role_id"])

        if not notification_channel or not admin_role:
            self.logger.error(f"无法在服务器 {guild.name} 中找到通知频道或管理员身份组。")
            return

        # 1. 创建自包含的View实例，它会自己处理所有逻辑
        view = ExpiredHonorNoticeView(self, guild, honor_def, admin_role.id)

        # 2. 从View获取它生成的初始Embed
        initial_embed = view.create_initial_embed()

        # 3. 发送消息
        try:
            await notification_channel.send(
                content=admin_role.mention,
                embed=initial_embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=[admin_role])
            )
            self.logger.info(f"已在服务器 {guild.name} 发送关于荣誉 {honor_def.name} 的增强版到期通知。")
        except discord.Forbidden:
            self.logger.error(f"无法在频道 {notification_channel.name} 发送通知，权限不足。")

    async def _process_role_sync(self, guild: discord.Guild, honor_uuid: str) -> dict:
        """
        【核心逻辑】处理单个荣誉的“从身份组同步”逻辑。
        返回一个包含执行结果统计的字典，不直接发送消息。
        """
        result = {
            "success": False,
            "honor_name": "未知",
            "role_mention": "未知",
            "total_members": 0,
            "newly_granted": 0,
            "already_had": 0,
            "error_msg": None
        }

        # 1. 获取荣誉定义
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            result["error_msg"] = f"找不到UUID为 `{honor_uuid}` 的荣誉定义。"
            return result

        result["honor_name"] = honor_def.name

        # 2. 验证身份组配置
        if not honor_def.role_id:
            result["error_msg"] = "该荣誉未关联任何身份组。"
            return result

        role = guild.get_role(honor_def.role_id)
        if not role:
            result["error_msg"] = f"服务器中找不到关联的身份组 (ID: {honor_def.role_id})。"
            return result

        result["role_mention"] = role.mention

        # 3. 执行同步逻辑
        members_with_role = role.members
        result["total_members"] = len(members_with_role)

        if not members_with_role:
            result["success"] = True  # 虽然没人，但逻辑是成功的
            return result

        newly_granted_count = 0
        already_had_count = 0

        for member in members_with_role:
            if member.bot:
                continue

            # grant_honor 返回定义对象表示新授予，返回None表示已存在
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted_count += 1
            else:
                already_had_count += 1

        result["success"] = True
        result["newly_granted"] = newly_granted_count
        result["already_had"] = already_had_count

        return result

    # --- 管理员指令 ---
    cup_honor_group = app_commands.Group(
        name="杯赛头衔", description="管理特殊的杯赛头衔",
        guild_only=True, default_permissions=discord.Permissions(manage_roles=True)
    )

    async def honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """
               为杯赛荣誉UUID参数提供自动补全选项。
               选项会按过期时间降序排列，并在结果过多时提示用户。
               """
        # 1. 获取所有杯赛荣誉
        all_cup_honors = self.cup_honor_manager.get_all_cup_honors()
        if not all_cup_honors:
            return []

        # 2. 按过期时间降序排序
        #    这样最新、最晚到期的荣誉会优先显示在列表顶部
        sorted_honors = sorted(
            all_cup_honors,
            key=lambda h: h.cup_honor.expiration_date,
            reverse=True
        )

        # 3. 根据用户输入进行筛选
        choices = []
        for honor_def in sorted_honors:
            # 为了更好的用户体验，我们可以在名称中也加入过期日期
            expiration_str = honor_def.cup_honor.expiration_date.strftime('%Y-%m-%d')
            choice_name = f"{honor_def.name} (至{expiration_str}) ({str(honor_def.uuid)[:8]})"

            # 模糊匹配用户输入
            if current.lower() in choice_name.lower():
                choices.append(app_commands.Choice(name=choice_name, value=str(honor_def.uuid)))

        # 4. 处理Discord的25个选项上限
        if len(choices) > 25:
            # 如果筛选出的结果超过25个，只返回前24个，并附带一条提示信息
            final_choices = choices[:24]
            final_choices.append(
                app_commands.Choice(
                    name="⚠️ 结果过多，请输入更精确的关键词进行搜索...",
                    # 这个value可以是任何不会被正常解析的字符串，防止用户意外选中
                    value="too_many_results_to_show"
                )
            )
            return final_choices
        else:
            # 如果结果在25个以内，直接返回
            return choices

    @cup_honor_group.command(name="管理", description="通过JSON编辑器管理所有杯赛头衔。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_cup_honors(self, interaction: discord.Interaction):
        """启动一个视图，用于管理所有杯赛荣誉。"""
        await interaction.response.defer(ephemeral=True)
        view = CupHonorManageView(self)
        await view.start(interaction)

    @cup_honor_group.command(name="授予", description="为用户手动授予一个杯赛头衔及其身份组。")
    @app_commands.describe(member="要授予头衔的成员", honor_uuid="要授予的杯赛头衔")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def grant(self, interaction: discord.Interaction, member: discord.Member, honor_uuid: str):
        await interaction.response.defer(ephemeral=True)

        # 1. 验证荣誉UUID是否已在配置中
        if not self.cup_honor_manager.get_cup_honor_by_uuid(honor_uuid):
            await interaction.followup.send("❌ **操作失败**：这个荣誉不是一个已配置的杯赛头衔。", ephemeral=True)
            return

        # 2. 尝试授予荣誉（核心数据库操作）
        #    - 如果成功授予，granted_def 会是 honor_def 对象
        #    - 如果用户已拥有，granted_def 会是 None
        granted_def = self.honor_data_manager.grant_honor(member.id, honor_uuid)
        honor_def = granted_def or self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)

        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        # 3. 根据授予结果决定后续操作和响应
        response_lines = []

        if not granted_def:
            # 情况A: 用户已拥有此荣誉
            response_lines.append(f"☑️ {member.mention} 已拥有荣誉 **{honor_def.name}**。")
            response_lines.append("ℹ️ 未进行身份组操作，因为用户已持有该荣誉。")

            await interaction.followup.send("\n".join(response_lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return  # 操作到此结束

        # 情况B: 成功为用户新授予了荣誉
        response_lines.append(f"🏅 已为 {member.mention} 授予荣誉 **{honor_def.name}**。")

        # 4. 尝试授予关联的身份组 (仅在荣誉是新授予时执行)
        if not honor_def.role_id:
            response_lines.append(f"⚠️ **提示**：此荣誉未关联任何身份组，无需佩戴。")
        else:
            role = interaction.guild.get_role(honor_def.role_id)
            if not role:
                response_lines.append(f"❌ **警告**：荣誉已授予，但在服务器中未找到对应的身份组 (ID: {honor_def.role_id})。请联系管理员检查配置。")
            elif role in member.roles:
                response_lines.append(f"☑️ 用户已佩戴身份组 {role.mention}。")
            else:
                try:
                    await member.add_roles(role, reason=f"由 {interaction.user} 手动授予杯赛头衔")
                    response_lines.append(f"✅ 已为用户佩戴身份组 {role.mention}。")
                except discord.Forbidden:
                    response_lines.append(f"❌ **权限不足**：荣誉已授予，但我无法为用户添加身份组 {role.mention}。")
                except Exception as e:
                    self.logger.error(f"为用户 {member} 添加杯赛角色 {role.name} 时出错: {e}", exc_info=True)
                    response_lines.append(f"❌ **未知错误**：荣誉已授予，但添加身份组时发生错误。")

        # 5. 发送最终的合并报告
        await interaction.followup.send("\n".join(response_lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @cup_honor_group.command(name="从身份组同步", description="将一个杯赛头衔授予所有拥有对应身份组的成员。")
    @app_commands.describe(honor_uuid="选择要同步的杯赛头衔，机器人将为拥有其身份组的成员补发荣誉。")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def sync_from_role(self, interaction: discord.Interaction, honor_uuid: str):
        """
        一个便捷工具，用于将荣誉授予所有已拥有对应身份组的成员。
        这对于修复那些被手动赋予身份组但未记录荣誉的成员很有用。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 调用标准化逻辑
        res = await self._process_role_sync(guild, honor_uuid)

        # 处理错误
        if not res["success"]:
            await interaction.followup.send(f"❌ **同步失败**：{res['error_msg']}", ephemeral=True)
            return

        # 记录日志
        self.logger.info(
            f"管理员 {interaction.user} 在服务器 {guild.name} "
            f"对荣誉 '{res['honor_name']}' 执行了从身份组同步操作。 "
            f"新授予: {res['newly_granted']}, 已拥有: {res['already_had']}."
        )

        # 发送报告
        if res["total_members"] == 0:
            await interaction.followup.send(f"🤷 **无需操作**：没有找到任何成员拥有 {res['role_mention']} 身份组。", ephemeral=True)
            return

        embed = discord.Embed(
            title="✅ 荣誉同步完成",
            description=f"已为所有拥有 {res['role_mention']} 身份组的成员检查并补发了荣誉 **{res['honor_name']}**。",
            color=discord.Color.green()
        )
        embed.add_field(name="总共检查成员", value=f"`{res['total_members']}` 人", inline=True)
        embed.add_field(name="新授予荣誉", value=f"`{res['newly_granted']}` 人", inline=True)
        embed.add_field(name="本就拥有荣誉", value=f"`{res['already_had']}` 人", inline=True)
        embed.set_footer(text="此操作确保了所有拥有身份组的成员都在荣誉系统中正确记录。")

        await interaction.followup.send(embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @cup_honor_group.command(name="全部从身份组同步", description="【批量】扫描所有的杯赛头衔，并根据身份组补发荣誉。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def sync_all_from_roles(self, interaction: discord.Interaction):
        """
        遍历当前系统中所有配置的杯赛荣誉，并依次执行“从身份组同步”操作。
        """
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 获取所有杯赛荣誉
        all_cup_honors = self.cup_honor_manager.get_all_cup_honors()
        if not all_cup_honors:
            await interaction.followup.send("❌ **操作中止**：当前没有任何杯赛荣誉配置。", ephemeral=True)
            return

        report_lines = []
        total_newly_granted = 0
        processed_honors_count = 0
        error_count = 0

        self.logger.info(f"管理员 {interaction.user} 开始在 {guild.name} 执行全量杯赛荣誉同步...")

        # 2. 遍历执行
        for honor_def in all_cup_honors:
            uuid_str = str(honor_def.uuid)

            # 调用复用的逻辑
            res = await self._process_role_sync(guild, uuid_str)

            if res["success"]:
                # 只有当确实发生了变动，或者虽然没变动但检测了成员时才记录，避免刷屏
                if res["total_members"] > 0:
                    status_icon = "✅" if res["newly_granted"] > 0 else "☑️"
                    report_lines.append(
                        f"{status_icon} **{res['honor_name']}**: 检查 {res['total_members']} 人, 新增 {res['newly_granted']} 人"
                    )
                    total_newly_granted += res["newly_granted"]
                processed_honors_count += 1
            else:
                # 忽略一些无伤大雅的错误（比如身份组已经被删除了），但如果是严重的则记录
                # 这里简单处理，只记录身份组不存在的情况作为调试信息
                # 如果不需要在最终报告显示太多错误，可以只在log里写
                self.logger.warning(f"全量同步跳过荣誉 {uuid_str}: {res['error_msg']}")
                if "找不到关联的身份组" in res["error_msg"]:
                    error_count += 1

        # 3. 构建汇总报告
        embed = discord.Embed(
            title="🔄 全量杯赛荣誉同步报告",
            description=f"已扫描并处理了 **{processed_honors_count}** 个有效的杯赛荣誉配置。",
            color=discord.Color.blue()
        )

        embed.add_field(name="累计新授予", value=f"`{total_newly_granted}` 人次", inline=False)

        if error_count > 0:
            embed.add_field(name="异常情况", value=f"有 `{error_count}` 个荣誉因找不到对应身份组而被跳过。", inline=False)

        # 分页或截断处理（防止Embed过长）
        msg_content = "\n".join(report_lines)
        if len(msg_content) > 1000:
            msg_content = msg_content[:950] + "\n... (列表过长已截断)"

        if not msg_content:
            msg_content = "没有发现任何拥有有效身份组的成员，或所有成员已拥有荣誉。"

        embed.add_field(name="详细变更记录", value=msg_content, inline=False)
        embed.set_footer(text="建议定期执行此操作以保持数据库与身份组状态一致。")

        await interaction.followup.send(embed=embed, ephemeral=True)
        self.logger.info(f"全量同步完成。新增授予: {total_newly_granted}")

    @cup_honor_group.command(name="批量授予", description="批量授予一个杯赛头衔给多个用户。")
    @app_commands.describe(
        honor_uuid="要授予的杯赛头衔。",
        user_ids="【模式一】要授予的用户的ID，用英文逗号分隔。",
        message_link="【模式二】包含目标用户的消息链接，将授予所有被提及的用户。"
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def bulk_grant(
            self,
            interaction: discord.Interaction,
            honor_uuid: str,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ):
        """批量授予杯赛头衔，支持从ID列表或消息链接中解析用户。"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 输入验证
        if not user_ids and not message_link:
            await interaction.followup.send("❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True)
            return
        if user_ids and message_link:
            await interaction.followup.send("❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True)
            return

        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        # 2. 收集用户
        try:
            members_to_process, error_logs = await self._parse_members_from_input(guild, user_ids, message_link)
        except (ValueError, IOError) as e:
            await interaction.followup.send(f"❌ **操作失败**：{e}", ephemeral=True)
            return

        if not members_to_process:
            final_message = "🤷 **操作终止**：未找到任何有效的、非机器人的用户进行操作。"
            if error_logs:
                final_message += "\n\n**解析遇到的问题：**\n" + "\n".join(error_logs)
            await interaction.followup.send(final_message, ephemeral=True)
            return

        # 3. 确认环节
        member_mentions = " ".join([m.mention for m in members_to_process])
        if len(member_mentions) > 1000:
            member_mentions = f"共 {len(members_to_process)} 人，列表过长已省略。"

        embed = discord.Embed(
            title="⚠️ 批量授予确认",
            description=f"你即将为以下 **{len(members_to_process)}** 位成员授予荣誉：\n**{honor_def.name}**",
            color=discord.Color.orange()
        )
        embed.add_field(name="目标成员", value=member_mentions, inline=False)
        embed.set_footer(text="请确认操作。此操作将在后台进行。")

        view = ConfirmationView(author=interaction.user)
        # 将消息对象存入视图，以便超时后编辑
        view.message = await interaction.followup.send(
            embed=embed, view=view, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
        )
        await view.wait()

        # 4. 执行或取消
        if view.value is None:  # 超时
            return
        if not view.value:
            await interaction.edit_original_response(content="操作已取消。", embed=None, view=None)
            return

        await interaction.edit_original_response(content="⚙️ 正在处理，请稍候...", embed=None, view=None)

        newly_granted, already_had, role_added, role_failed = [], [], [], []
        role = guild.get_role(honor_def.role_id) if honor_def.role_id else None

        for member in members_to_process:
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted.append(member)
            else:
                already_had.append(member)

            if role and role not in member.roles:
                try:
                    await member.add_roles(role, reason=f"由 {interaction.user} 批量授予杯赛头衔")
                    role_added.append(member)
                except discord.Forbidden:
                    role_failed.append(member)
                except Exception:
                    role_failed.append(member)

        # 5. 最终报告
        final_embed = discord.Embed(
            title="✅ 批量授予完成",
            description=f"已完成对 **{honor_def.name}** 荣誉的批量授予操作。",
            color=discord.Color.green()
        )
        final_embed.add_field(name="总处理人数", value=f"`{len(members_to_process)}` 人", inline=False)
        final_embed.add_field(name="新授予荣誉", value=f"`{len(newly_granted)}` 人", inline=True)
        final_embed.add_field(name="本已拥有", value=f"`{len(already_had)}` 人", inline=True)

        role_status_parts = []
        if role:
            role_status_parts.append(f"新佩戴: `{len(role_added)}`")
            if role_failed:
                role_status_parts.append(f"失败: `{len(role_failed)}`")
            role_status = " | ".join(role_status_parts)
        else:
            role_status = "未关联身份组"

        final_embed.add_field(name="身份组状态", value=role_status, inline=True)

        if error_logs:
            final_embed.add_field(name="解析警告", value="\n".join(error_logs[:5]), inline=False)

        await interaction.edit_original_response(content="", embed=final_embed)

    @staticmethod
    async def _parse_members_from_input(
            guild: discord.Guild,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ) -> Tuple[Set[discord.Member], List[str]]:
        """
        [辅助函数] 从用户ID列表或消息链接中解析成员。
        返回一个包含成员对象的集合和一份错误/警告日志。
        """
        members_to_process: Set[discord.Member] = set()
        error_logs: List[str] = []

        if user_ids:
            id_list = {uid.strip() for uid in user_ids.split(',')}
            for uid_str in id_list:
                if not uid_str.isdigit():
                    error_logs.append(f"无效ID格式: `{uid_str}`")
                    continue
                try:
                    # 使用 get_member 优先从缓存获取，失败再 fetch
                    member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                    if not member.bot:
                        members_to_process.add(member)
                except discord.NotFound:
                    error_logs.append(f"未找到用户: `{uid_str}`")

        elif message_link:
            match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)', message_link)
            if not match or int(match.group(1)) != guild.id:
                raise ValueError("无效的消息链接，或链接不属于本服务器。")

            channel_id, message_id = int(match.group(2)), int(match.group(3))
            try:
                channel = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
                message = await channel.fetch_message(message_id)

                all_mentioned_members = set(message.mentions)
                content_to_scan = message.content
                for embed in message.embeds:
                    if embed.description: content_to_scan += "\n" + embed.description
                    for field in embed.fields: content_to_scan += f"\n{field.name}\n{field.value}"

                mentioned_ids = re.findall(r'<@!?(\d+)>', content_to_scan)
                for uid_str in set(mentioned_ids):
                    try:
                        member = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
                        if member: all_mentioned_members.add(member)
                    except discord.NotFound:
                        error_logs.append(f"消息中提及的用户 `{uid_str}` 未找到。")

                for member in all_mentioned_members:
                    if not member.bot:
                        members_to_process.add(member)

            except (discord.NotFound, discord.Forbidden) as e:
                raise IOError(f"找不到指定的消息/频道，或我没有权限访问它: {e}")

        return members_to_process, error_logs

    @cup_honor_group.command(name="设置最终持有者-危险操作-仅必要时", description="设置头衔的最终持有者，并移除名单外成员的身份组。")
    @app_commands.describe(
        honor_uuid="要操作的杯赛头衔。",
        user_ids="【模式一】最终持有者的ID，用英文逗号分隔。",
        message_link="【模式二】包含最终持有者的消息链接。"
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def set_holders(
            self,
            interaction: discord.Interaction,
            honor_uuid: str,
            user_ids: Optional[str] = None,
            message_link: Optional[str] = None
    ):
        """将提供的用户列表设置为荣誉的唯一持有者，并从其他人身上移除对应身份组。"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = typing.cast(discord.Guild, interaction.guild)

        # 1. 输入验证和解析
        if not user_ids and not message_link:
            await interaction.followup.send("❌ **操作失败**：请提供 `user_ids` 或 `message_link` 中的一项。", ephemeral=True)
            return
        if user_ids and message_link:
            await interaction.followup.send("❌ **操作失败**：不能同时提供 `user_ids` 和 `message_link`。", ephemeral=True)
            return

        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            await interaction.followup.send(f"❌ **错误**：此荣誉未定义或未关联身份组，无法执行同步操作。", ephemeral=True)
            return

        role = guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(f"❌ **错误**：在服务器中找不到与荣誉关联的身份组 (ID: {honor_def.role_id})。", ephemeral=True)
            return

        try:
            definitive_members, error_logs = await self._parse_members_from_input(guild, user_ids, message_link)
        except (ValueError, IOError) as e:
            await interaction.followup.send(f"❌ **操作失败**：{e}", ephemeral=True)
            return

        # 2. 计算差异
        current_role_holders = set(role.members)

        members_to_add = definitive_members - current_role_holders
        members_to_remove = current_role_holders - definitive_members
        members_to_keep = definitive_members.intersection(current_role_holders)

        if not members_to_add and not members_to_remove:
            final_message = "🤷 **无需操作**：提供的名单与当前身份组持有者完全一致。"
            if error_logs:
                final_message += "\n\n**解析遇到的问题：**\n" + "\n".join(error_logs)
            await interaction.followup.send(final_message, ephemeral=True)
            return

        # 3. 确认环节
        embed = discord.Embed(
            title="‼️ 高危操作确认：设置最终持有者",
            description=f"你即将同步荣誉 **{honor_def.name}** 及其身份组 {role.mention}。\n"
                        f"**提供的名单将被视为唯一合法的持有者名单。**",
            color=discord.Color.red()
        )
        embed.add_field(name="✅ 新增身份组", value=f"`{len(members_to_add)}` 人", inline=True)
        embed.add_field(name="❌ 移除身份组", value=f"`{len(members_to_remove)}` 人", inline=True)
        embed.add_field(name="☑️ 保持不变", value=f"`{len(members_to_keep)}` 人", inline=True)
        embed.set_footer(text="请仔细核对，此操作不可逆！")

        if members_to_remove:
            remove_list_str = " ".join([m.mention for m in members_to_remove])
            if len(remove_list_str) > 1020:
                remove_list_str = f"共 {len(members_to_remove)} 人，列表过长已省略。"
            embed.add_field(name="将被移除身份组的成员", value=remove_list_str, inline=False)

        view = ConfirmationView(author=interaction.user, timeout=120.0)
        view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        await view.wait()

        # 4. 执行或取消
        if view.value is None: return
        if view.value is False:
            await interaction.edit_original_response(content="操作已取消。", embed=None, view=None)
            return

        await interaction.edit_original_response(content="⚙️ **正在执行同步...** 这可能需要一些时间。", embed=None, view=None)

        # 5. 执行操作并记录结果
        newly_granted, role_added_ok, role_removed_ok = 0, 0, 0
        role_add_failed, role_remove_failed = [], []

        # 授予荣誉给所有最终名单成员
        for member in definitive_members:
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted += 1

        # 添加身份组
        for member in members_to_add:
            try:
                await member.add_roles(role, reason=f"由 {interaction.user} 执行“设置持有者”操作")
                role_added_ok += 1
            except Exception:
                role_add_failed.append(member.mention)

        # 移除身份组
        for member in members_to_remove:
            try:
                await member.remove_roles(role, reason=f"由 {interaction.user} 执行“设置持有者”操作")
                role_removed_ok += 1
            except Exception:
                role_remove_failed.append(member.mention)

        # 6. 最终报告
        final_embed = discord.Embed(
            title="✅ 同步操作完成",
            description=f"已根据你的名单，完成对荣誉 **{honor_def.name}** ({role.mention}) 的持有者设置。",
            color=discord.Color.green()
        )
        final_embed.add_field(name="最终持有者总数", value=f"`{len(definitive_members)}` 人", inline=False)
        final_embed.add_field(name="新授予荣誉记录", value=f"`{newly_granted}` 人", inline=True)
        final_embed.add_field(name="新佩戴身份组", value=f"`{role_added_ok}` 人", inline=True)
        final_embed.add_field(name="被移除身份组", value=f"`{role_removed_ok}` 人", inline=True)

        if role_add_failed or role_remove_failed:
            error_details = ""
            if role_add_failed:
                error_details += f"**添加失败 ({len(role_add_failed)}人):** {' '.join(role_add_failed)}\n"
            if role_remove_failed:
                error_details += f"**移除失败 ({len(role_remove_failed)}人):** {' '.join(role_remove_failed)}"
            final_embed.add_field(name="⚠️ 操作失败详情 (通常为权限问题)", value=error_details, inline=False)

        await interaction.edit_original_response(content="", embed=final_embed)

    @cup_honor_group.command(name="重置通知状态", description="【维护】重置一个杯赛头衔的“已通知”状态，使其可以再次触发到期提醒。")
    @app_commands.describe(honor_uuid="选择要重置通知状态的杯赛头衔。")
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def reset_notification_status(self, interaction: discord.Interaction, honor_uuid: str):
        """
        允许管理员手动从 'cup_honor_notified.json' 中移除一个荣誉UUID。
        这在需要重新触发某个荣誉的到期通知时非常有用。
        """
        await interaction.response.defer(ephemeral=True)

        # 验证荣誉是否存在（可选，但推荐）
        honor_def = self.cup_honor_manager.get_cup_honor_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ **操作失败**：找不到UUID为 `{honor_uuid}` 的杯赛荣誉定义。", ephemeral=True)
            return

        # 调用 NotificationStateManager 的新方法
        was_removed = await self.notification_manager.remove_notified(honor_uuid)

        if was_removed:
            embed = discord.Embed(
                title="✅ 通知状态已重置",
                description=f"荣誉 **{honor_def.name}** 的“已通知”标记已被移除。\n"
                            f"在下一次到期检查时，如果它仍然符合过期条件，将会**重新发送通知**。",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"UUID: {honor_uuid}")
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(
                title="ℹ️ 无需操作",
                description=f"荣誉 **{honor_def.name}** 本来就**不**在已通知列表中。\n"
                            f"无需进行重置。",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"UUID: {honor_uuid}")
            await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CupHonorModuleCog(bot))
