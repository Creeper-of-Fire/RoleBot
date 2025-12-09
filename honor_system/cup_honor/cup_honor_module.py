# honor_system/cup_honor_module.py
from __future__ import annotations

import datetime
import json
import logging
import os
import re
import threading
import typing
import uuid
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from zoneinfo import ZoneInfo

import discord
from discord import app_commands, ui
from discord.ext import commands, tasks
from pydantic import ValidationError

import config_data
from utility.paginated_view import PaginatedView
from utility.views import ConfirmationView
from .cup_honor_json_manager import CupHonorJsonManager
from .cup_honor_models import CupHonorDefinition, CupHonorDetails
from honor_system.honor_data_manager import HonorDataManager
from honor_system.models import UserHonor, HonorDefinition

if typing.TYPE_CHECKING:
    from main import RoleBot

DATA_FILE_PATH = os.path.join('data', 'cup_honor_notified.json')


class NotificationStateManager:
    """
    一个单例类，用于管理已发送通知的杯赛荣誉状态，并将其持久化到JSON文件中。
    """
    _instance = None
    _lock = threading.Lock()

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.notified_uuids: set[str] = set()
        self._ensure_data_file()
        self.load_state()

    @classmethod
    def get_instance(cls, logger: logging.Logger) -> 'NotificationStateManager':
        """获取本类的单例实例。"""
        if cls._instance is None:
            if cls._instance is None:
                cls._instance = cls(logger)
        return cls._instance

    def _ensure_data_file(self):
        """确保数据文件和目录存在。"""
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
        if not os.path.exists(DATA_FILE_PATH):
            with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump([], f)  # 初始为空列表

    def load_state(self):
        """从JSON文件加载已通知的UUID列表。"""
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.notified_uuids = set(data)
                    self.logger.info(f"成功从 {DATA_FILE_PATH} 加载了 {len(self.notified_uuids)} 条已通知荣誉记录。")
            except (IOError, json.JSONDecodeError) as e:
                self.logger.error(f"无法加载杯赛荣誉通知状态: {e}", exc_info=True)
                self.notified_uuids = set()

    def _save_state(self):
        """将当前状态保存到JSON文件。"""
        with self._lock:
            try:
                with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
                    # JSON不支持set，需要转换为list
                    json.dump(list(self.notified_uuids), f, indent=4)
            except IOError as e:
                self.logger.error(f"无法保存杯赛荣誉通知状态: {e}", exc_info=True)

    def add_notified(self, honor_uuid: str):
        """将一个UUID标记为已通知，并立即保存。"""
        if honor_uuid not in self.notified_uuids:
            self.notified_uuids.add(honor_uuid)
            self._save_state()
            self.logger.info(f"已将荣誉 {honor_uuid} 标记为已通知并持久化。")

    def has_been_notified(self, honor_uuid: str) -> bool:
        """检查一个UUID是否已被通知。"""
        return honor_uuid in self.notified_uuids


class CupHonorEditModal(ui.Modal):
    """一个用于通过JSON编辑杯赛荣誉的模态框"""

    def __init__(self, cog: 'CupHonorModuleCog', guild_id: int, parent_view: 'CupHonorManageView', honor_def: Optional[CupHonorDefinition] = None):
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view  # 保存父视图实例
        self.original_uuid = str(honor_def.uuid) if honor_def else None
        self.is_new = honor_def is None

        super().__init__(title="编辑杯赛荣誉 (JSON)" if not self.is_new else "新增杯赛荣誉 (JSON)", timeout=1200)

        # 生成模板或现有数据的JSON
        if self.is_new:
            # 创建一个带新UUID的模板
            template_def = CupHonorDefinition(
                uuid=uuid.uuid4(),
                name="新杯赛荣誉",
                description="请填写描述",
                role_id=123456789012345678,
                cup_honor=CupHonorDetails(
                    expiration_date=datetime.datetime.now(ZoneInfo("Asia/Shanghai")) + datetime.timedelta(days=30)
                )
            )
            json_text = json.dumps(template_def.model_dump(mode='json'), indent=4, ensure_ascii=False)
        else:
            json_text = json.dumps(honor_def.model_dump(mode='json'), indent=4, ensure_ascii=False)

        self.json_input = ui.TextInput(
            label="荣誉定义 (JSON格式)",
            style=discord.TextStyle.paragraph,
            default=json_text,
            required=True,
            min_length=50
        )
        self.add_item(self.json_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        json_str = self.json_input.value

        # 1. 校验JSON格式
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            await interaction.followup.send(f"❌ **JSON格式错误！**\n请检查你的语法，错误信息: `{e}`", ephemeral=True)
            return

        # 2. Pydantic模型验证
        try:
            new_honor_def = CupHonorDefinition.model_validate(data)
        except ValidationError as e:
            error_details = "\n".join([f"- `{' -> '.join(map(str, err['loc']))}`: {err['msg']}" for err in e.errors()])
            await interaction.followup.send(f"❌ **数据校验失败！**\n请根据以下提示修改：\n{error_details}", ephemeral=True)
            return

        # 3. 唯一性校验 (UUID和名称)
        new_uuid_str = str(new_honor_def.uuid)
        new_name = new_honor_def.name

        # 检查点: 与配置文件中的普通荣誉冲突
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        for config_honor in guild_config.get("definitions", []):
            # 如果是编辑操作，需要排除掉自身
            if self.original_uuid and self.original_uuid == config_honor['uuid']:
                continue
            if config_honor['uuid'] == new_uuid_str:
                await interaction.followup.send(
                    f"❌ **操作被阻止！**\n此UUID (`{new_uuid_str[:8]}...`) 被核心荣誉 **“{config_honor['name']}”** 所保留。\n"
                    "杯赛荣誉系统不能修改由机器人配置文件定义的荣誉。请在JSON中更换一个新的UUID。",
                    ephemeral=True
                )
                return

        # 直接查询数据库，检查是否存在任何同名但UUID不同的荣誉（包括已归档的）
        with self.cog.honor_data_manager.get_db() as db:
            # 在执行操作前，精确判断最终的操作类型
            action_text = ""
            existing_record_for_uuid = db.query(HonorDefinition).filter_by(uuid=new_uuid_str).one_or_none()

            if self.is_new:
                # 从“新增”流程开始
                action_text = "覆盖" if existing_record_for_uuid else "创建"
            else:
                # 从“编辑”流程开始
                action_text = "更新"

        # 4. 同步到主荣誉数据库
        try:
            await self.cog.sync_cup_honor_to_db(self.guild_id, new_honor_def, self.original_uuid)
        except Exception as e:
            self.cog.logger.error(f"同步杯赛荣誉到数据库时出错: {e}", exc_info=True)
            await interaction.followup.send(f"❌ **数据库同步失败！**\n在更新主荣誉表时发生错误: `{e}`", ephemeral=True)
            return

        # 5. 保存到JSON文件
        # 如果是编辑且UUID变了，需要先删除旧的记录
        if self.original_uuid and self.original_uuid != new_uuid_str:
            self.cog.cup_honor_manager.delete_cup_honor(self.original_uuid)
        self.cog.cup_honor_manager.add_or_update_cup_honor(new_honor_def)

        # 6. 反馈
        embed = discord.Embed(
            title=f"✅ 成功{action_text}杯赛荣誉",
            description=f"已成功{action_text}荣誉 **{new_honor_def.name}**。",
            color=discord.Color.green()
        )
        embed.add_field(name="UUID", value=f"`{new_honor_def.uuid}`", inline=False)
        embed.add_field(name="关联身份组", value=f"<@&{new_honor_def.role_id}>", inline=True)
        embed.add_field(name="过期时间", value=f"<t:{int(new_honor_def.cup_honor.expiration_date.timestamp())}:F>", inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

        # 刷新管理面板
        await self.parent_view.refresh_panel()


class CupHonorManageView(PaginatedView):
    def __init__(self, cog: 'CupHonorModuleCog'):
        self.cog = cog

        # 定义一个函数，用于获取并按【过期时间】排序所有荣誉数据
        def data_provider():
            all_honors = self.cog.cup_honor_manager.get_all_cup_honors()
            # 按 expiration_date 降序排序 (最新/最晚到期的在前面)
            return sorted(all_honors, key=lambda h: h.cup_honor.expiration_date, reverse=True)

        # 调用父类的构造函数
        super().__init__(
            all_items_provider=data_provider,
            items_per_page=20,
            timeout=300
        )

    async def _rebuild_view(self):
        """
        【实现PaginatedView的抽象方法】
        根据当前页的数据，重建视图的UI组件和Embed。
        """
        # 1. 清空所有旧的组件
        self.clear_items()

        # 2. 创建并设置Embed
        self.embed = self.create_embed()

        # 3. 获取当前页要显示的荣誉
        current_page_honors: List[CupHonorDefinition] = self.get_page_items()

        # 4. 根据当前页的荣誉创建下拉菜单
        if current_page_honors:
            # 创建选项
            options = [
                discord.SelectOption(
                    label=f"{honor.name}",
                    description=f"过期: {honor.cup_honor.expiration_date.strftime('%Y-%m-%d')} | UUID: {str(honor.uuid)[:8]}...",
                    value=str(honor.uuid)
                ) for honor in current_page_honors
            ]

            # 编辑下拉菜单
            select_edit = ui.Select(placeholder="选择本页一个荣誉进行编辑...", options=options, custom_id="cup_honor_edit_select", row=0)
            select_edit.callback = self.on_edit_select
            self.add_item(select_edit)

            # 删除下拉菜单
            select_delete = ui.Select(placeholder="选择本页一个或多个荣誉进行删除...", options=options, custom_id="cup_honor_delete_select",
                                      max_values=len(options), row=1)
            select_delete.callback = self.on_delete_select
            self.add_item(select_delete)

        # 5. 添加不受分页影响的按钮
        button_add = ui.Button(label="➕ 新增荣誉", style=discord.ButtonStyle.success, custom_id="cup_honor_add", row=2)
        button_add.callback = self.on_add_button
        self.add_item(button_add)

        # 6. 添加分页控制按钮
        self._add_pagination_buttons(row=4)

    async def refresh_panel(self):
        """
        在不依赖特定交互对象的情况下，刷新视图自身附着的消息。
        主要由模态框回调等外部操作调用。
        """
        if not self.message:
            return

        # 调用 PaginatedView 的内部方法来更新数据和UI
        await self._update_data()
        await self._rebuild_view()

        try:
            # 使用 self.embeds_to_send 获取要发送的embed列表
            await self.message.edit(embeds=self.embeds_to_send, view=self)
        except discord.NotFound:
            self.cog.logger.warning(f"无法刷新杯赛荣誉管理面板，消息 {self.message.id} 可能已被删除。")
        except Exception as e:
            self.cog.logger.error(f"刷新荣誉管理视图时出错: {e}", exc_info=True)

    def create_embed(self) -> discord.Embed:
        embed = discord.Embed(title="杯赛荣誉管理面板 (JSON)", color=discord.Color.blue())
        embed.description = (
            "通过下方的控件来 **编辑**、**新增** 或 **删除** 杯赛荣誉。\n"
            "所有操作都将通过一个 **JSON编辑器** 完成，请谨慎操作。\n\n"
            "**操作指南:**\n"
            "1.  **编辑**: 从下拉菜单中选择一个现有荣誉，会弹出其JSON配置供您修改。\n"
            "2.  **新增**: 点击`新增荣誉`按钮，会弹出一个包含模板的JSON编辑器。\n"
            "3.  **删除**: 从下拉菜单中选择要删除的荣誉，点击后会要求确认。\n"
            "4.  **UUID**: 创建时会自动生成，**可以修改**，但必须是有效的UUID格式且全局唯一。\n"
            "5.  **AI辅助**: 如果不熟悉JSON，可以将模板或现有数据粘贴给AI，告诉它你的修改需求，然后将结果粘贴回来。"
        )
        if not self.all_items:
            embed.add_field(name="当前荣誉列表", value="*暂无杯赛荣誉定义。*", inline=False)
        else:
            honor_list_str = "\n".join([f"- **{h.name}** (`{str(h.uuid)[:8]}`...)" for h in self.get_page_items()])
            embed.add_field(name=f"当前荣誉列表 (共 {len(self.get_page_items())}/{len(self.all_items)} 个)", value=honor_list_str, inline=False)
        return embed

    async def on_edit_select(self, interaction: discord.Interaction):
        uuid_to_edit = interaction.data['values'][0]
        honor_def = self.cog.cup_honor_manager.get_cup_honor_by_uuid(uuid_to_edit)
        if not honor_def:
            await interaction.response.send_message("❌ 错误：找不到该荣誉，可能已被删除。", ephemeral=True)
            await self.refresh_panel()
            return

        modal = CupHonorEditModal(self.cog, interaction.guild_id, self, honor_def)
        await interaction.response.send_modal(modal)

    async def on_add_button(self, interaction: discord.Interaction):
        modal = CupHonorEditModal(self.cog, interaction.guild_id, self)
        await interaction.response.send_modal(modal)

    async def on_delete_select(self, interaction: discord.Interaction):
        uuids_to_delete = interaction.data['values']
        if not uuids_to_delete:
            await interaction.response.defer()
            return

        names_to_delete = []
        for uuid_str in uuids_to_delete:
            honor = self.cog.cup_honor_manager.get_cup_honor_by_uuid(uuid_str)
            if honor:
                names_to_delete.append(honor.name)

        confirm_view = ConfirmationView(interaction.user)
        await interaction.response.send_message(
            f"⚠️ **确认删除？**\n你即将删除以下 **{len(names_to_delete)}** 个荣誉：\n- " + "\n- ".join(names_to_delete) +
            "\n\n此操作会从JSON配置中移除它们，并**归档**其在数据库中的主定义（用户已获得的记录会保留，但荣誉将不再可用）。**此操作不可逆！**",
            view=confirm_view,
            ephemeral=True
        )
        await confirm_view.wait()

        if confirm_view.value:
            deleted_count = 0
            for uuid_str in uuids_to_delete:
                # 归档数据库记录
                await self.cog.archive_honor_in_db(uuid_str)
                # 从JSON删除
                if self.cog.cup_honor_manager.delete_cup_honor(uuid_str):
                    deleted_count += 1

            await interaction.edit_original_response(content=f"✅ 成功删除 {deleted_count} 个荣誉。", view=None)
            await self.refresh_panel()
        else:
            await interaction.edit_original_response(content="操作已取消。", view=None)


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
                self.notification_manager.add_notified(honor_uuid)

    async def _notify_admin_for_expired_honor(self, guild: discord.Guild, honor_uuid: str, exp_date: datetime.datetime,
                                              notify_cfg: dict):
        """为单个过期的荣誉构建并发送通知。
        此版本逻辑基于数据库记录，并确保即使没有成员佩戴身份组也会发送通知。
        """
        # 1. 获取荣誉定义
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def or not honor_def.role_id:
            self.logger.warning(f"荣誉 {honor_uuid} 定义无效或未关联身份组，无法发送到期通知。")
            return

        role = guild.get_role(honor_def.role_id)

        # 2. 从数据库获取所有拥有此荣誉的用户，并检查哪些人仍佩戴对应身份组
        members_to_action = []
        with self.honor_data_manager.get_db() as db:
            # 查找所有被授予该荣誉的用户记录
            user_honor_records = db.query(UserHonor).filter(UserHonor.honor_uuid == honor_uuid).all()

            # 仅当身份组实际存在时，才检查哪些成员仍需处理
            if role:
                for record in user_honor_records:
                    member = guild.get_member(record.user_id)
                    # 检查成员是否仍在服务器且拥有该身份组
                    if member and role in member.roles:
                        members_to_action.append(member)

        # 3. 获取通知所需的对象
        notification_channel = guild.get_channel(notify_cfg["channel_id"]) or await guild.fetch_channel(notify_cfg["channel_id"])
        admin_role = guild.get_role(notify_cfg["admin_role_id"])

        if not notification_channel or not admin_role:
            self.logger.error(f"无法在服务器 {guild.name} 中找到通知频道或管理员身份组。")
            return

        # 4. 构建并发送通知 (无论是否有人需要处理)
        embed = discord.Embed(
            title="🏆 杯赛头衔身份组到期提醒",
            color=discord.Color.orange()
        )
        embed.set_footer(text=f"荣誉: {honor_def.name} | UUID: {honor_uuid}")

        # 根据是否有人需要处理来定制消息
        if members_to_action:
            embed.description = (
                f"以下成员佩戴的荣誉身份组 {role.mention} "
                f"已于 `{exp_date.strftime('%Y-%m-%d')}` 到期。\n"
                f"请管理员手动移除他们的身份组，其荣誉勋章将被永久保留。"
            )
            member_mentions = " ".join([m.mention for m in members_to_action])
            embed.add_field(name="需要处理的成员列表", value=member_mentions, inline=False)
        else:
            role_mention = role.mention if role else f"`{honor_def.name}` (身份组可能已被删除)"
            embed.description = (
                f"荣誉 **{honor_def.name}** (关联身份组: {role_mention}) "
                f"已于 `{exp_date.strftime('%Y-%m-%d')}` 到期。"
            )
            embed.add_field(
                name="状态检查",
                value="根据数据库记录，当前没有成员佩戴此身份组。",
                inline=False
            )
            embed.add_field(
                name="建议操作",
                value="管理员可以考虑从服务器的身份组列表中删除此身份组，以保持列表整洁。",
                inline=False
            )

        try:
            await notification_channel.send(content=admin_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[admin_role]))
            self.logger.info(f"已在服务器 {guild.name} 发送关于荣誉 {honor_def.name} 的到期通知。")
        except discord.Forbidden:
            self.logger.error(f"无法在频道 {notification_channel.name} 发送通知，权限不足。")

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

        # 1. 验证荣誉和身份组
        honor_def = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ **错误**：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        if not honor_def.role_id:
            await interaction.followup.send(f"❌ **操作失败**：荣誉 **{honor_def.name}** 没有关联任何身份组，无法进行同步。", ephemeral=True)
            return

        role = guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(f"❌ **错误**：在服务器中找不到与荣誉关联的身份组（ID: {honor_def.role_id}）。", ephemeral=True)
            return

        # 2. 获取成员并处理
        members_with_role = role.members
        if not members_with_role:
            await interaction.followup.send(f"🤷 **无需操作**：没有找到任何成员拥有 {role.mention} 身份组。", ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())
            return

        newly_granted_count = 0
        already_had_count = 0

        for member in members_with_role:
            if member.bot:
                continue

            # grant_honor 方法如果成功授予则返回定义，如果已存在则返回None
            if self.honor_data_manager.grant_honor(member.id, honor_uuid):
                newly_granted_count += 1
            else:
                already_had_count += 1

        self.logger.info(
            f"管理员 {interaction.user} 在服务器 {guild.name} "
            f"对荣誉 '{honor_def.name}' 执行了从身份组同步操作。 "
            f"新授予: {newly_granted_count}, 已拥有: {already_had_count}."
        )

        # 3. 发送报告
        embed = discord.Embed(
            title="✅ 荣誉同步完成",
            description=f"已为所有拥有 {role.mention} 身份组的成员检查并补发了荣誉 **{honor_def.name}**。",
            color=discord.Color.green()
        )
        embed.add_field(name="总共检查成员", value=f"`{len(members_with_role)}` 人", inline=True)
        embed.add_field(name="新授予荣誉", value=f"`{newly_granted_count}` 人", inline=True)
        embed.add_field(name="本就拥有荣誉", value=f"`{already_had_count}` 人", inline=True)
        embed.set_footer(text="此操作确保了所有拥有身份组的成员都在荣誉系统中正确记录。")

        await interaction.followup.send(embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

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


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(CupHonorModuleCog(bot))
