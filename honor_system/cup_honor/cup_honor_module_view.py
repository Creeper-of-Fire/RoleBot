from __future__ import annotations

import datetime
import json
import typing
import uuid
from typing import Optional, List
from zoneinfo import ZoneInfo

import discord
from discord import ui
from pydantic import ValidationError

import config_data
from honor_system.cup_honor.cup_honor_models import CupHonorDefinition, CupHonorDetails
from honor_system.models import HonorDefinition
from utility.paginated_view import PaginatedView
from utility.views import ConfirmationView
if typing.TYPE_CHECKING:
    from honor_system.cup_honor.cup_honor_module import CupHonorModuleCog


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
