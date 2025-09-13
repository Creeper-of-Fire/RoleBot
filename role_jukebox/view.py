# jukebox/view.py
from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional, List

import discord
from discord import ui, Color, ButtonStyle, SelectOption, Interaction

from role_jukebox.role_jukebox_manager import Preset
from timed_role.timer import UTC8
from utility.helpers import safe_defer

if TYPE_CHECKING:
    from role_jukebox.cog import RoleJukeboxCog


class RoleJukeboxView(ui.View):
    """身份组点歌机的主交互视图。"""

    def __init__(self, cog: RoleJukeboxCog, user: discord.Member):
        super().__init__(timeout=1800)  # 30分钟超时
        self.cog = cog
        self.user = user
        self.guild = user.guild
        self.embed: Optional[discord.Embed] = None

        # 视图状态
        self.selected_queue_role_id: Optional[int] = None
        self.selected_preset: Optional[Preset] = None

    async def update_view(self, interaction: Optional[discord.Interaction] = None):
        """核心方法：重新构建整个视图和嵌入消息。"""
        self.clear_items()

        guild_config = self.cog.get_guild_config(self.guild.id)
        is_user_vip = self.cog.is_vip(self.user)

        # 1. 构建Embed
        self.embed = discord.Embed(
            title="🎶 身份组点歌机",
            description="任何人都可以随时加入/离开队列。队列锁定期间，可以通过排队来预约下一次变更。",
            color=Color.purple()
        )

        all_queues = guild_config.get("general_queue_role_ids", []) + \
                     (guild_config.get("vip_queue_role_ids", []) if is_user_vip else [])

        if not all_queues:
            self.embed.description = "本服务器尚未配置任何点歌队列。"
            if interaction: await interaction.edit_original_response(embed=self.embed, view=self)
            return

        for role_id in all_queues:
            role = self.guild.get_role(role_id)
            if not role: continue

            queue_state = self.cog.jukebox_manager.get_queue_state(self.guild.id, role_id)
            current_preset_uuid = queue_state.current_preset_uuid
            current_preset = self.cog.jukebox_manager.get_preset_by_uuid(current_preset_uuid) if current_preset_uuid else None

            value = ""
            name = f"🎵 {role.name}"

            if not current_preset:
                name = f"🎤 {role.name} (待点播)"
                value = "这个队列还未被点播过，来当第一个吧！"
            else:
                name = f"🎵 {current_preset.name}"
                value += f"**当前成员**: {len(role.members)} 人\n"

                unlock_timestamp = queue_state.unlock_timestamp
                if unlock_timestamp and datetime.fromisoformat(unlock_timestamp) > datetime.now(UTC8):
                    unlock_dt = datetime.fromisoformat(unlock_timestamp)
                    unlock_time_str = discord.utils.format_dt(unlock_dt, style='R')
                    value += f"**变更锁定**: {unlock_time_str} 解锁\n"
                    value += f"**排队人数**: {len(queue_state.pending_requests)} 人"
                else:
                    value += "✅ **变更权已解锁**，可立即变更外观！"

            self.embed.add_field(name=name, value=value, inline=False)

        self.embed.set_footer(text=f"由 {self.user.display_name} 操作")

        # 2. 添加组件
        # 2.1 队列选择器
        self.add_item(QueueSelect(all_queues, self.guild))

        # 2.2 如果已选择队列，显示更多操作
        if self.selected_queue_role_id:
            selected_queue_state = self.cog.jukebox_manager.get_queue_state(self.guild.id, self.selected_queue_role_id)

            # 预设选择器
            general_presets = self.cog.jukebox_manager.get_general_presets(self.guild.id)
            user_presets = self.cog.jukebox_manager.get_user_presets(self.user.id) if is_user_vip else []
            self.add_item(PresetSelect(general_presets, user_presets))

            # 操作按钮
            is_locked = selected_queue_state.is_locked

            is_in_role = any(r.id == self.selected_queue_role_id for r in self.user.roles)

            self.add_item(ClaimButton(disabled=is_locked))  # 只有解锁时才能变更
            self.add_item(QueueButton(disabled=not is_locked))  # 只有锁定时才能排队
            self.add_item(JoinButton(disabled=is_in_role))  # 只要不在队列里就能加入
            self.add_item(LeaveButton(disabled=not is_in_role))  # 只要在队列里就能离开

        # 3. 更新消息
        if interaction:
            await interaction.edit_original_response(content=None, embed=self.embed, view=self)


# --- Components ---

class QueueSelect(ui.Select):
    def __init__(self, queue_role_ids: List[int], guild: discord.Guild):
        options = []
        for role_id in queue_role_ids:
            role = guild.get_role(role_id)
            if role:
                options.append(SelectOption(label=f"队列: {role.name}", value=str(role_id)))

        super().__init__(placeholder="第一步: 选择一个队列...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        self.view.selected_queue_role_id = int(self.values[0])
        await self.view.update_view(interaction)


class PresetSelect(ui.Select):
    def __init__(self, general_presets: List[Preset], user_presets: List[Preset]):
        options = []
        if general_presets:
            options.append(SelectOption(label="--- 通用预设 ---", value="_disabled1"))
            for p in general_presets:
                options.append(SelectOption(label=p.name, value=p.uuid))
        if user_presets:
            options.append(SelectOption(label="--- 我的预设 ---", value="_disabled2"))
            for p in user_presets:
                options.append(SelectOption(label=p.name, value=p.uuid))

        if not options:
            options.append(SelectOption(label="没有可用的预设", value="_none"))

        super().__init__(placeholder="第二步: 选择一个身份组预设...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        preset_uuid = self.values[0]
        self.view.selected_preset = self.view.cog.jukebox_manager.get_preset_by_uuid(preset_uuid)

        if self.view.selected_preset:
            await interaction.followup.send(f"已选择预设: **{self.view.selected_preset.name}**", ephemeral=True)

        # 刷新主视图以启用/禁用按钮
        await self.view.update_view(interaction)


class ActionButton(ui.Button):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def common_checks(self, interaction: discord.Interaction) -> bool:
        if not self.view.selected_queue_role_id:
            await interaction.response.send_message("❌ 请先选择一个队列！", ephemeral=True)
            return False
        if not self.view.selected_preset and self.label in ["点播", "排队"]:
            await interaction.response.send_message("❌ 请先选择一个预设！", ephemeral=True)
            return False
        return True


class ClaimButton(ActionButton):
    def __init__(self, **kwargs):
        super().__init__(label="变更/点播", style=ButtonStyle.green, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        if not await self.common_checks(interaction): return

        # 调用Cog中的方法来处理所有逻辑
        await self.view.cog.user_claim_or_change_queue(interaction, self.view)


class QueueButton(ActionButton):
    def __init__(self, **kwargs):
        super().__init__(label="排队", style=ButtonStyle.primary, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        if not await self.common_checks(interaction): return

        success, msg = await self.view.cog.jukebox_manager.queue_request(
            self.view.guild.id, self.view.user.id, self.view.selected_queue_role_id, self.view.selected_preset
        )
        await interaction.followup.send(f"✅ {msg}" if success else f"❌ {msg}", ephemeral=True)
        await self.view.update_view(interaction)


class JoinButton(ActionButton):
    def __init__(self, **kwargs):
        super().__init__(label="加入当前", style=ButtonStyle.secondary, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        if not self.view.selected_queue_role_id:
            await interaction.response.send_message("❌ 请先选择一个队列！", ephemeral=True)
            return

        role = self.view.guild.get_role(self.view.selected_queue_role_id)
        await interaction.user.add_roles(role, reason="加入点播队列")
        await interaction.followup.send(f"✅ 已加入队列 **{role.name}**！", ephemeral=True)
        await self.view.update_view(interaction)


class LeaveButton(ActionButton):
    def __init__(self, **kwargs):
        super().__init__(label="离开队列", style=ButtonStyle.red, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        if not self.view.selected_queue_role_id:
            await interaction.response.send_message("❌ 请先选择一个队列！", ephemeral=True)
            return

        role = self.view.guild.get_role(self.view.selected_queue_role_id)
        await interaction.user.remove_roles(role, reason="离开点播队列")
        await interaction.followup.send(f"✅ 已离开队列 **{role.name}**！", ephemeral=True)
        await self.view.update_view(interaction)


class PresetEditModal(ui.Modal, title="创建/编辑身份组预设", ):
    def __init__(self, cog: 'RoleJukeboxCog', is_admin: bool, existing_preset: Optional[Preset] = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.is_admin = is_admin
        self.existing_preset = existing_preset

        self.preset_name = ui.TextInput(label="预设名称", placeholder="例如：深海之心", required=True, max_length=50,
                                        default=existing_preset.name if existing_preset else None)
        self.add_item(self.preset_name)

        self.preset_color = ui.TextInput(label="颜色 (HEX格式)", placeholder="例如：#4A90E2", required=True, min_length=7, max_length=7,
                                         default=existing_preset.color if existing_preset else None)
        self.add_item(self.preset_color)

        self.preset_icon = ui.TextInput(label="图标URL (可选)", placeholder="留空或输入 '无' 以移除图标", required=False,
                                        default=existing_preset.icon_url if existing_preset else None)
        self.add_item(self.preset_icon)

    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        # 验证颜色
        try:
            color_str = self.preset_color.value
            if not color_str.startswith("#"): color_str = f"#{color_str}"
            Color.from_str(color_str)
        except ValueError:
            await interaction.followup.send("❌ 颜色格式无效。", ephemeral=True)
            return

        icon_url_input = self.preset_icon.value
        icon_url = icon_url_input if icon_url_input and icon_url_input.lower() not in ['无', 'none'] else None

        if self.existing_preset:  # 更新模式
            # 创建一个新对象来更新，而不是修改旧的
            updated_preset = Preset(
                uuid=self.existing_preset.uuid,
                name=self.preset_name.value,
                color=color_str,
                icon_url=icon_url,
                owner_id=self.existing_preset.owner_id
            )
        else:  # 创建模式
            owner_id = None if self.is_admin else interaction.user.id
            updated_preset = Preset(
                uuid=str(uuid.uuid4()),
                name=self.preset_name.value,
                color=color_str,
                icon_url=icon_url,
                owner_id=owner_id
            )

        # PUT 操作
        success, result_msg = await self.cog.jukebox_manager.upsert_preset(
            updated_preset, guild_id=interaction.guild_id
        )

        await interaction.followup.send(result_msg, ephemeral=True)

        if success:
            # 触发实时更新
            await self.cog.live_update_role_by_preset_uuid(updated_preset.uuid)
