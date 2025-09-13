# jukebox/view.py
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, List

import discord
from discord import ui, Color, ButtonStyle, SelectOption

from role_jukebox.role_jukebox_manager import Preset
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
            # description 会在下面动态构建
            color=Color.purple()
        )

        general_queue_ids = guild_config.get("general_queue_role_ids", [])
        vip_queue_ids = guild_config.get("vip_queue_role_ids", []) if is_user_vip else []
        all_queues_ids = general_queue_ids + vip_queue_ids

        description_parts = ["任何人都可以随时加入/离开队列。队列锁定期间，可以通过排队来预约下一次变更。"]

        if not all_queues_ids:
            self.embed.description = "本服务器尚未配置任何点歌队列。"
            if interaction: await interaction.edit_original_response(embed=self.embed, view=self)
            return

        user_role_ids = {r.id for r in self.user.roles}

        if self.selected_queue_role_id:
            selected_role = self.guild.get_role(self.selected_queue_role_id)
            if selected_role:
                description_parts.append(f"**📍 已选择队列**: {selected_role.mention}")


        if self.selected_preset:
            description_parts.append(f"**🎯 已选择预设**: `{self.selected_preset.name}`")

        description_parts.append("\n" + "—" * 25 + "\n")  # 分隔符

        # 循环构建每个队列的显示信息
        queue_descriptions = []

        def build_queue_description(tmp_role_id: int):
            role = self.guild.get_role(tmp_role_id)
            if not role: return None

            queue_state = self.cog.jukebox_manager.get_queue_state(self.guild.id, tmp_role_id)
            current_preset_uuid = queue_state.current_preset_uuid
            current_preset = self.cog.jukebox_manager.get_preset_by_uuid(current_preset_uuid) if current_preset_uuid else None

            queue_lines = []

            # 添加用户加入状态的上下文标注
            is_joined_marker = " ▸ *已加入*" if role.id in user_role_ids else ""

            selection_marker = "➡️ " if role.id == self.selected_queue_role_id else ""

            if not current_preset:
                # 状态：待点播。显示身份组自身的名字和图标
                header = f"{selection_marker}🎤 {role.mention}{is_joined_marker}"
                if role.icon:
                    header += f" | [图标]({role.icon.url})"

                queue_lines.append(header)
                queue_lines.append("> *这个队列当前可被任何人点播*")
            else:
                # 状态：已被点播。显示预设的名字，但使用身份组的mention，并显示预设的图标
                header = f"{selection_marker}🎵 {role.mention}{is_joined_marker}"
                if current_preset.icon_url:
                    header += f" | [图标]({current_preset.icon_url})"

                queue_lines.append(header)

            queue_lines.append(f"> **当前成员**: {len(role.members)} 人")
            if queue_state.is_locked:
                unlock_dt = datetime.fromisoformat(queue_state.unlock_timestamp)
                unlock_time_str = discord.utils.format_dt(unlock_dt, style='R')
                queue_lines.append(f"> **变更锁定**: {unlock_time_str} 解锁")
                queue_lines.append(f"> **排队人数**: {len(queue_state.pending_requests)} 人")
            else:
                queue_lines.append(f"> ✅ **变更权已解锁**，可立即变更外观！")

            return "\n".join(queue_lines)

        # 处理通用队列
        for role_id in general_queue_ids:
            desc = build_queue_description(role_id)
            if desc: queue_descriptions.append(desc)

        # 为VIP用户添加分隔符
        if is_user_vip and general_queue_ids and vip_queue_ids:
            queue_descriptions.append("— ✨ **尊贵的 VIP 专属队列** —")

        # 处理VIP队列
        for role_id in vip_queue_ids:
            desc = build_queue_description(role_id)
            if desc: queue_descriptions.append(desc)

        description_parts.extend(queue_descriptions)
        self.embed.description = "\n\n".join(description_parts)  # 用两个换行符分隔每个队列块

        self.embed.set_footer(text=f"由 {self.user.display_name} 操作")

        # 2. 添加组件
        # 2.1 队列选择器
        self.add_item(QueueSelect(all_queues_ids, self.guild))

        # 2.2 如果已选择队列，显示更多操作
        if self.selected_queue_role_id:
            selected_queue_state = self.cog.jukebox_manager.get_queue_state(self.guild.id, self.selected_queue_role_id)

            # 预设选择器
            general_presets = self.cog.jukebox_manager.get_general_presets(self.guild.id)
            user_presets = self.cog.jukebox_manager.get_user_presets(self.user.id) if is_user_vip else []
            self.add_item(PresetSelect(general_presets, user_presets, is_user_vip))

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
    def __init__(self, general_presets: List[Preset], user_presets: List[Preset], is_user_vip: bool):
        options = []

        # 对于VIP用户，使用分组标题来区分预设
        if is_user_vip:
            if general_presets:
                options.append(SelectOption(label="--- 通用预设 ---", value="_disabled1"))
                for p in general_presets:
                    options.append(SelectOption(label=p.name, value=p.uuid))
            if user_presets:
                options.append(SelectOption(label="--- 我的预设 ---", value="_disabled2"))
                for p in user_presets:
                    options.append(SelectOption(label=p.name, value=p.uuid))
        # 对于普通用户，直接展示预设列表，不加任何分组标题
        else:
            for p in general_presets:
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
        super().__init__(label="离开", style=ButtonStyle.red, **kwargs)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        if not self.view.selected_queue_role_id:
            await interaction.response.send_message("❌ 请先选择一个队列！", ephemeral=True)
            return

        role = self.view.guild.get_role(self.view.selected_queue_role_id)
        await interaction.user.remove_roles(role, reason="离开点播队列")
        await interaction.followup.send(f"✅ 已离开队列 **{role.name}**！", ephemeral=True)
        await self.view.update_view(interaction)
