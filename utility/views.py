from __future__ import annotations

from typing import Optional

import discord
from discord import ui


class ConfirmationView(ui.View):
    def __init__(self, author: discord.User, timeout=60.0):
        super().__init__(timeout=timeout)
        self.value: Optional[bool] = None
        self.author = author

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """确保只有命令发起者可以点击按钮。"""
        if interaction.user.id != self.author.id:
            await interaction.response.send_message("❌ 你不能操作这个按钮。", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        """超时后禁用所有按钮。"""
        for item in self.children:
            item.disabled = True
        # 如果 self.message 存在，可以编辑原始消息
        if hasattr(self, 'message') and self.message:
            await self.message.edit(view=self)

    @ui.button(label="确认", style=discord.ButtonStyle.danger, custom_id="confirm_delete")
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        self.value = True
        self.stop()  # 停止视图的监听
        # 禁用按钮，提供视觉反馈
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)

    @ui.button(label="取消", style=discord.ButtonStyle.secondary, custom_id="cancel_delete")
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        self.value = False
        self.stop()
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
