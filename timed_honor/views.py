from __future__ import annotations

from typing import TYPE_CHECKING, List

import discord
from discord import ui

if TYPE_CHECKING:
    from .cog import TimedHonorCog


OPEN_PANEL_BUTTON_CUSTOM_ID = "timed_honor:open_upgrade_panel"
SELECT_HONOR_CUSTOM_ID = "timed_honor:select_honor"


class TimedHonorUserBoundView(ui.View):
    """仅允许指定用户操作的临时视图（ephemeral 场景）。"""

    def __init__(self, cog: "TimedHonorCog", owner_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await self.cog.send_ephemeral(interaction, "⚠️ 这不是你的面板，请点击你自己的入口按钮打开。")
            return False
        return True


class TimedHonorPublicOpenPanelView(ui.View):
    """公共消息上的持久化入口按钮。"""

    def __init__(self, cog: "TimedHonorCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(
        label="打开荣誉升级面板",
        style=discord.ButtonStyle.primary,
        emoji="🏅",
        custom_id=OPEN_PANEL_BUTTON_CUSTOM_ID,
    )
    async def open_upgrade_panel(self, interaction: discord.Interaction, _: ui.Button):
        await self.cog.handle_open_panel(interaction)


class TimedHonorSelect(ui.Select):
    def __init__(self, cog: "TimedHonorCog", options: List[discord.SelectOption]):
        super().__init__(
            placeholder="选择一个限时荣誉查看详情...",
            options=options,
            min_values=1,
            max_values=1,
            custom_id=SELECT_HONOR_CUSTOM_ID,
        )
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        if not self.values:
            await self.cog.send_ephemeral(interaction, "⚠️ 未选择任何荣誉。")
            return
        await self.cog.handle_select_honor(interaction, self.values[0])


class TimedHonorSelectView(TimedHonorUserBoundView):
    """荣誉选择列表视图。"""

    def __init__(self, cog: "TimedHonorCog", owner_id: int, options: List[discord.SelectOption]):
        super().__init__(cog=cog, owner_id=owner_id, timeout=300)
        self.add_item(TimedHonorSelect(cog, options))


class TimedHonorClaimButton(ui.Button):
    def __init__(self, cog: "TimedHonorCog", honor_uuid: str):
        super().__init__(
            label="领取并佩戴",
            style=discord.ButtonStyle.success,
            custom_id=f"timed_honor:claim:{honor_uuid}",
        )
        self.cog = cog
        self.honor_uuid = honor_uuid

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_claim_honor(interaction, self.honor_uuid)


class TimedHonorToggleWearButton(ui.Button):
    def __init__(self, cog: "TimedHonorCog", honor_uuid: str, is_wearing: bool):
        super().__init__(
            label="摘下身份组" if is_wearing else "佩戴身份组",
            style=discord.ButtonStyle.secondary,
            custom_id=f"timed_honor:toggle:{honor_uuid}",
            emoji="🧷",
        )
        self.cog = cog
        self.honor_uuid = honor_uuid

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_toggle_wear(interaction, self.honor_uuid)


class TimedHonorDetailView(TimedHonorUserBoundView):
    """单个荣誉详情页视图。"""

    def __init__(self, cog: "TimedHonorCog", owner_id: int, honor_uuid: str, owned: bool, is_wearing: bool):
        super().__init__(cog=cog, owner_id=owner_id, timeout=300)

        if owned:
            self.add_item(TimedHonorToggleWearButton(cog, honor_uuid, is_wearing=is_wearing))
        else:
            self.add_item(TimedHonorClaimButton(cog, honor_uuid))
