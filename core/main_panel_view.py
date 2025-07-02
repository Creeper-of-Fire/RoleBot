from __future__ import annotations

import typing

from discord import ui

if typing.TYPE_CHECKING:
    from core.cog import CoreCog
    from utility.feature_cog import FeatureCog


class MainPanelView(ui.View):
    """
    主控制面板的视图，包含所有主要功能的入口按钮。
    它会自动从所有已注册的 FeatureCog 中收集入口按钮。
    """

    def __init__(self, core_cog: CoreCog):
        super().__init__(timeout=None)
        self.core_cog = core_cog

        # 动态添加所有功能模块的按钮
        feature_cogs: list[FeatureCog] = self.core_cog.feature_cogs
        for cog in feature_cogs:
            buttons = cog.get_main_panel_buttons()
            if not buttons:
                continue
            for button in buttons:
                self.add_item(button)
