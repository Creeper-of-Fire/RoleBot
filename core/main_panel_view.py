from __future__ import annotations

import typing

import discord
from discord import ui

if typing.TYPE_CHECKING:
    from core.CoreCog import CoreCog
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
            entries = cog.get_main_panel_entries()
            if not entries:
                continue

            # 从每个 entry 中提取按钮并添加到视图
            for entry in entries:
                self.add_item(entry.button)


def create_main_panel_ui(core_cog: CoreCog) -> tuple[discord.Embed, MainPanelView]:
    """
    构建主控制面板的 Embed 和 View。
    将所有 UI 构建逻辑封装在此处，实现职责分离。

    Args:
        core_cog: CoreCog 实例，用于获取已注册的功能模块。

    Returns:
        一个包含 (embed, view) 的元组。
    """
    # 准备描述内容的列表
    description_lines = [
        "欢迎来到身份组自助中心！",
        "在这里管理你的身份组或查询状态。\n"
    ]

    # 遍历所有功能模块，收集它们的入口
    feature_cogs: list[FeatureCog] = core_cog.feature_cogs
    for cog in feature_cogs:
        # 调用新的统一接口
        entries = cog.get_main_panel_entries()
        if not entries:
            continue

        # 遍历该 Cog 提供的所有入口
        for entry in entries:
            if entry.description:  # 只为有描述的入口生成文本
                emoji_str = f"{entry.button.emoji} " if entry.button.emoji else ""
                description_lines.append(f"**{emoji_str}{entry.button.label}**")
                # 使用 entry.description，它现在与按钮直接关联
                description_lines.append(f"{entry.description.strip()}\n")

    # 构建最终的 Embed
    embed = discord.Embed(
        title="✨ 身份组自助中心 ✨",
        description="\n".join(description_lines),
        color=discord.Color.blurple()
    )
    embed.set_footer(text="所有操作都将在只有你自己可见的消息中进行。")

    # 创建 View 实例
    view = MainPanelView(core_cog)

    # 返回构建好的 UI 组件
    return embed, view