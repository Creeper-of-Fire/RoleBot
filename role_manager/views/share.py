from __future__ import annotations

import asyncio
import math
from abc import ABC, abstractmethod
from typing import List, Any, TYPE_CHECKING, Optional

import discord
from discord import ui, Color
from discord.ext import commands

if TYPE_CHECKING:
    from role_manager.cog import CoreCog
    from main import RoleBot


class PaginatedView(ui.View, ABC):
    """一个支持分页的视图基类。"""

    def __init__(self, cog: FeatureCog, user: discord.Member, items_per_page: int, timeout: float | None = 180.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.user = user
        self.guild = user.guild
        self.items_per_page = items_per_page
        self.page = 0
        self.total_pages = 0
        self.all_items: List[Any] = []
        self.embed = None

    def _try_get_safe_member(self):
        member = self.guild.get_member(self.user.id)
        if not member:
            self.cog.logger.warning(f"无法在 _rebuild_view 中找到用户 {self.user.id}。")
            self.embed = discord.Embed(title="错误", description="无法加载您的信息，您可能已离开服务器。", color=Color.red())
            self.add_item(ui.Button(label="错误", style=discord.ButtonStyle.danger, disabled=True))
            self.stop()
            return None
        return member

    def get_page_range(self):
        """返回当前页的起始和结束索引"""
        start = self.page * self.items_per_page
        end = start + self.items_per_page
        return start, end

    @abstractmethod
    async def _rebuild_view(self):
        """子类必须实现此方法来构建/重建视图内容和Embed。"""
        raise NotImplementedError

    def _update_page_info(self, all_items: List[Any]):
        """根据所有项目更新分页信息。"""
        self.all_items = all_items
        self.total_pages = math.ceil(len(self.all_items) / self.items_per_page) if self.items_per_page > 0 else 1

    def _add_pagination_buttons(self, row: int):
        """添加分页控制按钮。"""
        if self.total_pages > 1:
            self.add_item(PaginationButton(label="◀️ 上一页", custom_id="page_prev", disabled=self.page == 0, row=row))
            self.add_item(ui.Button(label=f"第 {self.page + 1}/{self.total_pages} 页", style=discord.ButtonStyle.secondary, disabled=True, row=row))
            self.add_item(PaginationButton(label="下一页 ▶️", custom_id="page_next", disabled=self.page >= self.total_pages - 1, row=row))

    async def pagination_callback(self, interaction: discord.Interaction):
        """处理分页按钮的点击事件。"""
        custom_id = interaction.data['custom_id']
        if custom_id == "page_prev":
            self.page -= 1
        elif custom_id == "page_next":
            self.page += 1

        await self._rebuild_view()
        if self.is_finished():
            await interaction.response.edit_message(content="操作已完成或出现错误。", view=None, embed=None)
        else:
            await interaction.response.edit_message(embed=self.embed, view=self)


class PaginationButton(ui.Button):
    """通用的分页按钮，将回调分发给父视图的 pagination_callback 方法。"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def callback(self, interaction: discord.Interaction):
        """将交互事件分发给父视图的分页回调。"""
        if isinstance(self.view, PaginatedView):
            await self.view.pagination_callback(interaction)


class FeatureCog(commands.Cog, ABC):
    """
    功能模块Cog的基类。

    自动处理 bot 和 logger 的初始化，并提供向 CoreCog 注册的标准流程。
    所有继承此基类的 Cog 都必须实现 `update_safe_roles_cache` 方法。
    """

    def __init__(self, bot: 'RoleBot'):
        """
        初始化基类，设置 bot 和 logger 实例。
        """
        self.bot = bot
        self.logger = bot.logger

    @property
    def core_cog(self) -> CoreCog | None:
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        return core_cog

    @property
    def role_name_cache(self) -> dict[int, str] | None:
        core_cog: CoreCog | None = self.core_cog
        if (core_cog is None) or (core_cog.role_name_cache is None):
            return None
        return core_cog.role_name_cache

    async def cog_load(self) -> None:
        """
        当Cog被加载时，等待并向 CoreCog 注册自己。
        子类如果需要覆盖此方法，必须调用 `await super().cog_load()`。
        """
        # 短暂等待，以确保 CoreCog 已经加载完毕
        await asyncio.sleep(1)

        core_cog: CoreCog | None = self.bot.get_cog("Core")

        if core_cog:
            # 调用 CoreCog 的注册方法，把自己传进去
            core_cog.register_feature_cog(self)
        else:
            self.logger.error(f"无法找到 CoreCog。模块 {self.qualified_name} 的功能将受限，无法自动更新缓存。")

    @abstractmethod
    async def update_safe_roles_cache(self):
        """
        【抽象接口方法】更新本模块的安全身份组缓存。

        CoreCog 会周期性地调用此方法。子类必须实现此方法，
        以定义如何从配置中读取身份组、检查其安全性，并更新自己的内部缓存。

        实现示例:
            core_cog = self.bot.get_cog("Core")
            # ... 遍历配置 ...
            # ... 检查角色危险性 ...
            # ... 更新 self.safe_..._cache ...
            # ... 更新 core_cog.role_name_cache ...
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 update_safe_roles_cache 方法。")

    # ===================================================================
    # 新增的抽象方法
    # ===================================================================
    @abstractmethod
    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        """
        【抽象接口方法】返回一个用于主控制面板的入口按钮。

        如果此模块没有主面板入口，则应返回 None。
        按钮的回调逻辑应在该模块的 Cog 或 View 中定义。

        返回:
            List[discord.ui.Button] | None: 代表此模块入口的按钮实例，或 None。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 get_main_panel_button 方法。")
