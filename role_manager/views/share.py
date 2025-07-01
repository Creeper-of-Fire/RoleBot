from __future__ import annotations
import math
from abc import ABC, abstractmethod
from typing import List, Any, TYPE_CHECKING

import discord
from discord import ui

if TYPE_CHECKING:
    from role_manager.cog import RoleManagerCog


class PaginatedView(ui.View, ABC):
    """一个支持分页的视图基类。"""

    def __init__(self, cog: RoleManagerCog, user: discord.Member, items_per_page: int, timeout: float | None = 180.0):
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
