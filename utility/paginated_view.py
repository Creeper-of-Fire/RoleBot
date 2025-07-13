from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import List, Any, Optional, Callable, Union, Awaitable

import discord
from discord import ui


class PageJumpModal(ui.Modal, title="跳转到指定页面"):
    """一个模态框，用于让用户输入并跳转到特定页面。"""

    def __init__(self, total_pages: int):
        super().__init__(timeout=120)
        self.total_pages = total_pages
        self.jump_to_page: Optional[int] = None

        self.page_input = ui.TextInput(
            label="输入页码",
            placeholder=f"请输入 1 到 {self.total_pages} 之间的数字",
            required=True,
            min_length=1,
            max_length=len(str(self.total_pages))
        )
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction):
        """处理模态框提交事件。"""
        try:
            page_num = int(self.page_input.value)
            if 1 <= page_num <= self.total_pages:
                self.jump_to_page = page_num - 1  # 转换为 0 索引
                # 仅延迟响应，让主视图处理后续的界面更新
                await interaction.response.defer()
            else:
                # 提示错误，且仅发送给用户看
                await interaction.response.send_message(f"❌ 页码必须在 1 到 {self.total_pages} 之间。", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ 请输入有效的数字。", ephemeral=True)


class PaginatedView(ui.View, ABC):
    """
    一个通用的、功能强大的分页视图基类。
    遵循不存储 interaction 对象的最佳实践。
    """

    def __init__(self, all_items_provider: Callable[[], Union[List[Any], Awaitable[List[Any]]]], items_per_page: int, *, timeout: float | None = 300.0):
        super().__init__(timeout=timeout)
        self.all_items_provider = all_items_provider
        self.items_per_page = items_per_page

        # 分页状态
        self.all_items: List[Any] = []
        self.page = 0
        self.total_pages = 0

        # 消息引用，通过 start() 方法设置
        self.message: Optional[discord.Message] = None

        # Embed 内容，由 _rebuild_view() 设置
        self.embed: Optional[discord.Embed | List[discord.Embed]] = None

    async def _update_data(self):
        """
        调用 all_items_provider 函数获取最新数据，并更新分页状态。
        """
        # all_items_provider 可以是同步函数或异步函数
        data = self.all_items_provider()
        if isinstance(data, Awaitable):
            self.all_items = await data
        else:
            self.all_items = data

        self.total_pages = math.ceil(len(self.all_items) / self.items_per_page) if self.items_per_page > 0 else 1
        # 确保当前页码在有效范围内
        if self.page >= self.total_pages:
            self.page = self.total_pages - 1 if self.total_pages > 0 else 0

    @property
    def embeds_to_send(self) -> List[discord.Embed]:
        if self.embed is None:
            return []
        embeds_to_send = self.embed if isinstance(self.embed, list) else [self.embed]
        return embeds_to_send

    def _get_page_range(self) -> tuple[int, int]:
        """返回当前页的起始和结束索引"""
        start = self.page * self.items_per_page
        end = start + self.items_per_page
        return start, end

    def get_page_items(self):
        """返回当前页的控件内容"""
        start, end = self._get_page_range()
        return self.all_items[start:end]

    @abstractmethod
    async def _rebuild_view(self):
        """
        【子类必须实现】构建/重建视图内容和 Embed。
        此方法需要：
        1. 调用 self.clear_items()
        2. 创建并设置 self.embed
        3. 添加该页特有的组件 (Selects, Buttons, etc.)
        4. 调用 self._add_pagination_buttons() 来添加分页控件
        """
        raise NotImplementedError

    def _add_pagination_buttons(self, row: int):
        """添加分页控制按钮。"""
        if self.total_pages > 1:
            # 首页和上一页
            self.add_item(ui.Button(label="⏮️", style=discord.ButtonStyle.secondary, disabled=self.page == 0, custom_id="page_first", row=row))
            self.add_item(ui.Button(label="◀️", style=discord.ButtonStyle.primary, disabled=self.page == 0, custom_id="page_prev", row=row))

            # 页码显示和跳转
            jump_button = ui.Button(label=f"{self.page + 1}/{self.total_pages}", style=discord.ButtonStyle.secondary, disabled=self.total_pages <= 1,
                                    custom_id="page_jump", row=row)
            self.add_item(jump_button)

            # 下一页和末页
            self.add_item(ui.Button(label="▶️", style=discord.ButtonStyle.primary, disabled=self.page >= self.total_pages - 1, custom_id="page_next", row=row))
            self.add_item(
                ui.Button(label="⏭️", style=discord.ButtonStyle.secondary, disabled=self.page >= self.total_pages - 1, custom_id="page_last", row=row))

    async def _handle_pagination(self, interaction: discord.Interaction):
        """处理所有分页按钮的点击事件。"""
        custom_id = interaction.data['custom_id']

        if custom_id == "page_first":
            self.page = 0
        elif custom_id == "page_prev":
            self.page -= 1
        elif custom_id == "page_next":
            self.page += 1
        elif custom_id == "page_last":
            self.page = self.total_pages - 1
        elif custom_id == "page_jump":
            modal = PageJumpModal(self.total_pages)
            await interaction.response.send_modal(modal)
            timed_out = await modal.wait()
            if not timed_out and modal.jump_to_page is not None:
                self.page = modal.jump_to_page
                # 模态框已 defer，我们直接更新视图
                await self.update_view(interaction)
            return  # 跳转操作后提前返回，避免重复更新

        await self.update_view(interaction)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """检查是否是分页按钮的交互。"""
        custom_id = interaction.data.get('custom_id')
        if custom_id in ["page_first", "page_prev", "page_jump", "page_next", "page_last"]:
            await self._handle_pagination(interaction)
            return False  # 阻止按钮原有的 callback 执行
        return True  # 其他按钮正常执行

    async def update_view(self, interaction: discord.Interaction):
        """使用新的交互对象，重建并编辑消息。"""
        await self._update_data()
        await self._rebuild_view()
        if self.is_finished():
            await interaction.edit_original_response(content="操作已完成或超时。", view=None, embed=None)
            return

        if interaction.response.is_done():
            await interaction.edit_original_response(embeds=self.embeds_to_send, view=self)
        # 如果 interaction 已经被响应 (例如，在 modal 之后)，我们直接编辑消息
        elif self.message:
            await interaction.response.edit_message(embeds=self.embeds_to_send, view=self)

    async def start(self, interaction: discord.Interaction, ephemeral: bool = False):
        """使用初始交互对象发送消息并启动视图。"""
        await self._update_data()
        await self._rebuild_view()

        if interaction.response.is_done():
            self.message = await interaction.followup.send(embeds=self.embeds_to_send, view=self, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(embeds=self.embeds_to_send, view=self, ephemeral=ephemeral)
            self.message = await interaction.original_response()

    async def on_timeout(self):
        """超时后禁用所有组件。"""
        for item in self.children:
            item.disabled = True
        try:
            if self.message:
                await self.message.edit(view=self)
        except discord.NotFound:
            pass  # 消息可能已被删除
