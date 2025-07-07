# activity_tracker/views.py
from __future__ import annotations

import typing
from datetime import datetime, timedelta

import discord
from discord import ui

from activity_tracker.data_manager import BEIJING_TZ
from activity_tracker.logic import UserReportData, SortedDisplayItem

if typing.TYPE_CHECKING:
    from .cog import TrackActivityCog

# --- 常量定义 ---
MAX_CHANNELS_PER_PAGE = 10
HEATMAP_STEP1 = 1
HEATMAP_STEP2 = 30
HEATMAP_STEP3 = 75
HEATMAP_STEP4 = 150
HEATMAP_EMOJIS = {0: '⬜', HEATMAP_STEP1: '🟩', HEATMAP_STEP2: '🟦', HEATMAP_STEP3: '🟨', HEATMAP_STEP4: '🟥'}
HEATMAP_THRESHOLDS = sorted(HEATMAP_EMOJIS.keys())


class ReportEmbeds:
    """
    【新增】一个专门用于创建报告 Embed 的辅助类。
    将显示逻辑 (渲染) 从 Cog 中完全分离。
    """

    @staticmethod
    def _render_heatmap_text(heatmap_data: dict[str, int], days_window: int) -> str:
        if not heatmap_data and days_window > 0: return "暂无消息记录。"

        today_utc8 = datetime.now(BEIJING_TZ)
        heatmap_output = []
        date_range = [today_utc8 - timedelta(days=i) for i in range(days_window - 1, -1, -1)]

        for i, current_date in enumerate(date_range):
            if i % 7 == 0:
                if i != 0: heatmap_output.append("\n")
                heatmap_output.append(f"`{current_date.strftime('%m-%d')}`: ")

            date_str = current_date.strftime('%Y-%m-%d')
            count = heatmap_data.get(date_str, 0)
            emoji = HEATMAP_EMOJIS[0]
            for threshold in reversed(HEATMAP_THRESHOLDS):
                if count >= threshold:
                    emoji = HEATMAP_EMOJIS[threshold]
                    break
            heatmap_output.append(emoji)

        if not heatmap_output: return "暂无消息记录。"

        legend = (f"**图例:** "
                  f"{HEATMAP_EMOJIS[0]} 0 "
                  f"{HEATMAP_EMOJIS[HEATMAP_STEP1]} {HEATMAP_STEP1}-{HEATMAP_STEP2 - 1} "
                  f"{HEATMAP_EMOJIS[HEATMAP_STEP2]} {HEATMAP_STEP2}-{HEATMAP_STEP3 - 1} "
                  f"{HEATMAP_EMOJIS[HEATMAP_STEP3]} {HEATMAP_STEP3}-{HEATMAP_STEP4 - 1} "
                  f"{HEATMAP_EMOJIS[HEATMAP_STEP4]} {HEATMAP_STEP4}+")
        return "\n" + "".join(heatmap_output) + "\n\n" + legend

    @classmethod
    def create_user_report_embed_template(cls, member: discord.Member, days_window: int, report_data: UserReportData) -> discord.Embed:
        """创建用户个人详细报告的 Embed 模板。"""
        embed = discord.Embed(
            title=f"📊 {member.display_name} 的活跃度报告",
            description=f"这是您在过去 **{days_window}** 天内的活跃概览。",
            color=discord.Color.blue(),
        )
        embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        embed.add_field(name="总消息数", value=f"`{report_data.total_messages}` 条", inline=False)
        heatmap_text = cls._render_heatmap_text(report_data.heatmap_data, days_window)
        embed.add_field(name="近况热力图 (消息数/天)", value=heatmap_text, inline=False)
        embed.set_footer(text=f"数据统计时间: {datetime.now(BEIJING_TZ):%Y-%m-%d %H:%M:%S} (UTC+8)")
        return embed

    @staticmethod
    def create_check_activity_embed(member: discord.Member, days_window: int, total_messages: int, threshold: int, action_text: str) -> discord.Embed:
        """创建活跃度检查结果的 Embed。"""
        is_eligible = total_messages >= threshold
        embed = discord.Embed(
            title="活跃度检查结果",
            description=f"你好，{member.mention}！\n这是你在过去 **{days_window}** 天内的活跃度报告：",
            color=discord.Color.green() if is_eligible else discord.Color.orange()
        )
        embed.add_field(name="统计消息数", value=f"`{total_messages}` 条", inline=True)
        embed.add_field(name="要求消息数", value=f"`{threshold}` 条", inline=True)
        embed.add_field(name="资格状态", value=f"**{'✅ 符合' if is_eligible else '❌ 不符合'}**", inline=True)
        embed.description += action_text
        return embed


class PageJumpModal(ui.Modal, title="跳转到指定页面"):
    def __init__(self, total_pages: int):
        super().__init__(timeout=120)
        self.total_pages = total_pages
        self.jump_to_page: typing.Optional[int] = None

        self.page_input = ui.TextInput(
            label="输入页码",
            placeholder=f"请输入 1 到 {self.total_pages} 之间的数字",
            required=True,
            min_length=1,
            max_length=len(str(self.total_pages))
        )
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            page_num = int(self.page_input.value)
            if 1 <= page_num <= self.total_pages:
                self.jump_to_page = page_num - 1  # 转换为 0 索引
                await interaction.response.defer()  # 确认交互，让主视图处理更新
            else:
                await interaction.response.send_message(f"❌ 页码必须在 1 到 {self.total_pages} 之间。", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ 请输入有效的数字。", ephemeral=True)


class GenericHierarchicalPaginationView(ui.View):
    """【已增强】通用的层级分页视图，支持首页、末页和页面跳转，使用装饰器实现。"""

    def __init__(self, interaction: discord.Interaction, embed_template: discord.Embed,
                 sorted_display_data: list[SortedDisplayItem],
                 field_name: str, value_suffix: str):
        super().__init__(timeout=300)
        self.interaction = interaction
        self.embed_template = embed_template
        self.sorted_display_data = sorted_display_data
        self.field_name = field_name
        self.value_suffix = value_suffix
        self.current_page = 0
        self.channels_per_page = MAX_CHANNELS_PER_PAGE
        self.total_pages = (len(self.sorted_display_data) + self.channels_per_page - 1) // self.channels_per_page or 1
        self.message: typing.Optional[discord.Message] = None

        self._update_buttons()

    def _update_buttons(self):
        """更新所有按钮的状态和标签。"""
        is_first_page = self.current_page == 0
        is_last_page = self.current_page >= self.total_pages - 1

        # 通过 custom_id 查找并更新按钮
        # 假设 custom_id 是固定的
        self.go_to_first.disabled = is_first_page
        self.previous_page.disabled = is_first_page

        # 特别处理动态标签按钮
        self.jump_to_page_button.label = f"{self.current_page + 1}/{self.total_pages}"
        self.jump_to_page_button.disabled = self.total_pages <= 1

        self.next_page.disabled = is_last_page
        self.go_to_last.disabled = is_last_page

    def _create_page_embed(self) -> discord.Embed:
        embed = self.embed_template.copy()
        # 移除旧的分页字段
        if embed.fields and embed.fields[-1].name.startswith(self.field_name):
            embed.remove_field(-1)

        start = self.current_page * self.channels_per_page
        end = start + self.channels_per_page
        page_data = self.sorted_display_data[start:end]

        if page_data:
            lines = [
                f"{'  └ ' if item.channel_dto.is_thread else ''}{item.channel_dto.mention}: `{item.count}` {self.value_suffix}"
                for item in page_data
            ]
            field_title = f"{self.field_name} (第 {self.current_page + 1}/{self.total_pages} 页)"
            embed.add_field(name=field_title, value="\n".join(lines), inline=False)
        elif self.current_page == 0:
            embed.add_field(name=self.field_name, value="没有找到任何符合条件的记录。", inline=False)
        return embed

    async def _update_view(self, interaction: discord.Interaction, edit_response: bool = True):
        """【辅助方法】统一处理视图更新逻辑。"""
        self._update_buttons()
        embed = self._create_page_embed()
        if edit_response:
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            # 用于 modal 回调后更新
            await self.message.edit(embed=embed, view=self)

    async def start(self):
        embed = self._create_page_embed()
        # 发送消息时，视图会自动添加它的子项（按钮）
        self.message = await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)

    # --- 按钮回调函数 (使用装饰器) ---

    @ui.button(label="⏮️", style=discord.ButtonStyle.secondary, row=1, custom_id="pagination_first")
    async def go_to_first(self, interaction: discord.Interaction, button: ui.Button):
        self.current_page = 0
        await self._update_view(interaction)

    @ui.button(label="◀️", style=discord.ButtonStyle.primary, row=1, custom_id="pagination_prev")
    async def previous_page(self, interaction: discord.Interaction, button: ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
        await self._update_view(interaction)

    @ui.button(label="1/1", style=discord.ButtonStyle.secondary, row=1, custom_id="pagination_jump")
    async def jump_to_page_button(self, interaction: discord.Interaction, button: ui.Button):
        modal = PageJumpModal(self.total_pages)
        await interaction.response.send_modal(modal)
        timed_out = await modal.wait()

        if not timed_out and modal.jump_to_page is not None:
            self.current_page = modal.jump_to_page
            # modal.on_submit 已经 defer 了交互，所以我们不能再次响应
            # 我们需要直接编辑原始消息
            await self._update_view(interaction, edit_response=False)

    @ui.button(label="▶️", style=discord.ButtonStyle.primary, row=1, custom_id="pagination_next")
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
        await self._update_view(interaction)

    @ui.button(label="⏭️", style=discord.ButtonStyle.secondary, row=1, custom_id="pagination_last")
    async def go_to_last(self, interaction: discord.Interaction, button: ui.Button):
        self.current_page = self.total_pages - 1
        await self._update_view(interaction)

    async def on_timeout(self):
        for item in self.children: item.disabled = True
        try:
            if self.message: await self.message.edit(view=self)
        except discord.NotFound:
            pass


class ActivityRoleView(ui.View):
    """【职责不变】按钮仅用于捕获用户意图并调用Cog的公共处理方法。"""

    def __init__(self, cog: TrackActivityCog):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="检查活跃度 & 申领身份组", style=discord.ButtonStyle.success, custom_id="check_activity_role")
    async def check_activity_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_check_activity(interaction)

    @ui.button(label="查看我的活跃报告", style=discord.ButtonStyle.primary, custom_id="view_activity_report")
    async def view_report_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_view_report(interaction)

    @ui.button(label="移除我的活跃度身份组", style=discord.ButtonStyle.danger, custom_id="remove_activity_role")
    async def remove_role_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.cog.handle_remove_role(interaction)
