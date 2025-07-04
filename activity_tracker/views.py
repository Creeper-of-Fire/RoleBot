# activity_tracker/views.py
from __future__ import annotations

import typing
from datetime import datetime, timedelta, timezone

import discord
from discord import ui

from activity_tracker.data_manager import BEIJING_TZ
from activity_tracker.logic import UserReportData, SortedDisplayItem
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from .cog import TrackActivityCog

# --- 常量定义 ---
MAX_CHANNELS_PER_PAGE = 10
HEATMAP_EMOJIS = {0: '⬜', 1: '🟨', 6: '🟩', 16: '🟦', 31: '🟥'}
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
                    emoji = HEATMAP_EMOJIS[threshold];
                    break
            heatmap_output.append(emoji)

        if not heatmap_output: return "暂无消息记录。"

        legend = f"**图例:** {HEATMAP_EMOJIS[0]} 0 {HEATMAP_EMOJIS[1]} 1-5 {HEATMAP_EMOJIS[6]} 6-15 {HEATMAP_EMOJIS[16]} 16-30 {HEATMAP_EMOJIS[31]} 31+"
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
    def create_check_activity_embed(member: discord.Member, days_window: int, total_messages: int, threshold: int, target_role: discord.Role,
                                    action_text: str) -> discord.Embed:
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


class GenericHierarchicalPaginationView(ui.View):
    """【职责不变】通用的层级分页视图，只负责分页和展示。"""

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
        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page >= self.total_pages - 1

    def _create_page_embed(self) -> discord.Embed:
        embed = self.embed_template.copy()
        # 移除旧的分页字段，保留模板中的其他字段
        embed.remove_field(-1) if embed.fields and embed.fields[-1].name.startswith(self.field_name) else None

        start = self.current_page * self.channels_per_page
        end = start + self.channels_per_page
        page_data = self.sorted_display_data[start:end]

        if page_data:
            lines = [
                f"{'  └ ' if isinstance(item.channel, discord.Thread) else ''}{item.channel.mention}: `{item.count}` {self.value_suffix}"
                for item in page_data
            ]
            field_title = f"{self.field_name} (第 {self.current_page + 1}/{self.total_pages} 页)"
            embed.add_field(name=field_title, value="\n".join(lines), inline=False)
        elif self.current_page == 0:
            embed.add_field(name=self.field_name, value="没有找到任何符合条件的记录。", inline=False)
        return embed

    async def start(self):
        embed = self._create_page_embed()
        self.message = await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)

    @ui.button(label="上一页", style=discord.ButtonStyle.primary, custom_id="pagination_prev")
    async def previous_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.edit_original_response(embed=self._create_page_embed(), view=self)

    @ui.button(label="下一页", style=discord.ButtonStyle.primary, custom_id="pagination_next")
    async def next_page(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.edit_original_response(embed=self._create_page_embed(), view=self)

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