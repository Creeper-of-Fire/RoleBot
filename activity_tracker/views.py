# activity_tracker/views.py
from __future__ import annotations

import typing
from datetime import datetime, timedelta

import discord
from discord import ui

from activity_tracker.data_manager import BEIJING_TZ
from activity_tracker.logic import UserReportData, SortedDisplayItem
from utility.paginated_view import PaginatedView

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


class UserReportDetailView(PaginatedView):
    """
    【新版】用于显示用户详细报告中频道/主题列表的分页视图。
    继承自通用的 PaginatedView，只需实现页面内容的格式化逻辑。
    """

    def __init__(self, embed_template: discord.Embed,
                 sorted_display_data: list[SortedDisplayItem],
                 field_name: str, value_suffix: str, *, timeout: float | None = 300.0):

        # 存储本视图特有的渲染信息
        self.embed_template = embed_template
        self.field_name = field_name
        self.value_suffix = value_suffix

        # 调用父类的构造函数，传入核心分页所需数据
        super().__init__(
            all_items=sorted_display_data,
            items_per_page=MAX_CHANNELS_PER_PAGE,
            timeout=timeout
        )

    # 实现抽象方法 _rebuild_view
    async def rebuild_view(self):
        """构建/重建视图内容和 Embed。"""
        self.clear_items()  # 清空旧的组件

        # 复制 embed 模板，并移除可能存在的旧的分页字段
        self.embed = self.embed_template.copy()
        # 安全地移除最后一个字段，如果它是分页字段
        if self.embed.fields and self.embed.fields[-1].name.startswith(self.field_name):
            self.embed.remove_field(-1)

        # 获取当前页的数据
        start, end = self.get_page_range()
        page_items: list[SortedDisplayItem] = self.all_items[start:end]

        # 动态生成分页字段的标题
        field_title = f"{self.field_name} (第 {self.page + 1}/{self.total_pages} 页)"

        if not page_items and self.page == 0:
            # 如果第一页就没有数据
            self.embed.add_field(name=self.field_name, value="没有找到任何符合条件的记录。", inline=False)
        elif page_items:
            # 格式化每一行的数据
            lines = [
                f"{'  └ ' if item.channel_dto.is_thread else ''}{item.channel_dto.mention}: `{item.count}` {self.value_suffix}"
                for item in page_items
            ]
            self.embed.add_field(name=field_title, value="\n".join(lines), inline=False)

        # 在底部添加标准分页按钮 (row=1 假设没有其他组件，或放在第一行)
        self._add_pagination_buttons(row=1)


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
