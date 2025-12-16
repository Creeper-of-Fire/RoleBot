"""
model_roles/view.py
处理模型身份组领取的界面逻辑。
"""
from __future__ import annotations

import typing
import discord
from discord import ui, Color

from utility.paginated_view import PaginatedView
from utility.helpers import safe_defer
from utility.auth import is_role_dangerous
from utility.role_service import update_member_roles
import config

if typing.TYPE_CHECKING:
    from model_fan_roles.cog import ModelFanRolesCog

# 每页显示多少个模型按钮
MODELS_PER_PAGE = 10


class ModelRolesView(PaginatedView):
    """模型身份组选择面板"""

    def __init__(self, cog: 'ModelFanRolesCog', user: discord.Member):
        self.cog = cog
        self.user = user
        self.guild = user.guild
        self.last_stats_time = None

        # 定义数据提供者：这里是一个异步函数，因为 Cog 需要去计算/获取统计
        async def get_sorted_data():
            data, update_time = await self.cog.get_ranked_model_data(self.guild)
            self.last_stats_time = update_time
            return data

        super().__init__(
            all_items_provider=get_sorted_data,
            items_per_page=MODELS_PER_PAGE,
            timeout=config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3) * 60
        )

    async def _rebuild_view(self):
        """重构建视图（PaginatedView 的抽象方法实现）"""
        self.clear_items()

        # 刷新用户信息以确保持有最新的身份组列表
        member = self.guild.get_member(self.user.id)
        if not member:
            self.embed = discord.Embed(title="错误", description="无法获取成员信息。", color=Color.red())
            self.stop()
            return

        member_role_ids = {role.id for role in member.roles}
        page_items = self.get_page_items()  # 获取当前页的模型配置列表

        # 获取统计数据 (为了显示在 Panel 上)
        stats = self.cog.stats_cache.get(self.guild.id, {})

        # --- 构建统计面板内容 (Rank List) ---
        description_lines = [
            "🏆 **当前阵营人气排行**",
            "请点击下方按钮选择您的唯一信仰 (互斥单选)。\n"
            "> -# 注：排行榜有1分钟的全局查询冷却。\n"
            "> -# 以及，排名最靠前的大模型的领取按钮会自动排到最前面，因为强者就是要狠狠羞辱弱者。\n\n"
        ]

        # 计算排名的起始序号
        start_rank = (self.page * self.items_per_page) + 1

        for idx, item in enumerate(page_items):
            rank = start_rank + idx
            role_id = item["role_id"]
            count = stats.get(role_id, 0)
            name = item["name"]

            # 高亮用户当前拥有的
            marker = "✅" if role_id in member_role_ids else f"`#{rank}`"
            description_lines.append(f"{marker} **{name}**: {count} 人")

        if not page_items:
            description_lines.append("*暂无数据*")

        time_str = self.last_stats_time.strftime("%H:%M") if self.last_stats_time else "未知"

        # 构建 Embed
        self.embed = discord.Embed(
            title="🤖 大模型阵营选择",
            description="\n".join(description_lines),
            color=Color.gold()
        )
        self.embed.set_footer(text=f"统计更新于: {time_str} | 第 {self.page + 1}/{self.total_pages} 页")

        # 生成按钮
        for i, model_data in enumerate(page_items):
            role_id = model_data["role_id"]
            role_name = model_data["name"]
            emoji = model_data.get("emoji")

            role = self.guild.get_role(role_id)
            if not role:
                continue

            is_owned = role_id in member_role_ids

            # 按钮样式：拥有则为绿色(Success)，未拥有则为灰色(Secondary)
            style = discord.ButtonStyle.success if is_owned else discord.ButtonStyle.secondary

            # 计算行号，每行5个按钮
            row_index = i // 5

            self.add_item(ModelRoleButton(
                cog=self.cog,
                role=role,
                label=role_name,
                emoji=emoji,
                style=style,
                row=row_index
            ))

        # 添加分页按钮 (PaginatedView 内置方法)
        self._add_pagination_buttons(row=2)


class ModelRoleButton(ui.Button):
    """单个模型身份组的切换按钮"""

    def __init__(self, cog: 'ModelFanRolesCog', role: discord.Role, label: str, emoji: str, style: discord.ButtonStyle, row: int):
        self.cog = cog
        self.role = role
        super().__init__(
            label=label,
            style=style,
            emoji=emoji,
            custom_id=f"toggle_model_role:{role.id}",
            row=row
        )

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction)
        member = interaction.user

        # 二次安全检查（防止缓存滞后）
        if is_role_dangerous(self.role):
            await interaction.followup.send(f"❌ 无法操作：身份组 **{self.role.name}** 包含敏感权限。", ephemeral=True)
            return

        # --- 互斥逻辑 ---
        # 1. 获取本服务器所有已配置的模型身份组ID
        guild_config = self.cog.safe_model_config_cache.get(member.guild.id, [])
        all_model_ids = {item["role_id"] for item in guild_config}

        # 2. 找出用户当前持有的模型身份组
        user_role_ids = {r.id for r in member.roles}
        current_model_roles = all_model_ids.intersection(user_role_ids)

        to_add = set()
        to_remove = set()

        # 3. 判断操作
        if self.role.id in current_model_roles:
            # 如果点击的是已经持有的 -> 卸载 (变无阵营)
            to_remove.add(self.role.id)
        else:
            # 如果点击的是未持有的 -> 卸载其他所有模型组，装备这个
            to_remove.update(current_model_roles)  # 移除旧爱
            to_add.add(self.role.id)  # 拥抱新欢

        # 4. 执行更新
        await update_member_roles(
            cog=self.cog,
            member=member,
            to_add_ids=to_add,
            to_remove_ids=to_remove,
            reason="模型身份组切换(互斥)"
        )

        # 5. 刷新视图 (会重新触发 get_sorted_data，但缓存未过期时不会重算统计)
        if isinstance(self.view, PaginatedView):
            await self.view.update_view(interaction)