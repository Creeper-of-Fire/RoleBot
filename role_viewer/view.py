# role_viewer/view.py

from __future__ import annotations

import typing
from typing import Sequence

import discord
from discord import Color

from role_viewer.role_view_config import SEPARATOR_ROLES

if typing.TYPE_CHECKING:
    from role_viewer.cog import RoleViewerCog


class RoleOrderView(discord.ui.View):
    """
    身份组顺序查看器界面。
    新版逻辑：一次性展示所有身份组，并根据配置的分隔符进行分块。
    """

    def __init__(self, cog: 'RoleViewerCog', user: discord.Member):
        super().__init__(timeout=300)  # 5分钟超时
        self.cog = cog
        self.user = user
        self.guild = user.guild

    async def start(self, interaction: discord.Interaction, ephemeral=True):
        """
        启动视图，生成并发送所有 Embeds。
        这个方法取代了 PaginatedView 的 start 方法。
        """
        # 1. 获取所有符合条件的、从高到低排序的身份组
        all_display_roles = self._fetch_and_filter_roles()

        # 2. 根据分隔符将身份组列表切分成块
        role_chunks = self._chunk_roles_by_separators(all_display_roles)

        # 3. 将每个块渲染成一个 Embed
        embeds = self._render_chunks_to_embeds(role_chunks)

        # 4. 一次性发送所有 Embeds（最多10个）
        if not embeds:
            await interaction.followup.send("此服务器没有配置分隔身份组或没有任何可显示的身份组。", ephemeral=ephemeral)
            return

        # Discord API 限制：一次最多发送 10 个 embeds
        if len(embeds) > 10:
            self.cog.logger.warning(f"服务器 {self.guild.name} 的身份组分块超过10个，只显示前10个。")
            embeds = embeds[:10]

        await interaction.followup.send(embeds=embeds, view=self, ephemeral=ephemeral)

    def _fetch_and_filter_roles(self) -> list[discord.Role]:
        """同步版本的数据获取与过滤，因为不再需要 await。"""
        all_roles: Sequence[discord.Role] = self.guild.roles

        # 获取本服务器配置的所有分隔符 ID 集合，用于快速查找
        separator_ids = set(SEPARATOR_ROLES.get(self.guild.id, []))

        filtered_roles = []
        for role in all_roles:
            # 1. 永远排除 @everyone
            if role.is_default():
                continue

            # 2. 检查各项属性
            is_separator = role.id in separator_ids
            has_color = role.color.value != 0
            has_img_icon = role.icon is not None
            has_emoji_icon = role.unicode_emoji is not None  # 【修复】增加对 Emoji 图标的检查

            # 3. 核心保留逻辑：
            # 如果是分隔符 -> 必须保留 (无论有没有颜色/图标，否则切割逻辑会断裂)
            # 或者 有颜色/有图/有Emoji -> 保留
            should_keep = is_separator or has_color or has_img_icon or has_emoji_icon

            if not should_keep:
                continue

            filtered_roles.append(role)

        # 反转列表：从高权限(底部索引大) -> 低权限(底部索引小)
        return filtered_roles[::-1]

    def _chunk_roles_by_separators(self, roles: list[discord.Role]) -> list[tuple[discord.Role | None, list[discord.Role]]]:
        """
        核心逻辑：根据配置的分隔身份组，将长列表切分成块。
        返回: [(分隔符Role, [其下的身份组...]), ...]
        """
        separator_ids = SEPARATOR_ROLES.get(self.guild.id)
        if not separator_ids:
            # 如果没有配置分隔符，所有身份组视为一个大块
            return [roles] if roles else []

        separator_set = set(separator_ids)
        chunks = []
        current_chunk = []

        # 我们从高到低遍历
        for role in roles:
            if role.id in separator_set:
                # 遇到一个新的分隔符，意味着上一个块结束了
                if current_chunk:
                    chunks.append(current_chunk)
                # 新的块以这个分隔符开始
                current_chunk = [role]
            else:
                # 普通身份组，加入当前块
                # 如果还没有遇到任何分隔符，这些最高权限的身份组会暂时放在这里
                current_chunk.append(role)

        # 循环结束后，保存最后一个块
        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def _render_chunks_to_embeds(self, chunks: list[tuple[discord.Role | None, list[discord.Role]]]) -> list[discord.Embed]:
        """将分好的块渲染成多个 Embed 对象。"""
        embeds = []
        member_role_ids = {role.id for role in self.user.roles}

        # 添加一个总览/说明 Embed
        info_embed = discord.Embed(
            title="📜 身份组层级总览",
            description=(
                "> 在Discord中，上层身份组的颜色或图标会覆盖下层。\n"
                "> 想要看到被覆盖的下层身份组，唯一的办法就是移除上层的身份组。\n"
                "以下是带颜色或图标的身份组的层级结构。（按优先级从上到下排列）"
            ),
            color=Color.blue()
        )
        embeds.append(info_embed)

        for chunk in chunks:
            if not chunk: continue

            # 使用块的第一个身份组（即分隔符）的颜色
            separator_role = chunk[0]
            color = separator_role.color

            lines = []
            for i, role in enumerate(chunk):
                marker = "✅" if role.id in member_role_ids else "▫️"
                # 将块的第一个身份组（分隔符）加粗显示
                if i == 0 and role.id in SEPARATOR_ROLES.get(self.guild.id, []):
                    lines.append(f"{marker} **{role.mention}**")
                else:
                    lines.append(f"{marker} {role.mention}")

            description = "\n".join(lines)
            if len(description) > 4096:
                description = description[:4090] + "\n... (内容过长)"

            embed = discord.Embed(description=description, color=color)
            embeds.append(embed)

        return embeds
