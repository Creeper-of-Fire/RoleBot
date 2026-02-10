# role_viewer/cog.py

from __future__ import annotations

import typing
from typing import Optional, List

import discord
from discord import ui, app_commands

import config
from role_system.role_viewer.data_manager import SeparatorDataManager
from role_system.role_viewer.view import RoleOrderView
from utility.feature_cog import FeatureCog, PanelEntry
from utility.helpers import safe_defer, try_get_member
from utility.views import ConfirmationView

if typing.TYPE_CHECKING:
    from main import RoleBot


class RoleViewerCog(FeatureCog, name="RoleViewer"):
    """提供查看服务器内所有身份组顺序的功能。"""

    def __init__(self, bot: 'RoleBot'):
        super().__init__(bot)
        # 初始化数据管理器
        self.data_manager = SeparatorDataManager()

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """【接口方法】返回显示在主面板上的入口按钮。"""
        return [
            PanelEntry(
                button=RoleViewerPanelButton(self),
                description="查看所有身份组的层级/覆盖顺序。"
            )
        ]

    async def update_safe_roles_cache(self):
        """【接口方法】此功能不涉及需验证的身份组，直接跳过。"""
        pass

    # =================================================================
    # CURD 指令区域
    # =================================================================

    viewer_group = app_commands.Group(
        name=f"{config.COMMAND_GROUP_NAME}丨身份组查看相关配置",
        description="身份组查看相关配置",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @viewer_group.command(name="分隔符设置", description="管理用于身份组查看器的分隔符身份组。")
    @app_commands.describe(
        action="执行的操作：添加、移除、查看列表或清空。",
        role="[添加/移除时必选] 选择目标身份组。"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="➕ 添加分隔符", value="add"),
        app_commands.Choice(name="➖ 移除分隔符", value="remove"),
        app_commands.Choice(name="📋 查看当前分隔符列表", value="list"),
        app_commands.Choice(name="🗑️ 清空所有分隔符", value="clear"),
    ])
    @app_commands.checks.has_permissions(manage_roles=True)
    async def manage_separator(self, interaction: discord.Interaction, action: str, role: Optional[discord.Role] = None):
        """分隔符身份组的增删改查单指令。"""

        # 1. 处理无需 role 参数的指令 (List, Clear)
        if action == "list":
            sep_ids = self.data_manager.get_separators(interaction.guild_id)
            if not sep_ids:
                await interaction.response.send_message("ℹ️ 当前没有配置任何分隔符。", ephemeral=True)
                return

            # 生成列表文本
            lines = ["**当前配置的分隔符身份组：**"]
            for rid in sep_ids:
                r = interaction.guild.get_role(rid)
                if r:
                    lines.append(f"• {r.mention} (ID: {r.id})")
                else:
                    lines.append(f"• `已删除的身份组` (ID: {rid})")

            await interaction.response.send_message("\n".join(lines), ephemeral=True)
            return

        elif action == "clear":
            # 二次确认
            view = ConfirmationView(author=interaction.user)
            await interaction.response.send_message(
                "⚠️ **确认操作**\n你确定要清空本服务器所有的身份组分隔符配置吗？\n这将导致身份组查看器无法正确分块显示。",
                view=view,
                ephemeral=True
            )
            await view.wait()

            if view.value:
                await self.data_manager.clear_separators(interaction.guild_id)
                await interaction.followup.send("🗑️ 已清空所有分隔符配置。", ephemeral=True)
            else:
                await interaction.followup.send("❌ 操作已取消。", ephemeral=True)
            return

        # 2. 处理需要 role 参数的指令 (Add, Remove)
        if not role:
            await interaction.response.send_message("❌ 执行添加或移除操作时，必须选择一个身份组 (`role`)。", ephemeral=True)
            return

        if action == "add":
            success = await self.data_manager.add_separator(interaction.guild_id, role.id)
            if success:
                await interaction.response.send_message(f"✅ 已将 {role.mention} 添加为分隔符。", ephemeral=True)
            else:
                await interaction.response.send_message(f"ℹ️ {role.mention} 已经是分隔符了，无需重复添加。", ephemeral=True)

        elif action == "remove":
            success = await self.data_manager.remove_separator(interaction.guild_id, role.id)
            if success:
                await interaction.response.send_message(f"✅ 已移除分隔符 {role.mention}。", ephemeral=True)
            else:
                await interaction.response.send_message(f"❌ {role.mention} 不是已配置的分隔符。", ephemeral=True)

class RoleViewerPanelButton(ui.Button):
    """主面板上的入口按钮：'身份组顺序查看'"""

    def __init__(self, cog: RoleViewerCog):
        super().__init__(
            label="身份组顺序",
            style=discord.ButtonStyle.secondary,
            custom_id="open_role_order_viewer",
            emoji="📜"
        )
        self.cog = cog

    async def callback(self, interaction: discord.Interaction):
        """点击后弹出身份组顺序查看视图"""
        await safe_defer(interaction, thinking=True)

        member = interaction.user
        if isinstance(member, discord.User):
            member = await try_get_member(interaction.guild, member.id)

        if not member:
            await interaction.followup.send("错误：无法获取成员信息。", ephemeral=True)
            return

        view = RoleOrderView(self.cog, member)
        await view.start(interaction, ephemeral=True)


async def setup(bot: 'RoleBot'):
    await bot.add_cog(RoleViewerCog(bot))
