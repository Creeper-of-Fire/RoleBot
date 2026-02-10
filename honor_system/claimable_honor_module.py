# honor_system/claimable_honor_module.py
from __future__ import annotations

from typing import cast, Optional, List, TYPE_CHECKING

import discord
from discord import app_commands, ui
from discord.ext import commands

import config
import config_data
from .cog import HonorCog  # 导入主模块的Cog和View
from .views import HonorManageView
from .honor_data_manager import HonorDataManager
from .json_data_manager import JsonDataManager

if TYPE_CHECKING:
    from main import RoleBot


class ClaimableHonorView(ui.View):
    """
    一个持久化的视图，用于让用户自助领取或卸下一个特定的荣誉身份组。
    通过 custom_id 的不同来区分按钮功能。
    - claim_honor:claim:<honor_uuid>
    - claim_honor:remove:<honor_uuid>
    - claim_honor:main_panel
    """

    def __init__(self, cog: 'ClaimableHonorModuleCog'):
        super().__init__(timeout=None)
        self.cog = cog
        self.data_manager = cog.data_manager

    async def _get_honor_and_role(self, interaction: discord.Interaction, honor_uuid: str):
        """辅助函数，获取荣誉定义和对应的角色对象。"""
        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send("❌ 错误：此面板关联的荣誉已不存在，请联系管理员。", ephemeral=True)
            return None, None

        if not honor_def.role_id:
            await interaction.followup.send(f"❌ 错误：荣誉 “{honor_def.name}” 未关联任何身份组，无法操作。", ephemeral=True)
            return honor_def, None

        role = interaction.guild.get_role(honor_def.role_id)
        if not role:
            await interaction.followup.send(f"❌ 错误：身份组 “{honor_def.name}” 在服务器中已不存在，请联系管理员。", ephemeral=True)
            return honor_def, None

        return honor_def, role

    @ui.button(label="领取头衔并佩戴", style=discord.ButtonStyle.success, custom_id="claim_honor:claim")
    async def claim_and_equip(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        member = cast(discord.Member, interaction.user)

        # 过期荣誉清理与角色一致性修复（仅清理 HonorCog 管理的 safe roles）
        main_honor_cog: Optional[HonorCog] = self.cog.bot.get_cog("Honor")
        if main_honor_cog:
            await main_honor_cog.cleanup_expired_honors_for_member(member, cast(discord.Guild, interaction.guild))

        panel_info = self.cog.json_manager.get_panel(interaction.message.id)
        if not panel_info:
            await interaction.followup.send("❌ 错误：无法识别此面板，它可能已被弃用。", ephemeral=True)
            return

        honor_uuid = panel_info['honor_uuid']

        # 检查该荣誉是否仍在配置文件的可领取列表中
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        claimable_uuids = guild_config.get("claimable_honors", [])
        if honor_uuid not in claimable_uuids:
            await interaction.followup.send("❌ 此荣誉当前已无法通过此面板领取，可能活动已结束/管理员已移除。", ephemeral=True)
            return

        honor_def, role = await self._get_honor_and_role(interaction, honor_uuid)
        if not honor_def or not role:
            return

        # 1. 授予荣誉 (如果尚未拥有)
        granted_def = self.data_manager.grant_honor(member.id, honor_uuid)
        if granted_def:
            await interaction.followup.send(f"🎉 恭喜你，成功领取荣誉 **{granted_def.name}**！", ephemeral=True)
        # else:
        #     await interaction.followup.send(f"☑️ 你已拥有荣誉 **{honor_def.name}**。", ephemeral=True)

        # 2. 佩戴身份组 (如果尚未佩戴)
        if role.id not in [r.id for r in member.roles]:
            try:
                await member.add_roles(role, reason="用户自助领取荣誉")
                await interaction.followup.send(content=f"✅ 成功佩戴身份组：{role.mention}", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send(content=f"❌ **操作失败！**\n我没有足够的权限为你添加身份组 {role.mention}。", ephemeral=True)
            except Exception as e:
                self.cog.logger.error(f"为用户 {member} 添加角色 {role.name} 时出错: {e}", exc_info=True)
                await interaction.followup.send(content=f"❌ 发生未知错误，请联系管理员。", ephemeral=True)
        else:
            await interaction.followup.send(content=f"你已经佩戴了身份组 {role.mention}，无需重复操作。", ephemeral=True)

    @ui.button(label="卸下身份组", style=discord.ButtonStyle.danger, custom_id="claim_honor:remove")
    async def remove_role(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        member = cast(discord.Member, interaction.user)

        # 过期荣誉清理与角色一致性修复
        main_honor_cog: Optional[HonorCog] = self.cog.bot.get_cog("Honor")
        if main_honor_cog:
            await main_honor_cog.cleanup_expired_honors_for_member(member, cast(discord.Guild, interaction.guild))

        panel_info = self.cog.json_manager.get_panel(interaction.message.id)
        if not panel_info:
            await interaction.followup.send("❌ 错误：无法识别此面板，它可能已被弃用。", ephemeral=True)
            return

        honor_uuid = panel_info['honor_uuid']
        honor_def, role = await self._get_honor_and_role(interaction, honor_uuid)
        if not honor_def or not role:
            return

        # 检查是否拥有该荣誉
        user_honors = self.data_manager.get_user_honors(member.id)
        if honor_uuid not in [uh.honor_uuid for uh in user_honors]:
            await interaction.followup.send(f"你尚未拥有荣誉 **{honor_def.name}**，无法执行卸下操作。", ephemeral=True)
            return

        if role.id in [r.id for r in member.roles]:
            try:
                await member.remove_roles(role, reason="用户自助卸下荣誉")
                await interaction.followup.send(f"✅ 成功卸下身份组：{role.mention}", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send(f"❌ **操作失败！**\n我没有足够的权限为你移除身份组 {role.mention}。",
                                                ephemeral=True)
            except Exception as e:
                self.cog.logger.error(f"为用户 {member} 移除角色 {role.name} 时出错: {e}", exc_info=True)
                await interaction.followup.send(f"❌ 发生未知错误，请联系管理员。", ephemeral=True)
        else:
            await interaction.followup.send(f"你当前未佩戴身份组 {role.mention}。", ephemeral=True)

    @ui.button(label="访问我的荣誉墙", style=discord.ButtonStyle.secondary, custom_id="claim_honor:main_panel")
    async def show_main_honor_panel(self, interaction: discord.Interaction, button: ui.Button):
        # 这个按钮的逻辑与主模块的按钮完全一致
        main_honor_cog: Optional[HonorCog] = self.cog.bot.get_cog("Honor")
        if not main_honor_cog:
            await interaction.response.send_message("❌ 荣誉主模块当前不可用，请联系管理员。", ephemeral=True)
            return

        # 直接调用主模块的面板显示逻辑
        # (这里简化处理，直接复用其核心代码)
        await interaction.response.defer(ephemeral=True)
        member = cast(discord.Member, interaction.user)
        guild = cast(discord.Guild, interaction.guild)

        # 确保在打开荣誉墙前做一次过期清理
        await main_honor_cog.cleanup_expired_honors_for_member(member, guild)
        view = HonorManageView(main_honor_cog, member, guild)
        await view.start(interaction, ephemeral=True)


class ClaimableHonorModuleCog(commands.Cog, name="ClaimableHonorModule"):
    """【荣誉子模块】管理可自助领取的荣誉面板。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger
        self.data_manager = HonorDataManager.getDataManager(logger=self.logger)
        self.json_manager = JsonDataManager.get_instance(logger=self.logger)

    @commands.Cog.listener()
    async def on_ready(self):
        """当机器人准备好时，重新注册所有持久化视图。"""
        self.logger.info("ClaimableHonorModule: 正在重新注册持久化视图...")
        # 创建一个视图实例以供机器人使用。由于 custom_id 是固定的，
        # 机器人会将所有匹配的交互路由到这个视图实例的方法中。
        self.bot.add_view(ClaimableHonorView(self))
        self.logger.info(f"ClaimableHonorView 已注册。")

    claim_honor_group = app_commands.Group(
        name="荣誉头衔丨自助领取面板",
        description="管理可自助领取的荣誉面板",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    async def honor_uuid_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> List[app_commands.Choice[str]]:
        """为荣誉UUID参数提供自动补全选项。"""
        guild_config = config_data.HONOR_CONFIG.get(interaction.guild_id, {})
        claimable_uuids = guild_config.get("claimable_honors", [])

        if not claimable_uuids:
            return []

        # 获取所有荣誉定义以显示名称
        all_defs = self.data_manager.get_all_honor_definitions(interaction.guild_id)
        defs_map = {d.uuid: d for d in all_defs}

        choices = []
        for uuid in claimable_uuids:
            honor_def = defs_map.get(uuid)
            if honor_def:
                choice_name = f"{honor_def.name} ({honor_def.uuid[:8]})"
                # 模糊匹配
                if current.lower() in choice_name.lower():
                    choices.append(app_commands.Choice(name=choice_name, value=uuid))

        return choices[:25]  # Discord 限制最多25个选项

    @claim_honor_group.command(name="发送面板", description="创建一个新的可自助领取荣誉面板。")
    @app_commands.describe(
        title="面板的标题",
        description="面板的描述文字，支持换行符 \\n",
        honor_uuid="要关联的荣誉 (从列表中选择)"
    )
    @app_commands.autocomplete(honor_uuid=honor_uuid_autocomplete)
    @app_commands.checks.has_permissions(manage_roles=True)
    async def create_claimable_panel(self, interaction: discord.Interaction, title: str, description: str, honor_uuid: str):
        await interaction.response.defer(ephemeral=True)

        honor_def = self.data_manager.get_honor_definition_by_uuid(honor_uuid)
        if not honor_def:
            await interaction.followup.send(f"❌ 错误：找不到UUID为 `{honor_uuid}` 的荣誉定义。", ephemeral=True)
            return

        if not honor_def.role_id:
            await interaction.followup.send(f"⚠️ 警告：荣誉 **{honor_def.name}** 没有关联任何身份组。面板仍会创建，但领取/卸下按钮将无法正常工作。", ephemeral=True)

        # 处理描述中的换行符
        description = description.replace("\\n", "\n")

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blue()
        )
        embed.add_field(
            name="可领取头衔",
            value=f"**{honor_def.name}**\n*└ {honor_def.description}*",
            inline=False
        )
        if honor_def.role_id:
            embed.add_field(
                name="对应身份组",
                value=f"<@&{honor_def.role_id}>",
                inline=False
            )

        view = ClaimableHonorView(self)

        try:
            # 发送到当前频道
            target_channel = cast(discord.TextChannel, interaction.channel)
            message = await target_channel.send(embed=embed, view=view)

            # 保存面板信息以供持久化
            self.json_manager.add_panel(
                message_id=message.id,
                channel_id=message.channel.id,
                guild_id=interaction.guild_id,
                honor_uuid=honor_uuid
            )
            await interaction.followup.send(f"✅ 成功在 {target_channel.mention} 创建了荣誉领取面板！", ephemeral=True)

        except Exception as e:
            self.logger.error(f"创建荣誉领取面板时出错: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 创建面板时发生未知错误: `{e}`", ephemeral=True)


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(ClaimableHonorModuleCog(bot))
