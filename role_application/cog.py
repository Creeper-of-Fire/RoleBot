# role_application/cog.py

from __future__ import annotations

import re
import typing

import discord
from discord import app_commands, ui
from discord.ext import commands

import config  # 导入你的主配置文件
from core.command_group import RoleBotMainGroup

if typing.TYPE_CHECKING:
    from main import RoleBot

# ===================================================================
# 功能一：社区建设者身份组 (旧功能, 无改动)
# ===================================================================

# --- 身份组ID ---
CREATOR_ROLE_ID = 1134611078203052122  # 创作者
CONTRIBUTOR_ROLE_ID = 1383835973384802396  # 社区助力者
BUILDER_ROLE_ID = 1383835063455842395  # 社区建设者


# --- 持久化视图 ---
class CommunityBuilderView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="管理我的社区建设者身份组", style=discord.ButtonStyle.blurple, custom_id="manage_community_builder_role")
    async def manage_role_button(self, interaction: discord.Interaction, button: ui.Button):
        # 此部分逻辑与之前完全相同
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild
        creator_role = guild.get_role(CREATOR_ROLE_ID)
        contributor_role = guild.get_role(CONTRIBUTOR_ROLE_ID)
        builder_role = guild.get_role(BUILDER_ROLE_ID)
        if not builder_role or not creator_role or not contributor_role:
            await interaction.followup.send("❌ 错误：相关身份组配置不完整，请联系管理员。", ephemeral=True)
            return
        member_role_ids = {r.id for r in member.roles}
        has_prereq = CREATOR_ROLE_ID in member_role_ids or CONTRIBUTOR_ROLE_ID in member_role_ids
        has_target = BUILDER_ROLE_ID in member_role_ids
        if has_target:
            try:
                await member.remove_roles(builder_role, reason="用户通过面板自行移除")
                await interaction.followup.send(f"✅ 已成功移除你的 `{builder_role.name}` 身份组。", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ 操作失败，请联系管理员：`{e}`", ephemeral=True)
        else:
            if has_prereq:
                try:
                    await member.add_roles(builder_role, reason="用户通过面板自行领取")
                    await interaction.followup.send(f"🎉 恭喜！你已成功领取 `{builder_role.name}` 身份组！", ephemeral=True)
                except Exception as e:
                    await interaction.followup.send(f"❌ 操作失败，请联系管理员：`{e}`", ephemeral=True)
            else:
                await interaction.followup.send(
                    f"🤔 你暂时无法领取 `{builder_role.name}` 身份组。\n\n"
                    f"**领取条件：** 拥有 `{creator_role.name}` 或 `{contributor_role.name}` 身份组之一。",
                    ephemeral=True
                )


# ===================================================================
# 功能二：创作者身份组申请 (新版逻辑)
# ===================================================================

# --- 配置常量 ---
CREATOR_TARGET_ROLE_ID = 1134611078203052122  # 创作者 (目标)
CREATOR_REACTION_THRESHOLD = 5  # 要求的反应数量


class CreatorApplicationModal(ui.Modal, title="作品审核提交"):
    """弹出的表单，用于让用户提交他们的作品链接。"""
    message_link = ui.TextInput(
        label="作品的帖子链接",
        placeholder="请在此处粘贴论坛帖子的链接...",
        style=discord.TextStyle.short,
        required=True
    )

    def __init__(self, cog: 'RoleApplicationCog'):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        member = interaction.user
        guild = interaction.guild

        # 1. 检查目标身份组是否存在
        creator_role = guild.get_role(CREATOR_TARGET_ROLE_ID)
        if not creator_role:
            await interaction.followup.send("❌ 错误：目标身份组“创作者”在本服务器不存在，请联系管理员。", ephemeral=True)
            return

        # 2. 检查用户是否已经拥有该身份组
        if creator_role in member.roles:
            await interaction.followup.send("✅ 你已经是创作者了，无需再次申请！", ephemeral=True)
            return

        # 3. 解析并验证链接
        link = self.message_link.value
        match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)(?:/\d+)?', link)
        if not match:
            await interaction.followup.send("❌ 你提交的链接格式不正确，请确保是有效的 Discord 帖子链接。", ephemeral=True)
            return

        # 即使链接包含消息ID，我们也只关心服务器ID和频道（帖子）ID
        link_guild_id, link_channel_id = map(int, match.groups())

        # 4. 验证链接是否属于当前服务器
        if link_guild_id != guild.id:
            await interaction.followup.send("❌ 链接必须来自本服务器。", ephemeral=True)
            return

        # 5. 核心逻辑：验证、抓取和检查
        try:
            # get_channel_or_thread 不会发起API请求，它会检查缓存
            channel = guild.get_channel_or_thread(link_channel_id)
            if not channel:
                # 如果缓存没有，尝试主动获取一次
                channel = await guild.fetch_channel(link_channel_id)

            # 5a. 必须是论坛中的帖子 (Thread in a ForumChannel)
            if not isinstance(channel, discord.Thread) or not isinstance(channel.parent, discord.ForumChannel):
                await interaction.followup.send("❌ 提交的链接必须指向一个**论坛帖子**，普通聊天消息无效。", ephemeral=True)
                return

            # 5b. 获取帖子的首楼消息 (Starter Message)
            # 直接在 Thread 对象 (channel) 上调用 fetch_message，而不是在它的父级 (ForumChannel) 上。
            # 帖子的ID (channel.id) 就是其起始消息的ID。
            starter_message = await channel.fetch_message(channel.id)

            # 5c. 提交者必须是帖子的作者
            if starter_message.author.id != member.id:
                await interaction.followup.send(f"❌ 你必须是帖子 **「{channel.name}」** 的创建者才能提交审核。", ephemeral=True)
                return

            # 5d. 检查首楼消息的反应数量
            has_enough_reactions = any(reaction.count >= CREATOR_REACTION_THRESHOLD for reaction in starter_message.reactions)
            if not has_enough_reactions:
                await interaction.followup.send(
                    f"😔 你的作品还未达到审核要求。\n\n"
                    f"**审核要求：** 作品帖子的**首楼**需要获得至少 **{CREATOR_REACTION_THRESHOLD}** 个反应。\n"
                    f"请在获得足够的人气后再来提交哦！",
                    ephemeral=True
                )
                return

            # 6. 所有检查通过，授予身份组
            await member.add_roles(creator_role, reason="通过作品审核自动授予")
            await interaction.followup.send(
                f"🎉 **恭喜！你的作品已通过审核！**\n\n"
                f"你已成功获得 `{creator_role.name}` 身份组。继续创作，为社区带来更多精彩内容吧！",
                ephemeral=True
            )
            self.cog.logger.info(f"用户 {member} ({member.id}) 通过审核获得创作者身份组，作品链接: {link}")

        except discord.NotFound as e:
            self.cog.logger.error(f"创作者审核时发生错误: {e}", exc_info=True)
            await interaction.followup.send("❌ 找不到你链接的帖子或频道，请检查链接是否正确或帖子是否已被删除。", ephemeral=True)
        except discord.Forbidden as e:
            self.cog.logger.error(f"创作者审核时发生错误: {e}", exc_info=True)
            await interaction.followup.send("❌ 我没有权限访问该论坛或帖子，无法进行审核。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"创作者审核时发生未知错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生了一个未知错误，请联系管理员。", ephemeral=True)


class CreatorApplicationView(ui.View):
    """包含“提交审核”按钮的持久化视图。"""

    def __init__(self, cog: 'RoleApplicationCog'):
        super().__init__(timeout=None)
        # 传入 cog 实例，以便弹窗可以访问 logger
        self.cog = cog

    @ui.button(label="提交审核", style=discord.ButtonStyle.primary, custom_id="submit_creator_application", emoji="🔎")
    async def submit_button(self, interaction: discord.Interaction, button: ui.Button):
        # 检查用户是否已经拥有角色，这是一个快速的前置检查
        creator_role = interaction.guild.get_role(CREATOR_TARGET_ROLE_ID)
        if creator_role and creator_role in interaction.user.roles:
            await interaction.response.send_message("✅ 你已经是创作者了，无需再次申请！", ephemeral=True)
            return

        # 发送弹窗，将cog实例传递给它
        modal = CreatorApplicationModal(self.cog)
        await interaction.response.send_modal(modal)


# ===================================================================
# 主 Cog 类
# ===================================================================
class RoleApplicationCog(commands.Cog, name="RoleApplication"):
    """处理特定身份组的申请和移除逻辑。"""

    def __init__(self, bot: 'RoleBot'):
        self.bot = bot
        self.logger = bot.logger

        # 在Cog初始化时，注册所有持久化视图
        self.bot.add_view(CommunityBuilderView())
        self.bot.add_view(CreatorApplicationView(self))

    application_group = app_commands.Group(
        name=f"申请面板",
        description="发送用于申请特殊身份组的面板",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
        parent=RoleBotMainGroup.getGroup()
    )

    @application_group.command(name="发送社区建设者申请面板", description="发送社区建设者身份组的申请/移除面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_builder_panel(self, interaction: discord.Interaction):
        # 此部分无改动
        await interaction.response.defer()
        guild = interaction.guild
        creator_role_name = guild.get_role(CREATOR_ROLE_ID).name if guild.get_role(CREATOR_ROLE_ID) else f"ID:{CREATOR_ROLE_ID}"
        contrib_role_name = guild.get_role(CONTRIBUTOR_ROLE_ID).name if guild.get_role(CONTRIBUTOR_ROLE_ID) else f"ID:{CONTRIBUTOR_ROLE_ID}"
        builder_role_name = guild.get_role(BUILDER_ROLE_ID).name if guild.get_role(BUILDER_ROLE_ID) else f"ID:{BUILDER_ROLE_ID}"
        embed = discord.Embed(
            title="🏗️ 社区建设者身份组申请",
            description=(
                f"如果你拥有 **{creator_role_name}** 或 **{contrib_role_name}** 身份组，"
                f"你可以在此领取专属的 **{builder_role_name}** 身份组。\n\n"
                f"**{builder_role_name}**可以在提案区发起提案，并参与讨论，深度参与建设社区。\n"
                f"并且每次有新的提案进入讨论时，系统会自动 **@{builder_role_name}**。\n"
                f"以便该身份组的所有成员都可以第一时间参与新提案的讨论。\n"
                f"如果你已经拥有 **{builder_role_name}** 的身份组并希望移除，也可以点击下方按钮移除。"
            ),
            color=discord.Color.gold()
        )
        embed.set_footer(text="点击下方按钮进行操作，所有响应都只有你自己可见。")
        await interaction.followup.send(embed=embed, view=CommunityBuilderView())
        self.logger.info(f"用户 {interaction.user} 在服务器 {interaction.guild.name} 发送了社区建设者申请面板。")

    # @application_group.command(name="发送创作者申请面板", description="发送创作者作品审核的提交入口面板。")
    # @app_commands.checks.has_permissions(manage_roles=True)
    # async def send_creator_panel(self, interaction: discord.Interaction):
    #     await interaction.response.defer()
    #     guild = interaction.guild
    #     creator_role = guild.get_role(CREATOR_TARGET_ROLE_ID)
    #     if not creator_role:
    #         await interaction.followup.send("❌ 错误：未能在服务器上找到“创作者”身份组，请检查配置或联系管理员。", ephemeral=True)
    #         return
    #     embed = discord.Embed(
    #         title="🔎 作品审核提交入口",
    #         description="请点击下方按钮提交您的作品链接进行审核。",
    #         color=discord.Color.blue()
    #     )
    #     embed.add_field(
    #         name="审核要求:",
    #         value=(
    #             "- 提交**论坛帖子**链接\n"
    #             f"- 帖子**首楼**需要达到 **{CREATOR_REACTION_THRESHOLD}** 个反应\n"
    #             f"- 审核通过后将获得 {creator_role.mention} 身份组"
    #         ),
    #         inline=False
    #     )
    #     embed.add_field(
    #         name="注意事项:",
    #         value=(
    #             "- 请确保作品帖子链接正确且可访问\n"
    #             "- 只有达到反应数要求的作品才能通过审核\n"
    #             "- 提交者必须是帖子的创建者"
    #         ),
    #         inline=False
    #     )
    #     view = CreatorApplicationView(self)
    #     await interaction.followup.send(embed=embed, view=view)
    #     self.logger.info(f"用户 {interaction.user} 在服务器 {guild.name} 的频道 {interaction.channel.name} 发送了创作者申请面板。")


async def setup(bot: 'RoleBot'):
    """Cog的入口点。"""
    await bot.add_cog(RoleApplicationCog(bot))
