# role_application/cog.py

from __future__ import annotations

import re
import typing

import discord
from discord import app_commands, ui
from discord.ext import commands

import config  # 导入你的主配置文件

if typing.TYPE_CHECKING:
    from main import RoleBot

# ===================================================================
# 功能一：社区建设者身份组
# ===================================================================

# --- 身份组ID (硬编码) ---
CREATOR_ROLE_ID = 1134611078203052122  # 创作者
CONTRIBUTOR_ROLE_ID = 1383835973384802396  # 社区助力者
BUILDER_ROLE_ID = 1383835063455842395  # 社区建设者


# --- 持久化视图 ---
class CommunityBuilderView(ui.View):
    """
    一个持久化的视图，包含一个用于管理“社区建设者”身份组的按钮。
    """

    def __init__(self):
        # timeout=None 使视图持久化
        super().__init__(timeout=None)

    @ui.button(label="管理我的社区建设者身份组", style=discord.ButtonStyle.blurple, custom_id="manage_community_builder_role")
    async def manage_role_button(self, interaction: discord.Interaction, button: ui.Button):
        """
        处理用户点击按钮的逻辑。
        该逻辑会根据用户是否满足条件、是否已拥有目标身份组来执行不同操作。
        """
        # 使用 defer 并设置为 ephemeral，所有后续响应都只有用户自己能看到
        await interaction.response.defer(ephemeral=True, thinking=True)

        member = interaction.user
        guild = interaction.guild

        # 获取相关的身份组对象
        creator_role = guild.get_role(CREATOR_ROLE_ID)
        contributor_role = guild.get_role(CONTRIBUTOR_ROLE_ID)
        builder_role = guild.get_role(BUILDER_ROLE_ID)

        # 检查身份组是否存在
        if not builder_role:
            await interaction.followup.send("❌ 错误：目标身份组“社区建设者”在本服务器不存在，请联系管理员。", ephemeral=True)
            return
        if not creator_role or not contributor_role:
            await interaction.followup.send("❌ 错误：先决条件身份组（创作者/社区助力者）不存在，请联系管理员。", ephemeral=True)
            return

        member_role_ids = {r.id for r in member.roles}
        has_prereq = CREATOR_ROLE_ID in member_role_ids or CONTRIBUTOR_ROLE_ID in member_role_ids
        has_target = BUILDER_ROLE_ID in member_role_ids

        # 逻辑判断
        if has_target:
            # --- 用户已拥有身份组，执行移除操作 ---
            try:
                await member.remove_roles(builder_role, reason="用户通过面板自行移除")
                await interaction.followup.send(f"✅ 已成功移除你的 `{builder_role.name}` 身份组。", ephemeral=True)
            except discord.Forbidden:
                await interaction.followup.send("❌ 操作失败：我没有足够的权限来移除你的身份组。", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)
        else:
            # --- 用户没有身份组，检查是否能领取 ---
            if has_prereq:
                # 符合条件，授予身份组
                try:
                    await member.add_roles(builder_role, reason="用户通过面板自行领取")
                    await interaction.followup.send(f"🎉 恭喜！你已成功领取 `{builder_role.name}` 身份组！", ephemeral=True)
                except discord.Forbidden:
                    await interaction.followup.send("❌ 操作失败：我没有足够的权限来授予你身份组。", ephemeral=True)
                except Exception as e:
                    await interaction.followup.send(f"❌ 发生未知错误，请联系管理员：`{e}`", ephemeral=True)
            else:
                # 不符合条件，提示用户
                await interaction.followup.send(
                    f"🤔 你暂时无法领取 `{builder_role.name}` 身份组。\n\n"
                    f"**领取条件：**\n"
                    f"- 拥有 `{creator_role.name}` 身份组\n"
                    f"**或**\n"
                    f"- 拥有 `{contributor_role.name}` 身份组",
                    ephemeral=True
                )


# ===================================================================
# 功能二：创作者身份组申请
# ===================================================================

# --- 配置常量 ---
CREATOR_TARGET_ROLE_ID = 1134611078203052122  # 创作者 (目标)
CREATOR_REACTION_THRESHOLD = 5  # 要求的反应数量


class CreatorApplicationModal(ui.Modal, title="作品审核提交"):
    """
    弹出的表单，用于让用户提交他们的作品链接。
    """
    message_link = ui.TextInput(
        label="作品的帖子链接",
        placeholder="请在此处粘贴帖子的链接，例如：https://discord.com/channels/...",
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
        # 使用正则表达式匹配 Discord 消息/帖子链接
        match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)', link)
        if not match:
            await interaction.followup.send("❌ 你提交的链接格式不正确，请确保是有效的 Discord 帖子链接。", ephemeral=True)
            return

        link_guild_id, link_channel_id, link_message_id = map(int, match.groups())

        # 4. 验证链接是否属于当前服务器
        if link_guild_id != guild.id:
            await interaction.followup.send("❌ 链接必须来自本服务器。", ephemeral=True)
            return

        # 5. 尝试获取消息并检查反应
        try:
            channel = guild.get_channel_or_thread(link_channel_id)
            if not channel:
                await interaction.followup.send("❌ 无法找到链接所在的频道。", ephemeral=True)
                return

            message = await channel.fetch_message(link_message_id)

            # 检查是否有任何一个反应的数量达到了阈值
            has_enough_reactions = any(reaction.count >= CREATOR_REACTION_THRESHOLD for reaction in message.reactions)

            if not has_enough_reactions:
                await interaction.followup.send(
                    f"😔 你的作品还未达到审核要求。\n\n"
                    f"**审核要求：** 作品需要获得至少 **{CREATOR_REACTION_THRESHOLD}** 个反应。\n"
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

        except discord.NotFound:
            await interaction.followup.send("❌ 找不到你链接的帖子，请检查链接是否正确或帖子是否已被删除。", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ 我没有权限访问该频道或帖子，无法进行审核。", ephemeral=True)
        except Exception as e:
            self.cog.logger.error(f"创作者审核时发生未知错误: {e}", exc_info=True)
            await interaction.followup.send(f"❌ 发生了一个未知错误，请联系管理员。", ephemeral=True)
        finally:
            # 无论成功或失败，都将用户从“处理中”状态移除
            self.cog.pending_creator_submissions.discard(interaction.user.id)


class CreatorApplicationView(ui.View):
    """
    包含“提交审核”按钮的持久化视图。
    """

    def __init__(self, cog: 'RoleApplicationCog'):
        super().__init__(timeout=None)
        self.cog = cog

    @ui.button(label="提交审核", style=discord.ButtonStyle.primary, custom_id="submit_creator_application", emoji="🔎")
    async def submit_button(self, interaction: discord.Interaction, button: ui.Button):
        # 检查用户是否已在处理中，防止重复提交
        if interaction.user.id in self.cog.pending_creator_submissions:
            await interaction.response.send_message("⏳ 你上一个提交正在处理中，请稍后再试。", ephemeral=True)
            return

        # 检查用户是否已经拥有角色
        creator_role = interaction.guild.get_role(CREATOR_TARGET_ROLE_ID)
        if creator_role and creator_role in interaction.user.roles:
            await interaction.response.send_message("✅ 你已经是创作者了，无需再次申请！", ephemeral=True)
            return

        # 将用户标记为“处理中”状态
        self.cog.pending_creator_submissions.add(interaction.user.id)
        # 发送弹窗
        modal = CreatorApplicationModal(self.cog)
        await interaction.response.send_modal(modal)

class RoleApplicationCog(commands.Cog, name="RoleApplication"):
    """
    处理特定身份组的申请和移除逻辑。
    这是一个独立的模块，不参与 CoreCog 的全局缓存管理。
    """

    def __init__(self, bot: RoleBot):
        self.bot = bot
        self.logger = bot.logger
        # 用于防止用户在短时间内重复提交创作者申请的集合
        self.pending_creator_submissions = set()

        # 在Cog初始化时，注册所有持久化视图
        self.bot.add_view(CommunityBuilderView())
        # 将 self (cog 实例) 传入视图，以便视图能访问 logger 和 pending_submissions
        self.bot.add_view(CreatorApplicationView(self))

    # 创建一个专属的指令组，方便管理
    application_group = app_commands.Group(
        name="申请面板",
        description="发送用于申请特殊身份组的面板",
        guild_ids=[gid for gid in config.GUILD_IDS],  # 确保指令只在配置的服务器中出现
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @application_group.command(name="发送社区建设者申请面板", description="在当前频道发送社区建设者身份组的申请/移除面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_builder_panel(self, interaction: discord.Interaction):
        """
        管理员指令，用于发送一个公共的、可交互的面板。
        """
        await interaction.response.defer()

        # 为了在发送前获取身份组名称，我们需要先拿到 guild 对象
        guild = interaction.guild
        creator_role_name = guild.get_role(CREATOR_ROLE_ID).name if guild.get_role(CREATOR_ROLE_ID) else f"ID:{CREATOR_ROLE_ID}"
        contrib_role_name = guild.get_role(CONTRIBUTOR_ROLE_ID).name if guild.get_role(CONTRIBUTOR_ROLE_ID) else f"ID:{CONTRIBUTOR_ROLE_ID}"
        builder_role_name = guild.get_role(BUILDER_ROLE_ID).name if guild.get_role(BUILDER_ROLE_ID) else f"ID:{BUILDER_ROLE_ID}"

        embed = discord.Embed(
            title="🏗️ 社区建设者身份组申请",
            description=(
                f"欢迎，社区的贡献者们！\n\n"
                f"如果你拥有 **{creator_role_name}** 或 **{contrib_role_name}** 身份组，"
                f"你可以点击下方按钮领取专属的 **{builder_role_name}** 身份组以彰显你的贡献。\n\n"
                f"如果你已经拥有该身份组并希望移除，也可以点击下方按钮移除。"
            ),
            color=discord.Color.gold()
        )
        embed.set_footer(text="点击下方按钮进行操作，所有响应都只有你自己可见。")

        view = CommunityBuilderView()
        await interaction.followup.send(embed=embed, view=view)
        self.logger.info(f"用户 {interaction.user} 在服务器 {interaction.guild.name} 发送了社区建设者申请面板。")

    @application_group.command(name="发送创作者申请面板", description="发送创作者作品审核的提交入口面板。")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def send_creator_panel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        guild = interaction.guild

        # 获取身份组对象用于@
        creator_role = guild.get_role(CREATOR_TARGET_ROLE_ID)
        if not creator_role:
            await interaction.followup.send("❌ 错误：未能在服务器上找到“创作者”身份组，请检查配置或联系管理员。", ephemeral=True)
            return

        embed = discord.Embed(
            title="🔎 作品审核提交入口",
            description="请点击下方按钮提交您的作品链接进行审核。",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="审核要求:",
            value=(
                "- 提交作品链接\n"
                f"- 作品需要达到 **{CREATOR_REACTION_THRESHOLD}** 个反应\n"
                f"- 审核通过后将获得 {creator_role.mention} 身份组"
            ),
            inline=False
        )
        embed.add_field(
            name="注意事项:",
            value=(
                "- 请确保作品帖子链接正确且可访问\n"
                "- 只有达到反应数要求的作品才能通过审核\n"
                "- 每个用户每次只能提交一个作品"
            ),
            inline=False
        )

        # 传入 self (cog 实例)
        view = CreatorApplicationView(self)
        await interaction.followup.send(embed=embed, view=view)
        self.logger.info(f"用户 {interaction.user} 在服务器 {guild.name} 的频道 {interaction.channel.name} 发送了创作者申请面板。")


async def setup(bot: commands.Bot):
    """Cog的入口点。"""
    await bot.add_cog(RoleApplicationCog(bot))
