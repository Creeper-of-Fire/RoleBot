"""Discord app_commands handler for toml config download/upload.

业务 cog 自己挂装饰器 + 调 handler；handler 不负责注册 group、不负责权限。

用法：

    from shared.config.toml_command import (
        handle_toml_download,
        handle_toml_upload,
        handle_toml_view_hash,
    )
    from honor_system.honor_config_manager import HonorConfigManager

    class HonorConfigCog(commands.Cog):
        honor_group = app_commands.Group(name="honor", description="荣誉配置")

        def __init__(self, bot):
            self.bot = bot
            # HonorConfigManager 是 TomlConfigManager 的单例子类，封装 honor
            # toml 的固定参数（data_dir / filename_pattern / doc_path）+ per-guild cache。
            self.manager = HonorConfigManager.get_instance()

        @honor_group.command(
            name="下载配置",
            description="下载 toml + doc；当前 SHA-256 前 12 字符在 embed 里显示",
        )
        async def cmd_download(self, interaction):
            await handle_toml_download(
                interaction, manager=self.manager, label="honor",
                permission_check=self._check_admin,
            )

        @honor_group.command(
            name="上传配置",
            description="上传修改后的 toml；本地有配置时必须把 SHA-256 粘到 hash_str 字段（前 12 字符即可）",
        )
        @app_commands.describe(
            toml_file="修改后的 toml 文件",
            hash_str="SHA-256 校验值（前 12 字符足够，完整 64 也可以）；首次上传（本地无配置）可省",
        )
        async def cmd_upload(
            self,
            interaction,
            toml_file: discord.Attachment,
            hash_str: str | None = None,
        ):
            await handle_toml_upload(
                interaction, manager=self.manager,
                toml_file=toml_file, hash_str=hash_str,
                label="honor", permission_check=self._check_admin,
            )

        @honor_group.command(
            name="查看配置哈希",
            description="查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）",
        )
        async def cmd_hash(self, interaction):
            await handle_toml_view_hash(
                interaction, manager=self.manager, label="honor",
                permission_check=self._check_admin,
            )

业务自己维护的 doc.md（建议放仓库 `docs/` 下，不要放 `data/`）建议包含：

- 当前 schema 字段解释（pydantic 模型各字段含义）
- 哈希机制：什么是 SHA-256、上传/下载流程、回滚方式
- 常用修改场景示例（加纪念身份组、加幻化的具体步骤）
- 注意事项（哪些字段关联什么角色、不要轻易改哪些值）
"""

from __future__ import annotations

import inspect
import logging
import re
from io import BytesIO
from typing import Awaitable, Callable

import discord
from discord import Interaction
from pydantic import ValidationError

from .toml_manager import HashMismatchError, TomlConfigManager

logger = logging.getLogger(__name__)


# 异步或同步的权限检查回调
PermissionCheck = Callable[[Interaction], Awaitable[bool] | bool]


# SHA-256 解释段，跨业务通用，给小白用户看。
HASH_EXPLAIN = (
    "**SHA-256** 是这份配置文件的内容指纹（一串 64 位十六进制字符）。\n"
    "上传时把 SHA-256 字符串粘到 `hash_str` 字段（前 12 字符即可），bot 会先比对哈希：\n"
    "• 一致 → 接受你的修改\n"
    "• 不一致 → 拒绝（说明你基于的版本已过期，得重新下载）\n"
    "这样做是为了防止多人同时改配置时互相覆盖。"
)


async def handle_toml_download(
    interaction: Interaction,
    *,
    manager: TomlConfigManager,
    label: str,
    permission_check: PermissionCheck | None = None,
) -> None:
    """/下载配置 handler。

    返回 toml + doc 两个附件 + 含 SHA-256 前 12 字符的 embed（ephemeral 私密消息）。
    """
    if not await _check(permission_check, interaction):
        await interaction.response.send_message("❌ 无权限", ephemeral=True)
        return

    guild_id = _guild_id(interaction)
    if guild_id is None:
        await interaction.response.send_message("❌ 只能在服务器中使用此指令", ephemeral=True)
        return

    toml_bytes = manager.read_raw(guild_id)
    if toml_bytes is None:
        await interaction.response.send_message(
            f"❌ 当前 **{label}** 还没有配置文件（首次初始化？）",
            ephemeral=True,
        )
        return

    hash_hex = manager.content_hash(guild_id) or ""
    toml_filename = manager.filename_pattern.format(guild_id=guild_id)
    doc_filename = manager.doc_path.name
    doc_text = manager.read_doc()

    if doc_text is None:
        # 文档缺失：仍发 toml + 提示"请联系管理员"
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"📥 {label} 当前配置",
                description=(
                    f"附件：\n"
                    f"• `{toml_filename}` — 配置本体\n\n"
                    f"⚠️ 文档 `{doc_filename}` 不存在，请联系管理员。\n\n"
                    f"**当前 SHA-256 前 12 字符**（下次上传时粘回即可）：\n```\n{hash_hex[:12]}\n```\n"
                    f"{HASH_EXPLAIN}"
                ),
                color=discord.Color.orange(),
            ),
            files=[discord.File(fp=BytesIO(toml_bytes), filename=toml_filename)],
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"📥 {label} 当前配置",
        description=(
            "附件：\n"
            f"• `{doc_filename}` — 给 AI 读的说明文档\n"
            f"• `{toml_filename}` — 配置本体\n\n"
            f"**当前 SHA-256 前 12 字符**（下次上传时粘回即可）：\n```\n{hash_hex[:12]}\n```\n"
            f"{HASH_EXPLAIN}"
        ),
        color=discord.Color.blue(),
    )

    await interaction.response.send_message(
        embed=embed,
        files=[
            discord.File(fp=BytesIO(doc_text.encode("utf-8")), filename=doc_filename),
            discord.File(fp=BytesIO(toml_bytes), filename=toml_filename),
        ],
        ephemeral=True,
    )


async def handle_toml_upload(
    interaction: Interaction,
    *,
    manager: TomlConfigManager,
    toml_file: discord.Attachment,
    label: str,
    hash_str: str | None = None,
    permission_check: PermissionCheck | None = None,
) -> None:
    """/上传配置 handler。

    hash 校验逻辑（写在 handler 里，业务不重复实现）：

    - 本地无配置：允许省略 hash_str → 警告 + 二次确认按钮
    - 本地有配置 + hash_str 缺失：拒绝 → 提示先 /下载配置
    - 本地有配置 + hash_str 不匹配：拒绝 → 显示新旧 hash
    - 本地有配置 + hash_str 匹配：通过 → validate_and_save
    """
    if not await _check(permission_check, interaction):
        await interaction.response.send_message("❌ 无权限", ephemeral=True)
        return

    guild_id = _guild_id(interaction)
    if guild_id is None:
        await interaction.response.send_message("❌ 只能在服务器中使用此指令", ephemeral=True)
        return

    # 0. 文件名校验：filename_pattern 里的 {guild_id} 必须等于当前 guild_id
    #    宽松 regex（容忍管理员自由改名/跨服务器，但前提是文件名里
    #    仍然含 guild_id——只是建议保持 `/下载配置` 的默认名以免误操作）。
    pattern_re = re.compile(
        re.escape(manager.filename_pattern).replace(r"\{guild_id\}", r"(\d+)")
    )
    m = pattern_re.fullmatch(toml_file.filename)
    expected_filename = manager.filename_pattern.format(guild_id=guild_id)
    if not m:
        await interaction.response.send_message(
            embed=discord.Embed(
                title="❌ 文件名格式不符合",
                description=(
                    f"你上传的文件名：`{toml_file.filename}`\n"
                    f"当前服务器期望的文件名（按 `{manager.filename_pattern}`）：`{expected_filename}`\n\n"
                    "建议保留 `/下载配置` 拿到的默认文件名，避免误操作。"
                ),
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return
    file_guild_id = int(m.group(1))
    if file_guild_id != guild_id:
        await interaction.response.send_message(
            embed=discord.Embed(
                title="❌ 文件名对应的服务器 ID 不匹配",
                description=(
                    f"你上传的文件名：`{toml_file.filename}`（服务器 `{file_guild_id}`）\n"
                    f"当前服务器 ID：`{guild_id}`\n\n"
                    f"建议上传 `{expected_filename}`，或从当前服务器 `/下载配置` 重新下载（不要把别的服务器的 toml 直接搬过来）。"
                ),
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return

    # 1. 缺失检查：本地已有配置时必须提供 hash_str（write-time 校验需要它）
    current_hash = manager.content_hash(guild_id)
    no_existing = current_hash is None
    if not no_existing and not (hash_str or "").strip():
        await interaction.response.send_message(
            embed=discord.Embed(
                title="❌ 需要 SHA-256 校验值",
                description=(
                    f"本地已有 **{label}** 配置。\n"
                    "上传时必须带上你下载时拿到的 SHA-256 前 12 字符（防止编辑期间被覆盖）。\n"
                    "取消本次操作，参考你下载时 embed 里记录的 hash 重新上传。"
                ),
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return

    # 读 toml 字节
    toml_bytes = await toml_file.read()

    # 2. 本地无配置 → 首次上传：二次确认（首次 = 低频路径，admin 反悔机会）
    if no_existing:
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"⚠️ 首次上传 **{label}**（未做版本校验）",
                description=(
                    "本地还没有这个配置文件。\n"
                    "由于没有现版本可对照，bot 不会做哈希校验。\n"
                    "请确认你清楚后果（一次错误的写入可能造成后续协作出错）。\n\n"
                    "点击下方「确认写入」按钮（30 秒内有效）。"
                ),
                color=discord.Color.orange(),
            ),
            view=_UploadConfirmView(
                manager=manager,
                guild_id=guild_id,
                toml_bytes=toml_bytes,
                label=label,
            ),
            ephemeral=True,
        )
        return

    # 3. 本地有配置 → write-time hash check + 保存（manager 内部做）
    try:
        manager.validate_and_save(toml_bytes, guild_id, expected_hash=hash_str)
    except HashMismatchError:
        # 不写入。prefix 不匹配的两种可能：粘错字符 / 编辑期间被更新
        await interaction.response.send_message(
            embed=discord.Embed(
                title="❌ 哈希前缀不匹配",
                description=(
                    f"你给的 hash_str 前缀：`{(hash_str or '').strip().lower()[:12] or '(空)'}`\n\n"
                    "可能原因：编辑期间本地配置已被别人更新，或 hash_str 粘错字符 / 用错版本。\n"
                    "取消本次操作，重新 `/下载配置`，基于最新版重新编辑后再上传。"
                ),
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return
    except (ValidationError, ValueError) as e:
        logger.exception("upload pydantic/toml 失败 %s guild %s", label, guild_id)
        # ValidationError 多行错误简短截断；用 code block 防 embed 溢出
        msg = str(e)
        if len(msg) > 1500:
            msg = msg[:1500] + "\n... (truncated)"
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"❌ TOML 验证失败",
                description=(
                    f"`{toml_file.filename}` 无法通过 `{manager.model_class.__name__}` 校验：\n"
                    f"```\n{msg}\n```"
                ),
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return
    except Exception as e:
        logger.exception("upload 写入失败 %s guild %s", label, guild_id)
        await interaction.response.send_message(
            embed=discord.Embed(
                title="❌ 保存失败",
                description=f"```\n{e}\n```",
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        return

    new_hash = manager.content_hash(guild_id) or ""
    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"✅ **{label}** 已更新",
            description=(
                f"新 SHA-256 前 12 字符：`{new_hash[:12]}`\n"
                "下次上传时使用这个新 hash 做版本校验。"
            ),
            color=discord.Color.green(),
        ),
        ephemeral=True,
    )


async def handle_toml_view_hash(
    interaction: Interaction,
    *,
    manager: TomlConfigManager,
    label: str,
    permission_check: PermissionCheck | None = None,
) -> None:
    """/查看配置哈希 handler。

    显示当前 toml 的 SHA-256 + 解释为什么需要它。
    """
    if not await _check(permission_check, interaction):
        await interaction.response.send_message("❌ 无权限", ephemeral=True)
        return

    guild_id = _guild_id(interaction)
    if guild_id is None:
        await interaction.response.send_message("❌ 只能在服务器中使用此指令", ephemeral=True)
        return

    current_hash = manager.content_hash(guild_id)
    if current_hash is None:
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"📌 **{label}** 当前无配置",
                description="本地还没有这个配置文件。可直接 `/上传配置` 创建。",
                color=discord.Color.greyple(),
            ),
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"📌 **{label}** 当前 SHA-256",
            description=(
                f"```\n{current_hash}\n```\n"
                "**为什么需要这个？**\n"
                f"{HASH_EXPLAIN}\n\n"
                "**上传时只需粘前 12 字符**——"
                "粘完整 64 字符也能通过（系统只看前 12 位做 prefix match）。"
            ),
            color=discord.Color.blue(),
        ),
        ephemeral=True,
    )


# --- helpers ---


async def _check(
    fn: PermissionCheck | None,
    interaction: Interaction,
) -> bool:
    """调用 permission_check；支持 async 和 sync 签名；None 视为放行。"""
    if fn is None:
        return True
    result = fn(interaction)
    if inspect.isawaitable(result):
        result = await result  # type: ignore[union-attr]
    return bool(result)


def _guild_id(interaction: Interaction) -> int | None:
    """从 interaction 拿 guild_id；DM 环境返回 None。"""
    if interaction.guild_id is None:
        return None
    return int(interaction.guild_id)


class _UploadConfirmView(discord.ui.View):
    """首次上传（本地无配置）时的二次确认按钮。30 秒超时。

    confirm 内部调 `validate_and_save(expected_hash=None)`——首次无版本可比，
    所以 hash check 自动跳过。
    """

    def __init__(
        self,
        *,
        manager: TomlConfigManager,
        guild_id: int,
        toml_bytes: bytes,
        label: str,
    ) -> None:
        super().__init__(timeout=30.0)
        self.manager = manager
        self.guild_id = guild_id
        self.toml_bytes = toml_bytes
        self.label = label
        self._resolved = False

    @discord.ui.button(label="✅ 确认写入", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: Interaction, button: discord.ui.Button) -> None:
        if self._resolved:
            await interaction.response.send_message("已经处理过，请重试。", ephemeral=True)
            return
        self._resolved = True
        for child in self.children:
            child.disabled = True

        try:
            self.manager.validate_and_save(self.toml_bytes, self.guild_id, expected_hash=None)
        except (ValidationError, ValueError) as e:
            logger.exception("首次上传确认后 pydantic/toml 失败 %s guild %s", self.label, self.guild_id)
            msg = str(e)
            if len(msg) > 1500:
                msg = msg[:1500] + "\n... (truncated)"
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="❌ TOML 验证失败",
                    description=(
                        f"`{self.label}` 无法通过 `{self.manager.model_class.__name__}` 校验：\n"
                        f"```\n{msg}\n```"
                    ),
                    color=discord.Color.red(),
                ),
                view=None,
            )
            return
        except Exception as e:
            logger.exception("首次上传确认后写入失败 %s guild %s", self.label, self.guild_id)
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="❌ 写入失败",
                    description=f"```\n{e}\n```",
                    color=discord.Color.red(),
                ),
                view=None,
            )
            return

        new_hash = self.manager.content_hash(self.guild_id) or ""
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"✅ **{self.label}** 已创建",
                description=(
                    f"新 SHA-256 前 12 字符：`{new_hash[:12]}`\n"
                    "下次上传时使用这个新 hash 做版本校验。"
                ),
                color=discord.Color.green(),
            ),
            view=None,
        )
        self.stop()

    @discord.ui.button(label="❌ 取消", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: Interaction, button: discord.ui.Button) -> None:
        if self._resolved:
            return
        self._resolved = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="已取消写入",
                description=f"**{self.label}** 本地仍为无配置状态。",
                color=discord.Color.greyple(),
            ),
            view=None,
        )
        self.stop()

    async def on_timeout(self) -> None:
        # 按钮自动失效；不主动 edit（interaction 已过期）
        self.stop()


__all__ = [
    "handle_toml_download",
    "handle_toml_upload",
    "handle_toml_view_hash",
    "HASH_EXPLAIN",
]  