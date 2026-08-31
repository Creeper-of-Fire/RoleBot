"""荣誉头衔 toml 配置的 Discord 命令封装。

跟 ComplaintCog 对称：用 _shared/config/toml_command 的 handle_toml_* 三个 handler，
本地 cog 只挂装饰器 + 透传参数。

命令组：`荣誉头衔丨配置`
- /下载配置
- /上传配置（含 toml 写盘 + HonorCog DB 热同步——免重启）
- /查看配置哈希

注意：本 cog 跟 HonorCog 解耦——HonorCog 拥有 honor_definitions 表的同步逻辑。
本 cog 只管 toml 文件的 CRUD；上传后调一次 HonorCog.synchronize_all_honor_definitions()
把 toml → SQLite，让运行中的 bot 立刻看到新条目。
"""

from __future__ import annotations

import logging
import uuid as uuid_lib
from typing import TYPE_CHECKING, List, Optional

import discord
from discord import app_commands

import config
from honor_system.getCogs import getHonorCog
from honor_system.honor_config_manager import HonorConfigManager
from shared.config.toml_command import (
    handle_toml_download,
    handle_toml_upload,
    handle_toml_view_hash,
)
from utility.feature_cog import FeatureCog, PanelEntry
from utility.permison import is_admin

if TYPE_CHECKING:
    from main import RoleBot

logger = logging.getLogger(__name__)


class HonorConfigCog(FeatureCog):
    """荣誉头衔 toml 配置的 admin 命令入口。

    继承 FeatureCog（按项目约定所有 honor_system 下的 cog 都通过 FeatureCog
    注册到 CoreCog）。本 cog 不管理身份组缓存，也不进主面板——
    两个 abstractmethod 实现为空操作（HonorCog 已经管了所有 honor role 缓存）。

    注：本 cog 的 ``manager`` 是 HonorConfigManager 单例；cache 也由 manager 维护。
    """

    def __init__(self, bot: "RoleBot"):
        super().__init__(bot)
        self.manager = HonorConfigManager.get_instance()
        self.logger.info("荣誉配置 Cog 已加载")

    async def update_safe_roles_cache(self) -> None:
        """FeatureCog 抽象接口：本 cog 不管理任何身份组，no-op。"""
        return

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """FeatureCog 抽象接口：本 cog 不进主面板。"""
        return None

    def _trigger_honor_sync(self) -> None:
        """触发 HonorCog 把 toml 同步到 SQLite。

        接到任务后立即返回；同步是 background task，不阻塞 slash 指令响应。
        HonorCog 不可用（cog 没加载）时 no-op，不影响主流程。
        """
        honor_cog = getHonorCog(self)
        if not honor_cog:
            logger.warning(
                "HonorCog 未加载，跳过 DB 热同步（toml 已落盘但 SQLite 暂未同步，需重启 bot）"
            )
            return
        self.bot.loop.create_task(honor_cog.synchronize_all_honor_definitions())

    # ================= 斜杠命令 =================

    honor_config_group = app_commands.Group(
        name="荣誉头衔丨配置",
        description="下载/上传/查看荣誉头衔的 toml 配置",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @honor_config_group.command(
        name="下载配置",
        description="下载 toml + doc；当前 SHA-256 前 12 字符在 embed 里显示",
    )
    @is_admin()
    async def cmd_download_config(self, interaction: discord.Interaction):
        await handle_toml_download(
            interaction,
            manager=self.manager,
            label="honor",
            permission_check=None,
        )

    @honor_config_group.command(
        name="上传配置",
        description="上传修改后的 toml；本地有配置时必须把 SHA-256 粘到 hash_str 字段（前 12 字符即可）",
    )
    @app_commands.rename(config_file="配置文件")
    @app_commands.describe(
        config_file="上传编辑后的 TOML 配置文件",
        hash_str="SHA-256 校验值（前 12 字符足够，完整 64 也可以）；首次上传（本地无配置）可省",
    )
    @is_admin()
    async def cmd_upload_config(
        self,
        interaction: discord.Interaction,
        config_file: discord.Attachment,
        hash_str: str | None = None,
    ):
        guild_id = interaction.guild.id if interaction.guild else 0
        await handle_toml_upload(
            interaction,
            manager=self.manager,
            toml_file=config_file,
            hash_str=hash_str,
            label="honor",
            permission_check=None,
        )
        # 上传成功后触发 HonorCog 热同步到 SQLite。
        # toml 已经写到磁盘（HonorConfigManager.validate_and_save 内已回填 cache），
        # synchronize 全量重读；无论上传成败 idem 是幂等的，上传失败时
        # （旧 toml 还在盘上）也能正常工作。
        if guild_id:
            self._trigger_honor_sync()

    @honor_config_group.command(
        name="查看配置哈希",
        description="查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）",
    )
    @is_admin()
    async def cmd_view_hash(self, interaction: discord.Interaction):
        await handle_toml_view_hash(
            interaction,
            manager=self.manager,
            label="honor",
            permission_check=None,
        )

    # 这里 UUID 不能大写，很神秘。
    # 按照目前版本的 discord.py 实现，中文 OK ，大写英文不 OK。
    # 关卡 1：字符集 regex —— ^[-_\w + 泰文组合 + 梵文组合]{1,32}$
    #         \w 在 Python 3 默认 UNICODE 模式，包含中文，所以纯中文 OK
    # 关卡 2：name.lower() != name —— 任何大写字符触发
    #         中文没有大小写概念 → "生成".lower() == "生成" → 通过
    #         "UUID" 含 U → "生成UUID".lower() == "生成uuid" ≠ "生成UUID" → 触发
    @honor_config_group.command(
        name="生成uuid",
        description="生成一个新 UUID，用于 honor toml [[definitions]] 块的 uuid 字段（手机友好）",
    )
    @is_admin()
    async def cmd_gen_uuid(self, interaction: discord.Interaction):
        """生成一个新 UUID 字符串，admin 一键复制粘贴到 honor toml 的 [[definitions]] 块。

        设计动机：手机端没有方便的 UUID 生成工具；admin 在 discord bot 里点一下
        就能拿到一个 token，复制到 toml 的 `uuid = "..."` 字段。ephemeral 消息
        只对调用者可见，避免泄露到公共频道。
        """
        new_uuid = str(uuid_lib.uuid4())
        embed = discord.Embed(
            title="🆔 新 UUID",
            description=(
                f"复制下面这串到 `data/honor_<guild_id>.toml` 的 `[[definitions]]` 块：\n\n"
                f"`{new_uuid}`\n\n"
                f"⚠️ 这是 token，请只粘贴到 toml 文件，不要发到公共频道。"
            ),
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


__all__ = ["HonorConfigCog"]
