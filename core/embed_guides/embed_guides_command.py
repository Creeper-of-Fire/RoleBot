"""embed_guides_{guild_id}.toml 的 Discord 命令入口（薄壳）。

复用 ``shared/config/toml_command.handle_toml_*`` 三个 handler——跟
``HonorConfigCog`` 同 pattern（详见 ``shared/docs/toml-config-design.md``）。

跟 ``HonorConfigCog`` 的对称::

    HonorConfigCog        → honor_{guild_id}.toml        via shared handler
    EmbedGuidesConfigCog  → embed_guides_{guild_id}.toml via shared handler

本 cog 本身没有自定义 handler——所有交互走共享框架。文件名、guild_id 校验、
hash 校验、首次上传二次确认、回调通知等逻辑都已经在 ``shared/config/toml_command``
里实现。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

import discord
from discord import app_commands

import config
from core.embed_guides.embed_guides_manager import EmbedGuidesConfigManager
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


class EmbedGuidesConfigCog(FeatureCog):
    """embed_guides_{guild_id}.toml 的 admin 命令入口。

    继承 FeatureCog（按项目约定所有 honor_system / role_system 之外的 cog 也走
    CoreCog 注册流程）。本 cog 不管理身份组缓存，也不进主面板——两个抽象方法
    实现为空操作。
    """

    embed_guides_group = app_commands.Group(
        name="指引文案丨配置",
        description="下载/上传/查看 embed 指引文案的 toml 配置（幻化 / 自助 / 荣誉活动）",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    def __init__(self, bot: "RoleBot"):
        super().__init__(bot)
        self.manager = EmbedGuidesConfigManager.get_instance()
        self.logger.info("指引文案配置 Cog 已加载")

    async def update_safe_roles_cache(self) -> None:
        """FeatureCog 抽象接口：本 cog 不管理任何身份组，no-op。"""
        return

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """FeatureCog 抽象接口：本 cog 不进主面板。"""
        return None

    @embed_guides_group.command(
        name="下载配置",
        description="下载 toml + doc；当前 SHA-256 前 12 字符在 embed 里显示",
    )
    @is_admin()
    async def cmd_download(self, interaction: discord.Interaction):
        await handle_toml_download(
            interaction,
            manager=self.manager,
            label="embed_guides",
            permission_check=None,
        )

    @embed_guides_group.command(
        name="上传配置",
        description="上传修改后的 toml；本地有配置时必须把 SHA-256 粘到 hash_str 字段（前 12 字符即可）",
    )
    @app_commands.rename(config_file="配置文件")
    @app_commands.describe(
        config_file="上传编辑后的 TOML 配置文件（建议命名 embed_guides_<guild_id>.toml）",
        hash_str="SHA-256 校验值（前 12 字符足够，完整 64 也可以）；首次上传（本地无配置）可省",
    )
    @is_admin()
    async def cmd_upload(
        self,
        interaction: discord.Interaction,
        config_file: discord.Attachment,
        hash_str: Optional[str] = None,
    ):
        await handle_toml_upload(
            interaction,
            manager=self.manager,
            toml_file=config_file,
            hash_str=hash_str,
            label="embed_guides",
            permission_check=None,
        )

    @embed_guides_group.command(
        name="查看配置哈希",
        description="查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）",
    )
    @is_admin()
    async def cmd_view_hash(self, interaction: discord.Interaction):
        await handle_toml_view_hash(
            interaction,
            manager=self.manager,
            label="embed_guides",
            permission_check=None,
        )


__all__ = ["EmbedGuidesConfigCog"]