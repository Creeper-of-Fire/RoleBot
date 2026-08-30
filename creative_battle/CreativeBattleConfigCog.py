"""creative_battle toml 配置 cog —— /合战丨配置 命令组。

仿 ``honor_system/HonorConfigCog.py`` 模式，复用 ``shared/config/toml_command.py``
的通用 handler。

设计原则（按 ``role_bot/AGENTS.md``）：

- toml 是 admin 单一控制源：admin 通过 ``/上传配置`` 改 toml，bot 是只读消费者
- ``validate_and_save`` 失败时**不写入**，内容指纹机制防止 TOCTOU
- handler 返回的 embed 都含「为什么需要 sha256」解释段（小白友好）
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import discord
from discord import app_commands

import config
from creative_battle.creative_battle_config_manager import CreativeBattleConfigManager
from shared.config.toml_command import (
    handle_toml_download,
    handle_toml_upload,
    handle_toml_view_hash,
)
from utility.feature_cog import FeatureCog
from utility.permison import is_admin

if TYPE_CHECKING:
    from main import RoleBot


class CreativeBattleConfigCog(FeatureCog, name="CreativeBattleConfig"):
    """creative_battle toml 配置 cog。"""

    def get_main_panel_entries(self):
        return None

    async def update_safe_roles_cache(self) -> None:
        return

    def __init__(self, bot: "RoleBot") -> None:
        super().__init__(bot)
        self.manager = CreativeBattleConfigManager.get_instance()
        self.logger.info("创作大会配置 Cog 已加载")

    # --- 命令组 ---

    config_group = app_commands.Group(
        name="合战丨配置",
        description="创作大会 toml 配置（下载 / 上传 / 查看哈希）",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    @config_group.command(
        name="下载配置",
        description="下载 toml + doc；当前 SHA-256 前 12 字符在 embed 里显示",
    )
    @is_admin()
    async def cmd_download(self, interaction: discord.Interaction) -> None:
        await handle_toml_download(
            interaction,
            manager=self.manager,
            label="creative_battle",
            permission_check=None,
        )

    @config_group.command(
        name="上传配置",
        description="上传修改后的 toml；本地有配置时必须把 SHA-256 粘到 hash_str 字段（前 12 字符即可）",
    )
    @app_commands.describe(
        toml_file="修改后的 toml 文件",
        hash_str=(
            "SHA-256 校验值（前 12 字符足够，完整 64 也可以）；"
            "首次上传（本地无配置）可省"
        ),
    )
    @is_admin()
    async def cmd_upload(
        self,
        interaction: discord.Interaction,
        toml_file: discord.Attachment,
        hash_str: str | None = None,
    ) -> None:
        await handle_toml_upload(
            interaction,
            manager=self.manager,
            toml_file=toml_file,
            hash_str=hash_str,
            label="creative_battle",
            permission_check=None,
        )

    @config_group.command(
        name="查看配置哈希",
        description="查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）",
    )
    @is_admin()
    async def cmd_view_hash(self, interaction: discord.Interaction) -> None:
        await handle_toml_view_hash(
            interaction,
            manager=self.manager,
            label="creative_battle",
            permission_check=None,
        )


async def setup(bot: "RoleBot") -> None:
    await bot.add_cog(CreativeBattleConfigCog(bot))


__all__ = ["CreativeBattleConfigCog", "setup"]