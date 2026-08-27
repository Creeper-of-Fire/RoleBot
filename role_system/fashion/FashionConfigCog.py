"""幻化 toml 配置的 Discord 命令入口（薄壳）。

跟 ``HonorConfigCog`` / ``EmbedGuidesConfigCog`` 同套 pattern——复用
``shared/config/toml_command.handle_toml_*`` 三个 handler，本 cog 只挂装饰器
+ 透传参数。

命令组：`幻化衣橱丨配置`
- /下载配置
- /上传配置（含 toml 写盘 + FashionCog.safe_fashion_map_cache 热刷新——免重启）
- /查看配置哈希

注意：本 cog 跟 FashionCog 解耦——FashionCog 持有 ``safe_fashion_map_cache``，
本 cog 只管 toml 文件的 CRUD。上传后调一次 ``FashionCog.update_safe_roles_cache()``
让运行中的 bot 立刻看到新映射。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

import discord
from discord import app_commands

import config
from role_system.fashion.fashion_config_manager import FashionConfigManager
from role_system.fashion.FashionCog import FashionCog
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


def _get_fashion_cog(bot: "RoleBot") -> Optional[FashionCog]:
    """拿 FashionCog 引用——若未加载返回 None。

    FashionCog 跟本 cog 解耦（FeatureCog 注册顺序不一定），所以这里用 getattr
    而不是直接 import 触发 setup。找不到就 log warning 但不阻塞上传——admin
    自己重启 bot 也能让新 toml 生效。
    """
    cog = bot.get_cog("Fashion")
    return cog if isinstance(cog, FashionCog) else None


class FashionConfigCog(FeatureCog):
    """幻化 toml 配置的 admin 命令入口。

    继承 FeatureCog（按项目约定所有 toml config cog 都通过 FeatureCog 注册到
    CoreCog）。本 cog 不管理身份组缓存（FashionCog 已经管了所有 fashion role 缓存），
    也不进主面板——两个抽象方法实现为空操作。

    注：本 cog 的 ``manager`` 是 FashionConfigManager 单例；cache 也由 manager 维护。
    """

    fashion_config_group = app_commands.Group(
        name="幻化衣橱丨配置",
        description="下载/上传/查看幻化衣橱的 toml 配置（fashion_map + NOT_NORMAL 列表）",
        guild_ids=[gid for gid in config.GUILD_IDS],
        default_permissions=discord.Permissions(manage_roles=True),
    )

    def __init__(self, bot: "RoleBot"):
        super().__init__(bot)
        self.manager = FashionConfigManager.get_instance()
        self.logger.info("幻化衣橱配置 Cog 已加载")

    async def update_safe_roles_cache(self) -> None:
        """FeatureCog 抽象接口：本 cog 不管理任何身份组，no-op。"""
        return

    def get_main_panel_entries(self) -> Optional[List[PanelEntry]]:
        """FeatureCog 抽象接口：本 cog 不进主面板。"""
        return None

    def _trigger_fashion_cache_refresh(self, guild_id: int) -> None:
        """触发 FashionCog 重新读 toml 填充 ``safe_fashion_map_cache``。

        接到任务后立即返回；刷新是 background task，不阻塞 slash 指令响应。
        FashionCog 不可用（cog 没加载）时 no-op，admin 可手动重启 bot 兜底。
        """
        fashion_cog = _get_fashion_cog(self.bot)
        if not fashion_cog:
            logger.warning(
                "FashionCog 未加载，跳过 safe_fashion_map_cache 热刷新（toml 已落盘但 cache 暂未刷新，需重启 bot）"
            )
            return
        self.bot.loop.create_task(fashion_cog.update_safe_roles_cache())

    # ================= 斜杠命令 =================

    @fashion_config_group.command(
        name="下载配置",
        description="下载 toml + doc；当前 SHA-256 前 12 字符在 embed 里显示",
    )
    @is_admin()
    async def cmd_download_config(self, interaction: discord.Interaction):
        await handle_toml_download(
            interaction,
            manager=self.manager,
            label="fashion",
            permission_check=None,
        )

    @fashion_config_group.command(
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
        hash_str: Optional[str] = None,
    ):
        guild_id = interaction.guild.id if interaction.guild else 0
        await handle_toml_upload(
            interaction,
            manager=self.manager,
            toml_file=config_file,
            hash_str=hash_str,
            label="fashion",
            permission_check=None,
        )
        # 上传成功后触发 FashionCog 热刷新 cache（toml 已经写到磁盘 + manager 已回填 cache，
        # 但 FashionCog.safe_fashion_map_cache 是 cog 自己的 in-memory 结构，需要重新跑一遍
        # update_safe_roles_cache 把 cache 也填上）。无论上传成败 idem 是幂等的，上传失败时
        # （旧 toml 还在盘上）也能正常工作。
        if guild_id:
            self._trigger_fashion_cache_refresh(guild_id)

    @fashion_config_group.command(
        name="查看配置哈希",
        description="查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）",
    )
    @is_admin()
    async def cmd_view_hash(self, interaction: discord.Interaction):
        await handle_toml_view_hash(
            interaction,
            manager=self.manager,
            label="fashion",
            permission_check=None,
        )


__all__ = ["FashionConfigCog"]
