"""Honor toml 配置的单例 manager + per-guild cache。

``HonorGuildConfig`` 含 required fields（``definitions`` 等），所以 ``get(guild_id)``
继承基类默认实现——无 toml → 返回 ``None``，调用方明确感知"无配置"分支。

基类 ``CachedTomlConfigManager``（``role_bot/utility/``）封装：
- 单例（get_instance / _reset_instance_for_tests）
- per-guild cache
- invalidate(guild_id) / invalidate_all()
- validate_and_save override（写盘后自动回填 cache）

将来 fashion / role_sync 等也迁 toml 时，把基类推到 ``_shared/config/``，
三个 bot 仓库 subtree pull 共享。

详见 ``shared/docs/toml-config-design.md``。
"""

from __future__ import annotations

import logging
from pathlib import Path

from honor_system.config_models import HonorGuildConfig
from utility.cached_toml_config_manager import CachedTomlConfigManager

logger = logging.getLogger(__name__)


# Honor system 配置的固定参数——单点维护，所有 honor 相关 cog 都从这里拿 manager。
# 模块级常量方便测试 / 排查时一眼看到当前路径，不靠 grep 调用点。
_DATA_DIR = Path("data")
_FILENAME_PATTERN = "honor_{guild_id}.toml"
_DOC_PATH = Path("docs") / "荣誉系统使用手册.md"


class HonorConfigManager(CachedTomlConfigManager[HonorGuildConfig]):
    """Honor toml 的单例 manager + per-guild cache。

    所有 honor 相关模块（HonorCog / HonorConfigCog / anniversary / claimable /
    cup_honor / HonorManageView）共用一个实例；per-guild cache 自动管理。

    行为：
    - ``get_instance()`` / ``_reset_instance_for_tests()``：继承基类
    - ``get(guild_id) -> Optional[HonorGuildConfig]``：继承基类默认实现
      （HonorGuildConfig 含 required fields，无 toml → None）
    - ``invalidate(guild_id)`` / ``invalidate_all()``：继承基类
    - ``validate_and_save()``：继承基类（写盘后回填 cache）
    """

    def __init__(self) -> None:
        super().__init__(
            data_dir=_DATA_DIR,
            filename_pattern=_FILENAME_PATTERN,
            model_class=HonorGuildConfig,
            doc_path=_DOC_PATH,
        )


__all__ = ["HonorConfigManager"]
