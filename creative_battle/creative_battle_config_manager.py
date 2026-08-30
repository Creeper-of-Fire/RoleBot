"""creative_battle toml 配置的单例 manager + per-guild cache。

仿 ``role_bot/honor_system/honor_config_manager.py`` 模式：

- 单例（``get_instance()`` / ``_reset_instance_for_tests()``）
- per-guild cache（自动继承 ``CachedTomlConfigManager``）
- ``invalidate(guild_id)`` / ``invalidate_all()``
- ``validate_and_save()`` 写盘后自动回填 cache

详见 ``shared/docs/toml-config-design.md`` 与 ``role_bot/AGENTS.md``。
"""
from __future__ import annotations

import logging
from pathlib import Path

from creative_battle.creative_battle_models import CreativeBattleGuildConfig
from utility.cached_toml_config_manager import CachedTomlConfigManager

logger = logging.getLogger(__name__)


# creative_battle 配置的固定参数——单点维护，所有 creative_battle 相关 cog 都从这里拿 manager。
# 模块级常量方便测试 / 排查时一眼看到当前路径，不靠 grep 调用点。
_DATA_DIR = Path("data")
_FILENAME_PATTERN = "creative_battle_{guild_id}.toml"
_DOC_PATH = Path("docs") / "creative-battle-admin-doc.md"  # admin doc（v2 写）


class CreativeBattleConfigManager(CachedTomlConfigManager[CreativeBattleGuildConfig]):
    """creative_battle toml 的单例 manager + per-guild cache。

    所有 creative_battle 相关模块（CreativeBattleCog / CreativeBattleConfigCog /
    CreativeBattleSeasonLoop）共用一个实例；per-guild cache 自动管理。

    行为：
    - ``get_instance()`` / ``_reset_instance_for_tests()``：继承基类
    - ``get(guild_id) -> Optional[CreativeBattleGuildConfig]``：继承基类默认实现
      （含 required fields，无 toml → None）
    - ``invalidate(guild_id)`` / ``invalidate_all()``：继承基类
    - ``validate_and_save()``：继承基类（写盘后回填 cache）
    """

    def __init__(self) -> None:
        super().__init__(
            data_dir=_DATA_DIR,
            filename_pattern=_FILENAME_PATTERN,
            model_class=CreativeBattleGuildConfig,
            doc_path=_DOC_PATH,
        )


__all__ = ["CreativeBattleConfigManager"]