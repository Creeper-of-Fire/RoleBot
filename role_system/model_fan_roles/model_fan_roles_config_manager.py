"""model_fan_roles toml 配置的单例 manager + per-guild cache。

跟 honor / embed_guides / fashion 同一套 pattern——继承
``CachedTomlConfigManager`` 获得单例 + per-guild cache + invalidate +
validate_and_save 回填 cache。

``ModelFanRolesGuildConfig`` 没有 required fields，但语义上"无 toml"应该跟
"已配置空 models 列表"区分（前者 = 服务器未启用 model_fan_roles，后者 = 启用但
没模型）。所以本 manager **不** override 基类的 ``get()``——保留 ``Optional[T]``
行为，让调用方拿到 ``None`` 时明确处理"该服未启用模型阵营"分支。

基类 ``CachedTomlConfigManager``（``role_bot/utility/``）封装：
- 单例（get_instance / _reset_instance_for_tests）
- per-guild cache
- invalidate(guild_id) / invalidate_all()
- validate_and_save override（写盘后自动回填 cache）

将来 CachedTomlConfigManager 推到 ``_shared/config/`` 时三个 bot 仓库共享。

详见 ``shared/docs/toml-config-design.md``。
"""

from __future__ import annotations

import logging
from pathlib import Path

from role_system.model_fan_roles.model_fan_roles_config_models import (
    ModelFanRolesGuildConfig,
)
from utility.cached_toml_config_manager import CachedTomlConfigManager

logger = logging.getLogger(__name__)


# model_fan_roles 配置的固定参数——单点维护，所有 model_fan_roles 相关 cog 都从这里拿 manager。
# 模块级常量方便测试 / 排查时一眼看到当前路径，不靠 grep 调用点。
_DATA_DIR = Path("data")
_FILENAME_PATTERN = "model_fan_roles_{guild_id}.toml"
_DOC_PATH = Path("docs") / "model-fan-roles-doc.md"


class ModelFanRolesConfigManager(CachedTomlConfigManager[ModelFanRolesGuildConfig]):
    """model_fan_roles toml 的单例 manager + per-guild cache。

    所有 model_fan_roles 相关模块（ModelFanRolesCog / ModelFanRolesConfigCog /
    ModelRolesView）共用一个实例；per-guild cache 自动管理。

    行为：
    - ``get_instance()`` / ``_reset_instance_for_tests()``：继承基类
    - ``get(guild_id) -> Optional[ModelFanRolesGuildConfig]``：继承基类默认实现
      （model_fan_roles 的"无 toml"语义是"未启用模型阵营"，需要 ``None`` 区分）
    - ``invalidate(guild_id)`` / ``invalidate_all()``：继承基类
    - ``validate_and_save()``：继承基类（写盘后回填 cache）
    """

    def __init__(self) -> None:
        super().__init__(
            data_dir=_DATA_DIR,
            filename_pattern=_FILENAME_PATTERN,
            model_class=ModelFanRolesGuildConfig,
            doc_path=_DOC_PATH,
        )


__all__ = ["ModelFanRolesConfigManager"]
