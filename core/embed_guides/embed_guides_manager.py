"""embed_guides_{guild_id}.toml 的单例 manager + per-guild cache。

设计要点
--------

**Per-guild 文件**（``data/embed_guides_{guild_id}.toml``），跟 ``honor_*.toml`` /
``cup_honors_*.toml`` 等同套 pattern。这跟 ``shared/docs/toml-config-design.md`` 的
"toml = per-guild" 红线一致——``TomlConfigManager`` 的所有 API 都以
``guild_id`` 为强制参数，本 manager 不会绕过这层约束。

**所有 section 自带默认**——``EmbedGuidesConfig`` 的每个 section 都有
``default_factory``，所以 ``get(guild_id)`` 重写基类默认实现——无 toml 时返回
完整默认 config（每个 section 都是有效 ``EmbedGuideSection``），调用方不需要
None 检查，直接 ``cfg.fashion_guide.to_embed()``。

基类 ``CachedTomlConfigManager``（``role_bot/utility/``）封装：
- 单例（get_instance / _reset_instance_for_tests）
- per-guild cache
- invalidate(guild_id) / invalidate_all()
- validate_and_save override（写盘后自动回填 cache）

将来 fashion / role_sync 等也迁 toml 时，把基类推到 ``_shared/config/``，
三个 bot 仓库 subtree pull 共享。
"""

from __future__ import annotations

import logging
from pathlib import Path

from core.embed_guides.embed_guides_models import EmbedGuidesConfig
from utility.cached_toml_config_manager import CachedTomlConfigManager

logger = logging.getLogger(__name__)


# embed_guides 配置的固定参数——单点维护
_DATA_DIR = Path("data")
_FILENAME_PATTERN = "embed_guides_{guild_id}.toml"  # per-guild（跟 honor 一致）
_DOC_PATH = Path("docs") / "embed-guides-doc.md"


class EmbedGuidesConfigManager(CachedTomlConfigManager[EmbedGuidesConfig]):
    """embed_guides_{guild_id}.toml 的单例 manager + per-guild cache。

    用法::

        mgr = EmbedGuidesConfigManager.get_instance()
        cfg = mgr.get(guild_id)
        embed = cfg.fashion_guide.to_embed()  # 始终有效，无 None 检查

    跟 ``HonorConfigManager`` 的差别：
    - ``get(guild_id) -> EmbedGuidesConfig``（**非** ``Optional``）——因为
      ``EmbedGuidesConfig`` 所有 section 自带 default_factory，无 toml 时也是
      完整有效对象（默认 ``model_class()``）。
    - 基类默认 ``get() -> Optional[T]`` 不适合本场景（强制调用方 None 检查
      是冗余防御），所以重写为直接返回 ``T``，covariant return（``T ⊆ Optional[T]``）。
    """

    def __init__(self) -> None:
        super().__init__(
            data_dir=_DATA_DIR,
            filename_pattern=_FILENAME_PATTERN,
            model_class=EmbedGuidesConfig,
            doc_path=_DOC_PATH,
        )

    def get(self, guild_id: int) -> EmbedGuidesConfig:
        """按 guild_id 取配置；优先 cache。

        与基类默认实现的差别：
        - 基类 ``get() -> Optional[T]`` 用 ``read_raw + _parse`` 拿真实 toml，
          文件不存在 → 返回 ``None``。
        - 本实现用 ``self.load(guild_id)``——基类 ``TomlConfigManager.load()``
          在文件不存在时返回 ``model_class()`` 默认值（in-memory）。配合
          ``EmbedGuidesConfig`` 的全 default_factory schema，调用方拿到的
          ``cfg`` 永远有效，无 None 检查。

        Returns:
            ``EmbedGuidesConfig``：cache 命中或从 toml 加载成功或模型默认值
            （永远非 None；covariant return 收窄类型）。

        Raises:
            ``pydantic.ValidationError``：toml 内容无法通过 schema 验证。
            ``ValueError``：TOML 解析失败。
        """
        if guild_id in self._cache:
            return self._cache[guild_id]
        cfg = self.load(guild_id)  # 基类：文件不存在 → 返回 model_class() 默认值
        self._cache[guild_id] = cfg
        return cfg


__all__ = ["EmbedGuidesConfigManager"]
