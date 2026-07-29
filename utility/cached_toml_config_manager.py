"""带缓存 + 单例的 TomlConfigManager 抽象基类。

设计要点
--------

``TomlConfigManager``（``shared/config/toml_manager.py``）只做单文件 I/O + pydantic 验证；
本基类在其之上加两层常用功能：

1. **进程内单例**：每个 ``CachedTomlConfigManager`` 子类在自己 ``_instance`` 槽里占一个
   ——``HonorConfigManager.get_instance() != EmbedGuidesConfigManager.get_instance()``。
   跟 ``shared/data/json_manager.py`` 的 ``AsyncJsonDataManager.get_instance()`` 同套思路。
2. **per-guild cache**：``_cache: dict[int, T]`` 字典；``get(guild_id)`` 优先查 cache；
   ``validate_and_save()`` 覆盖后写盘 + 立即回填 cache（不等下次 ``get()`` 触发 lazy load）。

跟 ``AsyncJsonDataManager`` 的差别：
- 不程序化改 in-memory state（toml 是 admin 编辑的脚本，bot 是只读消费者），
  所以不需要 throttle / dirty flag / 后台落盘。
- ``get()`` 默认返回 ``Optional[T]``（无 toml → None）——子类按需 override。
  例如 ``EmbedGuidesConfig``（全 default_factory）重写为 ``get() -> T``，
  利用 covariant return 特性实现"无 toml → 默认 config"。

子类使用
--------

```python
from pathlib import Path
from honor_system.config_models import HonorGuildConfig
from utility.cached_toml_config_manager import CachedTomlConfigManager


class HonorConfigManager(CachedTomlConfigManager[HonorGuildConfig]):
    def __init__(self) -> None:
        super().__init__(
            data_dir=Path("data"),
            filename_pattern="honor_{guild_id}.toml",
            model_class=HonorGuildConfig,
            doc_path=Path("docs") / "荣誉系统使用手册.md",
        )

# 使用
cfg = HonorConfigManager.get_instance().get(guild_id)
if cfg is None:
    ...  # 无 toml 配置
```

未来推广
--------

设计目标：等有第 3 个 toml 配置落地（fashion / role_sync / 其他），把本基类推上
``_shared/config/cached_toml_config_manager.py``——三个 bot 仓库 subtree pull 共享。

为什么先放 ``role_bot/utility/`` 而不是 ``_shared/``：
- ``_shared`` 改动需要 subtree push + 三个 bot 仓库 pull，门槛高
- 本地 ``utility/`` 即用即改，反馈快
- 等有 3 个消费者证明抽象稳定后，再推 ``_shared``

参考：
- ``shared/config/toml_manager.py`` —— 基类的基类
- ``shared/data/json_manager.py`` 的 ``AsyncJsonDataManager.get_instance()`` —— 单例 pattern 来源
- ``honor_system/honor_config_manager.py`` / ``core/embed_guides/embed_guides_manager.py`` —— 当前 2 个子类
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel
from shared.config.toml_manager import TomlConfigManager

logger = logging.getLogger(__name__)


# 绑 BaseModel：所有 toml config 都是 pydantic 模型
T = TypeVar("T", bound=BaseModel)


class CachedTomlConfigManager(TomlConfigManager[T], Generic[T]):
    """带缓存 + 单例的 ``TomlConfigManager`` 抽象基类。

    ## 基类提供

    - 单例（``get_instance()`` / ``_reset_instance_for_tests()``）
    - per-guild cache（``_cache: dict[int, T]``）
    - ``invalidate(guild_id)`` / ``invalidate_all()``
    - ``validate_and_save()`` 覆盖：写盘后自动回填 cache

    ## 子类必须提供

    - ``__init__()``：调 ``super().__init__()`` 传 ``data_dir`` / ``filename_pattern``
      / ``model_class`` / ``doc_path``
    - 通常还有模块级常量 ``_DATA_DIR`` / ``_FILENAME_PATTERN`` / ``_DOC_PATH``

    ## 子类可选 override

    - ``get(guild_id) -> Optional[T]`` 默认实现：cache 命中或 None（适用于
      ``HonorGuildConfig`` 这类含 required fields 的 config）。
      若子类 config **全部 default_factory**（如 ``EmbedGuidesConfig``），
      重写为 ``get(guild_id) -> T``：用 ``self.load(guild_id)`` 替代
      ``read_raw + _parse + None check``。covariant return 允许子类 override
      返回更窄类型（T ⊆ Optional[T]）。

    ## 单例机制

    每个 ``CachedTomlConfigManager`` 子类在自己 ``_instance`` 槽里占一个——
    Python 类属性继承 + first-write-wins 实现子类隔离。
    子类不需要再声明 ``_instance`` / ``_creation_lock``，直接 ``super().get_instance()``
    即可（``get_instance()`` 用 ``cls._instance``，自动路由到子类）。
    """

    # 单例：每个子类在自己 _instance 槽里占一个（Python 类属性继承 + 首次写时 shadow）
    _instance: Optional["CachedTomlConfigManager[T]"] = None
    _creation_lock = threading.Lock()

    # per-guild cache
    _cache: dict[int, T]

    def __init__(
        self,
        *,
        data_dir: Path | str,
        filename_pattern: str,
        model_class: type[T],
        doc_path: Path | str,
    ) -> None:
        super().__init__(
            data_dir=data_dir,
            filename_pattern=filename_pattern,
            model_class=model_class,
            doc_path=doc_path,
        )
        self._cache = {}
        logger.info(
            "%s 初始化：data_dir=%s, filename_pattern=%s, doc_path=%s",
            type(self).__name__,
            self.data_dir,
            self.filename_pattern,
            self.doc_path,
        )

    # --- 单例 ---

    @classmethod
    def get_instance(cls) -> "CachedTomlConfigManager[T]":
        """进程内唯一实例。

        每个 ``CachedTomlConfigManager`` 子类在自己 ``_instance`` 槽里占一个——
        ``HonorConfigManager.get_instance() != EmbedGuidesConfigManager.get_instance()``。

        实现细节：``cls._instance`` 用 Python 类属性查找——子类首次调用时
        ``cls._instance`` 解析到父类的 ``None``（继承），写入时在子类本身
        创建 ``_instance`` 槽，shadow 父类。
        """
        if cls._instance is None:
            with cls._creation_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def _reset_instance_for_tests(cls) -> None:
        """**仅测试用**：清理单例让下次 ``get_instance()`` 重建。生产代码别调。"""
        cls._instance = None

    # --- cache 接口（默认实现：Optional[T]） ---

    def get(self, guild_id: int) -> Optional[T]:
        """按 guild_id 取配置；优先 cache。

        默认实现：无 toml → 返回 ``None``（适用于 ``HonorGuildConfig`` 这类
        含 required fields 的 config）。

        与基类 ``load(guild_id)`` 的差别：
        - ``load``：无 toml → 返回 ``model_class()`` 默认值（in-memory），
          但调用方需要感知"这是默认还是配置的"——不友好。
        - 本方法：通过 ``read_raw + _parse`` 拿真实 toml 内容；文件不存在
          → 返回 ``None`` 让调用方明确处理"无配置"分支。

        子类 config 若**全部 default_factory**（如 ``EmbedGuidesConfig``），
        可重写为返回 ``T``（covariant return：``T ⊆ Optional[T]``），
        利用 ``self.load(guild_id)`` 在无 toml 时返回 ``model_class()`` 默认值。

        Returns:
            ``T``：cache 命中或从 toml 加载成功。
            ``None``：该 guild 没有 toml 文件（仅默认实现场景）。

        Raises:
            ``pydantic.ValidationError``：toml 内容无法通过 schema 验证。
            ``ValueError``：TOML 解析失败。
            调用方应 ``try/except`` 决定如何降级。
        """
        if guild_id in self._cache:
            return self._cache[guild_id]

        raw = self.read_raw(guild_id)
        if raw is None:
            return None  # 没 toml——明确返回 None，不缓存默认值

        cfg = self._parse(raw, guild_id)
        self._cache[guild_id] = cfg
        return cfg

    def invalidate(self, guild_id: int) -> None:
        """失效某个 guild 的 cache。下次 ``get()`` 会重新从磁盘加载。"""
        self._cache.pop(guild_id, None)

    def invalidate_all(self) -> None:
        """失效所有 cache（极少用——比如运行时改了 ``data_dir``）。"""
        self._cache.clear()

    # --- 上传（admin 写盘路径）覆盖基类方法，自动维护 cache ---

    def validate_and_save(
        self,
        raw: bytes,
        guild_id: int,
        *,
        expected_hash: Optional[str] = None,
    ) -> T:
        """落盘 + 立即回填 cache（不等下次 ``get()`` 触发 lazy load）。

        基类的逻辑（pydantic 验证 + write-time hash check + 原样落盘）完整继承；
        重写只是多一步 cache 回填——上传完成后立即生效，无需重启 bot。
        """
        cfg = super().validate_and_save(raw, guild_id, expected_hash=expected_hash)
        self._cache[guild_id] = cfg
        return cfg


__all__ = ["CachedTomlConfigManager"]
