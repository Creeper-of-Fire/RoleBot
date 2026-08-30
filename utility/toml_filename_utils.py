"""toml 文件名解析 utility —— 按 {prefix}{guild_id}{suffix} 命名规范提取 guild_id。

适用场景
--------

cog 启动时遍历所有 per-guild toml 配置。例如：

- ``honor_system/cup_honor/cup_honor_module.py`` 按 ``honor_*.toml`` 列 guild_id
- ``creative_battle/CreativeBattleCog.py`` 按 ``creative_battle_*.toml`` 列 guild_id

场景共性：每个 cog 都写过 ``data_dir.glob(...)`` + ``re.match(r"prefix_(\\d+).toml")`` 模板代码。
本 utility 把模板代码抽到一处。

设计原则（按 ``role_bot/AGENTS.md``）
-------------------------------------

- 不硬编码 prefix / suffix（参数化）
- 不耦合任何具体 cog（函数式，按需调用）
- 不在 ``_shared`` 层（按 ``cached_toml_config_manager.py`` 的先例——等 3+ 个消费者再推）

Why 不在 ``_shared/``
-------------------

- ``_shared`` 改动需要 subtree push + 三个 bot 仓库 pull，门槛高
- 本地 ``role_bot/utility/`` 即用即改，反馈快
- 等 honor / creative_battle / fashion / role_sync 都用上后，再推 ``_shared/config/``
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator


def iter_guild_ids_from_toml_files(
    data_dir: Path | str,
    prefix: str,
    suffix: str = ".toml",
) -> Iterator[int]:
    """从 {prefix}{guild_id}{suffix} 文件里提取所有 guild_id。

    Args:
        data_dir: toml 文件所在目录（如 ``Path("data")``）。
        prefix: 文件名前缀（如 ``"honor_"`` / ``"creative_battle_"``）。
        suffix: 文件后缀（默认 ``".toml"``）。

    Yields:
        guild_id（snowflake int）。文件名不匹配 ``{prefix}\\d+{suffix}`` 规范的被跳过。

    Example::

        # 仿 honor_system.CupHonorModuleCog._iter_configured_guild_ids()
        for guild_id in iter_guild_ids_from_toml_files(Path("data"), "honor_"):
            cfg = honor_config_mgr.get(guild_id)
    """
    data_dir = Path(data_dir)
    if not data_dir.exists():
        return
    # 用 pattern 确保文件名严格符合 {prefix}{guild_id}{suffix}
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+){re.escape(suffix)}$")
    for path in data_dir.glob(f"{prefix}*{suffix}"):
        m = pattern.match(path.name)
        if m:
            yield int(m.group(1))


__all__ = ["iter_guild_ids_from_toml_files"]