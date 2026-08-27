"""model_fan_roles_{guild_id}.toml 的 pydantic schema。

每个 guild 一份 toml（per-guild 设计，遵循 ``shared/docs/toml-config-design.md``
的 "toml = per-guild" 红线）。文件结构::

    data/model_fan_roles_{guild_id}.toml

字段定义
--------

- ``models``: list[ModelRoleConfig] —— 该服可领取的"大模型粉丝"身份组列表。
  每个条目 ``ModelRoleConfig`` 含 ``name`` / ``display_name`` / ``role_id`` /
  ``emoji`` 四个字段，bot 启动时按列表渲染按钮面板。

跟 honor 的对称
---------------

- honor 的 ``HonorGuildConfig.definitions`` 是 ``list[HonorDefinitionItem]``，
  本 model 的 ``models`` 是 ``list[ModelRoleConfig]``——同构。
- 都标 ``Annotated[list[...], TomlMergeAsTableList()]``，让 ``TomlConfigManager``
  写时按 array-of-tables (``[[models]]``) 替换，保留 array 中间项的注释。
- ``ModelRoleConfig`` 跟 ``HonorDefinitionItem`` 同样是 pydantic BaseModel +
  Field(...) schema，但更简单（4 个字段，无嵌套）。

历史
----

2026-08 前：``role_system/model_fan_roles/model_config.py`` 里 ``MODEL_ROLES_CONFIG``
是**硬编码** pydantic 实例字典——要加模型 / 改名字都得改 Python + 部署 + 重启 bot。
本 schema 把这些实例移到 toml，admin 用 ``/模型阵营丨配置`` 命令组下载 / 上传。
"""

from __future__ import annotations

from typing import Annotated, Optional

from pydantic import BaseModel, Field

from shared.config.toml_merge import TomlMergeAsTableList


class ModelRoleConfig(BaseModel):
    """单个模型身份组的配置。

    Attributes:
        name: 内部名称（如 ``"Gemini"``），用作 toml 注释辨识。
        display_name: 用户可见的按钮标签（如 ``"哈基米"``）。
        role_id: Discord 身份组 ID（snowflake int）。
        emoji: 按钮前缀 emoji（可选）。
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="内部名称（如 'Gemini'），用作 toml 注释辨识",
    )
    display_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="用户可见的按钮标签（如 '哈基米'）",
    )
    role_id: int = Field(
        ...,
        description="Discord 身份组 ID（snowflake int）",
    )
    emoji: Optional[str] = Field(
        None,
        max_length=64,
        description="按钮前缀 emoji（可选）",
    )


class ModelFanRolesGuildConfig(BaseModel):
    """一个 guild 的完整模型粉丝身份组配置。

    Attributes:
        models: 该服可领取的模型身份组列表。``Annotated[..., TomlMergeAsTableList()]``
            让 manager 写时按 array-of-tables 替换，保留 array 中间项的注释。
    """

    models: Annotated[list[ModelRoleConfig], TomlMergeAsTableList()] = Field(
        default_factory=list,
        description="该服可领取的模型身份组列表（[[models]] array-of-tables 表达）",
    )


__all__ = ["ModelRoleConfig", "ModelFanRolesGuildConfig"]
