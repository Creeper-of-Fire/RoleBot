"""fashion_{guild_id}.toml 的 pydantic schema。

每个 guild 一份 toml（per-guild 设计，遵循 ``shared/docs/toml-config-design.md``
的 "toml = per-guild" 红线）。文件结构::

    data/fashion_{guild_id}.toml

字段定义
--------

顶层 schema 只有一个字段：``fashion_map: list[FashionMapEntry]``，每条 entry
是一个独立对象，描述"哪个 base role → 哪些 fashion role + UI 行为"。

**为什么用 list-of-object 而不是 dict-of-list？**

最初设计是 ``fashion_map: dict[int, list[int]]`` 配全局 ``not_normal_role_ids`` 列表，
判断"锁定后是否隐藏"靠 set 成员资格——典型的过程式 / 数值计算风格。改成
list-of-object 后：

- 每条 entry 自带 ``hidden_when_locked`` 字段，UI 行为直接绑定到数据本身
- 未来加 ``display_name`` / ``priority`` / ``archived`` 等属性都在 entry 上扩展
- 跟 honor 的 ``HonorGuildConfig.definitions: list[HonorDefinitionItem]`` 同构

字段类型决策
-----------

- ``base_role_id`` 和 ``fashion_role_ids``: int（Discord snowflake 数字）——
  toml 自然支持，跟 honor 的 role_id 保持一致。pydantic 验证同时校验 Discord
  snowflake 范围（17-20 位）。
- ``hidden_when_locked``: bool，**必填无默认**——强制 admin 显式声明 UI 行为，
  不允许"忘了写 = 默认藏了"或"忘了写 = 默认不藏"的隐式约定。

跟 honor 的对齐
--------------

honor 的 ``definitions`` 用 ``Annotated[list[HonorDefinitionItem], TomlMergeAsTableList()]``
——本 model 的 ``fashion_map`` 也是同套 AoT pattern，调用方对 honor/fashion 一视同仁。
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from shared.config.toml_merge import TomlMergeAsTableList


class FashionMapEntry(BaseModel):
    """单个「基础身份组 → 幻化身份组」映射条目。

    Attributes:
        base_role_id: 基础身份组 ID（Discord snowflake）。用户持有该身份组
            即可选择佩戴 ``fashion_role_ids`` 中的任一幻化组。
        fashion_role_ids: 可幻化身份组 ID 列表。可空（base_role 持有者没有可用幻化）。
        hidden_when_locked: 当用户**未持有**该基础身份组、且 ``fashion_role_ids``
            中任何一个都未持有时，UI 是否隐藏对应的幻化选项。

            - true：隐藏——典型用于 "Server Boosted / BOT 维护员 / 答疑 AI" 等
              "非普通" 基础组。普通用户既没基础组也没幻化组时，不应该看到幻化
              选项（避免"我能不能领"的误导）。
            - false：不隐藏——典型用于 "创作者 / 助力者 / 破限组" 等普通基础组。
              普通用户即使没基础组，幻化选项也展示出来，作为引导。

            **必填无默认**：强制 admin 显式声明 UI 行为，不允许隐式约定。
    """

    base_role_id: int = Field(
        ...,
        description="基础身份组 ID（Discord snowflake int）",
    )
    fashion_role_ids: list[int] = Field(
        ...,
        description="可幻化身份组 ID 列表",
    )
    hidden_when_locked: bool = Field(
        ...,
        description=(
            "未解锁时是否在 UI 隐藏对应幻化选项。"
            "true = 非普通基础组（如 Server Boosted），false = 普通基础组。"
            "必填无默认——admin 必须显式声明。"
        ),
    )


class FashionGuildConfig(BaseModel):
    """一个 guild 的完整幻化配置。

    ``fashion_map`` 用 ``Annotated[list[FashionMapEntry], TomlMergeAsTableList()]``
    让 manager 写时按 array-of-tables (``[[fashion_map]]``) 替换，保留 array
    中间项的注释。

    Attributes:
        fashion_map: 幻化映射条目列表（每条自带 base_role_id + fashion_role_ids +
            hidden_when_locked 三个字段，OO 风格）。
    """

    fashion_map: Annotated[list[FashionMapEntry], TomlMergeAsTableList()] = Field(
        default_factory=list,
        description=(
            "幻化映射条目列表（[[fashion_map]] array-of-tables 表达）。"
            "每条 entry 自带 hidden_when_locked 字段，不再依赖外部全局列表。"
        ),
    )


__all__ = ["FashionMapEntry", "FashionGuildConfig"]
