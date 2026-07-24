"""TOML 文档合并工具。

设计目标：
- 读用 tomllib（标准库，严格）
- 写用 tomlkit（保留注释、键顺序）
- "merge" 是为了既能更新现有 toml 文档（保留注释），又能用 pydantic
  dump 出来的 dict 替换/新增字段

核心场景：业务把读到的 toml（保留注释）merge 进 pydantic dump 出来的 dict。
业务模型可用 `TomlMergeAsTableList()` 标记 list-of-dict 字段，让 merge
函数知道这个字段该用 tomlkit item（array of tables）处理。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import tomlkit


@dataclass(frozen=True)
class TomlMergeAsTableList:
    """pydantic 字段注解：标记 list-of-dict 字段合并时用 tomlkit item 处理。

    用法：

        from typing import Annotated
        from pydantic import BaseModel, Field
        from shared.config.toml_merge import TomlMergeAsTableList

        class Foo(BaseModel):
            items: Annotated[list[dict], TomlMergeAsTableList()] = Field(
                default_factory=list
            )

    合并时这个字段会被 `tomlkit.item(value)` 替换为 array of tables；
    不带标记的 list-of-dict 字段会被当普通 list 处理（dump 成 inline array）。
    """

    pass


def collect_table_list_fields(model_class: type) -> frozenset[str]:
    """从 pydantic V2 model_fields 提取所有 `TomlMergeAsTableList` 标记的字段名。

    Args:
        model_class: 任意 pydantic BaseModel 子类。

    Returns:
        字段名集合。若 model_class 不是 BaseModel 子类，返回空 frozenset。
    """
    from pydantic import BaseModel

    if not (isinstance(model_class, type) and issubclass(model_class, BaseModel)):
        return frozenset()
    return frozenset(
        name
        for name, field in model_class.model_fields.items()
        if any(isinstance(m, TomlMergeAsTableList) for m in field.metadata)
    )


def merge_into_doc(
    doc: tomlkit.TOMLDocument,
    data: dict[str, Any],
    *,
    table_list_fields: frozenset[str] = frozenset(),
) -> None:
    """把 pydantic dump 出来的 dict 合并到现有 tomlkit 文档。

    保留注释和键顺序。已被新 data 删除的字段一并删除。

    合并规则：
    - 顶层 key：
      - 若 value 是 None：跳过（TOML 没 null 类型，pydantic load 时取默认值）
      - 若 key 在 `table_list_fields` 且 value 是 list-of-dict：用
        `tomlkit.item(value)` 创建 array of tables，整体覆盖
      - 若 value 是 dict 且 doc 同 key 是 Table / TOMLDocument：递归合并
      - 否则直接覆盖（删除旧值）

    注意：data 里任何位置的 None（包括嵌套 dict 和 list 里的 dict 字段）
    都会被递归剥除——TOML 没 null 表达，pydantic schema 里有 `Optional[...] = None`
    默认值的字段 round-trip 时取默认。
    """
    cleaned = _strip_none(data)
    for key, value in cleaned.items():
        if key in table_list_fields and _is_table_list(value):
            doc[key] = tomlkit.item(value)
            continue
        if isinstance(value, dict) and _is_mergeable_table(doc.get(key)):
            _merge_dict(doc[key], value, table_list_fields)
        else:
            doc[key] = value


def _merge_dict(
    table: tomlkit.TOMLDocument | tomlkit.items.Table,
    data: dict[str, Any],
    table_list_fields: frozenset[str] = frozenset(),
) -> None:
    """递归合并 dict 到 tomlkit table。"""
    for k, v in data.items():
        if v is None:
            continue
        if k in table_list_fields and _is_table_list(v):
            table[k] = tomlkit.item(v)
        elif isinstance(v, dict) and _is_mergeable_table(table.get(k)):
            _merge_dict(table[k], v, table_list_fields)
        else:
            table[k] = v


def _strip_none(value: Any) -> Any:
    """递归剥除 dict / list 里的 None 字段（TOML 没 null 类型）。

    - dict：去掉 value 为 None 的 key
    - list：逐元素递归
    - 其他原样返回
    """
    if isinstance(value, dict):
        return {k: _strip_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_strip_none(x) for x in value]
    return value


def _is_mergeable_table(value: Any) -> bool:
    """检查 tomlkit item 是不是可递归合并的 table（不是 AoT）。

    Table / TOMLDocument 是 dict-like，可递归合并；AoT 是 array-like，
    只能整体替换，不能 merge。
    """
    if value is None:
        return False
    return isinstance(value, (tomlkit.items.Table, tomlkit.TOMLDocument))


def _is_table_list(value: Any) -> bool:
    """检查 value 是不是 list-of-dict（适合做 tomlkit array of tables）。

    空 list 不算（dump 成 `[]` 比 array of tables 更干净）。
    """
    return isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict)