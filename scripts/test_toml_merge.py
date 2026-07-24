"""边界测试：toml_merge.merge_into_doc 在 list 中间项修改/追加/删除三种情况下的行为。

直接运行：
    cd D:\\Dev\\Workspace\\discord-bots
    python _shared/scripts/test_toml_merge.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# 把 _shared/ 加到 sys.path，使 `from config.toml_merge` 能找到
_SHARED_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SHARED_DIR))

import tomlkit  # noqa: E402

from config.toml_merge import (  # noqa: E402
    TomlMergeAsTableList,
    collect_table_list_fields,
    merge_into_doc,
)


SOURCE_TOML = """\
# 顶部注释：配置标题
top_str = "old"
top_int = 1

[section]
key1 = "old_k1"
key2 = "old_k2"

[[items]]
id = "a"
val = 1

# items 的中间注释
[[items]]
id = "b"
val = 2

[[items]]
id = "c"
val = 3
"""


def _dump(doc: tomlkit.TOMLDocument) -> str:
    return tomlkit.dumps(doc)


def test_middle_modify() -> None:
    """中间项修改：把 id=b 的 val 从 2 改成 20。

    期望：
    - 顶部注释保留
    - top_str 被覆盖
    - top_int 未在新数据中，仍保留
    - section.key1 被覆盖，section.key2 保留（未被新数据触碰）
    - items 整体被替换为新 array of tables
    """
    doc = tomlkit.loads(SOURCE_TOML)
    new_data: dict = {
        "top_str": "new",
        "section": {"key1": "new_k1"},
        "items": [
            {"id": "a", "val": 1},
            {"id": "b", "val": 20},  # modified
            {"id": "c", "val": 3},
        ],
    }
    merge_into_doc(doc, new_data, table_list_fields={"items"})
    result = _dump(doc)

    assert "# 顶部注释：配置标题" in result, "顶部注释丢失"
    assert 'top_str = "new"' in result, "top_str 未更新"
    assert "top_int = 1" in result, "未触碰字段 top_int 被错误删除"
    assert 'key1 = "new_k1"' in result, "section.key1 未更新"
    assert 'key2 = "old_k2"' in result, "section.key2 被错误删除"
    assert "val = 20" in result, "items[b].val=20 未写入"
    assert "\nval = 2\n" not in result, "items[b] 旧 val=2 未删除"
    assert 'id = "a"' in result and 'id = "c"' in result, "items 其他项丢失"
    print("✓ test_middle_modify")


def test_append() -> None:
    """追加项：在 items 末尾加一项。

    期望：items 末尾追加 {id=d, val=4}，前 3 项保留。
    """
    doc = tomlkit.loads(SOURCE_TOML)
    new_data: dict = {
        "items": [
            {"id": "a", "val": 1},
            {"id": "b", "val": 2},
            {"id": "c", "val": 3},
            {"id": "d", "val": 4},  # 新增
        ],
    }
    merge_into_doc(doc, new_data, table_list_fields={"items"})
    result = _dump(doc)

    assert 'id = "d"' in result and "val = 4" in result, "追加项未写入"
    assert 'id = "a"' in result and 'id = "b"' in result and 'id = "c"' in result, "原项丢失"
    print("✓ test_append")


def test_delete() -> None:
    """删除项：把 items 中的 id=c 删掉。

    期望：id=c 从输出消失，但 a、b 保留。
    """
    doc = tomlkit.loads(SOURCE_TOML)
    new_data: dict = {
        "items": [
            {"id": "a", "val": 1},
            {"id": "b", "val": 2},
            # c 已删
        ],
    }
    merge_into_doc(doc, new_data, table_list_fields={"items"})
    result = _dump(doc)

    assert 'id = "c"' not in result, "删除项仍存在"
    assert "val = 3" not in result, "删除项的值仍存在"
    assert 'id = "a"' in result and 'id = "b"' in result, "保留项丢失"
    print("✓ test_delete")


def test_no_marker() -> None:
    """不带 TomlMergeAsTableList 标记的 list-of-dict。

    期望：不带标记时不应该走 array of tables 分支，不能崩。
    具体格式（inline array 还是别的）由 tomlkit 内部决定，只测不崩。
    """
    doc = tomlkit.loads(SOURCE_TOML)
    new_data: dict = {
        "items": [
            {"id": "a", "val": 1},
        ],
    }
    merge_into_doc(doc, new_data)  # 无 table_list_fields
    _dump(doc)  # 不抛异常即通过
    print("✓ test_no_marker")


def test_nested_dict() -> None:
    """嵌套 dict（dict-of-dict 风格）：[role_groups.total_admin] 等。

    期望：递归合并到现有 table，保留未被新数据触碰的子表。
    """
    source = """\
[role_groups.total_admin]
label = "总管理"
role_ids = [1]

[role_groups.discipline]
label = "风纪委员"
role_ids = [2]
"""
    doc = tomlkit.loads(source)
    new_data: dict = {
        "role_groups": {
            "total_admin": {"label": "总管理（改）"},  # 修改
            # discipline 整块未传，应保留
        },
    }
    merge_into_doc(doc, new_data)
    result = _dump(doc)

    assert "总管理（改）" in result, "total_admin.label 未更新"
    assert "风纪委员" in result, "discipline 被错误删除"
    print("✓ test_nested_dict")


def test_collect_table_list_fields() -> None:
    """collect_table_list_fields 应正确识别 Annotated[..., TomlMergeAsTableList()] 字段。"""
    from typing import Annotated
    from pydantic import BaseModel, Field

    class M(BaseModel):
        items: Annotated[list[dict], TomlMergeAsTableList()] = Field(default_factory=list)
        plain: list[dict] = Field(default_factory=list)

    fields = collect_table_list_fields(M)
    assert fields == frozenset({"items"}), f"期望仅 items，实际 {fields}"
    print("✓ test_collect_table_list_fields")


if __name__ == "__main__":
    test_middle_modify()
    test_append()
    test_delete()
    test_no_marker()
    test_nested_dict()
    test_collect_table_list_fields()
    print("\n所有边界测试通过 ✓")