"""toml_filename_utils 单元测试。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

# 让脚本能直接 import utility 包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utility.toml_filename_utils import iter_guild_ids_from_toml_files


def test_iter_basic():
    """基本场景：列出所有 honor_*.toml。"""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "honor_111.toml").write_text("# 1")
        (d / "honor_222.toml").write_text("# 2")
        (d / "honor_333.toml").write_text("# 3")

        ids = sorted(iter_guild_ids_from_toml_files(d, "honor_"))
        assert ids == [111, 222, 333], f"expected [111, 222, 333], got {ids}"
        print(f"✅ iter basic OK: {ids}")


def test_iter_skips_non_matching():
    """文件名不严格匹配 → 跳过。"""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "creative_battle_111.toml").write_text("# 1")
        (d / "creative_battle_abc.toml").write_text("# x")  # 非数字
        (d / "creative_battle_222.tomlx").write_text("# 2")  # 错误后缀
        (d / "creative_battle.txt").write_text("# 3")  # 无数字

        ids = sorted(iter_guild_ids_from_toml_files(d, "creative_battle_"))
        assert ids == [111], f"expected [111], got {ids}"
        print(f"✅ iter skips non-matching OK: {ids}")


def test_iter_missing_dir():
    """目录不存在 → 不抛异常，返回空。"""
    ids = sorted(iter_guild_ids_from_toml_files("/nonexistent_dir_xxx", "honor_"))
    assert ids == [], f"expected [], got {ids}"
    print(f"✅ iter missing dir OK: {ids}")


def test_iter_custom_suffix():
    """自定义 suffix（如 .json）。"""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "fashion_111.json").write_text("# 1")
        (d / "fashion_222.json").write_text("# 2")
        (d / "fashion_333.toml").write_text("# 3")  # 错误后缀，跳过

        ids = sorted(iter_guild_ids_from_toml_files(d, "fashion_", suffix=".json"))
        assert ids == [111, 222], f"expected [111, 222], got {ids}"
        print(f"✅ iter custom suffix OK: {ids}")


if __name__ == "__main__":
    test_iter_basic()
    test_iter_skips_non_matching()
    test_iter_missing_dir()
    test_iter_custom_suffix()
    print("\n🎉 All toml_filename_utils tests passed.")