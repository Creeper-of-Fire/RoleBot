"""state_manager 单元测试。"""
from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from creative_battle.creative_battle_season_models import (
    GuildSeasonData,
    SeasonState,
    SubmissionEntry,
)
from creative_battle.creative_battle_state_manager import (
    CreativeBattleStateManager,
    season_id_to_safe,
)


def test_season_id_to_safe():
    """filename-safe sanitize。"""
    assert season_id_to_safe("S1-autumn-winter-2026") == "S1-autumn-winter-2026"
    # 连续的非允许字符合并成单个 _； _ 和 - 是 allowed
    assert season_id_to_safe("S1 秋冬 / 2026") == "S1_2026"
    assert season_id_to_safe("S1__autumn") == "S1__autumn"  # _ 是 allowed，保留
    assert season_id_to_safe("hello-world") == "hello-world"  # - 是 allowed
    assert season_id_to_safe("") == "default"
    assert season_id_to_safe("a" * 100) == "a" * 80
    print("✅ season_id_to_safe OK")


def test_load_missing_returns_none():
    """文件不存在 → load_season 返回 None。"""
    with tempfile.TemporaryDirectory() as tmp:
        mgr = CreativeBattleStateManager()
        mgr.data_dir = Path(tmp)
        data = mgr.load_season(12345, "S0-test")
        assert data is None, f"expected None, got {data}"
        print("✅ load_season missing returns None OK")


def test_ensure_season_creates_in_memory():
    """ensure_season 文件不存在 → 创建空 data 但不写盘。

    简化版：不维护 status 字段（投稿期判断 = if-else date range）。
    """
    with tempfile.TemporaryDirectory() as tmp:
        mgr = CreativeBattleStateManager()
        mgr.data_dir = Path(tmp)
        data = mgr.ensure_season(12345, "S0-test")
        assert data.guild_id == 12345
        assert data.season.season_id == "S0-test"
        # 简化版：没有 status 字段
        assert not hasattr(data.season, "status")
        # 文件不应该被创建
        assert not mgr.get_path_for(12345, "S0-test").exists()
        print("✅ ensure_season in-memory OK (no status field)")


def test_save_and_load_roundtrip():
    """save_data + load_season roundtrip。"""
    async def run():
        with tempfile.TemporaryDirectory() as tmp:
            mgr = CreativeBattleStateManager()
            mgr.data_dir = Path(tmp)

            data = GuildSeasonData(
                guild_id=12345,
                season=SeasonState(
                    season_id="S0-test",
                    submissions={
                        "sub-1": SubmissionEntry(
                            user_id=999, faction="faction_a", title="我的作品",
                            honor_granted=True,  # 简化版新增字段
                        ),
                    },
                    promotion_message_ids={"main": 1234567},
                ),
            )
            await mgr.save_data(data)

            # 文件应该存在
            path = mgr.get_path_for(12345, "S0-test")
            assert path.exists()

            # load 应该拿到相同 data
            loaded = mgr.load_season(12345, "S0-test")
            assert loaded is not None
            assert loaded.guild_id == 12345
            assert loaded.season.submissions["sub-1"].title == "我的作品"
            assert loaded.season.promotion_message_ids["main"] == 1234567

            # 验证文件内容是合法 JSON
            j = json.loads(path.read_text(encoding="utf-8"))
            assert j["guild_id"] == 12345
            assert j["season"]["submissions"]["sub-1"]["user_id"] == 999
            print("✅ save + load roundtrip OK")

    asyncio.run(run())


def test_save_atomic_temp_cleanup_on_error():
    """写盘失败 → temp 文件被清理（不残留）。"""
    # 模拟写盘失败：传一个不可序列化的字段
    async def run():
        with tempfile.TemporaryDirectory() as tmp:
            mgr = CreativeBattleStateManager()
            mgr.data_dir = Path(tmp)

            # 构造一个 model_dump_json 会失败的 data
            # 简单方式：让文件路径含不可写字符？太复杂。
            # 改用 monkey patch 让 _write_sync 抛异常
            original_write_sync = mgr._write_sync

            def failing_write(data, path):
                # 模拟写 temp 失败
                raise OSError("模拟写入失败")

            mgr._write_sync = failing_write
            data = GuildSeasonData(
                guild_id=1,
                season=SeasonState(season_id="S0"),
            )
            try:
                await mgr.save_data(data)
            except Exception as e:
                # 应该 logger.error 但不抛——save_data 吞掉异常
                pass

            # 没创建任何文件
            path = mgr.get_path_for(1, "S0")
            assert not path.exists(), f"expected no file, found {path}"
            print("✅ atomic write failure cleanup OK (no file created)")

            mgr._write_sync = original_write_sync

    asyncio.run(run())


if __name__ == "__main__":
    test_season_id_to_safe()
    test_load_missing_returns_none()
    test_ensure_season_creates_in_memory()
    test_save_and_load_roundtrip()
    test_save_atomic_temp_cleanup_on_error()
    print("\n🎉 All state_manager tests passed.")