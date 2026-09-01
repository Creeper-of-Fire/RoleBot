"""Schema smoke test — 验证 toml / json schema 能正常工作。

跑法：python -m creative_battle._schema_smoke_test

2026-09-01 改造：FactionConfig.supporter_role_id / contributor_role_id / *_role_ids 字段全部删除
改用 supporter_honor_uuid / *_honor_uuids（honor uuid string）。
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

# 让脚本能直接 import creative_battle 包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from creative_battle.creative_battle_models import (
    CreativeBattleGuildConfig,
    FactionConfig,
)
from creative_battle.creative_battle_season_models import (
    GuildSeasonData,
    SeasonState,
    SubmissionEntry,
)


# 2026-09-01 改造：FactionConfig 不再有 supporter_role_id / contributor_role_id / *_role_ids
# 全用 honor uuid 字段
_BASE_FACTION_FIELDS = {
    "supporter_honor_uuid": "aaaaaaaa-bbbb-cccc-dddd-aaaaaaaaaaaa",
    "submission_channel_id": 444,
    "submission_blacklist_honor_uuids": [],
    "submission_whitelist_honor_uuids": [],
    "contributor_honor_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
}


def test_toml_schema_basic():
    """toml schema：最小有效配置。"""
    cfg = CreativeBattleGuildConfig(
        enabled=True,
        meta={
            "season_label": "S0 秋冬",
            "season_id": "S0-2026-autumn-winter",
            "theme": "秋冬",
            "start_date": dt.date(2026, 9, 1),
            "end_date": dt.date(2026, 12, 31),
        },
        promotion={
            "main_intro_text": "S0 秋冬创作大会",
            "anonymize_options": ["数字"],
        },
        factions=[
            {
                "key": "faction_a",
                "display_name": "A 组",
                "emoji": "🅰️",
                "supporter_honor_uuid": "aaaaaaaa-bbbb-cccc-dddd-aaaaaaaaaaaa",
                "submission_channel_id": 444,
                "submission_blacklist_honor_uuids": [
                    "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                ],
                "submission_whitelist_honor_uuids": [],
                "contributor_honor_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            },
            {
                "key": "faction_b",
                "display_name": "B 组",
                "emoji": "🅱️",
                "supporter_honor_uuid": "bbbbbbbb-cccc-dddd-eeee-bbbbbbbbbbbb",
                "submission_channel_id": 888,
                "submission_blacklist_honor_uuids": [],
                "submission_whitelist_honor_uuids": [
                    "11111111-2222-3333-4444-555555555555",
                    "22222222-3333-4444-5555-666666666666",
                ],
                "contributor_honor_uuid": "11111111-2222-3333-4444-555555555555",
            },
        ],
        notification={"channel_id": 999, "admin_role_id": 1000},
    )
    assert cfg.meta.season_id == "S0-2026-autumn-winter"
    assert len(cfg.factions) == 2
    assert cfg.factions[0].key == "faction_a"
    assert cfg.factions[1].submission_channel_id == 888
    # supporter_honor_uuid
    assert cfg.factions[0].supporter_honor_uuid == "aaaaaaaa-bbbb-cccc-dddd-aaaaaaaaaaaa"
    # 删除字段检查
    assert not hasattr(cfg.factions[0], "supporter_role_id")
    assert not hasattr(cfg.factions[0], "contributor_role_id")
    assert not hasattr(cfg.factions[0], "winner_role_id")
    # 黑/白名单改成 *_honor_uuids
    assert cfg.factions[0].submission_blacklist_honor_uuids == [
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
    ]
    assert cfg.factions[1].submission_whitelist_honor_uuids == [
        "11111111-2222-3333-4444-555555555555",
        "22222222-3333-4444-5555-666666666666",
    ]
    # grant_honor uuid
    assert cfg.factions[0].contributor_honor_uuid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    print(f"✅ toml schema basic OK: season={cfg.meta.season_id}, factions={[f.key for f in cfg.factions]}")


def test_toml_schema_rejects_3_factions():
    """toml schema：必须恰好 2 个 faction。"""
    try:
        CreativeBattleGuildConfig(
            enabled=True,
            meta={
                "season_label": "x",
                "season_id": "s",
                "theme": "t",
                "start_date": dt.date(2026, 1, 1),
                "end_date": dt.date(2026, 2, 1),
            },
            promotion={
                "main_intro_text": "test",
                "anonymize_options": ["1"],
            },
            factions=[
                {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "A", "emoji": "✨", "submission_channel_id": 10},
                {**_BASE_FACTION_FIELDS, "key": "faction_b", "display_name": "B", "emoji": "✨", "submission_channel_id": 11},
                {**_BASE_FACTION_FIELDS, "key": "faction_c", "display_name": "C", "emoji": "✨", "submission_channel_id": 12},
            ],
            notification={"channel_id": 1, "admin_role_id": 2},
        )
        raise AssertionError("should have raised ValueError")
    except ValueError as e:
        assert "恰好" in str(e)
        print(f"✅ toml schema rejects 3 factions OK: {e}")


def test_toml_schema_rejects_duplicate_key():
    """toml schema：faction key 必须唯一。"""
    try:
        CreativeBattleGuildConfig(
            enabled=True,
            meta={
                "season_label": "x",
                "season_id": "s",
                "theme": "t",
                "start_date": dt.date(2026, 1, 1),
                "end_date": dt.date(2026, 2, 1),
            },
            promotion={
                "main_intro_text": "test",
                "anonymize_options": ["1"],
            },
            factions=[
                {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "A", "emoji": "✨", "submission_channel_id": 10},
                {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "B", "emoji": "✨", "submission_channel_id": 11},
            ],
            notification={"channel_id": 1, "admin_role_id": 2},
        )
        raise AssertionError("should have raised ValueError")
    except ValueError as e:
        assert "唯一" in str(e)
        print(f"✅ toml schema rejects duplicate key OK: {e}")


def test_toml_schema_rejects_bad_date_range():
    """toml schema：end_date 必须晚于 start_date。"""
    try:
        CreativeBattleGuildConfig(
            enabled=True,
            meta={
                "season_label": "x",
                "season_id": "s",
                "theme": "t",
                "start_date": dt.date(2026, 12, 31),
                "end_date": dt.date(2026, 1, 1),  # 早于 start
            },
            promotion={
                "main_intro_text": "test",
                "anonymize_options": ["1"],
            },
            factions=[
                {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "A", "emoji": "✨", "submission_channel_id": 10},
                {**_BASE_FACTION_FIELDS, "key": "faction_b", "display_name": "B", "emoji": "✨", "submission_channel_id": 11},
            ],
            notification={"channel_id": 1, "admin_role_id": 2},
        )
        raise AssertionError("should have raised ValueError")
    except ValueError as e:
        assert "晚于" in str(e)
        print(f"✅ toml schema rejects bad date range OK: {e}")


def test_toml_schema_accepts_empty_black_white_lists():
    """2026-09-01 改造：submission_blacklist_honor_uuids/submission_whitelist_honor_uuids 空 list 是合法的（不限制）。"""
    cfg = CreativeBattleGuildConfig(
        enabled=True,
        meta={
            "season_label": "x",
            "season_id": "s",
            "theme": "t",
            "start_date": dt.date(2026, 9, 1),
            "end_date": dt.date(2026, 12, 31),
        },
        promotion={
            "main_intro_text": "test",
            "anonymize_options": ["1"],
        },
        factions=[
            {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "A", "emoji": "✨", "submission_channel_id": 10,
             "submission_blacklist_honor_uuids": [], "submission_whitelist_honor_uuids": []},
            {**_BASE_FACTION_FIELDS, "key": "faction_b", "display_name": "B", "emoji": "✨", "submission_channel_id": 11,
             "submission_blacklist_honor_uuids": [], "submission_whitelist_honor_uuids": []},
        ],
        notification={"channel_id": 1, "admin_role_id": 2},
    )
    assert cfg.factions[0].submission_blacklist_honor_uuids == []
    assert cfg.factions[0].submission_whitelist_honor_uuids == []
    print("✅ empty submission black/white lists accepted (semantic: no restriction)")


def test_toml_schema_accepts_optional_honor_uuid():
    """简化版：contributor_honor_uuid 可选（None = 不 grant_honor）。"""
    cfg = CreativeBattleGuildConfig(
        enabled=True,
        meta={
            "season_label": "x",
            "season_id": "s",
            "theme": "t",
            "start_date": dt.date(2026, 9, 1),
            "end_date": dt.date(2026, 12, 31),
        },
        promotion={
            "main_intro_text": "test",
            "anonymize_options": ["1"],
        },
        factions=[
            {**_BASE_FACTION_FIELDS, "key": "faction_a", "display_name": "A", "emoji": "✨", "submission_channel_id": 10,
             "contributor_honor_uuid": None},
            {**_BASE_FACTION_FIELDS, "key": "faction_b", "display_name": "B", "emoji": "✨", "submission_channel_id": 11,
             "contributor_honor_uuid": None},
        ],
        notification={"channel_id": 1, "admin_role_id": 2},
    )
    assert cfg.factions[0].contributor_honor_uuid is None
    print("✅ optional contributor_honor_uuid accepted (semantic: no grant_honor)")


def test_json_schema_basic():
    """json schema：最小有效配置 + roundtrip。"""
    gsd = GuildSeasonData(
        guild_id=12345,
        season=SeasonState(
            season_id="S0-test",
            submissions={
                "sub-1": SubmissionEntry(
                    user_id=999,
                    faction="faction_a",
                    title="我的作品",
                    description="描述",
                    contributor_role_granted=True,
                    honor_granted=True,
                ),
            },
        ),
    )
    assert gsd.season.season_id == "S0-test"
    assert len(gsd.season.submissions) == 1
    assert gsd.season.submissions["sub-1"].title == "我的作品"
    assert gsd.season.submissions["sub-1"].honor_granted is True

    # roundtrip via JSON
    j = gsd.model_dump_json()
    restored = GuildSeasonData.model_validate_json(j)
    assert restored.season.season_id == "S0-test"
    assert restored.season.submissions["sub-1"].user_id == 999
    assert restored.season.submissions["sub-1"].honor_granted is True
    print(f"✅ json schema basic + roundtrip OK: season={gsd.season.season_id}")


def test_json_schema_promotion_dict():
    """json schema：promotion_message_ids 按 channel_key 索引。"""
    gsd = GuildSeasonData(
        guild_id=1,
        season=SeasonState(
            season_id="S0",
            promotion_message_ids={
                "main": 100,
                "faction_a": 200,
                "faction_b": 300,
            },
        ),
    )
    assert gsd.season.promotion_message_ids["main"] == 100
    assert gsd.season.promotion_message_ids["faction_a"] == 200
    assert gsd.season.promotion_message_ids["faction_b"] == 300
    print(f"✅ json schema promotion_message_ids dict OK: {list(gsd.season.promotion_message_ids.keys())}")


def test_json_schema_no_status_field():
    """简化版：删除 status / started_at / ended_at（不维护状态字段）。

    投稿期判断 = if-else (start_date <= today <= end_date)，不需要状态字段。
    """
    gsd = GuildSeasonData(
        guild_id=1,
        season=SeasonState(season_id="S0"),
    )
    # 简化版删除字段
    assert not hasattr(gsd.season, "status")
    assert not hasattr(gsd.season, "started_at")
    assert not hasattr(gsd.season, "ended_at")
    print("✅ json schema simplified (no status / started_at / ended_at)")


if __name__ == "__main__":
    test_toml_schema_basic()
    test_toml_schema_rejects_3_factions()
    test_toml_schema_rejects_duplicate_key()
    test_toml_schema_rejects_bad_date_range()
    test_toml_schema_accepts_empty_black_white_lists()
    test_toml_schema_accepts_optional_honor_uuid()
    test_json_schema_basic()
    test_json_schema_promotion_dict()
    test_json_schema_no_status_field()
    print("\n🎉 All schema smoke tests passed.")
