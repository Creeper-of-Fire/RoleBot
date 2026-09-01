"""CreativeBattleCog 最小化测试（mock bot）。"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from creative_battle.CreativeBattleCog import (
    CreativeBattleCog,
    FactionPanelView,
    MainPanelView,
    SubmissionModal,
)


logging.basicConfig(level=logging.WARNING)


class MockBot:
    def __init__(self):
        self.logger = logging.getLogger("mock")
        self.added_views = []

    def add_view(self, view):
        self.added_views.append(view)


def _make_test_factions():
    """撤回 cup_honor 模式：无 expire_at；honor_uuid 由 honor toml 控制 expiration_date"""
    from creative_battle.creative_battle_models import FactionConfig
    return [
        FactionConfig(
            key="faction_a", display_name="A 组", emoji="🅰️",
            supporter_honor_uuid="aaaaaaaa-bbbb-cccc-dddd-aaaaaaaaaaaa",
            submission_channel_id=10,
            submission_blacklist_honor_uuids=["ffffffff-ffff-ffff-ffff-ffffffffffff"],
            submission_whitelist_honor_uuids=[],
            contributor_honor_uuid="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ),
        FactionConfig(
            key="faction_b", display_name="B 组", emoji="🅱️",
            supporter_honor_uuid="bbbbbbbb-cccc-dddd-eeee-bbbbbbbbbbbb",
            submission_channel_id=11,
            submission_blacklist_honor_uuids=[],
            submission_whitelist_honor_uuids=["bbbbbbbb-cccc-dddd-eeee-bbbbbbbbbbbb"],
            contributor_honor_uuid=None,
        ),
    ]


def test_cog_init_and_views():
    """cog 初始化 + view 注册。"""
    mock = MockBot()
    cog = CreativeBattleCog(mock)
    factions = _make_test_factions()

    # 仿 cog_load 的 view 注册（简化版：只注册 FactionPanelView）
    a_view = FactionPanelView(cog, faction_key="faction_a")
    b_view = FactionPanelView(cog, faction_key="faction_b")
    mock.add_view(a_view)
    mock.add_view(b_view)
    # MainPanelView 由 cmd_send_panel 动态生成（不通过 cog_load 注册）
    main_view = MainPanelView(cog, factions)

    # 注册的 view 应该是 2 个（faction 视图）
    assert len(mock.added_views) == 2, f"expected 2 views registered, got {len(mock.added_views)}"

    # MainPanelView 应有 2 个按钮（faction_a / faction_b）
    main_buttons = [c for c in main_view.children if hasattr(c, "custom_id")]
    assert len(main_buttons) == 2, f"expected 2 main buttons, got {len(main_buttons)}"
    print(f"✅ MainPanelView buttons: {[c.custom_id for c in main_buttons]}")

    # FactionPanelView 应有 1 个按钮
    for view, expected_key in [(a_view, "faction_a"), (b_view, "faction_b")]:
        btns = [c for c in view.children if hasattr(c, "custom_id")]
        assert len(btns) == 1, f"expected 1 button, got {len(btns)}"
        print(f"✅ FactionPanelView({expected_key}) button: {btns[0].custom_id}")
        assert view.faction_key == expected_key

    print(f"✅ cog init OK; views registered: {len(mock.added_views)}")


def test_cog_has_required_attributes():
    """cog 应该有必要的属性 + 方法（简化版：删 season_loop / expire_check_loop 等）。"""
    mock = MockBot()
    cog = CreativeBattleCog(mock)

    assert hasattr(cog, "config_mgr"), "missing config_mgr"
    # state_mgr 已从 cog 移除——3f99469 改用 per (guild_id, season_id) 单例，
    # 访问入口是 _state_for(guild_id, season_id) helper
    assert hasattr(cog, "_state_for"), "missing _state_for (per-key state singleton accessor)"
    assert hasattr(cog, "honor_data_manager"), "missing honor_data_manager"
    assert hasattr(cog, "admin_group"), "missing admin_group"
    assert hasattr(cog, "_handle_join"), "missing _handle_join"
    assert hasattr(cog, "_handle_submission"), "missing _handle_submission"
    assert hasattr(cog, "cmd_send_panel"), "missing cmd_send_panel"
    assert hasattr(cog, "cmd_revoke_submission"), "missing cmd_revoke_submission"
    assert hasattr(cog, "_is_submission_open"), "missing _is_submission_open"
    assert hasattr(cog, "_check_submission_blacklist_whitelist"), "missing _check_submission_blacklist_whitelist"

    # 简化版删除：expire / 状态机 / winner 相关
    assert not hasattr(cog, "cmd_end_season"), "ver4 final 已删 cmd_end_season"
    assert not hasattr(cog, "season_loop"), "简化版已删 season_loop（用 if-else 替代）"
    assert not hasattr(cog, "expire_check_loop"), "简化版已删 expire_check_loop（unix 哲学）"
    assert not hasattr(cog, "_close_season"), "简化版已删 _close_season"
    assert not hasattr(cog, "_announce_season_close"), "简化版已删 _announce_season_close"
    assert not hasattr(cog, "_check_guild_for_expired_roles"), "简化版已删 _check_guild_for_expired_roles"
    assert not hasattr(cog, "_reclaim_supporter_roles"), "简化版已删 _reclaim_supporter_roles"
    assert not hasattr(cog, "_reclaim_winner_contributor_roles"), "简化版已删 _reclaim_winner_contributor_roles"
    assert not hasattr(cog, "_determine_winner_for_cleanup"), "简化版已删 _determine_winner_for_cleanup"
    assert not hasattr(cog, "loser_contributor_reclaim_loop"), "简化版已删 loser_contributor_reclaim_loop"

    assert hasattr(cog, "_build_main_embed"), "missing _build_main_embed"
    assert hasattr(cog, "_build_faction_embed"), "missing _build_faction_embed"
    assert hasattr(cog, "promotion_loop"), "missing promotion_loop"
    print("✅ cog has all required attributes (simplified: unix 哲学 + if-else 投稿期)")


def test_modal_construction():
    """SubmissionModal 构造（需要 mock interaction，但仅构造部分不需要）。"""
    mock = MockBot()
    cog = CreativeBattleCog(mock)

    modal = SubmissionModal(cog, faction_key="faction_a")
    assert modal.faction_key == "faction_a"
    # 应该有 2 个 TextInput
    assert len(modal.children) == 2, f"expected 2 inputs, got {len(modal.children)}"
    print(f"✅ SubmissionModal constructed with {len(modal.children)} inputs")


def test_view_custom_ids():
    """custom_id 必须稳定（重启后 bot 用 custom_id 路由 button click）。"""
    mock = MockBot()
    cog = CreativeBattleCog(mock)
    factions = _make_test_factions()
    main = MainPanelView(cog, factions)
    a_view = FactionPanelView(cog, faction_key="faction_a")
    b_view = FactionPanelView(cog, faction_key="faction_b")

    main_ids = sorted(c.custom_id for c in main.children if hasattr(c, "custom_id"))
    a_ids = [c.custom_id for c in a_view.children if hasattr(c, "custom_id")]
    b_ids = [c.custom_id for c in b_view.children if hasattr(c, "custom_id")]

    # MainPanelView 按钮 custom_id 含 join 段（互斥领取）
    assert main_ids == ["cb:main:join:faction_a", "cb:main:join:faction_b"], main_ids
    # FactionPanelView 含 submit 段
    assert a_ids == ["cb:submit:faction_a"], a_ids
    assert b_ids == ["cb:submit:faction_b"], b_ids
    print(f"✅ custom_ids stable: main={main_ids}")
    print(f"   faction: a={a_ids}, b={b_ids}")


def test_admin_group_name():
    """admin_group.name 应该是「合战丨核心」。"""
    mock = MockBot()
    cog = CreativeBattleCog(mock)
    assert cog.admin_group.name == "合战丨核心", f"got {cog.admin_group.name}"
    print(f"✅ admin_group.name = {cog.admin_group.name}")


def test_is_submission_open_if_else():
    """_is_submission_open 是 if-else（不是状态机）。"""
    import datetime as _dt
    from creative_battle.creative_battle_models import (
        CreativeBattleGuildConfig, CreativeBattleMeta, NotificationConfig,
        PromotionConfig,
    )

    cfg = CreativeBattleGuildConfig(
        enabled=True,
        meta=CreativeBattleMeta(
            season_label="S0", season_id="S0", theme="t",
            start_date=_dt.date(2026, 9, 1),
            end_date=_dt.date(2026, 12, 31),
        ),
        promotion=PromotionConfig(
            main_intro_text="投稿期：%s ~ %s 之间可投稿" % ("2026-09-01", "2026-12-31"),
            anonymize_options=["数字"],
        ),
        notification=NotificationConfig(channel_id=1, admin_role_id=2),
    )
    # 投稿期内
    assert CreativeBattleCog._is_submission_open(cfg, _dt.date(2026, 9, 1)) is True  # 边界
    assert CreativeBattleCog._is_submission_open(cfg, _dt.date(2026, 11, 15)) is True
    assert CreativeBattleCog._is_submission_open(cfg, _dt.date(2026, 12, 31)) is True  # 边界
    # 投稿期外
    assert CreativeBattleCog._is_submission_open(cfg, _dt.date(2026, 8, 31)) is False
    assert CreativeBattleCog._is_submission_open(cfg, _dt.date(2027, 1, 1)) is False
    print("✅ _is_submission_open if-else OK (start <= today <= end)")


if __name__ == "__main__":
    test_cog_init_and_views()
    test_cog_has_required_attributes()
    test_modal_construction()
    test_view_custom_ids()
    test_admin_group_name()
    test_is_submission_open_if_else()
    print("\n🎉 All cog init tests passed.")
