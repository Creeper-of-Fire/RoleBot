"""creative_battle_{guild_id}.toml 的 pydantic schema。

每个 guild 一份 toml（per-guild 设计，遵循 ``shared/docs/toml-config-design.md``
的 "toml = per-guild" 红线）。文件结构::

    data/creative_battle_{guild_id}.toml

字段组织
--------

- ``[meta]`` 赛季元信息（赛季标签、主题、起止日期）
- ``[promotion]`` 推广配置（主入口频道、刷新频率、每组随机展示数）
- ``[[factions]]`` array-of-tables：两阵营定义（A 组 / B 组）
- ``[notification]`` 通知频道 + @ 管理组

设计哲学（按 ``role_bot/AGENTS.md`` + 简化版拍板记录）
--------------------------------------------------

- toml 是 admin 单一控制源：bot 是只读消费者（除 admin 通过 ``/上传配置`` 改 toml 外）
- 不硬编码任何 faction key——所有 faction 处理走 ``for f in cfg.factions`` 遍历
- **unix 哲学**：bot 只做最少的、互相独立的事
  - 投稿面板（每个分区频道各一个）：用户点投稿 → 写 json + grant_honor + 手动 add role
  - 主入口面板：A/B 互斥领取
  - 撤销投稿：admin 命令删 json（**不** remove role——admin 自己管）
- **不做**：身份组过期提醒、winner_role、自动 remove、状态机、游客领取面板（admin 复用 honor claimable）
- **投稿期判断 = if-else**：bot 读 toml 的 ``start_date`` / ``end_date``，
  ``now in [start_date, end_date]`` 就接受投稿，否则拒。不是状态机。
- **互斥以荣誉为基准**（2026-09-01 改造）：bot 检查 honor DB，不查 Discord role。
  用户通过荣誉墙卸下 role 不会绕过 mutex——DB 里的 honor 记录仍然存在。
  - 加入阵营：检查用户是否已持有任何 faction supporter honor；如有且不是本阵营 → 拒绝
  - 投稿：白名单 = 必须持有本阵营 supporter honor；黑名单 = 持有任何其他阵营 supporter honor → 拒绝
- **黑/白名单 per-faction**：每个阵营各自一份（仅投稿路径生效）
  - ``submission_blacklist_honor_uuids``：持任一即拒绝投稿
  - ``submission_whitelist_honor_uuids``：非空时持任一才允许投稿
  - 两者并存时：**黑名单优先**
- **grant_honor 是单一真相源**（2026-09-01 改造）：bot grant_honor 后从返回的
  ``HonorDefinition.role_id`` 手动 add role。后续计划让 ``grant_honor`` 自身加 role
  （所有 caller 受益），但当前仍是手动两步。
"""
from __future__ import annotations

import datetime as _dt
from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator

from shared.config.toml_merge import TomlMergeAsTableList


# --- [meta] ---


class CreativeBattleMeta(BaseModel):
    """赛季元信息。"""

    season_label: str = Field(
        ..., max_length=100, description="赛季标签（用户可见，如 '第零赛季 秋冬'）",
    )
    season_id: str = Field(
        ..., max_length=100,
        description="赛季 ID。bot 用作 json 文件名后缀（admin 自由编辑）",
    )
    theme: str = Field(..., max_length=50, description="主题（如 '秋冬'）")
    start_date: _dt.date = Field(
        ..., description="投稿期开始日期（含）；早于此日期 bot 拒绝投稿",
    )
    end_date: _dt.date = Field(
        ..., description="投稿期结束日期（含）；晚于此日期 bot 拒绝投稿",
    )

    @field_validator("season_id")
    @classmethod
    def _season_id_no_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("season_id 不能为空")
        return v

    @field_validator("end_date")
    @classmethod
    def _end_after_start(cls, v: _dt.date, info) -> _dt.date:
        start = info.data.get("start_date")
        if start and v <= start:
            raise ValueError("end_date 必须晚于 start_date")
        return v


# --- [[factions]] ---


class FactionConfig(BaseModel):
    """单个阵营/组的配置（A 组 / B 组）。

    key 必须小写英文 + 下划线（UI 按钮 + custom_id 都基于此）。

    简化版（unix 哲学 + per-faction 黑/白名单）：
    - bot 提供的核心是投稿面板 + 主入口阵营按钮
    - 过期机制 / winner_role / 自动回收 → 全部删除（按拍板记录）
    - 投稿后 bot 直接 ``grant_honor(contributor_honor_uuid)`` 给投稿用户
    - Discord role 从 ``HonorDefinition.role_id`` 反查（forward 查 honor toml）——
      互斥判定查 honor DB 而非 Discord role，role 通过 ``role_sync_honor=true`` 自动附
    """

    key: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]{0,30}$",
        description="内部 key（小写英文 + 下划线）；UI 按钮 + custom_id 都基于此",
    )
    display_name: str = Field(..., min_length=1, max_length=50, description="用户可见名")
    emoji: str = Field(..., min_length=1, max_length=64, description="用户可见 emoji")

    # --- 荣誉 UUID（互斥判定的真相源）---
    # 2026-09-01 改造：原先 supporter_role_id 改为 supporter_honor_uuid。
    # bot 查 honor DB 而不是 Discord role。Role 通过 HonorDefinitionItem.role_id 反查。

    supporter_honor_uuid: str = Field(
        ...,
        description=(
            "支持者荣誉 UUID（引用 honor toml）。用户点主入口面板'加入阵营'按钮后 "
            "bot grant_honor 此 uuid；已持有其他阵营 supporter honor 时 bot 拒绝（不 remove）。"
            "对应 Discord role 从 HonorDefinitionItem.role_id 查，bot 手动 add。"
            "过期 / 移除由 admin 手动管理。"
        ),
    )

    submission_channel_id: Optional[int] = Field(
        None,
        description="该阵营分区频道 ID（投稿按钮 + 分区推广面板）。未配置则不发该组分区面板",
    )

    # --- 投稿黑/白名单（per-faction，unix 哲学：黑名单优先） ---
    # ★ 仅 _handle_submission（投稿路径）使用；加入路径靠 supporter_honor 互斥检查。
    # 2026-09-01 改造：原来是 list[int] role_id，现改为 list[str] honor uuid。

    submission_blacklist_honor_uuids: list[str] = Field(
        default_factory=list,
        description=(
            "投稿黑名单 honor uuid 列表。**持任一即拒**（拒绝投稿该阵营）。"
            "黑名单优先于白名单——即使持白名单 honor 之一，黑名单命中也拒。"
            "**仅**作用于投稿路径；加入路径不调本字段。"
        ),
    )
    submission_whitelist_honor_uuids: list[str] = Field(
        default_factory=list,
        description=(
            "投稿白名单 honor uuid 列表。**非空时持任一才允许**投稿该阵营；"
            "**空列表 = 不限制**。黑名单优先于白名单。"
            "**仅**作用于投稿路径；加入路径不调本字段。"
        ),
    )

    # --- 投稿成功后授予的永久荣誉（per-faction，引用 honor toml 的 UUID） ---

    contributor_honor_uuid: Optional[str] = Field(
        None,
        description=(
            "投稿成功后 bot 调 grant_honor(member.id, contributor_honor_uuid) 给投稿用户。"
            "UUID 须在 honor toml 已存在。可选——不配则不 grant_honor。"
            "对应 Discord role 从 HonorDefinitionItem.role_id 反查后 add。"
        ),
    )

    # --- 跨 faction 互斥时使用的"对方阵营 supporter_honor_uuid"集合 ---

    @property
    def other_factions_supporter_honor_uuids(self, all_factions: list["FactionConfig"]) -> set[str]:
        """返回"其他阵营的 supporter_honor_uuid 集合"——互斥检查用。

        不缓存，调用方应在 hot path 外预先算好。
        """
        return {f.supporter_honor_uuid for f in all_factions if f.key != self.key}


# --- [promotion] ---


class PromotionConfig(BaseModel):
    """推广面板配置。"""

    main_channel_id: Optional[int] = Field(
        None,
        description="主入口频道 ID（总览 + A/B 阵营选择按钮）。未配置则不发主入口面板",
    )
    refresh_minutes: int = Field(
        5, ge=1, le=1440, description="推广面板刷新间隔（分钟）",
    )
    random_count_per_faction: int = Field(
        2, ge=1, le=10, description="每个分区推广 random 展示几个投稿",
    )

    # --- 文案（admin 必填；不设 default 避免事务耦合到代码层） ---

    main_intro_text: str = Field(
        ...,
        min_length=1,
        description=(
            "主面板 intro 文案（admin 必填）。"
            "建议说明：阵营互斥、选择后不可更换、每人只有一次机会。"
            "嵌入主入口面板 embed description 顶部——为空会让 bot 加载时 fail-fast。"
        ),
    )

    # --- 数字梗化开关（OFF 默认值合理：开关默认是关闭状态） ---

    main_anonymize_enabled: bool = Field(
        False,
        description=(
            "主面板数字梗化开关。ON 时刷新面板会把"
            "'支持 X 人 / 参赛 X 人'数字随机替换成 anonymize_options 里的梗。"
            "OFF 显示真实数字。"
        ),
    )
    faction_anonymize_enabled: bool = Field(
        False,
        description=(
            "分区面板数字梗化开关。ON 时刷新分区面板会把"
            "'当前支持者 / 参赛者'数字随机替换成 anonymize_options 里的梗。"
            "OFF 显示真实数字。"
        ),
    )
    anonymize_options: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "梗选项（admin 必填，至少 1 项）。"
            "梗开关 ON 时每次刷新从这里 random.choice 抽一个替换数字。"
            "示例：[\"乱码\", \"114514\", \"黑条\", \"我不告诉你\"]。"
        ),
    )


# --- [notification] ---


class NotificationConfig(BaseModel):
    """通知配置。"""

    channel_id: int = Field(..., description="赛季开始公告频道 ID")
    admin_role_id: int = Field(..., description="公告要 @ 的管理员身份组 ID")


# --- 顶层 schema ---


class CreativeBattleGuildConfig(BaseModel):
    """一个 guild 的完整 creative_battle 配置。"""

    enabled: bool = Field(False, description="该服是否启用创作大会")
    meta: CreativeBattleMeta
    promotion: PromotionConfig = Field(default_factory=PromotionConfig)
    factions: Annotated[list[FactionConfig], TomlMergeAsTableList()] = Field(
        default_factory=list,
        description="阵营定义（按 UI 顺序排列；按惯例恰好 2 个）",
    )
    notification: NotificationConfig

    @field_validator("factions")
    @classmethod
    def _exactly_two_factions(cls, v: list[FactionConfig]) -> list[FactionConfig]:
        if len(v) != 2:
            raise ValueError(
                f"创作大会必须恰好定义两个组（A/B），实际有 {len(v)} 个"
            )
        keys = [f.key for f in v]
        if len(set(keys)) != 2:
            raise ValueError(f"组 key 必须唯一，发现重复：{keys}")
        return v


__all__ = [
    "CreativeBattleMeta",
    "FactionConfig",
    "PromotionConfig",
    "NotificationConfig",
    "CreativeBattleGuildConfig",
]
