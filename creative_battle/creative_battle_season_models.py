"""creative_battles_{guild_id}_{season_id_safe}.json 的 pydantic schema。

每个赛季一个独立文件（per-season 设计，遵循 unix 哲学"单赛季数据小、加载快"）。
文件名生成见 ``creative_battle_state_manager.season_id_to_safe(...)``。

数据组织
--------

- ``GuildSeasonData`` —— 一个 guild 的某个赛季数据（顶层）
- ``season: SeasonState`` —— 当前赛季的状态
  - ``supporters: dict[user_id, ParticipantEntry]`` —— 点阵营按钮的支持者
  - ``submissions: dict[submission_id, SubmissionEntry]`` —— 提交作品的参赛者
  - ``promotion_message_ids: dict[channel_key, message_id]`` —— 推广面板消息 ID

设计哲学
--------

- 赛季数据永久保留（不归档清理）——跟 honor 系统的 UserHonor 表一样原则
- ``promotion_message_ids`` 按 channel_key 索引："main"（主入口）+ 任意 faction.key（分区）
- **简化版不存状态机**：投稿期判断 = if-else (``start_date <= today <= end_date``)，
  不维护 ``status`` / ``started_at`` / ``ended_at`` 字段。bot 不需要这些字段。
- 投票 / 私密子区 / forum 监听 / 胜负判断 / 管理组投稿管理 / 最终身份组发放
  —— **全部不做**（按 design doc 简化版拍板）
- **撤销投稿**：admin 命令只删 ``submissions`` 里的 entry，**不** remove contributor_role
  ——admin 自己到 Discord remove
"""
from __future__ import annotations

import datetime as _dt
import uuid as _uuid_lib
from typing import Optional

from pydantic import BaseModel, Field


# --- supporters ---


class ParticipantEntry(BaseModel):
    """一个支持者的状态（点主入口面板'加入阵营'按钮）。"""

    user_id: int
    faction: str = Field(..., description="阵营 key（= toml factions[*].key）")
    joined_at: _dt.datetime
    supporter_role_granted: bool = Field(
        False, description="是否已 add supporter_role（幂等标志）",
    )


# --- submissions ---


class SubmissionEntry(BaseModel):
    """一个参赛者的投稿（点投稿按钮 + 填 Modal）。"""

    submission_id: str = Field(
        default_factory=lambda: str(_uuid_lib.uuid4()),
        description="投稿 UUID",
    )
    user_id: int
    faction: str = Field(..., description="阵营 key（= toml factions[*].key）")
    title: str = Field(..., max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    submitted_at: _dt.datetime = Field(default_factory=lambda: _dt.datetime.now())
    contributor_role_granted: bool = Field(
        False, description="是否已 add contributor_role（幂等标志）",
    )
    honor_granted: bool = Field(
        False,
        description="是否已 grant_honor(contributor_honor_uuid)（幂等标志，admin 撤销投稿不撤销 honor）",
    )


# --- season state ---


class SeasonState(BaseModel):
    """一个赛季的运行时状态——永久保留。

    简化版**不维护状态字段**：投稿期判断 = ``cfg.meta.start_date <= today <= cfg.meta.end_date``。
    bot 不需要 ``status`` / ``started_at`` / ``ended_at`` 等字段——这些是 ver4 final
    "状态机" 设计的残留，简化版彻底删除。
    """

    season_id: str
    supporters: dict[int, ParticipantEntry] = Field(
        default_factory=dict,
        description="user_id -> 支持者条目",
    )
    submissions: dict[str, SubmissionEntry] = Field(
        default_factory=dict,
        description="submission_id -> 投稿条目",
    )
    promotion_message_ids: dict[str, int] = Field(
        default_factory=dict,
        description=(
            'channel_key -> Discord message_id；channel_key ∈ {"main"} ∪ factions[*].key'
        ),
    )


# --- 顶层 ---


class GuildSeasonData(BaseModel):
    """一个 guild 的某个赛季数据——per-season 文件存这个。"""

    guild_id: int
    season: SeasonState


__all__ = [
    "ParticipantEntry",
    "SubmissionEntry",
    "SeasonState",
    "GuildSeasonData",
]
