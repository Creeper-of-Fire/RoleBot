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
  - 投稿面板（每个分区频道各一个）：用户点投稿 → 写 json + add contributor_role + grant_honor
  - 主入口面板：A/B 互斥领取
  - 撤销投稿：admin 命令删 json（**不** remove role——admin 自己管）
- **不做**：身份组过期提醒、winner_role、自动 remove、状态机、游客领取面板（admin 复用 honor claimable）
- **投稿期判断 = if-else**：bot 读 toml 的 ``start_date`` / ``end_date``，
  ``now in [start_date, end_date]`` 就接受投稿，否则拒。不是状态机。
- **互斥**：用户已选 A 阵营后点 B → bot 拒绝（ephemeral 提示），
  不 remove A。如需更换请管理组手动 remove A 后再点 B。
- **黑/白名单 per-faction**：每个阵营各自一份
  - ``blacklist_role_ids``：持任一即拒绝加入 / 投稿（黑名单语义）
  - ``whitelist_role_ids``：非空时持任一才允许加入 / 投稿（白名单语义）
  - 两者并存时：**黑名单优先**（黑名单中的角色即使在白名单也拒）
- **grant_honor**：bot 在投稿成功后**直接调** honor 系统的 ``grant_honor`` 接口，
  uuid 在 toml 的 ``contributor_honor_uuid`` 配置（per-faction，从 honor toml 引用）
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
    """

    key: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]{0,30}$",
        description="内部 key（小写英文 + 下划线）；UI 按钮 + custom_id 都基于此",
    )
    display_name: str = Field(..., min_length=1, max_length=50, description="用户可见名")
    emoji: str = Field(..., min_length=1, max_length=64, description="用户可见 emoji")

    # --- 身份组 ID ---
    supporter_role_id: int = Field(
        ...,
        description=(
            "支持者身份组 ID。用户点主入口面板'加入阵营'按钮后 bot add；"
            "已持有 A 阵营时点 B 按钮 bot 拒绝（不 remove）。"
            "过期 / 移除由 admin 手动管理。"
        ),
    )
    submission_channel_id: Optional[int] = Field(
        None,
        description="该阵营分区频道 ID（投稿按钮 + 分区推广面板）。未配置则不发该组分区面板",
    )

    # --- 黑/白名单（per-faction，unix 哲学：黑名单优先） ---

    blacklist_role_ids: list[int] = Field(
        default_factory=list,
        description=(
            "黑名单身份组 ID 列表。**持任一即拒**（拒绝加入 / 拒绝投稿该阵营）。"
            "黑名单优先于白名单——即使持白名单角色之一，黑名单命中也拒。"
        ),
    )
    whitelist_role_ids: list[int] = Field(
        default_factory=list,
        description=(
            "白名单身份组 ID 列表。**非空时持任一才允许**加入 / 投稿该阵营；"
            "**空列表 = 不限制**。黑名单优先于白名单。"
        ),
    )

    # --- 投稿成功后授予的永久荣誉（per-faction，引用 honor toml 的 UUID） ---

    contributor_honor_uuid: Optional[str] = Field(
        None,
        description=(
            "投稿成功后 bot 调 grant_honor(member.id, contributor_honor_uuid) 给投稿用户。"
            "UUID 须在 honor toml 已存在。可选——不配则不 grant_honor。"
        ),
    )

    # --- 跨 faction 互斥时使用的"对方阵营 supporter_role_id"集合 ---

    @property
    def other_factions_supporter_role_ids(self, all_factions: list["FactionConfig"]) -> set[int]:
        """返回"其他阵营的 supporter_role_id 集合"——互斥检查用。

        不缓存，调用方应在 hot path 外预先算好。
        """
        return {f.supporter_role_id for f in all_factions if f.key != self.key}


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
