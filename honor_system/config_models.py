"""HONOR_CONFIG 的 pydantic schema。

每个 guild 一份 `data/honor_{guild_id}.toml`，整体通过 TomlConfigManager 读写。

跟 complaint 同构：
- 顶层标 `Annotated[list[...], TomlMergeAsTableList()]` 让 manager 写时
  走 array-of-tables 替换，保留 array 中间项的注释。
- 其他字段 pydantic 普通验证。

字段类型决策（与用户确认过）：
- `cutoff_date` / `start_time` / `end_time` 保持 **str**（"2024-07-30 00:00:00"，空格分隔），
  因为这是 admin 编辑的 toml，字符串最直观，anniversary_module 仍然用
  `datetime.fromisoformat` 解析（Py3.11+ 兼容空格）。
- `milestones` 改 **list[{post_count, honor_uuid}]**（不再是 dict[int, str]），
  方便 toml 表达（`[[milestone_honor.milestones]]`）和 pydantic 验证。
"""

from __future__ import annotations

import datetime as _dt
from typing import Annotated, Optional

from pydantic import BaseModel, Field

from shared.config.toml_merge import TomlMergeAsTableList


# --- definitions ---


class HonorDefinitionItem(BaseModel):
    """单个 honor 定义（基类来自 common_models.BaseHonorDefinition）。"""

    uuid: str = Field(
        ...,
        description="唯一 UUID 字符串，数据库主键",
    )
    name: str = Field(..., min_length=1, max_length=100, description="荣誉名")
    description: str = Field(..., max_length=255, description="描述")
    role_id: Optional[int] = Field(None, description="对应 Discord 身份组 ID；可空")
    hidden_until_earned: bool = Field(True, description="未获得时是否隐藏")
    role_sync_honor: bool = Field(False, description="是否参与角色自同步")
    icon_url: Optional[str] = Field(None, max_length=255, description="荣誉图标 URL")
    # ★ expiration_date：对应 Discord **身份组**的过期时间（**配置层**，不在 SQLAlchemy db）。
    #   荣誉本身是永久记录；这里配的是"对应身份组何时过期"，到期时
    #   HonorExpirationCog.expiration_check_loop 推提醒到 notification 频道，admin 手动 remove role。
    #   None = 永不过期（普通 honor 默认；cup_honor 走 cup_honors.json 自己的 expiration_date）。
    #   时区约定：naive datetime 视为 Asia/Shanghai（上海时区）。
    expiration_date: Optional[_dt.datetime] = Field(
        None,
        description="对应身份组的过期时间。None=永不过期。HonorExpirationCog 24h 轮询检查，到期推提醒。",
    )


# --- claimable ---


class ClaimableConfig(BaseModel):
    """可自助领取的 honor 配置（用 [claimable] section 包住，避免和 definitions AoT 混）。"""

    uuids: list[str] = Field(
        default_factory=list,
        description="可自助领取的 honor uuid 列表；引用 definitions 里某条的 uuid",
    )


# --- anniversary_honor ---


class AnniversaryTier(BaseModel):
    """周年纪念一个等级：哪个 honor + 截止日期。"""

    honor_uuid: str = Field(..., description="指向 definitions 里的 honor uuid")
    cutoff_date: str = Field(
        ...,
        description='加入时间早于此值的成员获得。格式 "YYYY-MM-DD HH:MM:SS"（UTC+8）',
    )


class AnniversaryHonorConfig(BaseModel):
    """周年纪念子配置。"""

    enabled: bool = Field(False, description="是否启用周年纪念检查")
    tiers: list[AnniversaryTier] = Field(
        default_factory=list,
        description="周年等级列表，按 cutoff_date 升序",
    )


# --- cup_honor（元配置；数据走 cup_honors.json） ---


class CupHonorNotification(BaseModel):
    """杯赛过期通知配置。"""

    channel_id: int = Field(..., description="发送过期通知的频道 ID")
    admin_role_id: int = Field(..., description="需要 @ 的管理组身份组 ID")


class CupHonorConfig(BaseModel):
    """杯赛元配置（数据本身由 cup_honor_json_manager 读 json）。"""

    enabled: bool = Field(False, description="杯赛功能总开关")
    notification: CupHonorNotification = Field(..., description="通知目标")


# --- 顶层 schema ---
#
# 注意：原本 schema 里有 EventHonorConfig / MilestoneHonorConfig / MilestoneItem
# 用于按时间段或发帖里程碑发荣誉。检查发现这俩功能在生产代码里始终 enabled=False，
# 已成死代码。已在此版本中移除——以后真要启用，请重新加回并配合 post_module 改造。


class HonorGuildConfig(BaseModel):
    """一个 guild 的完整 honor 配置。"""

    definitions: Annotated[list[HonorDefinitionItem], TomlMergeAsTableList()] = Field(
        default_factory=list,
        description="荣誉定义（数据库同步源）",
    )
    claimable: ClaimableConfig = Field(
        default_factory=ClaimableConfig,
        description="可自助领取的 honor 配置（[claimable] section）",
    )
    anniversary_honor: AnniversaryHonorConfig = Field(
        default_factory=AnniversaryHonorConfig,
        description="周年纪念子配置",
    )
    cup_honor: CupHonorConfig = Field(
        default_factory=lambda: CupHonorConfig(
            notification=CupHonorNotification(channel_id=0, admin_role_id=0)
        ),
        description="杯赛元配置（数据走 json）",
    )


__all__ = [
    "HonorDefinitionItem",
    "ClaimableConfig",
    "AnniversaryTier",
    "AnniversaryHonorConfig",
    "CupHonorNotification",
    "CupHonorConfig",
    "HonorGuildConfig",
]  
