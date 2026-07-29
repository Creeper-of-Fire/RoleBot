"""embed_guides_{guild_id}.toml 的 pydantic schema。

每个 guild 一份 toml（per-guild 设计，遵循 ``shared/docs/toml-config-design.md``
的 "toml = per-guild" 红线）。文件结构::

    data/embed_guides_{guild_id}.toml
    data/honor_{guild_id}.toml
    data/cup_honors_{guild_id}.toml
    ...

所有 section 用同一 schema，且**自带默认值**——整份 toml 缺失或某个 section
缺失时，调用方拿到的也是有效 EmbedGuideSection 实例，直接 ``cfg.fashion_guide.to_embed()``
即可，**不需要 None 检查**。

历史：之前由 ``core/embed_link/embed_manager.py`` + JSON 文件承担此职责，
admin 在 Discord 发指引帖 + 用 ``/配置embed_link`` 改 URL → bot 拉 Discord
消息 → 缓存 embed。每 15 分钟刷新一次。

新设计：toml 直接是 source of truth；hot reload 通过 ``/上传配置`` 走 hash
校验机制。详见 ``shared/docs/toml-config-design.md`` 的 "EmbedLinkManager
迁移" 段。
"""

from __future__ import annotations

import discord
from pydantic import BaseModel, Field


class EmbedGuideSection(BaseModel):
    """单个指引 section 的配置。

    所有字段都有默认值——section 整体缺失时也返回有效实例，调用方直接
    ``section.to_embed()`` 即可，不需要 None 检查。

    Attributes:
        title: embed 标题。
        content: embed 正文（description）；支持 Discord markdown。
        color: embed 颜色（int 表示，如 ``0xFFA500``）。默认 orange。
    """

    title: str = Field(
        default="指引加载中",
        description="embed 标题",
    )
    content: str = Field(
        default="管理员尚未配置此指引，或指引正在加载中。",
        description="embed 正文（description，支持 Discord markdown）",
    )
    color: int = Field(
        default=0xFFA500,
        description="embed 颜色（int 表示，如 0xFFA500）",
    )

    def to_embed(self) -> discord.Embed:
        """把 section 转成可发送给用户的 ``discord.Embed``。"""
        return discord.Embed(
            title=self.title,
            description=self.content,
            color=discord.Color(self.color),
        )


def _default_fashion_section() -> EmbedGuideSection:
    return EmbedGuideSection(
        title="👗 幻化身份入门指引",
        content="管理员尚未配置入门指引，或指引正在加载中。",
        color=0xFFA500,
    )


def _default_self_service_section() -> EmbedGuideSection:
    return EmbedGuideSection(
        title="🛠️ 自助身份组身份入门指引",
        content="管理员尚未配置入门指引，或指引正在加载中。",
        color=0xFFA500,
    )


def _default_honor_celebrate_section() -> EmbedGuideSection:
    return EmbedGuideSection(
        title="🎊 当前进行中的荣誉获取活动",
        content="管理员尚未配置，或正在加载中。",
        color=0xFFA500,
    )


class EmbedGuidesConfig(BaseModel):
    """embed_guides_{guild_id}.toml 的 schema。

    三个 section 都是 ``EmbedGuideSection``（**不是 Optional**），各自带默认文案。
    即使整份 toml 缺失，``cfg.fashion_guide`` 也是有效实例——调用方不需要
    任何 None 检查，直接 ``cfg.fashion_guide.to_embed()``。

    Section 命名跟原 ``EmbedLinkManager`` 的 key 一一对应（``fashion_guide`` /
    ``self_service_guide`` / ``honor_celebrate_guide``），迁移无破坏。
    """

    fashion_guide: EmbedGuideSection = Field(
        default_factory=_default_fashion_section,
        description="幻化身份组入门指引",
    )
    self_service_guide: EmbedGuideSection = Field(
        default_factory=_default_self_service_section,
        description="自助身份组指引",
    )
    honor_celebrate_guide: EmbedGuideSection = Field(
        default_factory=_default_honor_celebrate_section,
        description="荣誉获取活动指引",
    )


__all__ = ["EmbedGuideSection", "EmbedGuidesConfig"]