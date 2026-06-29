# src/role_manager/utility/utility.py
from __future__ import annotations

import re
from typing import Optional

import discord
import pytz

BEIJING_TZ = pytz.timezone('Asia/Shanghai')


async def safe_defer(interaction: discord.Interaction, *, thinking: bool = False):
    """安全地延迟响应一个交互，如果它还没有被响应。"""
    if not interaction.response.is_done():
        await interaction.response.defer(ephemeral=True, thinking=thinking)


async def try_get_member(guild: discord.Guild, member_id: int) -> discord.Member | None:
    """尝试通过缓存或API获取一个成员对象。"""
    member = guild.get_member(member_id)
    if member: return member
    try:
        return await guild.fetch_member(member_id)
    except discord.NotFound:
        return None


def create_progress_bar(current: int, total: int, bar_length: int = 20) -> str:
    """创建一个文本格式的进度条。"""
    if total == 0:
        return f"[{'░' * bar_length}] 0.0%"
    fraction = current / total
    filled_length = int(bar_length * fraction)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    return f"[{bar}] {fraction:.1%}"


def create_jump_url(guild_id: int, channel_id: Optional[int] = None, message_id: Optional[int] = None) -> str:
    if channel_id is None:
        return f"https://discord.com/channels/{guild_id}"
    if message_id is None:
        return f"https://discord.com/channels/{guild_id}/{channel_id}"
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def format_duration_hms(total_seconds: int) -> str:
    """将总秒数格式化为 'X小时 Y分钟 Z秒' 的字符串。"""
    if total_seconds <= 0: return "0 秒"
    seconds, hours, minutes = int(total_seconds), 0, 0
    if seconds >= 3600: hours, seconds = divmod(seconds, 3600)
    if seconds >= 60: minutes, seconds = divmod(seconds, 60)
    parts = []
    if hours > 0: parts.append(f"{hours} 小时")
    if minutes > 0: parts.append(f"{minutes} 分钟")
    if seconds > 0 or not parts: parts.append(f"{seconds} 秒")
    return " ".join(parts)


# Discord 消息链接：https://discord.com/channels/{guild_id|@me}/{channel_id}/{message_id}
MESSAGE_LINK_PATTERN = re.compile(
    r"https?://(?:ptb\.|canary\.)?discord(?:app)?\.com/channels/(?:@me|\d+)/(\d+)/(\d+)"
)


def parse_message_link(link: str) -> Optional[tuple[int, int]]:
    """从 Discord 消息链接解析出 (channel_id, message_id)。无法解析返回 None。

    支持标准 / ptb / canary 域名，以及私聊场景下的 @me。
    """
    match = MESSAGE_LINK_PATTERN.search(link)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


async def fetch_message_from_link(bot: discord.Client, link: str) -> Optional[discord.Message]:
    """根据消息链接拉取消息对象。

    链接非法、频道/消息不存在、无权限或频道不可读时返回 None，不抛异常。
    """
    parsed = parse_message_link(link)
    if not parsed:
        return None
    channel_id, message_id = parsed
    try:
        channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None
    if not isinstance(channel, discord.abc.Messageable):
        return None
    try:
        return await channel.fetch_message(message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None
