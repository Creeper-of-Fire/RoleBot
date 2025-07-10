# src/role_manager/utility/utility.py
from __future__ import annotations

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