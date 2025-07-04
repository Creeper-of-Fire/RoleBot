# activity_tracker/logic.py
from __future__ import annotations

import collections
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import typing

import discord

from activity_tracker.data_manager import DataManager, BEIJING_TZ

if typing.TYPE_CHECKING:
    from main import RoleBot


# --- 数据结构定义 ---
@dataclass
class UserReportData:
    """封装单个用户报告所需的所有数据。"""
    total_messages: int
    channel_activity: list[tuple[int, int]]  # (channel_id, count)
    heatmap_data: dict[str, int]  # { 'YYYY-MM-DD': count }


@dataclass
class SortedDisplayItem:
    """封装用于层级视图展示的单个项目。"""
    channel: discord.abc.GuildChannel
    count: int


# --- 业务逻辑处理器 ---
class ActivityProcessor:
    """
    活动数据业务逻辑处理器。
    负责从 DataManager 获取原始数据，然后执行所有必要的业务逻辑：
    数据过滤、聚合、层级排序和格式化。
    这个类的目标是为 Cog 提供可以直接用于展示的、干净的数据结构。
    """

    def __init__(self, bot: RoleBot, guild: discord.Guild, data_manager: DataManager, guild_cfg: dict):
        self.bot = bot
        self.guild = guild
        self.data_manager = data_manager
        self.guild_cfg = guild_cfg
        self.ignored_channels = set(self.guild_cfg.get("ignored_channels", []))
        self.ignored_categories = set(self.guild_cfg.get("ignored_categories", []))
        self._channel_cache: dict[int, typing.Optional[discord.abc.GuildChannel]] = {}

    async def build_channel_cache(self, channel_ids: set[int]) -> None:
        """构建或更新内部频道对象缓存，以减少API调用。"""
        ids_to_fetch = {cid for cid in channel_ids if cid not in self._channel_cache}
        if not ids_to_fetch:
            return

        async def fetch_one(channel_id):
            try:
                return await self.bot.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden):
                return None

        tasks = [fetch_one(cid) for cid in ids_to_fetch]
        results = await asyncio.gather(*tasks)
        for cid, channel_obj in zip(ids_to_fetch, results):
            self._channel_cache[cid] = channel_obj

    def is_channel_included(self, channel_id: int) -> bool:
        """
        【核心过滤逻辑】
        根据服务器配置，判断一个频道是否应该被包含在统计内。
        这是所有数据处理的统一过滤入口，确保逻辑一致性。
        """
        channel_obj = self._channel_cache.get(channel_id)
        if not channel_obj: return False

        if channel_obj.id in self.ignored_channels: return False

        category_id = channel_obj.parent.category_id if isinstance(channel_obj, discord.Thread) and channel_obj.parent else channel_obj.category_id
        if category_id and category_id in self.ignored_categories: return False

        return True

    async def get_user_activity_summary(self, user_id: int, days_window: int) -> tuple[int, list[tuple[int, int]]]:
        """获取单个用户的活动摘要，返回过滤后的总数和分频道数据。"""
        raw_data = await self.data_manager.get_user_activity_summary(self.guild.id, user_id, days_window)
        if not raw_data: return 0, []

        await self.build_channel_cache({cid for cid, _ in raw_data})

        filtered_data = []
        total_messages = 0
        for channel_id, count in raw_data:
            if self.is_channel_included(channel_id):
                filtered_data.append((channel_id, count))
                total_messages += count
        return total_messages, filtered_data

    async def generate_user_report_data(self, user_id: int, days_window: int) -> UserReportData:
        """为单个用户生成一份完整的、纯净的报告数据。"""
        total_messages, channel_activity = await self.get_user_activity_summary(user_id, days_window)

        raw_heatmap_messages = await self.data_manager.get_heatmap_data(self.guild.id, user_id, days_window)
        heatmap_counts = collections.defaultdict(int)
        if raw_heatmap_messages:
            # 缓存已在 get_user_activity_summary 中建立，此处无需重复
            for channel_id, timestamp in raw_heatmap_messages:
                if self.is_channel_included(channel_id):
                    dt_utc8 = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone(BEIJING_TZ)
                    date_str = dt_utc8.strftime('%Y-%m-%d')
                    heatmap_counts[date_str] += 1

        return UserReportData(
            total_messages=total_messages,
            channel_activity=channel_activity,
            heatmap_data=dict(heatmap_counts)
        )

    async def process_and_sort_for_display(self, activity_data: list[tuple[int, int]]) -> list[SortedDisplayItem]:
        """【核心排序和层级化逻辑】将过滤好的数据转换为有序的、用于展示的列表。"""
        if not activity_data: return []

        all_ids = {cid for cid, _ in activity_data}
        await self.build_channel_cache(all_ids)

        parent_ids_to_fetch = {
            channel.parent_id
            for cid, _ in activity_data
            if (channel := self._channel_cache.get(cid)) and isinstance(channel,
                                                                        discord.Thread) and channel.parent_id and channel.parent_id not in self._channel_cache
        }
        if parent_ids_to_fetch: await self.build_channel_cache(parent_ids_to_fetch)

        top_level_activity = {}
        threads_by_parent = collections.defaultdict(list)
        for channel_id, count in activity_data:
            channel = self._channel_cache.get(channel_id)
            if not channel: continue
            if isinstance(channel, discord.Thread) and channel.parent_id:
                threads_by_parent[channel.parent_id].append((channel, count))
            else:
                top_level_activity[channel_id] = (channel, count)

        aggregate_scores = collections.defaultdict(int)
        for channel_id, (_, count) in top_level_activity.items():
            aggregate_scores[channel_id] += count
        for parent_id, children in threads_by_parent.items():
            aggregate_scores[parent_id] += sum(c for _, c in children)

        sorted_parent_ids = sorted(aggregate_scores.keys(), key=lambda pid: aggregate_scores[pid], reverse=True)

        final_sorted_list = []
        for pid in sorted_parent_ids:
            if pid in top_level_activity:
                channel, count = top_level_activity[pid]
                final_sorted_list.append(SortedDisplayItem(channel=channel, count=count))
            elif pid in threads_by_parent and (parent_obj := self._channel_cache.get(pid)):
                final_sorted_list.append(SortedDisplayItem(channel=parent_obj, count=0))

            if pid in threads_by_parent:
                sorted_threads = sorted(threads_by_parent[pid], key=lambda item: item[1], reverse=True)
                for thread, count in sorted_threads:
                    final_sorted_list.append(SortedDisplayItem(channel=thread, count=count))

        return final_sorted_list