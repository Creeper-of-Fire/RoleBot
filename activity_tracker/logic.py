# activity_tracker/logic.py
from __future__ import annotations

import asyncio
import collections
import time
import typing
from dataclasses import dataclass
from datetime import datetime, timezone

import discord

from activity_tracker.data_manager import DataManager, BEIJING_TZ

if typing.TYPE_CHECKING:
    from main import RoleBot


# --- 新的数据结构定义 ---

@dataclass
class ChannelInfoDTO:
    """
    【新增】频道信息数据传输对象 (DTO)。
    一个轻量级的、标准化的频道数据容器，用于内部逻辑处理。
    """
    id: int
    name: str
    mention: str
    is_thread: bool
    parent_id: typing.Optional[int]
    category_id: typing.Optional[int]


@dataclass
class UserReportData:
    """封装单个用户报告所需的所有数据。"""
    total_messages: int
    channel_activity: list[tuple[int, int]]
    heatmap_data: dict[str, int]


@dataclass
class SortedDisplayItem:
    """
    【修改】封装用于层级视图展示的单个项目。
    现在它持有 DTO 而不是完整的 discord Channel 对象，以实现解耦。
    """
    channel_dto: ChannelInfoDTO
    count: int


# --- 业务逻辑处理器 ---
class ActivityProcessor:
    """
    活动数据业务逻辑处理器。
    负责从 DataManager 获取原始数据，然后执行所有必要的业务逻辑：
    数据过滤、聚合、层级排序和格式化。
    """

    def __init__(self, bot: RoleBot, guild: discord.Guild, data_manager: DataManager, guild_cfg: dict):
        self.bot = bot
        self.guild = guild
        self.data_manager = data_manager
        self.guild_cfg = guild_cfg
        self.ignored_channels = set(self.guild_cfg.get("ignored_channels", []))
        self.ignored_categories = set(self.guild_cfg.get("ignored_categories", []))
        # 【新增】带TTL的DTO缓存
        self.channel_info_cache: dict[int, tuple[float, typing.Optional[ChannelInfoDTO]]] = {}
        self.CACHE_TTL_SECONDS = 3600  # 1 hour
        self._fetch_semaphore = asyncio.Semaphore(8)

    # --- 核心数据获取与过滤逻辑 (重构核心) ---

    async def get_or_fetch_channel_info(self, channel_id: int,
                                        channel_obj: typing.Optional[discord.abc.GuildChannel] = None) -> typing.Optional[ChannelInfoDTO]:
        """
        【新增核心方法】获取频道的DTO。
        优先从带TTL的缓存中读取。如果未命中或已过期，则通过API获取。
        此方法是所有频道信息需求的唯一入口。
        可以传入一个已有的 channel_obj 来“预热”或更新缓存，避免API调用。

        :param channel_id: 目标频道的ID。
        :param channel_obj: (可选) 一个已有的discord频道对象，用于填充缓存。
        :return: ChannelInfoDTO 或 None (如果频道不存在或无权限)。
        """
        now = time.time()
        if channel_id in self.channel_info_cache:
            timestamp, cached_dto = self.channel_info_cache[channel_id]
            if now - timestamp < self.CACHE_TTL_SECONDS:
                return cached_dto

        if not channel_obj:
            # 【修改】将 API 调用包裹在 Semaphore 上下文中
            async with self._fetch_semaphore:
                try:
                    # 在进入API调用前，先礼貌性地等待一小段时间，打散请求峰值
                    await asyncio.sleep(0.1)
                    channel_obj = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden):
                    self.channel_info_cache[channel_id] = (now, None)
                    return None
                except discord.HTTPException as e:
                    # 如果即使有节流仍然被限速（可能是全局限速），记录错误并返回None
                    if e.status == 429:
                        self.bot.logger.warning(f"获取频道 {channel_id} 时遭遇速率限制 (HTTP 429)。")
                    else:
                        self.bot.logger.error(f"获取频道 {channel_id} 时发生 HTTP 异常: {e}")
                    self.channel_info_cache[channel_id] = (now, None)  # 缓存失败结果
                    return None

        if not isinstance(channel_obj, (discord.abc.GuildChannel, discord.Thread)):
            return None

        is_thread = isinstance(channel_obj, discord.Thread)
        dto = ChannelInfoDTO(
            id=channel_obj.id,
            name=channel_obj.name,
            mention=channel_obj.mention,
            is_thread=is_thread,
            parent_id=channel_obj.parent_id if is_thread else None,
            category_id=channel_obj.category_id,
        )
        self.channel_info_cache[channel_id] = (now, dto)
        return dto

    async def is_channel_included(self, channel_id: int,
                                  channel_obj: typing.Optional[discord.abc.GuildChannel] = None) -> bool:
        """
        【重构】核心过滤逻辑。
        现在完全依赖于 _get_or_fetch_channel_info 方法获取的DTO。
        这是所有数据处理的统一过滤入口，确保逻辑一致性。
        """
        dto = await self.get_or_fetch_channel_info(channel_id, channel_obj)
        if not dto:
            return False

        if dto.id in self.ignored_channels:
            return False

        category_to_check = dto.category_id
        # 如果是帖子，我们关心的是其父频道的分类
        if dto.is_thread and dto.parent_id:
            parent_dto = await self.get_or_fetch_channel_info(dto.parent_id)
            if parent_dto:
                category_to_check = parent_dto.category_id

        if category_to_check and category_to_check in self.ignored_categories:
            return False

        return True

    # --- 上层业务逻辑 (适配新的过滤和数据结构) ---

    async def get_scannable_channels(self, target: typing.Optional[typing.Union[discord.abc.GuildChannel, discord.CategoryChannel]] = None) -> list[int]:
        """
        【修正】获取一个服务器内所有符合条件的可扫描频道的 ID 列表。
        此方法利用并更新DTO缓存来高效决策，但只返回最终的ID列表，将获取对象的责任交给调用者。

        :param target: (可选) 限定扫描范围。
        :return: 一个可扫描频道的 ID 列表。
        """
        scannable_ids: set[int] = set()

        # 1. 确定初始检查列表 (来自 discord.py 的缓存)
        initial_channels: list[discord.abc.GuildChannel] = []
        if isinstance(target, discord.CategoryChannel):
            initial_channels = list(target.channels)
        elif isinstance(target, (discord.TextChannel, discord.ForumChannel, discord.Thread)):
            initial_channels = [target]
        else:  # 全服扫描
            initial_channels = list(self.guild.channels)
            try:
                # 额外包括所有帖子，避免遗漏
                initial_channels.extend(self.guild.threads)
            except discord.ClientException:
                pass

        # 2. 机会主义地更新DTO缓存并进行初步筛选
        # 这是一个无API调用的高效操作，利用了 discord.py 的内部缓存
        update_tasks = [
            self.get_or_fetch_channel_info(channel.id, channel)
            for channel in initial_channels
            if isinstance(channel, (discord.TextChannel, discord.Thread, discord.ForumChannel))  # 只处理我们关心的类型
        ]
        await asyncio.gather(*update_tasks)

        # 3. 迭代所有已知的频道DTO，进行最终过滤
        # 我们直接迭代初始列表，因为 get_or_fetch_channel_info 已经填充了我们的缓存
        for channel in initial_channels:
            if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
                continue

            # 使用核心过滤逻辑
            if not await self.is_channel_included(channel.id, channel):
                continue

            # 特殊处理论坛频道
            if isinstance(channel, discord.ForumChannel):
                for thread in channel.threads:
                    if thread.permissions_for(self.guild.me).read_message_history and await self.is_channel_included(thread.id, thread):
                        scannable_ids.add(thread.id)
            # 只添加可读历史的文本频道和帖子
            elif isinstance(channel, (discord.TextChannel, discord.Thread)):
                if channel.permissions_for(self.guild.me).read_message_history:
                    scannable_ids.add(channel.id)

        return list(scannable_ids)

    async def get_user_activity_summary(self, user_id: int, days_window: int) -> tuple[int, list[tuple[int, int]]]:
        """【适配】获取单个用户的活动摘要，使用新的过滤方法。"""
        raw_data = await self.data_manager.get_user_activity_summary(self.guild.id, user_id, days_window)
        if not raw_data:
            return 0, []

        filtered_data = []
        total_messages = 0
        # 批量执行异步过滤
        tasks = [self.is_channel_included(channel_id) for channel_id, _ in raw_data]
        results = await asyncio.gather(*tasks)

        for (channel_id, count), is_included in zip(raw_data, results):
            if is_included:
                filtered_data.append((channel_id, count))
                total_messages += count
        return total_messages, filtered_data

    async def generate_user_report_data(self, user_id: int, days_window: int) -> UserReportData:
        """【适配】为单个用户生成一份完整的、纯净的报告数据。"""
        total_messages, channel_activity = await self.get_user_activity_summary(user_id, days_window)

        raw_heatmap_messages = await self.data_manager.get_heatmap_data(self.guild.id, user_id, days_window)
        heatmap_counts = collections.defaultdict(int)
        if raw_heatmap_messages:
            # 批量执行异步过滤
            channel_ids = [cid for cid, _ in raw_heatmap_messages]
            tasks = [self.is_channel_included(cid) for cid in channel_ids]
            results = await asyncio.gather(*tasks)

            for (channel_id, timestamp), is_included in zip(raw_heatmap_messages, results):
                if is_included:
                    dt_utc8 = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone(BEIJING_TZ)
                    date_str = dt_utc8.strftime('%Y-%m-%d')
                    heatmap_counts[date_str] += 1

        return UserReportData(
            total_messages=total_messages,
            channel_activity=channel_activity,
            heatmap_data=dict(heatmap_counts)
        )

    async def process_and_sort_for_display(self, activity_data: list[tuple[int, int]]) -> list[SortedDisplayItem]:
        """【重构】核心排序和层级化逻辑，现在完全基于DTO。"""
        if not activity_data:
            return []

        # 1. 批量获取所有相关频道的 DTO
        all_ids = {cid for cid, _ in activity_data}
        tasks = [self.get_or_fetch_channel_info(cid) for cid in all_ids]
        dtos = [dto for dto in await asyncio.gather(*tasks) if dto]

        # 2. 识别并获取父频道的DTO (如果它们还不在缓存中)
        parent_ids_to_fetch = {
            dto.parent_id for dto in dtos if dto.is_thread and dto.parent_id and not self.channel_info_cache.get(dto.parent_id)
        }
        if parent_ids_to_fetch:
            parent_tasks = [self.get_or_fetch_channel_info(pid) for pid in parent_ids_to_fetch]
            await asyncio.gather(*parent_tasks)  # 结果已存入缓存

        # 3. 数据处理和聚合
        activity_map = dict(activity_data)
        top_level_activity = {}
        threads_by_parent = collections.defaultdict(list)

        for dto in dtos:
            count = activity_map.get(dto.id, 0)
            if dto.is_thread and dto.parent_id:
                threads_by_parent[dto.parent_id].append((dto, count))
            else:
                top_level_activity[dto.id] = (dto, count)

        aggregate_scores = collections.defaultdict(int)
        for channel_id, (_, count) in top_level_activity.items():
            aggregate_scores[channel_id] += count
        for parent_id, children in threads_by_parent.items():
            aggregate_scores[parent_id] += sum(c for _, c in children)

        sorted_parent_ids = sorted(aggregate_scores.keys(), key=lambda it: aggregate_scores[it], reverse=True)

        # 4. 构建最终用于展示的列表
        final_sorted_list = []
        for pid in sorted_parent_ids:
            # 添加主频道/父频道
            if pid in top_level_activity:
                dto, count = top_level_activity[pid]
                final_sorted_list.append(SortedDisplayItem(channel_dto=dto, count=count))
            elif pid in threads_by_parent:
                # 如果父频道本身没有消息，但其下有帖子，我们仍需展示父频道
                parent_dto = (await self.get_or_fetch_channel_info(pid))
                if parent_dto:
                    final_sorted_list.append(SortedDisplayItem(channel_dto=parent_dto, count=0))

            # 添加子帖子
            if pid in threads_by_parent:
                sorted_threads = sorted(threads_by_parent[pid], key=lambda item: item[1], reverse=True)
                for thread_dto, count in sorted_threads:
                    final_sorted_list.append(SortedDisplayItem(channel_dto=thread_dto, count=count))

        return final_sorted_list