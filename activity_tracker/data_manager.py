# activity_tracker/data_manager.py

from __future__ import annotations

import asyncio
import collections
import logging
import typing
from datetime import datetime, timedelta, timezone

import pytz
import redis.asyncio as redis
from redis import exceptions
from redis.asyncio.client import Pipeline

# --- 定义时区常量 ---
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

# --- Redis 键名模板 ---
CHANNEL_ACTIVITY_KEY_TEMPLATE = "activity:{guild_id}:{channel_id}:{user_id}"
ACTIVE_BACKFILLS_KEY = "active_backfills"  # 存储正在回填的guild ID，防止重复触发


class DataManager:
    """
    负责所有 Redis 数据操作的单例管理器。
    封装了消息记录、查询、回填锁定以及数据清理等功能。
    """
    _instance: typing.Optional[DataManager] = None
    _lock = asyncio.Lock()  # 用于单例模式的异步锁定

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, db: int, logger: logging.Logger):
        if not hasattr(self, '_initialized'):  # 确保只初始化一次
            self.logger = logger
            self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            self._initialized = True

    async def check_connection(self):
        """异步检查 Redis 连接。"""
        try:
            await self.redis.ping()
            self.logger.info("DataManager: 成功连接到 Redis 服务器 (异步客户端)。")
            return True
        except exceptions.ConnectionError as e:
            self.logger.critical(f"DataManager: 无法连接到 Redis！错误: {e}")
            return False

    async def record_message(self, guild_id: int, channel_id: int, user_id: int,
                             message_id: int, created_at_timestamp: float, retention_days: int):
        """
        记录用户发送的消息。
        参数:
            guild_id: 服务器ID
            channel_id: 频道ID
            user_id: 用户ID
            message_id: 消息ID
            created_at_timestamp: 消息创建时间的Unix时间戳 (UTC)
            retention_days: 数据保留天数
        """
        key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
            guild_id=guild_id,
            channel_id=channel_id,
            user_id=user_id
        )
        try:
            async with self.redis.pipeline() as pipe:
                await pipe.zadd(key, {str(message_id): created_at_timestamp})
                cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp()
                await pipe.zremrangebyscore(key, '-inf', cutoff_timestamp)
                await pipe.execute()
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 记录消息到 Redis 失败 (Key: {key}): {e}", exc_info=True)

    async def get_user_activity_summary(self, guild_id: int, user_id: int, days_window: int) -> list[tuple[int, int]]:
        """
        获取用户在指定天数窗口内的分频道消息数。
        此方法将扫描所有属于该用户在给定服务器的频道键。
        参数:
            guild_id: 服务器ID
            user_id: 用户ID
            days_window: 统计天数窗口
        返回: [(channel_id, count), ...]
        """
        # --- 【代码修改】不再接收 channel_ids_to_check，而是扫描所有该用户在服务器下的频道键 ---
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()

        user_channel_counts: dict[int, int] = collections.defaultdict(int)

        # 扫描所有此用户在指定服务器下的活动键
        pattern = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id="*", user_id=user_id)

        keys_to_query: list[str] = []
        channel_id_from_key: dict[str, int] = {}

        async for key in self.redis.scan_iter(pattern):
            keys_to_query.append(key)
            try:
                parts = key.split(':')
                # parts[2] 是 channel_id
                channel_id_from_key[key] = int(parts[2])
            except (IndexError, ValueError):
                self.logger.warning(f"DataManager: 无法解析Redis键 '{key}' 中的频道ID，将跳过。")
                continue

        if not keys_to_query:
            return []

        pipe = self.redis.pipeline()
        for key in keys_to_query:
            await pipe.zcount(key, cutoff_timestamp, '+inf')

        try:
            results = await pipe.execute()
            for i, count in enumerate(results):
                if count > 0:
                    channel_id = channel_id_from_key.get(keys_to_query[i])
                    if channel_id is not None:
                        user_channel_counts[channel_id] += count
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取用户活跃度概览失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return []

        # 返回未排序的列表，排序将在 Cog 中完成
        return list(user_channel_counts.items())

    async def get_heatmap_data(self, guild_id: int, user_id: int, days_window: int) -> list[tuple[int, float]]:
        """
        获取用户在指定天数窗口内所有消息的 (channel_id, timestamp) 对。
        此方法将扫描所有属于该用户在给定服务器的频道键。
        参数:
            guild_id: 服务器ID
            user_id: 用户ID
            days_window: 统计天数窗口
        返回: [(channel_id, timestamp), ...]
        """
        # --- 【代码修改】不再接收 channel_ids_to_check，而是扫描所有该用户在服务器下的频道键 ---
        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(days=days_window)

        all_messages_data: list[tuple[int, float]] = []  # 存储 (channel_id, timestamp)

        # 扫描所有此用户在指定服务器下的活动键
        pattern = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id="*", user_id=user_id)

        keys_to_query: list[str] = []
        channel_id_from_key: dict[str, int] = {}

        async for key in self.redis.scan_iter(pattern):
            keys_to_query.append(key)
            try:
                parts = key.split(':')
                channel_id_from_key[key] = int(parts[2])
            except (IndexError, ValueError):
                self.logger.warning(f"DataManager: 无法解析Redis键 '{key}' 中的频道ID，将跳过。")
                continue

        if not keys_to_query:
            return []

        pipe = self.redis.pipeline()
        for key in keys_to_query:
            await pipe.zrangebyscore(key, start_utc.timestamp(), end_utc.timestamp(), withscores=True)

        try:
            results = await pipe.execute()
            for i, channel_messages in enumerate(results):
                channel_id = channel_id_from_key.get(keys_to_query[i])
                if channel_id is not None:
                    for _, timestamp in channel_messages:
                        all_messages_data.append((channel_id, float(timestamp)))
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取热力图数据失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return []

        return all_messages_data

    # --- 获取频道活跃用户数 (通用统计) ---
    async def get_channel_activity_summary(self, guild_id: int, days_window: int) -> dict[int, dict[int, int]]:
        """
        统计指定服务器内，所有频道在过去一段时间内的用户活动数据。
        此方法将扫描服务器下所有活动键。
        参数:
            guild_id: 服务器ID
            days_window: 统计天数窗口
        返回: {user_id: {channel_id: message_count}, ...} (未过滤，未总计)
        """
        # --- 【代码修改】不再接收 channels_to_check_ids，直接扫描所有键 ---
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()

        # 存储所有用户在所有频道的活动数据
        # 结构: {user_id: {channel_id: message_count}}
        all_activity_data: dict[int, dict[int, int]] = collections.defaultdict(lambda: collections.defaultdict(int))

        # 扫描所有属于该服务器的活动键
        pattern = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id="*", user_id="*")

        keys_to_query: list[str] = []
        parsed_key_info: dict[str, tuple[int, int]] = {}  # {key: (channel_id, user_id)}

        async for key in self.redis.scan_iter(pattern):
            keys_to_query.append(key)
            try:
                parts = key.split(':')
                # parts[2] 是 channel_id, parts[3] 是 user_id
                parsed_key_info[key] = (int(parts[2]), int(parts[3]))
            except (IndexError, ValueError):
                self.logger.warning(f"DataManager: 无法解析Redis键 '{key}' 中的频道或用户ID，将跳过。")
                continue

        if not keys_to_query:
            return {}

        pipe = self.redis.pipeline()
        for key in keys_to_query:
            await pipe.zcount(key, cutoff_timestamp, '+inf')

        try:
            results = await pipe.execute()
            for i, count_for_key in enumerate(results):
                if count_for_key > 0:
                    key_str = keys_to_query[i]
                    channel_id, user_id = parsed_key_info.get(key_str, (None, None))

                    if channel_id is not None and user_id is not None:
                        all_activity_data[user_id][channel_id] += count_for_key
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 执行批量 ZCOUNT 失败 (Guild: {guild_id}): {e}", exc_info=True)
            return {}

        return all_activity_data

    async def is_backfill_locked(self, guild_id: int) -> bool:
        """检查指定服务器是否有回填任务正在运行。"""
        try:
            return await self.redis.sismember(ACTIVE_BACKFILLS_KEY, str(guild_id))
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 检查回填锁定状态失败 (Guild: {guild_id}): {e}", exc_info=True)
            return False

    async def lock_backfill(self, guild_id: int):
        """锁定指定服务器的回填任务。"""
        try:
            await self.redis.sadd(ACTIVE_BACKFILLS_KEY, str(guild_id))
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 锁定回填任务失败 (Guild: {guild_id}): {e}", exc_info=True)

    async def unlock_backfill(self, guild_id: int):
        """解锁指定服务器的回填任务。"""
        try:
            await self.redis.srem(ACTIVE_BACKFILLS_KEY, str(guild_id))
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 解锁回填任务失败 (Guild: {guild_id}): {e}", exc_info=True)

    async def delete_guild_activity_data(self, guild_id: int) -> int:
        """
        删除一个服务器的所有活动数据键。
        返回被删除的键的数量。
        """
        pattern = f"activity:{guild_id}:*"
        self.logger.warning(f"DataManager: 开始为服务器 {guild_id} 清除活动数据，匹配模式: {pattern}")

        keys_to_delete = []
        try:
            async for key in self.redis.scan_iter(pattern):
                keys_to_delete.append(key)

            if not keys_to_delete:
                self.logger.info(f"DataManager: 服务器 {guild_id} 没有找到需要清除的活动数据。")
                return 0

            await self.redis.delete(*keys_to_delete)
            self.logger.warning(f"DataManager: 成功为服务器 {guild_id} 清除了 {len(keys_to_delete)} 个键。")
            return len(keys_to_delete)
        except exceptions.RedisError as e:
            self.logger.critical(f"DataManager: 清除服务器 {guild_id} 活动数据失败: {e}", exc_info=True)
            return -1  # 表示失败

    @staticmethod
    async def add_message_to_pipeline(pipe: Pipeline, guild_id: int, channel_id: int,
                                      user_id: int, message_id: int, created_at_timestamp: float):
        """
        将一个消息记录添加到传入的 Redis pipeline 中。
        用于批量操作，如回填。
        """
        key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
            guild_id=guild_id,
            channel_id=channel_id,
            user_id=user_id
        )
        await pipe.zadd(key, {str(message_id): created_at_timestamp})

    async def execute_pipeline(self, pipe: Pipeline):
        """执行传入的 Redis pipeline。"""
        try:
            await pipe.execute()
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 执行 Redis pipeline 失败: {e}", exc_info=True)