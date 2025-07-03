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
ACTIVE_BACKFILLS_KEY = "active_backfills" # 存储正在回填的guild ID，防止重复触发

class DataManager:
    """
    负责所有 Redis 数据操作的单例管理器。
    封装了消息记录、查询、回填锁定以及数据清理等功能。
    """
    _instance: typing.Optional[DataManager] = None
    _lock = asyncio.Lock() # 用于单例模式的异步锁定

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, db: int, logger: logging.Logger):
        if not hasattr(self, '_initialized'): # 确保只初始化一次
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

    async def get_user_activity_summary(self, guild_id: int, user_id: int, days_window: int,
                                        channel_ids_to_check: typing.Iterable[int]) -> tuple[int, list[tuple[int, int]]]:
        """
        获取用户在指定天数窗口内的总消息数和分频道消息数。
        参数:
            guild_id: 服务器ID
            user_id: 用户ID
            days_window: 统计天数窗口
            channel_ids_to_check: 需要检查的频道ID列表 (已排除忽略频道)
        返回: (总消息数, [(channel_id, count), ...])
        """
        total_message_count = 0
        channel_counts: list[tuple[int, int]] = []
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()

        pipe = self.redis.pipeline()
        for channel_id in channel_ids_to_check:
            key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=channel_id, user_id=user_id)
            await pipe.zcount(key, cutoff_timestamp, '+inf')

        try:
            results = await pipe.execute()
            for i, count in enumerate(results):
                channel_id = list(channel_ids_to_check)[i] # 保持顺序一致
                if count > 0:
                    channel_counts.append((channel_id, count))
                    total_message_count += count
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取用户活跃度概览失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return 0, []

        channel_counts.sort(key=lambda x: x[1], reverse=True)
        return total_message_count, channel_counts

    async def get_heatmap_data(self, guild_id: int, user_id: int, days_window: int,
                               channel_ids_to_check: typing.Iterable[int]) -> dict[str, int]:
        """
        获取用户在指定天数窗口内每天的消息数，用于热力图。
        参数:
            guild_id: 服务器ID
            user_id: 用户ID
            days_window: 统计天数窗口
            channel_ids_to_check: 需要检查的频道ID列表 (已排除忽略频道)
        返回: {'YYYY-MM-DD': count, ...}
        """
        heatmap_counts = collections.defaultdict(int)

        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(days=days_window)

        pipe = self.redis.pipeline()
        for channel_id in channel_ids_to_check:
            key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=channel_id, user_id=user_id)
            await pipe.zrangebyscore(key, start_utc.timestamp(), end_utc.timestamp(), withscores=True)

        try:
            results = await pipe.execute()
            for channel_messages in results:
                for _, timestamp in channel_messages:
                    dt_utc8 = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone(BEIJING_TZ)
                    date_str = dt_utc8.strftime('%Y-%m-%d')
                    heatmap_counts[date_str] += 1
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取热力图数据失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return {}

        return heatmap_counts

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
            return -1 # 表示失败


    async def add_message_to_pipeline(self, pipe: Pipeline, guild_id: int, channel_id: int,
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