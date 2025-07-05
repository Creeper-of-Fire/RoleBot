# activity_tracker/data_manager.py

from __future__ import annotations

import asyncio
import collections
import csv
import gzip
import io
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
# --- 引入索引键，彻底告别 SCAN ---
CHANNEL_ACTIVITY_KEY_TEMPLATE = "activity:{guild_id}:{channel_id}:{user_id}"  # ZSET: {message_id: timestamp}
GUILD_USERS_KEY_TEMPLATE = "index:guild_users:{guild_id}"  # SET: {user_id, ...}
USER_CHANNELS_KEY_TEMPLATE = "index:user_channels:{guild_id}:{user_id}"  # SET: {channel_id, ...}
ACTIVE_BACKFILLS_KEY = "active_backfills"  # SET: {guild_id, ...}
LAST_SYNC_TIMESTAMP_KEY_TEMPLATE = "sync_timestamp:{guild_id}"


class DataManager:
    """
    负责所有 Redis 数据操作的单例管理器。
    【已重构】使用索引来加速查询，避免了耗时的 SCAN 操作。
    封装了消息记录、查询、回填锁定以及数据清理等功能。
    """
    _instance: typing.Optional[DataManager] = None
    _lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, db: int, logger: logging.Logger):
        if not hasattr(self, '_initialized'):
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
        记录用户发送的消息，并同步更新索引。
        """
        activity_key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
            guild_id=guild_id, channel_id=channel_id, user_id=user_id
        )
        guild_users_key = GUILD_USERS_KEY_TEMPLATE.format(guild_id=guild_id)
        user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)

        try:
            async with self.redis.pipeline() as pipe:
                # 1. 记录消息到 ZSET
                await pipe.zadd(activity_key, {str(message_id): created_at_timestamp})
                # 2. 更新索引
                await pipe.sadd(guild_users_key, str(user_id))
                await pipe.sadd(user_channels_key, str(channel_id))
                # 3. 清理过期数据
                cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp()
                await pipe.zremrangebyscore(activity_key, '-inf', cutoff_timestamp)
                # 4. 执行
                await pipe.execute()
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 记录消息到 Redis 失败 (Key: {activity_key}): {e}", exc_info=True)

    async def get_user_activity_summary(self, guild_id: int, user_id: int, days_window: int) -> list[tuple[int, int]]:
        """
        【已重构】获取用户在指定天数窗口内的分频道消息数。
        使用索引直接定位，不再扫描。
        """
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()
        user_channel_counts: dict[int, int] = collections.defaultdict(int)

        # 1. 从索引获取用户所有活跃过的频道
        user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)
        try:
            channel_ids_str = await self.redis.smembers(user_channels_key)
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 从索引获取用户频道列表失败 (Key: {user_channels_key}): {e}", exc_info=True)
            return []

        if not channel_ids_str:
            return []

        # 2. 构建所有需要查询的 activity ZSET 键
        keys_to_query = [
            CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=cid, user_id=user_id)
            for cid in channel_ids_str
        ]
        channel_ids = [int(cid) for cid in channel_ids_str]  # 用于后续映射

        # 3. 使用 pipeline 批量查询
        pipe = self.redis.pipeline()
        for key in keys_to_query:
            await pipe.zcount(key, cutoff_timestamp, '+inf')

        try:
            results = await pipe.execute()
            for i, count in enumerate(results):
                if count > 0:
                    channel_id = channel_ids[i]
                    user_channel_counts[channel_id] += count
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取用户活跃度概览失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return []

        return list(user_channel_counts.items())

    async def get_heatmap_data(self, guild_id: int, user_id: int, days_window: int) -> list[tuple[int, float]]:
        """
        【已重构】获取用户在指定天数窗口内所有消息的 (channel_id, timestamp) 对。
        使用索引直接定位，不再扫描。
        """
        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(days=days_window)
        all_messages_data: list[tuple[int, float]] = []

        # 1. 从索引获取用户所有活跃过的频道
        user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)
        try:
            channel_ids_str = await self.redis.smembers(user_channels_key)
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 从索引获取用户频道列表失败 (Key: {user_channels_key}): {e}", exc_info=True)
            return []

        if not channel_ids_str:
            return []

        # 2. 构建所有需要查询的 activity ZSET 键
        keys_to_query = [
            CHANNEL_ACTIVITY_KEY_TEMPLATE.format(guild_id=guild_id, channel_id=cid, user_id=user_id)
            for cid in channel_ids_str
        ]
        channel_ids = [int(cid) for cid in channel_ids_str]  # 用于后续映射

        # 3. 使用 pipeline 批量查询
        pipe = self.redis.pipeline()
        for key in keys_to_query:
            await pipe.zrangebyscore(key, start_utc.timestamp(), end_utc.timestamp(), withscores=True)

        try:
            results = await pipe.execute()
            for i, channel_messages in enumerate(results):
                channel_id = channel_ids[i]
                for _, timestamp in channel_messages:
                    all_messages_data.append((channel_id, float(timestamp)))
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取热力图数据失败 (Guild: {guild_id}, User: {user_id}): {e}", exc_info=True)
            return []

        return all_messages_data

    async def get_channel_activity_summary(self, guild_id: int, days_window: int) -> dict[int, dict[int, int]]:
        """
        【已重构】统计指定服务器内，所有频道在过去一段时间内的用户活动数据。
        使用索引链式查询，不再扫描。
        """
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=days_window)).timestamp()
        all_activity_data: dict[int, dict[int, int]] = collections.defaultdict(lambda: collections.defaultdict(int))

        # 1. 从主索引获取所有活跃用户
        guild_users_key = GUILD_USERS_KEY_TEMPLATE.format(guild_id=guild_id)
        try:
            user_ids_str = await self.redis.smembers(guild_users_key)
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 从主索引获取服务器用户列表失败 (Key: {guild_users_key}): {e}", exc_info=True)
            return {}

        if not user_ids_str:
            return {}

        # 2. 批量获取所有用户的频道列表
        user_ids = [int(uid) for uid in user_ids_str]
        pipe_get_channels = self.redis.pipeline()
        for user_id in user_ids:
            user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)
            await pipe_get_channels.smembers(user_channels_key)

        try:
            all_users_channels_results = await pipe_get_channels.execute()
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 批量获取用户频道列表失败 (Guild: {guild_id}): {e}", exc_info=True)
            return {}

        # 3. 构建所有需要查询的 activity ZSET 键
        keys_to_query: list[str] = []
        parsed_key_info: dict[str, tuple[int, int]] = {}  # {key: (channel_id, user_id)}

        for i, user_channel_set in enumerate(all_users_channels_results):
            user_id = user_ids[i]
            for channel_id_str in user_channel_set:
                key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
                    guild_id=guild_id, channel_id=channel_id_str, user_id=user_id
                )
                keys_to_query.append(key)
                parsed_key_info[key] = (int(channel_id_str), user_id)

        if not keys_to_query:
            return {}

        # 4. 最终批量查询 ZCOUNT
        pipe_count = self.redis.pipeline()
        for key in keys_to_query:
            await pipe_count.zcount(key, cutoff_timestamp, '+inf')

        try:
            results = await pipe_count.execute()
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
        【已增强】删除一个服务器的所有活动数据键及其关联的索引和同步时间戳。
        """
        self.logger.warning(f"DataManager: 开始为服务器 {guild_id} 清除所有活动数据和索引。")
        keys_to_delete = []
        deleted_count = 0

        try:
            # 1. 查找并删除所有 activity:* ZSETs
            activity_pattern = f"activity:{guild_id}:*"
            async for key in self.redis.scan_iter(activity_pattern):
                keys_to_delete.append(key)

            # 2. 查找并删除所有 user_channels:* SETs
            user_channels_pattern = f"index:user_channels:{guild_id}:*"
            async for key in self.redis.scan_iter(user_channels_pattern):
                keys_to_delete.append(key)

            # 3. 添加主索引键和同步时间戳键
            keys_to_delete.append(GUILD_USERS_KEY_TEMPLATE.format(guild_id=guild_id))
            keys_to_delete.append(LAST_SYNC_TIMESTAMP_KEY_TEMPLATE.format(guild_id=guild_id))

            if not keys_to_delete:
                self.logger.info(f"DataManager: 服务器 {guild_id} 没有找到需要清除的活动数据或索引。")
                return 0

            # 使用 unlink 可能是个好主意，如果键很多的话
            deleted_count = await self.redis.delete(*keys_to_delete)
            self.logger.warning(f"DataManager: 成功为服务器 {guild_id} 清除了 {deleted_count} 个键 (包括数据、索引和时间戳)。")
            return int(deleted_count)  # delete returns int
        except exceptions.RedisError as e:
            self.logger.critical(f"DataManager: 清除服务器 {guild_id} 活动数据失败: {e}", exc_info=True)
            return -1

    @staticmethod
    async def add_message_to_pipeline(pipe: Pipeline, guild_id: int, channel_id: int,
                                      user_id: int, message_id: int, created_at_timestamp: float):
        """
        【已重构】将一个消息记录（包括索引更新）添加到传入的 Redis pipeline 中。
        """
        activity_key = CHANNEL_ACTIVITY_KEY_TEMPLATE.format(
            guild_id=guild_id, channel_id=channel_id, user_id=user_id
        )
        guild_users_key = GUILD_USERS_KEY_TEMPLATE.format(guild_id=guild_id)
        user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id)

        await pipe.zadd(activity_key, {str(message_id): created_at_timestamp})
        await pipe.sadd(guild_users_key, str(user_id))
        await pipe.sadd(user_channels_key, str(channel_id))

    async def execute_pipeline(self, pipe: Pipeline):
        """执行传入的 Redis pipeline。"""
        try:
            await pipe.execute()
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 执行 Redis pipeline 失败: {e}", exc_info=True)

    # --- 【新】索引重建方法 ---
    async def rebuild_indexes_for_guild(self, guild_id: int) -> tuple[int, int]:
        """
        【一次性工具】为指定服务器扫描所有活动数据并重建索引。
        这是一个非常耗时的操作，仅用于从旧数据结构迁移。
        返回 (扫描的活动键数量, 创建的索引条目总数)。
        """
        self.logger.warning(f"DataManager: 开始为服务器 {guild_id} 重建活动数据索引。这是一个高负载操作！")

        activity_pattern = f"activity:{guild_id}:*"
        scanned_keys_count = 0
        index_entries_created = 0

        # 我们分批处理，避免一次性加载过多键到内存
        cursor = '0'
        while cursor != 0:
            cursor, keys = await self.redis.scan(cursor=cursor, match=activity_pattern, count=500)
            if not keys:
                continue

            scanned_keys_count += len(keys)
            pipe = self.redis.pipeline()

            for key in keys:
                try:
                    parts = key.split(':')
                    # key format: activity:{guild_id}:{channel_id}:{user_id}
                    _activity, _gid, channel_id_str, user_id_str = parts

                    # 构建索引键
                    guild_users_key = GUILD_USERS_KEY_TEMPLATE.format(guild_id=guild_id)
                    user_channels_key = USER_CHANNELS_KEY_TEMPLATE.format(guild_id=guild_id, user_id=user_id_str)

                    # 添加到 pipeline
                    await pipe.sadd(guild_users_key, user_id_str)
                    await pipe.sadd(user_channels_key, channel_id_str)

                except (ValueError, IndexError):
                    self.logger.warning(f"DataManager-Rebuild: 无法解析活动键 '{key}'，已跳过。")
                    continue

            try:
                # 执行这批 pipeline
                results = await pipe.execute()
                # SADD 返回1表示新添加，0表示已存在。我们把所有返回值加起来就是新创建的索引条目数。
                index_entries_created += sum(results)
                self.logger.info(f"DataManager-Rebuild: [Guild {guild_id}] 已处理 {scanned_keys_count} 个活动键，创建了 {index_entries_created} 个新索引条目...")
            except exceptions.RedisError as e:
                self.logger.error(f"DataManager-Rebuild: 执行索引重建 pipeline 时出错: {e}", exc_info=True)
                # 即使出错也继续尝试下一批

        self.logger.warning(
            f"DataManager: 服务器 {guild_id} 的索引重建完成。总共扫描了 {scanned_keys_count} 个活动键，创建了 {index_entries_created} 个新索引条目。")
        return scanned_keys_count, index_entries_created

    async def get_last_sync_timestamp(self, guild_id: int) -> typing.Optional[float]:
        """获取指定服务器最后一次成功同步的Unix时间戳。"""
        key = LAST_SYNC_TIMESTAMP_KEY_TEMPLATE.format(guild_id=guild_id)
        try:
            timestamp_str = await self.redis.get(key)
            if timestamp_str:
                return float(timestamp_str)
            return None
        except (exceptions.RedisError, ValueError) as e:
            self.logger.error(f"DataManager: 获取最后同步时间戳失败 (Key: {key}): {e}", exc_info=True)
            return None

    async def set_last_sync_timestamp(self, guild_id: int, timestamp: float):
        """设置指定服务器最后一次成功同步的Unix时间戳。"""
        key = LAST_SYNC_TIMESTAMP_KEY_TEMPLATE.format(guild_id=guild_id)
        try:
            await self.redis.set(key, str(timestamp))
            self.logger.info(f"DataManager: 已更新服务器 {guild_id} 的最后同步时间戳为 {datetime.fromtimestamp(timestamp, tz=timezone.utc)}")
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 设置最后同步时间戳失败 (Key: {key}): {e}", exc_info=True)

    async def get_redis_info(self) -> typing.Optional[dict]:
        """获取并解析 Redis 服务器信息。"""
        try:
            info = await self.redis.info()
            uptime_seconds = info.get('uptime_in_seconds', 0)
            uptime_delta = timedelta(seconds=uptime_seconds)

            # 将 timedelta 格式化为 "X天 Y时 Z分"
            days, remainder = divmod(uptime_delta.total_seconds(), 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, _ = divmod(remainder, 60)
            uptime_str = f"{int(days)}天 {int(hours)}时 {int(minutes)}分"

            return {
                "version": info.get('redis_version', 'N/A'),
                "uptime": uptime_str,
                "memory": info.get('used_memory_human', 'N/A'),
                "clients": info.get('connected_clients', 'N/A'),
                "keys": info.get('db0', {}).get('keys', 'N/A')
            }
        except exceptions.RedisError as e:
            self.logger.error(f"DataManager: 获取 Redis INFO 失败: {e}", exc_info=True)
            return None

    async def generate_activity_csv(self, guild_id: int, aggregation_level: str, use_compression: bool) -> tuple[bytes, str]:
        """
        【新增】扫描、聚合服务器的所有活动数据，并生成 CSV 文件字节流。

        这是一个高负载操作，但通过流式处理和内存聚合进行了优化。
        它不依赖于新的索引，而是直接扫描原始数据键，以确保完整性。

        :param guild_id: 服务器 ID.
        :param aggregation_level: 聚合级别 ('daily' 或 'hourly').
        :param use_compression: 是否使用 Gzip 压缩.
        :return: 一个包含 (文件字节流, 文件名) 的元组.
        """
        self.logger.info(f"DataManager: 开始为服务器 {guild_id} 生成 {aggregation_level} 聚合的活动数据 CSV。")
        
        # 聚合数据的容器，键是 (时间点, channel_id, user_id)，值是 count
        aggregated_data = collections.defaultdict(int)

        activity_pattern = f"activity:{guild_id}:*"
        keys_scanned = 0

        # 1. 使用 scan_iter 流式扫描所有相关的活动键
        async for key in self.redis.scan_iter(match=activity_pattern, count=1000):
            keys_scanned += 1
            try:
                # 解析键以获取 channel_id 和 user_id
                # 格式: activity:{guild_id}:{channel_id}:{user_id}
                parts = key.split(':')
                channel_id = int(parts[2])
                user_id = int(parts[3])

                # 2. 对每个键，使用 zrange 流式获取所有消息的时间戳
                # 我们只需要 score (timestamp)，所以 withscores=True
                async for _, timestamp in self.redis.zscan_iter(key):
                    dt_utc8 = datetime.fromtimestamp(float(timestamp), tz=BEIJING_TZ)
                    
                    if aggregation_level == 'daily':
                        time_key = dt_utc8.strftime('%Y-%m-%d')
                    else: # hourly
                        time_key = dt_utc8.strftime('%Y-%m-%d %H:00')

                    aggregation_key = (time_key, channel_id, user_id)
                    aggregated_data[aggregation_key] += 1
            
            except (ValueError, IndexError):
                self.logger.warning(f"DataManager-Export: 无法解析活动键 '{key}'，已跳过。")
                continue
        
        self.logger.info(f"DataManager: [Guild {guild_id}] 扫描了 {keys_scanned} 个键，聚合了 {len(aggregated_data)} 条记录。")

        if not aggregated_data:
            return b'', '' # 返回空，表示没有数据

        # 3. 使用 io.StringIO 和 csv 模块在内存中生成 CSV
        output = io.StringIO()
        writer = csv.writer(output)

        # 写入表头
        header = ['timestamp_utc8', 'channel_id', 'user_id', 'message_count']
        writer.writerow(header)
        
        # 写入数据行
        for (time_key, channel_id, user_id), count in aggregated_data.items():
            writer.writerow([time_key, channel_id, user_id, count])
            
        # 4. 获取字节流并进行可选的压缩
        csv_data = output.getvalue()
        file_bytes = csv_data.encode('utf-8')

        if use_compression:
            final_bytes = gzip.compress(file_bytes)
            filename = f"activity_export_{guild_id}_{aggregation_level}.csv.gz"
        else:
            final_bytes = file_bytes
            filename = f"activity_export_{guild_id}_{aggregation_level}.csv"
            
        self.logger.info(f"DataManager: [Guild {guild_id}] CSV 生成完毕。文件名: {filename}, 大小: {len(final_bytes) / 1024:.2f} KB")

        return final_bytes, filename
