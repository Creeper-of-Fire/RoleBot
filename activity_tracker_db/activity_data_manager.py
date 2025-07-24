# honor_system/data_manager.py
from __future__ import annotations

import contextlib
import datetime
import logging
from typing import List, Optional, Self

from sqlalchemy import select, func
from sqlalchemy.dialects.sqlite import insert

from .models import SessionLocal, TrackedPost, JoinRecord


class ActivityDataManager:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    _data_manager: Optional[Self] = None

    @classmethod
    def getDataManager(cls, logger) -> Self:
        if not cls._data_manager:
            cls._data_manager = cls(logger=logger)
        return cls._data_manager

    @staticmethod
    @contextlib.contextmanager
    def get_db():
        """获取一个数据库会话"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def add_tracked_post(self, post_id: int, author_id: int, parent_channel_id: int):
        """添加一条新的帖子记录"""
        with self.get_db() as db:
            # 检查帖子是否已记录
            exists = db.execute(
                select(TrackedPost).where(TrackedPost.post_id == post_id)
            ).scalar_one_or_none()
            if not exists:
                new_post = TrackedPost(
                    post_id=post_id,
                    author_id=author_id,
                    parent_channel_id=parent_channel_id
                )
                db.add(new_post)
                db.commit()

    def get_user_post_count(self, user_id: int) -> int:
        """获取用户的总发帖数"""
        with self.get_db() as db:
            count = db.execute(
                select(func.count(TrackedPost.id)).where(TrackedPost.author_id == user_id)
            ).scalar_one()
            return count or 0

    def get_join_record(self, user_id: int, guild_id: int) -> Optional[JoinRecord]:
        """获取单个用户的加入记录"""
        with self.get_db() as db:
            record = db.execute(
                select(JoinRecord).where(
                    JoinRecord.user_id == user_id,
                    JoinRecord.guild_id == guild_id
                )
            ).scalar_one_or_none()
            return record

    def upsert_join_record(self, user_id: int, guild_id: int, joined_at: datetime.datetime) -> None:
        """插入或更新单条加入记录。"""
        self.bulk_upsert_join_records([
            {"user_id": user_id, "guild_id": guild_id, "joined_at": joined_at}
        ])

    def bulk_upsert_join_records(self, records: List[dict]):
        """
        高效地批量插入或更新加入记录。
        只在新的加入时间早于现有记录时才更新，确保保留最早的加入时间。
        records: 一个字典列表，每个字典包含 'user_id', 'guild_id', 'joined_at'。
        """
        if not records:
            return

        # 定义 SQLite 默认的 SQL 变量限制
        # 虽然有些编译版本可能是 32766，但为了兼容性，使用 999 是更安全的默认值。
        SQLITE_MAX_VARIABLES = 999
        # 每个记录有 3 个字段 (user_id, guild_id, joined_at)
        VARIABLES_PER_RECORD = 3
        # 计算每个批次最多能处理的记录数
        # 999 / 3 = 333
        BATCH_SIZE = SQLITE_MAX_VARIABLES // VARIABLES_PER_RECORD

        # 确保至少处理一个记录，即使 BATCH_SIZE 计算为 0（虽然通常不会）
        if BATCH_SIZE == 0:
            BATCH_SIZE = 1

        self.logger.info(f"开始批量处理 {len(records)} 条加入记录，每批次 {BATCH_SIZE} 条。")

        # 将大的记录列表分割成小的批次
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            with self.get_db() as db:
                try:
                    # 准备 upsert 语句
                    stmt = insert(JoinRecord).values(batch)

                    # 定义冲突时的更新操作
                    # 如果 (user_id, guild_id) 已存在，则只有当新提供的 joined_at
                    # (stmt.excluded.joined_at) 早于数据库中已有的 joined_at
                    # (JoinRecord.joined_at) 时，才执行更新。
                    update_stmt = stmt.on_conflict_do_update(
                        index_elements=['user_id', 'guild_id'],
                        set_=dict(joined_at=stmt.excluded.joined_at),
                        where=(stmt.excluded.joined_at < JoinRecord.joined_at)
                    )

                    db.execute(update_stmt)
                    db.commit()
                    self.logger.info(f"成功处理批次 {i // BATCH_SIZE + 1} ({len(batch)} 条记录)。")
                except Exception as e:
                    db.rollback()
                    self.logger.error(f"处理批次 {i // BATCH_SIZE + 1} 时发生错误: {e}", exc_info=True)
                    # 如果发生错误，可以决定是继续处理下一批还是中止
                    # 这里选择继续处理，以最大化成功写入的记录
