from __future__ import annotations

import datetime
import uuid
from typing import List

from sqlalchemy import (
    create_engine,
    ForeignKey,
    DateTime,
    String,
    Boolean,
    BigInteger, UniqueConstraint
)
from sqlalchemy.orm import (
    sessionmaker,
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship
)
DATABASE_URL = "sqlite:///data/activity_tracking.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    """所有模型的基础类"""
    pass

class TrackedPost(Base):
    """记录用户在指定区域发布的帖子，用于实现里程碑荣誉"""
    __tablename__ = "tracked_posts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, comment="Discord 帖子的 ID")
    author_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True, comment="发帖人 ID")
    parent_channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False, comment="帖子所在的父频道 ID")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=lambda: datetime.datetime.now(datetime.UTC))


class JoinRecord(Base):
    """记录用户的准确加入时间，作为荣誉发放的数据源"""
    __tablename__ = "join_records"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    joined_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment="用户加入服务器的时间 (UTC)")

    __table_args__ = (
        UniqueConstraint('user_id', 'guild_id', name='_user_guild_uc'),
    )