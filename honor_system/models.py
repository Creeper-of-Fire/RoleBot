# honor_system/models.py
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

# --- 数据库基础设置 ---
# 这部分通常放在一个单独的 db_setup.py 文件中，但为了简化，我们先放在这里
# 'data/honors.db' 是你的 SQLite 文件路径
DATABASE_URL = "sqlite:///data/honors.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    """所有模型的基础类"""
    pass


# --- 模型定义 ---

class HonorDefinition(Base):
    """定义一个“荣誉”本身是什么。例如：“2024夏季活动参与者”"""
    __tablename__ = "honor_definitions"

    uuid: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="荣誉名称")
    description: Mapped[str] = mapped_column(String(255), comment="荣誉描述")
    # 对应的 Discord 身份组 ID，可以没有
    role_id: Mapped[int | None] = mapped_column(BigInteger)
    # 荣誉图标的URL，可以没有
    icon_url: Mapped[str | None] = mapped_column(String(255))

    # 标记该荣誉是否已在配置中弃用，但为了保留用户历史记录而不删除
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # 反向关系，可以让我们通过一个 HonorDefinition 找到所有拥有它的用户
    owners: Mapped[List["UserHonor"]] = relationship(back_populates="definition")

    # 确保 guild_id 和 name 的组合是唯一的
    __table_args__ = (
        UniqueConstraint('guild_id', 'name', name='_guild_name_uc'),
    )


class UserHonor(Base):
    """记录哪个用户拥有哪个荣誉，这是用户和荣誉之间的“桥梁”"""
    __tablename__ = "user_honors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    honor_uuid: Mapped[str] = mapped_column(ForeignKey("honor_definitions.uuid"), nullable=False)

    earned_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=lambda: datetime.datetime.now(datetime.UTC))

    # 关系，让我们可以通过 UserHonor.definition 访问到荣誉的详细信息
    definition: Mapped["HonorDefinition"] = relationship(back_populates="owners", foreign_keys=[honor_uuid])


class TrackedPost(Base):
    """记录用户在指定区域发布的帖子，用于实现里程碑荣誉"""
    __tablename__ = "tracked_posts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, comment="Discord 帖子的 ID")
    author_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True, comment="发帖人 ID")
    parent_channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False, comment="帖子所在的父频道 ID")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=lambda: datetime.datetime.now(datetime.UTC))
