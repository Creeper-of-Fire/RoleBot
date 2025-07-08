# honor_system/data_manager.py
from __future__ import annotations

import contextlib
from typing import List, Optional, TypeVar, Type

from sqlalchemy import select, func
from sqlalchemy.orm import class_mapper

from .models import SessionLocal, HonorDefinition, UserHonor, TrackedPost

T = TypeVar("T")


def clone_orm_object(obj: T) -> T:
    """
    创建一个 SQLAlchemy ORM 对象的非持久化副本。
    副本拥有与原对象相同的数据，但不与任何 Session 关联。
    """
    if obj is None:
        return None

    cls: Type[T] = obj.__class__
    mapper = class_mapper(cls)

    # 创建一个新的空实例
    new_obj = cls()

    # 遍历所有列属性并复制值
    for prop in mapper.iterate_properties:
        # 我们只关心映射到数据库列的属性
        if hasattr(prop, 'columns'):
            # 获取属性名
            prop_name = prop.key
            # 从原对象获取值并设置到新对象上
            setattr(new_obj, prop_name, getattr(obj, prop_name))

    return new_obj

class HonorDataManager:
    @staticmethod
    @contextlib.contextmanager
    def get_db():
        """获取一个数据库会话"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def get_all_honor_definitions(self, guild_id: int) -> List[HonorDefinition]:
        """获取指定服务器所有未归档的荣誉定义"""
        with self.get_db() as db:
            definitions = db.execute(
                select(HonorDefinition).where(
                    HonorDefinition.guild_id == guild_id,
                    HonorDefinition.is_archived == False
                )
            ).scalars().all()
            return definitions

    def grant_honor(self, user_id: int, honor_uuid: str) -> Optional[HonorDefinition]:
        """
        授予用户一个荣誉（通过荣誉UUID）。
        如果成功授予，返回该荣誉的 HonorDefinition 对象。
        如果用户已拥有该荣誉或荣誉不存在，则返回 None。
        """
        with self.get_db() as db:
            # 1. 查找荣誉定义
            honor_def: HonorDefinition = db.execute(
                select(HonorDefinition).where(HonorDefinition.uuid == honor_uuid)
            ).scalar_one_or_none()

            if not honor_def:
                print(f"错误：找不到UUID为 '{honor_uuid}' 的荣誉定义。")
                return None

            # 2. 检查用户是否已拥有该荣誉
            existing_honor = db.execute(
                select(UserHonor).where(
                    UserHonor.user_id == user_id,
                    UserHonor.honor_uuid == honor_uuid
                )
            ).scalar_one_or_none()

            if existing_honor:
                return None  # 已拥有，不重复授予

            # 3. 创建新的授予记录
            new_user_honor = UserHonor(user_id=user_id, honor_uuid=honor_def.uuid)
            db.add(new_user_honor)
            db.commit()

            return clone_orm_object(honor_def)

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

    def get_user_honors(self, user_id: int) -> List[UserHonor]:
        """获取一个用户拥有的所有荣誉"""
        with self.get_db() as db:
            # 使用 eager loading (joinedload) 来一次性加载关联的 HonorDefinition
            # 这样在后续访问 user_honor.definition 时不会再触发新的数据库查询
            from sqlalchemy.orm import joinedload

            honors: List[UserHonor] = db.execute(
                select(UserHonor)
                .where(UserHonor.user_id == user_id)
                .options(joinedload(UserHonor.definition))
            ).scalars().all()
            # 注意：这里的 'definition' 是关联对象，也需要处理。
            # 为了安全，我们也克隆关联的对象。
            safe_honors = []
            for h in honors:
                # 克隆 UserHonor 本身
                safe_h = clone_orm_object(h)
                # 克隆其关联的 HonorDefinition
                safe_h.definition = clone_orm_object(h.definition)
                safe_honors.append(safe_h)
            return safe_honors
