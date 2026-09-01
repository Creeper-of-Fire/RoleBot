"""per-(guild_id, user_id) 的 asyncio.Lock 池，带 soft TTL。

设计要点（2026-09-01）：
- per-user 互斥序列化，避免 callback 之间的 race condition
- soft TTL：acquire 时如果已有锁被持有超过 TTL，认为"已经坏了"，新建一个锁
  - 旧持有者仍在跑（用户接受的漏洞）——callback 应 <1 秒完成，30 秒 TTL 几乎不会触发
- 不 cleanup：dict 永久增长。规模可控（几千 entries × ~100B ≈ 几 MB）
- 不可重入：asyncio.Lock 本身不可重入，调用方需注意不要在持锁时 acquire 同一 key

用法：
    pool = ExpiringLockPool(ttl=30.0)
    lock = await pool.get_or_create(guild_id, user_id)
    async with lock:
        # critical section
        ...
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional


class ExpiringLock:
    """asyncio.Lock 包装，带 soft TTL（仅记录是否过期，不强制释放）。

    单独使用需要自己管 dict——大部分场景用 ExpiringLockPool。
    """

    def __init__(self, ttl: float):
        self._ttl = ttl
        self._lock = asyncio.Lock()
        self._acquired_at: Optional[float] = None

    @property
    def is_expired(self) -> bool:
        """如果当前被持有且超过 TTL，返回 True。未持有时始终返回 False。"""
        if self._acquired_at is None:
            return False
        return (time.monotonic() - self._acquired_at) > self._ttl

    async def acquire(self) -> None:
        await self._lock.acquire()
        self._acquired_at = time.monotonic()

    def release(self) -> None:
        if self._acquired_at is None:
            # 未持有却 release——交给 asyncio.Lock 抛 RuntimeError，保持其原始语义
            self._lock.release()
            return
        self._acquired_at = None
        self._lock.release()

    async def __aenter__(self) -> "ExpiringLock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class ExpiringLockPool:
    """Per-(guild_id, user_id) 的 ExpiringLock 池，不 cleanup。

    get_or_create 行为：
    - 无现有 lock → 新建并返回
    - 现有 lock 未被持有 → 直接返回（可重用）
    - 现有 lock 持有中且未过 TTL → 返回，调用方 await acquire 时正确排队
    - 现有 lock 持有中且已过 TTL → 新建（**已知漏洞**：旧持有者仍在跑，
      不再受新锁保护。callback 应 <1 秒完成，30 秒 TTL 几乎不会触发）
    """

    def __init__(self, ttl: float = 30.0):
        self._ttl = ttl
        self._locks: dict[tuple[int, int], ExpiringLock] = {}
        # meta-lock 保护 dict 自身的并发读写
        self._meta_lock = asyncio.Lock()

    async def get_or_create(self, guild_id: int, user_id: int) -> ExpiringLock:
        key = (guild_id, user_id)
        async with self._meta_lock:
            existing = self._locks.get(key)
            if existing is not None and not existing.is_expired:
                return existing
            new_lock = ExpiringLock(ttl=self._ttl)
            self._locks[key] = new_lock
            return new_lock


__all__ = ["ExpiringLock", "ExpiringLockPool"]
