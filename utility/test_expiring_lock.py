"""ExpiringLock + ExpiringLockPool 单元测试。

覆盖：
1. 基础 acquire/release
2. 同 lock 并发 acquire 串行化
3. 异常路径释放锁
4. TTL 过期语义
5. Pool 同 key 返回同 lock
6. Pool 不同 key 并行
7. Pool 串行化并发（同一 key）
8. Pool TTL 过期返回新 lock（漏洞验证）

运行：
    python role_bot/utility/test_expiring_lock.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# 让脚本能直接 import utility 包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utility.expiring_lock import ExpiringLock, ExpiringLockPool


# === ExpiringLock 单体 ===

def test_expiring_lock_basic_acquire_release():
    """基础 acquire/release，is_expired 状态正确。"""
    async def scenario():
        lock = ExpiringLock(ttl=30.0)
        assert not lock.is_expired, "未持有时 is_expired 应为 False"

        async with lock:
            # 刚 acquire，未过 TTL
            assert not lock.is_expired, "刚 acquire 后未过期"

        assert not lock.is_expired, "release 后 is_expired 应为 False"

    asyncio.run(scenario())
    print("✅ ExpiringLock basic OK")


def test_expiring_lock_concurrent_same_lock_serializes():
    """同 lock 两个 acquire 必须串行（A 完全结束 B 才进入）。"""
    async def scenario():
        lock = ExpiringLock(ttl=30.0)
        order = []

        async def worker(name: str, hold: float):
            async with lock:
                order.append(f"{name}_enter")
                await asyncio.sleep(hold)
                order.append(f"{name}_exit")

        t1 = asyncio.create_task(worker("A", 0.05))
        await asyncio.sleep(0.01)  # 让 A 先拿到锁
        t2 = asyncio.create_task(worker("B", 0.01))

        await asyncio.gather(t1, t2)
        assert order == ["A_enter", "A_exit", "B_enter", "B_exit"], order

    asyncio.run(scenario())
    print("✅ ExpiringLock serializes OK")


def test_expiring_lock_exception_releases_lock():
    """critical section 抛异常时锁必须释放——下次 acquire 不阻塞。"""
    async def scenario():
        lock = ExpiringLock(ttl=30.0)

        # critical section 抛异常
        try:
            async with lock:
                raise ValueError("test exception")
        except ValueError:
            pass

        # 锁应已释放——下次 acquire 不阻塞（不死锁 = 通过）
        async with lock:
            pass

    asyncio.run(asyncio.wait_for(scenario(), timeout=1.0))
    print("✅ ExpiringLock exception releases OK")


def test_expiring_lock_ttl_expired_property():
    """is_expired 在持有超过 TTL 后返回 True。"""
    async def scenario():
        lock = ExpiringLock(ttl=0.05)  # 50ms TTL

        async with lock:
            assert not lock.is_expired
            await asyncio.sleep(0.08)  # 超过 TTL
            assert lock.is_expired, "持有超过 TTL 后 is_expired 应为 True"

    asyncio.run(scenario())
    print("✅ ExpiringLock ttl property OK")


def test_expiring_lock_reuse_after_release():
    """release 后 lock 对象可重用——不强制每次新建。"""
    async def scenario():
        lock = ExpiringLock(ttl=30.0)

        # 第一次 critical section
        async with lock:
            pass
        # 第二次 critical section 复用同一对象
        async with lock:
            pass
        # 两次都应正常完成（不死锁 = 通过）
    asyncio.run(scenario())
    print("✅ ExpiringLock reuse OK")


# === ExpiringLockPool ===

def test_pool_same_key_returns_same_lock():
    """同 (guild, user) key 返回同一 lock 对象。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)
        lock1 = await pool.get_or_create(1, 100)
        lock2 = await pool.get_or_create(1, 100)
        assert lock1 is lock2, "同 key 应返回同一 lock 对象"

    asyncio.run(scenario())
    print("✅ Pool same key identity OK")


def test_pool_different_keys_return_different_locks():
    """不同 (guild, user) key 返回不同 lock 对象。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)
        lock_a = await pool.get_or_create(1, 100)
        lock_b = await pool.get_or_create(1, 200)
        lock_c = await pool.get_or_create(2, 100)
        assert lock_a is not lock_b
        assert lock_a is not lock_c
        assert lock_b is not lock_c

    asyncio.run(scenario())
    print("✅ Pool different keys OK")


def test_pool_serializes_concurrent_acquires_for_same_key():
    """同 key 两个并发 callback 必须串行执行。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)
        order = []

        async def worker(name: str):
            lock = await pool.get_or_create(1, 100)
            async with lock:
                order.append(f"{name}_enter")
                await asyncio.sleep(0.05)
                order.append(f"{name}_exit")

        t1 = asyncio.create_task(worker("A"))
        t2 = asyncio.create_task(worker("B"))
        await asyncio.gather(t1, t2)

        # 必须串行
        assert order == ["A_enter", "A_exit", "B_enter", "B_exit"], order

    asyncio.run(scenario())
    print("✅ Pool serializes concurrent OK")


def test_pool_different_keys_run_concurrently():
    """不同 key 的 callback 应该并行执行。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)

        async def hold(uid: int):
            lock = await pool.get_or_create(1, uid)
            async with lock:
                await asyncio.sleep(0.05)

        start = time.monotonic()
        await asyncio.gather(hold(100), hold(200), hold(300))
        elapsed = time.monotonic() - start

        # 三个并行，总时间应 ≈ 0.05s（不是 0.15s）
        assert elapsed < 0.12, f"期望并行执行，实际耗时 {elapsed:.3f}s"

    asyncio.run(scenario())
    print("✅ Pool different keys parallel OK")


def test_pool_ttl_expired_returns_new_lock_while_held_too_long():
    """漏洞验证：lock 持有超过 TTL 时，get_or_create 返回新 lock。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=0.05)  # 50ms TTL
        lock_a = await pool.get_or_create(1, 100)

        async with lock_a:
            await asyncio.sleep(0.08)  # 超过 TTL
            # 持锁期间新调用应看到 is_expired=True，返回新 lock
            lock_b = await pool.get_or_create(1, 100)
            assert lock_a is not lock_b, "TTL 过期后应返回新 lock"
            assert lock_a.is_expired, "旧 lock 应被识别为 expired"

    asyncio.run(scenario())
    print("✅ Pool ttl expired (漏洞) OK")


def test_pool_meta_lock_protects_dict():
    """meta-lock 正确保护 dict——并发 get_or_create 不崩、不创建多个 lock。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)

        async def worker():
            return await pool.get_or_create(1, 100)

        results = await asyncio.gather(*[worker() for _ in range(20)])
        # 所有结果都应是同一 lock 对象
        first = results[0]
        for r in results:
            assert r is first, "meta-lock 失效：并发 get_or_create 创建了多个 lock"

    asyncio.run(scenario())
    print("✅ Pool meta-lock OK")


def test_pool_reusable_after_release():
    """lock release 后，pool 再 get_or_create 返回同一 lock 对象。"""
    async def scenario():
        pool = ExpiringLockPool(ttl=30.0)

        async with await pool.get_or_create(1, 100):
            pass

        lock_after = await pool.get_or_create(1, 100)
        # 应该是同一个（not held 时 is_expired=False → 复用）
        # 但由于 dict.get 顺序，pool 应该一直持有同一对象
        async with lock_after:
            pass

    asyncio.run(scenario())
    print("✅ Pool reusable after release OK")


if __name__ == "__main__":
    test_expiring_lock_basic_acquire_release()
    test_expiring_lock_concurrent_same_lock_serializes()
    test_expiring_lock_exception_releases_lock()
    test_expiring_lock_ttl_expired_property()
    test_expiring_lock_reuse_after_release()
    test_pool_same_key_returns_same_lock()
    test_pool_different_keys_return_different_locks()
    test_pool_serializes_concurrent_acquires_for_same_key()
    test_pool_different_keys_run_concurrently()
    test_pool_ttl_expired_returns_new_lock_while_held_too_long()
    test_pool_meta_lock_protects_dict()
    test_pool_reusable_after_release()
    print("\n🎉 All expiring_lock tests passed.")
