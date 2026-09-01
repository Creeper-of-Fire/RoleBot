"""声明式调度装饰器 ``scheduled_loop``。

设计目标：在保留 d.py :func:`discord.ext.tasks.loop` 全部行为的前提下，
用关键字参数表达两项声明式策略——避免在每个 cog 里都重复
``__init__`` 里 ``xxx_task.start()`` + ``before_loop`` 里
``wait_until_ready()`` + 手写 ``asyncio.sleep(interval)`` 的样板。

Args 设计：

- ``time`` / ``hours`` / ``minutes`` / ``seconds`` / ``count``：与
  :func:`discord.ext.tasks.loop` 完全等价（透传）。
- ``run_on_startup`` (default ``False``)：
    - ``True``  → ready 后立即首跑。
    - ``False`` → sleep 一个完整间隔后才首跑（与 d.py 原生语义相反——
      d.py 是 start() 之后立刻跑第一次）。
  ``time=`` 模式下被忽略（d.py 的 ``time=`` 自然首跑 = 下一个时刻）。
- ``run_in_background`` (default ``False``)：
    - ``False``（默认）→ 保持 d.py 原生 event loop 行为，coroutine 必须
      async def；sync def 会立刻 raise ``ValueError``。
    - ``True``  → sync def 自动包装成 async wrapper，丢到 bot 默认线程池
      （``self.bot.loop.run_in_executor(None, partial(sync_fn, self))``）。
      async def + ``True`` raise ``ValueError``（明确意图——async 路径
      已经有 await 能力，再包 run_in_executor 是反模式）。

Raises
------
ValueError
    - ``time=`` 和 ``hours``/``minutes``/``seconds``/``count`` 同时设置
      （互斥，与 d.py 一致）。
    - ``async def`` + ``run_in_background=True``。
    - ``sync def`` + ``run_in_background=False``。

before_loop 链式语义
---------------------
d.py 的 ``Loop.before_loop`` 是单一槽位（赋值式，不是列表）。我们的策略：

- ``@scheduled_loop(...)`` 应用时，装饰器注册一个 **包装后的** ``before_loop``：
  ``user_func(self) → asyncio.sleep(interval_seconds)``（当且仅当
  ``run_on_startup=False`` 且间隔模式）。
- 如果用户 **没有** 自己写 ``@xxx.before_loop``，装饰器注入默认值
  ``await self.bot.wait_until_ready() → asyncio.sleep(interval_seconds)``。
- 如果用户写了 ``@xxx.before_loop(user_func)``，包装把 user_func 串在前面，
  sleep 串在后面——user 先，新加的后。

注意：``__get__`` 创建 per-instance Loop 时会把 ``_before_loop`` 整个
拷过去，所以"装饰器注入的包装"在 cog 实例化后仍然生效。
"""
from __future__ import annotations

import asyncio
import datetime as _dt
from functools import partial, wraps
from types import MethodType
from typing import Any, Callable, Optional

from discord.ext import tasks as _dpy_tasks


def scheduled_loop(
    *,
    time: Optional[_dt.time] = None,
    hours: Optional[float] = None,
    minutes: Optional[float] = None,
    seconds: Optional[float] = None,
    count: Optional[int] = None,
    run_on_startup: bool = False,
    run_in_background: bool = False,
) -> Callable[[Callable[..., Any]], _dpy_tasks.Loop]:
    """声明式调度装饰器——包装 d.py :func:`tasks.loop` 并注入启动策略。

    完整语义见本模块 docstring。
    """
    # ---- 参数预校验 1：time= 与间隔参数互斥 ----
    has_interval = any(v is not None for v in (hours, minutes, seconds))
    if time is not None and has_interval:
        raise ValueError(
            "scheduled_loop: time= 与 hours/minutes/seconds 互斥，不能同时设置"
        )

    # interval_seconds：仅在间隔模式下有意义，time= 模式不需要 sleep
    interval_seconds = (
        (hours or 0) * 3600 + (minutes or 0) * 60 + (seconds or 0)
        if has_interval
        else 0
    )

    def decorator(coro: Callable[..., Any]) -> _dpy_tasks.Loop:
        is_coro = asyncio.iscoroutinefunction(coro)

        # ---- 参数预校验 2/3：async/sync 与 run_in_background 组合 ----
        if is_coro and run_in_background:
            raise ValueError(
                f"scheduled_loop({coro.__qualname__}): async def + "
                f"run_in_background=True 无意义。async 函数本身就可以 await，"
                f"不需要 run_in_executor。如果只想让其中某段同步代码在后台线程跑，"
                f"请把那段同步代码单独写成 sync def 并用 asyncio.to_thread(...) 包。"
            )
        if not is_coro and not run_in_background:
            raise ValueError(
                f"scheduled_loop({coro.__qualname__}): sync def 必须配合 "
                f"run_in_background=True，否则 tasks.loop 会因为无法 await "
                f"同步函数而失败。"
            )

        # ---- 1. 必要时把 sync def 包装成 async wrapper ----
        if is_coro:
            # async def：原样交给 tasks.loop（d.py 内部会校验 iscoroutinefunction）
            actual_coro = coro
        else:
            # sync def：包成 async def，丢到 bot 默认线程池
            @wraps(coro)
            async def _async_wrapper(self, *args, **kwargs):  # noqa: ANN001
                # self 由 d.py 的 Loop.__get__ 注入（_injected = obj）
                func = partial(coro, self, *args, **kwargs)
                loop = self.bot.loop
                await loop.run_in_executor(None, func)
            actual_coro = _async_wrapper

        # ---- 2. 构造 tasks.loop 实例 ----
        # 注意：d.py 用 MISSING 区分"未传"，传 None 会让 change_interval 算 sleep 时
        # 报 TypeError；所以 None 的字段一律不进 kwargs。
        loop_kwargs: dict[str, Any] = {}
        if time is not None:
            loop_kwargs["time"] = time
        else:
            if hours is not None:
                loop_kwargs["hours"] = hours
            if minutes is not None:
                loop_kwargs["minutes"] = minutes
            if seconds is not None:
                loop_kwargs["seconds"] = seconds
        if count is not None:
            loop_kwargs["count"] = count

        loop_obj = _dpy_tasks.loop(**loop_kwargs)(actual_coro)

        # ---- 3. 注入 chained before_loop ----
        # 间隔模式：注入 wait_until_ready + 间隔 sleep（run_on_startup=False 时）
        # count= 模式：只注入 wait_until_ready（count 模式没有"间隔"语义，但仍需 ready 保护）
        if has_interval or count is not None:
            _install_chained_before_loop(loop_obj, interval_seconds, run_on_startup)

        return loop_obj

    return decorator


def _install_chained_before_loop(
    loop_obj: _dpy_tasks.Loop,
    interval_seconds: float,
    run_on_startup: bool,
) -> None:
    """包装 ``loop_obj.before_loop``，让 user 的 ``before_loop`` 与 sleep 串行。

    注入策略：

    - 如果用户写了 ``@xxx.before_loop(user_func)``：
        实际 ``_before_loop`` = ``user_func(self)`` →
        若 ``not run_on_startup`` 则 ``asyncio.sleep(interval_seconds)``。
    - 如果用户没写：
        实际 ``_before_loop`` = ``await self.bot.wait_until_ready()`` →
        若 ``not run_on_startup`` 则 ``asyncio.sleep(interval_seconds)``。

    实现：把 ``loop_obj.before_loop`` 实例方法替换成"先存 user_func，
    再注册 combined _before_loop"的装饰器；user_func 通过闭包被
    ``_combined`` 捕获。

    注意：``Loop.__get__`` 创建 per-instance Loop 时只拷贝
    ``_before_loop``，不会拷贝我们 monkey-patch 的 ``before_loop`` 方法。
    所以这里改的是 class-level descriptor 的方法绑定，cog 实例化时
    已经把"包装好的 _before_loop"整个拷走，运行时不会再走我们的包装。
    """
    # 注意签名：(self_arg, coro) —— 第一个是 MethodType 注入的 loop_obj，
    # 第二个才是 user 写的 async 函数。这样 `@xxx.before_loop` 这种
    # decorator 写法（`user_func = xxx.before_loop(user_func)`）能走通。
    def _before_loop_decorator(self_arg: Any, coro: Callable[..., Any]) -> Callable[..., Any]:
        """User ``@xxx.before_loop`` 实际走的路径。"""
        if not asyncio.iscoroutinefunction(coro):
            raise TypeError(
                f"scheduled_loop before_loop: expected coroutine function, "
                f"got {coro.__class__.__name__}"
            )

        # 构造 combined：user_func 先跑，再决定是否 sleep
        @wraps(coro)
        async def _combined(self, *args, **kwargs):  # noqa: ANN001
            # self 由 d.py 的 _call_loop_function('before_loop') 注入（_injected = obj）
            await coro(self, *args, **kwargs)
            if not run_on_startup and interval_seconds > 0:
                await asyncio.sleep(interval_seconds)

        # 直接替换底层 _before_loop；保留 Loop 实例上的其他状态
        loop_obj._before_loop = _combined

        # 保持 d.py 的语义：装饰器返回原函数给 user（user 看到的还是自己的 coro）
        return coro

    # 把方法绑到 loop_obj（descriptor-style）
    # 这样 @xxx.before_loop 触发的属性查找会走到我们的 _before_loop_decorator
    loop_obj.before_loop = MethodType(_before_loop_decorator, loop_obj)

    # 默认 _before_loop：用户没写 before_loop 时兜底，确保 wait_until_ready 不丢
    if loop_obj._before_loop is None:
        async def _default_before_loop(self) -> None:
            await self.bot.wait_until_ready()
            if not run_on_startup and interval_seconds > 0:
                await asyncio.sleep(interval_seconds)
        loop_obj._before_loop = _default_before_loop


__all__ = ["scheduled_loop"]
