from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod, ABCMeta
from typing import List, TYPE_CHECKING, Optional

import discord
from discord.ext import commands

if TYPE_CHECKING:
    from core.cog import CoreCog
    from main import RoleBot


# 1. 定义一个新的组合元类
#    这个新元类同时继承了 CogMeta 和 ABCMeta，解决了冲突
class CogABCMeta(commands.CogMeta, ABCMeta):
    pass

class FeatureCog(commands.Cog, ABC, metaclass=CogABCMeta):
    """
    功能模块Cog的基类。

    自动处理 bot 和 logger 的初始化，并提供向 CoreCog 注册的标准流程。
    所有继承此基类的 Cog 都必须实现 `update_safe_roles_cache` 方法。
    """

    def __init__(self, bot: 'RoleBot'):
        """
        初始化基类，设置 bot 和 logger 实例。
        """
        self.bot = bot
        self.logger = bot.logger

    @property
    def core_cog(self) -> CoreCog | None:
        core_cog: CoreCog | None = self.bot.get_cog("Core")
        return core_cog

    @property
    def role_name_cache(self) -> dict[int, str] | None:
        core_cog: CoreCog | None = self.core_cog
        if (core_cog is None) or (core_cog.role_name_cache is None):
            return None
        return core_cog.role_name_cache

    async def cog_load(self) -> None:
        """
        当Cog被加载时，等待并向 CoreCog 注册自己。
        子类如果需要覆盖此方法，必须调用 `await super().cog_load()`。
        """
        # 短暂等待，以确保 CoreCog 已经加载完毕
        await asyncio.sleep(1)

        core_cog: CoreCog | None = self.bot.get_cog("Core")

        if core_cog:
            # 调用 CoreCog 的注册方法，把自己传进去
            core_cog.register_feature_cog(self)
        else:
            self.logger.error(f"无法找到 CoreCog。模块 {self.qualified_name} 的功能将受限，无法自动更新缓存。")

    @abstractmethod
    async def update_safe_roles_cache(self):
        """
        【抽象接口方法】更新本模块的安全身份组缓存。

        CoreCog 会周期性地调用此方法。子类必须实现此方法，
        以定义如何从配置中读取身份组、检查其安全性，并更新自己的内部缓存。

        实现示例:
            core_cog = self.bot.get_cog("Core")
            # ... 遍历配置 ...
            # ... 检查角色危险性 ...
            # ... 更新 self.safe_..._cache ...
            # ... 更新 core_cog.role_name_cache ...
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 update_safe_roles_cache 方法。")

    # ===================================================================
    # 新增的抽象方法
    # ===================================================================
    @abstractmethod
    def get_main_panel_buttons(self) -> Optional[List[discord.ui.Button]]:
        """
        【抽象接口方法】返回一个用于主控制面板的入口按钮。

        如果此模块没有主面板入口，则应返回 None。
        按钮的回调逻辑应在该模块的 Cog 或 View 中定义。

        返回:
            List[discord.ui.Button] | None: 代表此模块入口的按钮实例，或 None。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 get_main_panel_button 方法。")
