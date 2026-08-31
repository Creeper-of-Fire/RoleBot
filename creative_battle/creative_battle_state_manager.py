"""creative_battles_{guild_id}_{season_id_safe}.json 的 per-season 状态管理器。

继承 ``AsyncJsonDataManager``（``_shared/data/json_manager.py``）拿到：
- ``asyncio.Lock``（line 53）—— 防止 to_thread 并发 save 撕 .tmp（之前 race 的根源）
- ``_background_save_loop``（line 158）—— 节流 + 持锁序列化
- ``force_save()``（line 206）—— 立即同步落盘
- 异常隔离 + 临时文件清理（line 137-152）

**per (guild_id, season_id) 单例**：每个赛季一个独立文件、独立锁实例。

**与 base 的两点差异**：
1. ``file_path`` 不是从 class attr ``DATA_FILENAME`` 算，而是 per instance 算（按 guild_id + season_id）
2. ``GuildSeasonData`` 的 ``season`` 字段是 required（base 的 ``_reset_data`` 会跑挂），所以 override
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple

from creative_battle.creative_battle_season_models import (
    GuildSeasonData,
    SeasonState,
)
from shared.data.json_manager import AsyncJsonDataManager, DATA_DIR


_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9_.\-]+")


def season_id_to_safe(season_id: str) -> str:
    """filename-safe sanitize：admin 可读字符变 ``_``，长度截断 80。

    沿用原实现，避免破坏磁盘上已有的文件名（_shared 没有这个 helper）。
    """
    safe = _FILENAME_SAFE_RE.sub("_", season_id)
    return safe[:80] or "default"


class CreativeBattleStateManager(AsyncJsonDataManager[GuildSeasonData]):
    """per-season 文件 + 锁 + 节流——直接复用 ``AsyncJsonDataManager`` 基建。

    一赛季一文件：``data/creative_battles_{guild_id}_{season_id_safe}.json``。
    """

    DATA_MODEL = GuildSeasonData

    # per-key 单例（base class 是 per-class-name 单例）
    _instances: Dict[Tuple[int, str], "CreativeBattleStateManager"] = {}
    _creation_lock = threading.Lock()

    def __init__(
        self,
        guild_id: int,
        season_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        # ★ 不调 super().__init__——base 默认从 DATA_FILENAME 算 file_path，
        #   而我们要 per (guild_id, season_id) 的路径。手动复制 base 的 setup。
        self.guild_id = guild_id
        self.season_id = season_id
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        safe = season_id_to_safe(season_id)
        self.file_path = os.path.join(
            DATA_DIR, f"creative_battles_{guild_id}_{safe}.json"
        )

        self.model_cls = self.DATA_MODEL
        self.data: GuildSeasonData = self._default_data()

        # 来自 base AsyncJsonDataManager.__init__
        self._lock = asyncio.Lock()
        self._save_task: Optional[asyncio.Task] = None
        self._dirty = False
        self._is_cooling_down = False
        self._throttle_interval = self.THROTTLE_INTERVAL

        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_data()

    @classmethod
    def get_instance(
        cls,
        guild_id: int,
        season_id: str,
        logger: Optional[logging.Logger] = None,
    ) -> "CreativeBattleStateManager":
        """per (guild_id, season_id) 单例。"""
        key = (guild_id, season_id)
        if key not in cls._instances:
            with cls._creation_lock:
                if key not in cls._instances:
                    cls._instances[key] = cls(
                        guild_id=guild_id,
                        season_id=season_id,
                        logger=logger,
                    )
        return cls._instances[key]

    @classmethod
    def _reset_instance_for_tests(cls) -> None:
        """**仅测试用**：清理所有 per-key 单例。"""
        cls._instances.clear()

    # --- API（保持旧签名兼容 cog 调用方） ---

    def ensure_season(self) -> GuildSeasonData:
        """返回当前 (guild_id, season_id) 对应的 season data。

        注：旧签名 ``ensure_season(guild_id, season_id)`` 改成 ``get_instance(guild_id, season_id).ensure_season()``。
        调用方迁移见 ``CreativeBattleCog``。
        """
        return self.data

    async def save_data(self) -> None:
        """节流落盘（默认 3s 内多次调用只写一次）。需要立即同步用 ``force_save()``。"""
        await super().save_data()

    async def force_save(self) -> None:
        """立即落盘——拿锁同步写，绕过节流。"""
        await super().force_save()

    # --- override base 的 reset 行为 ---

    def _default_data(self) -> GuildSeasonData:
        """构造带必填字段的默认 state（base 的 ``model_cls()`` 会因为 ``season`` 是 required 失败）。"""
        return GuildSeasonData(
            guild_id=self.guild_id,
            season=SeasonState(season_id=self.season_id),
        )

    def _reset_data(self) -> None:
        """override base：``GuildSeasonData.model_validate({})`` 会因为 ``season`` 必填跑挂。"""
        self.data = self._default_data()


__all__ = ["CreativeBattleStateManager", "season_id_to_safe"]
