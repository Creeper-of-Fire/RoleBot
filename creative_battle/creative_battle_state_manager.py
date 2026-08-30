"""creative_battles_{guild_id}_{season_id_safe}.json 的单例 manager。

**per-season 独立文件**——按 unix 哲学"单赛季数据小、加载快、损坏影响范围小"。

设计要点
--------

- **不继承** ``AsyncGuildDataManager``：基类不支持动态文件名模式（``{guild_id}_{season_id_safe}.json``）
- 单例（仿 ``cup_honor_json_manager.py`` 风格）
- 文件不存在 → ``load_season`` 返回 None / ``ensure_season`` 返回空 ``GuildSeasonData``（不写盘）
- 写盘：atomic via temp + replace（防半写入）
- 简单 IO（``asyncio.to_thread`` 异步）—— per-season 文件小，不需要节流 / 锁

数据组织
--------

文件名格式::

    data/creative_battles_{guild_id}_{season_id_safe}.json

``season_id_safe`` 规则：
- 非 ``[A-Za-z0-9_.-]`` 字符替换成 ``_``
- 长度截断到 80
- 空字符串兜底为 ``"default"``
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from pathlib import Path
from typing import Optional

from creative_battle.creative_battle_season_models import (
    GuildSeasonData,
    SeasonState,
)


logger = logging.getLogger(__name__)


_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9_.\-]+")
_DATA_DIR = "data"


def season_id_to_safe(season_id: str) -> str:
    """filename-safe sanitize：admin 可读字符变 ``_``，长度截断 80。"""
    safe = _FILENAME_SAFE_RE.sub("_", season_id)
    return safe[:80] or "default"


class CreativeBattleStateManager:
    """per-season 独立 json + 简单 IO + 单例。

    用法::

        manager = CreativeBattleStateManager.get_instance()
        data = manager.ensure_season(guild_id, season_id)
        # ... 改 data
        await manager.save_data(data)
    """

    _instance: Optional["CreativeBattleStateManager"] = None
    _creation_lock = threading.Lock()

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.data_dir = Path(_DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_instance(cls, logger: Optional[logging.Logger] = None) -> "CreativeBattleStateManager":
        """进程内唯一实例。"""
        if cls._instance is None:
            with cls._creation_lock:
                if cls._instance is None:
                    cls._instance = cls(logger=logger)
        return cls._instance

    @classmethod
    def _reset_instance_for_tests(cls) -> None:
        """**仅测试用**：清理单例让下次 ``get_instance()`` 重建。生产代码别调。"""
        cls._instance = None

    # --- 路径 ---

    def get_path_for(self, guild_id: int, season_id: str) -> Path:
        """返回指定赛季的 json 文件绝对路径（不一定存在）。"""
        return self.data_dir / f"creative_battles_{guild_id}_{season_id_to_safe(season_id)}.json"

    # --- 读 ---

    def load_season(self, guild_id: int, season_id: str) -> Optional[GuildSeasonData]:
        """加载指定赛季的数据，文件不存在 → None。

        解析失败 → None + error log（不抛异常——损坏数据不挂 bot）。
        """
        path = self.get_path_for(guild_id, season_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return GuildSeasonData.model_validate(data)
        except (ValueError, OSError) as e:
            self.logger.error(f"加载 {path} 失败: {e}。返回 None。")
            return None

    def ensure_season(self, guild_id: int, season_id: str) -> GuildSeasonData:
        """加载或创建赛季数据（**不**写盘）。

        文件不存在 → 创建 in-memory 默认 ``GuildSeasonData``，调用方改完再 ``save_data``。

        简化版：不维护 status 字段（投稿期判断 = if-else date range）。
        """
        data = self.load_season(guild_id, season_id)
        if data is None:
            return GuildSeasonData(
                guild_id=guild_id,
                season=SeasonState(season_id=season_id),
            )
        return data

    # --- 写 ---

    async def save_data(self, data: GuildSeasonData) -> None:
        """异步 atomic 写入。"""
        path = self.get_path_for(data.guild_id, data.season.season_id)
        await asyncio.to_thread(self._write_sync, data, path)

    def _write_sync(self, data: GuildSeasonData, path: Path) -> None:
        """同步写盘（atomic via temp + replace）。"""
        temp = path.with_suffix(path.suffix + ".tmp")
        try:
            content = data.model_dump_json(indent=2, ensure_ascii=False)
            temp.write_text(content, encoding="utf-8")
            temp.replace(path)  # atomic
        except Exception as e:
            self.logger.error(f"写入 {path} 失败: {e}")
            if temp.exists():
                try:
                    temp.unlink()
                except Exception:
                    pass


__all__ = ["CreativeBattleStateManager", "season_id_to_safe"]