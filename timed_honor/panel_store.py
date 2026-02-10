from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


class PanelStore:
    """限时荣誉面板元数据存储（轻量 JSON）。"""

    def __init__(
        self,
        logger: logging.Logger,
        file_path: str | Path = "data/timed_honor_panels.json",
    ):
        self.logger = logger
        self.file_path = Path(file_path)
        self._lock = threading.Lock()
        self._panels: Dict[str, Dict[str, Any]] = {}

        self._ensure_file()
        self._load()

    def _ensure_file(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self.file_path.write_text("{}", encoding="utf-8")

    def _load(self) -> None:
        with self._lock:
            try:
                raw = self.file_path.read_text(encoding="utf-8")
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("面板数据文件顶层必须是对象")
                self._panels = data
            except Exception as e:
                self.logger.error(f"PanelStore: 加载失败，已重置为空。错误: {e}", exc_info=True)
                self._panels = {}

    def _save(self) -> None:
        with self._lock:
            try:
                self.file_path.write_text(
                    json.dumps(self._panels, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as e:
                self.logger.error(f"PanelStore: 保存失败。错误: {e}", exc_info=True)

    def add_panel(
        self,
        *,
        message_id: int,
        channel_id: int,
        guild_id: int,
        created_by: int,
        panel_type: str = "timed_honor_upgrade",
    ) -> None:
        self._panels[str(message_id)] = {
            "message_id": message_id,
            "channel_id": channel_id,
            "guild_id": guild_id,
            "created_by": created_by,
            "panel_type": panel_type,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def get_panel(self, message_id: int) -> Optional[Dict[str, Any]]:
        return self._panels.get(str(message_id))

    def remove_panel(self, message_id: int) -> None:
        key = str(message_id)
        if key in self._panels:
            self._panels.pop(key, None)
            self._save()

    def list_panels(self, guild_id: int | None = None) -> List[Dict[str, Any]]:
        items = list(self._panels.values())
        if guild_id is not None:
            items = [x for x in items if x.get("guild_id") == guild_id]
        return items
