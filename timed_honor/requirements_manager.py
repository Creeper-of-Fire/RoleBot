import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord

from activity_tracker.data_manager import DataManager as ActivityDataManager
from honor_system.honor_data_manager import HonorDataManager

logger = logging.getLogger(__name__)


@dataclass
class RequirementEvaluateResult:
    ok: bool
    reasons: List[str] = field(default_factory=list)
    gaps: List[str] = field(default_factory=list)
    debug_meta: Dict[str, Any] = field(default_factory=dict)


class RequirementsManager:
    """限时荣誉条件管理器：负责 requirements.json 加载、校验与评估。"""

    def __init__(
        self,
        honor_data_manager: HonorDataManager,
        activity_data_manager: ActivityDataManager,
        requirements_path: str | Path = "timed_honor/requirements.json",
        available_honors_path: str | Path = "timed_honor/available_honors.json",
    ):
        self.honor_data_manager = honor_data_manager
        self.activity_data_manager = activity_data_manager
        self.requirements_path = Path(requirements_path)
        self.available_honors_path = Path(available_honors_path)

        self._requirements: Dict[str, Dict[str, Any]] = {}
        self._available_honors: List[str] = []

        self.reload()

    # -------------------------
    # Public API
    # -------------------------
    def reload(self) -> None:
        self._available_honors = self._load_available_honors()
        self._requirements = self._load_requirements()
        self._validate_honor_definitions()

    def get_available_honors(self) -> List[str]:
        return list(self._available_honors)

    def get_requirement(self, honor_uuid: str) -> Optional[Dict[str, Any]]:
        return self._requirements.get(honor_uuid)

    def get_duration_hours(self, honor_uuid: str) -> Optional[int]:
        data = self.get_requirement(honor_uuid)
        if not data:
            return None
        return int(data["duration_hours"])

    async def evaluate(self, guild: discord.Guild, member: discord.Member, honor_uuid: str) -> RequirementEvaluateResult:
        req = self.get_requirement(honor_uuid)
        if not req:
            return RequirementEvaluateResult(ok=False, reasons=["该限时荣誉未配置领取条件"], gaps=[])

        reasons: List[str] = []
        gaps: List[str] = []
        debug_meta: Dict[str, Any] = {}

        honor_name_cache: Dict[str, str] = {}
        role_name_cache: Dict[int, str] = {}

        def get_honor_label(target_honor_uuid: str) -> str:
            cached = honor_name_cache.get(target_honor_uuid)
            if cached is not None:
                return cached

            definition = self.honor_data_manager.get_honor_definition_by_uuid(target_honor_uuid)
            if definition and definition.name:
                label = definition.name
            else:
                label = f"未知荣誉(ID:{target_honor_uuid})"

            honor_name_cache[target_honor_uuid] = label
            return label

        def get_role_label(role_id: int) -> str:
            cached = role_name_cache.get(role_id)
            if cached is not None:
                return cached

            role = guild.get_role(role_id)
            if role:
                label = f"@{role.name}"
            else:
                label = f"未知身份组(ID:{role_id})"

            role_name_cache[role_id] = label
            return label

        # honor 持有信息
        user_honors = self.honor_data_manager.get_user_honors(member.id)
        owned_honor_ids = {h.honor_uuid for h in user_honors}
        debug_meta["owned_honor_ids"] = list(owned_honor_ids)

        # role 持有信息
        member_role_ids = {r.id for r in member.roles}
        debug_meta["member_role_ids"] = list(member_role_ids)

        # 1) 荣誉 all
        honor_all = req["prerequisite_honor_all"]
        missing_honor_all = [x for x in honor_all if x not in owned_honor_ids]
        if missing_honor_all:
            missing_honor_labels = [get_honor_label(x) for x in missing_honor_all]
            reasons.append("未满足全部前置荣誉条件")
            gaps.append(f"缺少前置荣誉: {', '.join(missing_honor_labels)}")

        # 2) 荣誉 any
        honor_any = req["prerequisite_honor_any"]
        if honor_any and not any(x in owned_honor_ids for x in honor_any):
            honor_any_labels = [get_honor_label(x) for x in honor_any]
            reasons.append("未满足任一前置荣誉条件")
            gaps.append(f"至少需要拥有以下荣誉之一: {', '.join(honor_any_labels)}")

        # 3) 角色 all
        role_all = req["prerequisite_role_all"]
        missing_role_all = [rid for rid in role_all if rid not in member_role_ids]
        if missing_role_all:
            missing_role_all_labels = [get_role_label(rid) for rid in missing_role_all]
            reasons.append("未满足全部前置身份组条件")
            gaps.append(f"缺少身份组: {', '.join(missing_role_all_labels)}")

        # 4) 角色 any
        role_any = req["prerequisite_role_any"]
        if role_any and not any(rid in member_role_ids for rid in role_any):
            role_any_labels = [get_role_label(rid) for rid in role_any]
            reasons.append("未满足任一前置身份组条件")
            gaps.append(f"至少需要拥有以下身份组之一: {', '.join(role_any_labels)}")

        # 5) 角色 none
        role_none = req["prerequisite_role_none"]
        hit_role_none = [rid for rid in role_none if rid in member_role_ids]
        if hit_role_none:
            hit_role_none_labels = [get_role_label(rid) for rid in hit_role_none]
            reasons.append("存在互斥身份组，暂不可领取")
            gaps.append(f"请先移除身份组: {', '.join(hit_role_none_labels)}")

        # 6) 发言条件
        channel_messages = req["channel_messages"]
        if channel_messages:
            # 按 lookback_days 分组查询，避免同一轮评估内重复查询 Redis
            lookback_days_set = {int(item["lookback_days"]) for item in channel_messages}
            channel_counts_by_days: Dict[int, Dict[str, int]] = {}
            parent_thread_counts_by_days: Dict[int, Dict[int, int]] = {}

            for lookback_days in sorted(lookback_days_set):
                summary = await self.activity_data_manager.get_user_activity_summary(guild.id, member.id, lookback_days)
                channel_counts = self._extract_channel_counts(summary)
                parent_thread_counts = self._build_parent_text_thread_count_map(guild, channel_counts)

                channel_counts_by_days[lookback_days] = channel_counts
                parent_thread_counts_by_days[lookback_days] = parent_thread_counts

            debug_meta["channel_counts_by_days"] = {
                str(days): counts for days, counts in channel_counts_by_days.items()
            }
            debug_meta["parent_thread_counts_by_days"] = {
                str(days): counts for days, counts in parent_thread_counts_by_days.items()
            }

            for item in channel_messages:
                channel_id = item["channel_id"]
                lookback_days = item["lookback_days"]
                require_count = item["value"]

                channel_counts = channel_counts_by_days.get(lookback_days, {})
                parent_thread_counts = parent_thread_counts_by_days.get(lookback_days, {})

                channel_obj = self._get_channel_or_thread(guild, channel_id)
                channel_label = f"<#{channel_id}>"
                if channel_obj is None:
                    logger.warning("timed_honor: guild=%s channel_id=%s 未找到频道，按0处理", guild.id, channel_id)
                    got = 0
                else:
                    if isinstance(channel_obj, discord.ForumChannel) or not isinstance(channel_obj, discord.TextChannel):
                        logger.warning("timed_honor: guild=%s channel_id=%s 不是可支持的文字频道配置", guild.id, channel_id)
                        reasons.append(f"频道 {channel_label} 配置无效（仅支持文字频道）")
                        gaps.append(f"频道 {channel_label} 的发言条件无法计算")
                        continue

                    # 规则：子区计入（这里按文本频道下的 Thread 聚合）
                    got = int(channel_counts.get(str(channel_id), 0)) + int(parent_thread_counts.get(channel_id, 0))

                    if str(channel_id) not in channel_counts and parent_thread_counts.get(channel_id, 0) == 0:
                        logger.warning(
                            "timed_honor: guild=%s user=%s channel_id=%s lookback_days=%s 活跃度缺失，按0处理",
                            guild.id,
                            member.id,
                            channel_id,
                            lookback_days,
                        )

                if got < require_count:
                    diff = require_count - got
                    reasons.append(f"频道 {channel_label} 发言数不足（最近 {lookback_days} 天）")
                    gaps.append(f"频道 {channel_label} 最近 {lookback_days} 天还差 {diff} 条消息（当前 {got}/{require_count}）")

        return RequirementEvaluateResult(ok=(len(reasons) == 0), reasons=reasons, gaps=gaps, debug_meta=debug_meta)

    @staticmethod
    def build_failure_text(result: RequirementEvaluateResult) -> str:
        lines: List[str] = ["❌ 领取条件未满足："]
        if result.reasons:
            lines.append("\n【未通过项】")
            lines.extend([f"- {x}" for x in result.reasons])
        if result.gaps:
            lines.append("\n【还差】")
            lines.extend([f"- {x}" for x in result.gaps])
        return "\n".join(lines)

    # -------------------------
    # Internal: load / validate
    # -------------------------
    def _load_available_honors(self) -> List[str]:
        if not self.available_honors_path.exists():
            logger.warning("timed_honor: available_honors.json 不存在，按空列表处理")
            return []

        with self.available_honors_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("timed_honor/available_honors.json 必须是数组")

        result: List[str] = []
        for i, item in enumerate(data):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"available_honors[{i}] 必须是非空字符串")
            result.append(item.strip())
        return result

    def _load_requirements(self) -> Dict[str, Dict[str, Any]]:
        if not self.requirements_path.exists():
            logger.warning("timed_honor: requirements.json 不存在，按空配置处理")
            return {}

        with self.requirements_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        if not isinstance(raw, dict):
            raise ValueError("timed_honor/requirements.json 顶层必须是对象")

        validated: Dict[str, Dict[str, Any]] = {}
        for honor_uuid, cfg in raw.items():
            validated[honor_uuid] = self._validate_one_requirement(honor_uuid, cfg)

        for honor_uuid in self._available_honors:
            if honor_uuid not in validated:
                logger.warning("timed_honor: available_honors 中的 %s 未在 requirements.json 配置", honor_uuid)

        return validated

    def _validate_one_requirement(self, honor_uuid: str, cfg: Any) -> Dict[str, Any]:
        if not isinstance(cfg, dict):
            raise ValueError(f"requirements[{honor_uuid}] 必须是对象")

        duration_hours = cfg.get("duration_hours")
        if not isinstance(duration_hours, int) or duration_hours < 1:
            raise ValueError(f"requirements[{honor_uuid}].duration_hours 必须是 >=1 的整数")

        honor_all = self._ensure_str_list(cfg.get("prerequisite_honor_all", []), f"{honor_uuid}.prerequisite_honor_all")
        honor_any = self._ensure_str_list(cfg.get("prerequisite_honor_any", []), f"{honor_uuid}.prerequisite_honor_any")
        role_all = self._ensure_int_list(cfg.get("prerequisite_role_all", []), f"{honor_uuid}.prerequisite_role_all")
        role_any = self._ensure_int_list(cfg.get("prerequisite_role_any", []), f"{honor_uuid}.prerequisite_role_any")
        role_none = self._ensure_int_list(cfg.get("prerequisite_role_none", []), f"{honor_uuid}.prerequisite_role_none")
        channel_messages = self._ensure_channel_messages(cfg.get("channel_messages", []), honor_uuid)

        has_honor_cond = bool(honor_all or honor_any)
        has_role_cond = bool(role_all or role_any or role_none)
        has_message_cond = bool(channel_messages)
        if not (has_honor_cond or has_role_cond or has_message_cond):
            raise ValueError(f"requirements[{honor_uuid}] 至少需要一类条件非空")

        return {
            "duration_hours": duration_hours,
            "prerequisite_honor_all": honor_all,
            "prerequisite_honor_any": honor_any,
            "prerequisite_role_all": role_all,
            "prerequisite_role_any": role_any,
            "prerequisite_role_none": role_none,
            "channel_messages": channel_messages,
        }

    def _validate_honor_definitions(self) -> None:
        """交叉校验 available_honors / requirements 中的 honor_uuid 是否存在于荣誉定义。"""
        all_uuids = set(self._available_honors) | set(self._requirements.keys())
        if not all_uuids:
            return

        missing: set[str] = set()
        for honor_uuid in all_uuids:
            definition = self.honor_data_manager.get_honor_definition_by_uuid(honor_uuid)
            if definition is None:
                missing.add(honor_uuid)

        for honor_uuid in self._available_honors:
            if honor_uuid in missing:
                logger.warning("timed_honor: available_honors 中的 %s 不存在于 honor_definitions", honor_uuid)

        for honor_uuid in self._requirements.keys():
            if honor_uuid in missing:
                logger.warning("timed_honor: requirements 中的 %s 不存在于 honor_definitions", honor_uuid)

    @staticmethod
    def _ensure_str_list(value: Any, field_name: str) -> List[str]:
        if not isinstance(value, list):
            raise ValueError(f"{field_name} 必须是数组")
        result: List[str] = []
        for i, x in enumerate(value):
            if not isinstance(x, str) or not x.strip():
                raise ValueError(f"{field_name}[{i}] 必须是非空字符串")
            result.append(x.strip())
        return result

    @staticmethod
    def _ensure_int_list(value: Any, field_name: str) -> List[int]:
        if not isinstance(value, list):
            raise ValueError(f"{field_name} 必须是数组")
        result: List[int] = []
        for i, x in enumerate(value):
            if not isinstance(x, int):
                raise ValueError(f"{field_name}[{i}] 必须是整数")
            result.append(x)
        return result

    @staticmethod
    def _ensure_channel_messages(value: Any, honor_uuid: str) -> List[Dict[str, int]]:
        if not isinstance(value, list):
            raise ValueError(f"{honor_uuid}.channel_messages 必须是数组")

        result: List[Dict[str, int]] = []
        for i, item in enumerate(value):
            if not isinstance(item, dict):
                raise ValueError(f"{honor_uuid}.channel_messages[{i}] 必须是对象")
            channel_id = item.get("channel_id")
            lookback_days = item.get("lookback_days", 30)
            msg_value = item.get("value")

            if not isinstance(channel_id, int):
                raise ValueError(f"{honor_uuid}.channel_messages[{i}].channel_id 必须是整数")
            if not isinstance(lookback_days, int) or lookback_days < 1:
                raise ValueError(f"{honor_uuid}.channel_messages[{i}].lookback_days 必须是 >=1 的整数")
            if not isinstance(msg_value, int) or msg_value < 1:
                raise ValueError(f"{honor_uuid}.channel_messages[{i}].value 必须是 >=1 的整数")

            result.append({"channel_id": channel_id, "lookback_days": lookback_days, "value": msg_value})
        return result

    @staticmethod
    def _get_channel_or_thread(guild: discord.Guild, channel_id: int):
        # discord.py 2.x 提供 get_channel_or_thread；向下兼容时回退 get_channel
        getter = getattr(guild, "get_channel_or_thread", None)
        if callable(getter):
            return getter(channel_id)
        return guild.get_channel(channel_id)

    def _build_parent_text_thread_count_map(self, guild: discord.Guild, channel_counts: Dict[str, int]) -> Dict[int, int]:
        """
        聚合每个文本频道下 Thread 的发言量，满足“子区计入”规则。

        返回：
        - key: 文本频道 ID
        - value: 该文本频道下所有 thread 的消息总数
        """
        parent_thread_counts: Dict[int, int] = {}
        for channel_id_str, count in channel_counts.items():
            if not str(channel_id_str).isdigit():
                continue

            channel_id = int(channel_id_str)
            channel_obj = self._get_channel_or_thread(guild, channel_id)
            if not isinstance(channel_obj, discord.Thread):
                continue

            parent_id = channel_obj.parent_id
            if not parent_id:
                continue

            parent_obj = self._get_channel_or_thread(guild, parent_id)
            if isinstance(parent_obj, discord.TextChannel):
                parent_thread_counts[parent_id] = parent_thread_counts.get(parent_id, 0) + int(count)

        return parent_thread_counts

    @staticmethod
    def _extract_channel_counts(summary: Any) -> Dict[str, int]:
        if summary is None:
            return {}

        if isinstance(summary, dict):
            if "channel_message_counts" in summary and isinstance(summary["channel_message_counts"], dict):
                return {str(k): int(v) for k, v in summary["channel_message_counts"].items()}
            if "channels" in summary and isinstance(summary["channels"], dict):
                return {str(k): int(v) for k, v in summary["channels"].items()}

            direct = {}
            for k, v in summary.items():
                if str(k).isdigit() and isinstance(v, int):
                    direct[str(k)] = v
            if direct:
                return direct

        if isinstance(summary, list):
            # 兼容 activity_tracker.DataManager.get_user_activity_summary 的返回：list[tuple[channel_id, count]]
            parsed: Dict[str, int] = {}
            for item in summary:
                if isinstance(item, (tuple, list)) and len(item) == 2:
                    channel_id, count = item[0], item[1]
                    if isinstance(channel_id, int) and isinstance(count, int):
                        parsed[str(channel_id)] = count
            return parsed
        return {}
