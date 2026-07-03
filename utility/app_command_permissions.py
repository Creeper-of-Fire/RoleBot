# utility/app_command_permissions.py
"""通过 Discord HTTP API 写入应用命令的权限覆盖（Application Command Permissions）。

协议层参考：
    GET  /applications/{app}/guilds/{guild}/commands/permissions
    GET  /applications/{app}/guilds/{guild}/commands/{cmd}/permissions
    PUT  /applications/{app}/guilds/{guild}/commands/{cmd}/permissions

本文件封装 PUT，用来把 bot 端 ``requires_capability`` 装饰器标记的 capability
对应的身份组 + 维护者用户，写入 Discord 端，让 Discord 客户端按这些身份组
/ 用户过滤命令可见性。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, Optional

import discord
from discord import app_commands

import config

if TYPE_CHECKING:
    from main import RoleBot


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 底层：写一条命令的 overrides
# ---------------------------------------------------------------------------

async def set_command_permissions(
    bot: "RoleBot",
    guild_id: int,
    command_id: Optional[int],
    *,
    allow_roles: Iterable[int] = (),
    deny_roles: Iterable[int] = (),
    allow_users: Iterable[int] = (),
    deny_users: Iterable[int] = (),
    allow_channels: Iterable[int] = (),
    deny_channels: Iterable[int] = (),
) -> None:
    """通过 PUT 写入单条命令（或 application 级默认）的 overrides。

    Args:
        bot: RoleBot 实例。
        guild_id: 目标服务器 ID。
        command_id: 具体命令 ID。``None`` 表示写入 application_id，
            即对所有没单独 override 的命令生效。
        allow_* / deny_*: 允许/拒绝的身份组、用户、频道 ID 集合。
    """
    state = bot._connection
    application_id = int(state.application_id) if state.application_id else None
    if application_id is None:
        raise RuntimeError("Bot 没有 application_id，无法写入 command permissions")

    perms: list[dict] = []
    for rid in allow_roles:    perms.append({"id": rid, "type": 1, "permission": True})
    for rid in deny_roles:     perms.append({"id": rid, "type": 1, "permission": False})
    for uid in allow_users:    perms.append({"id": uid, "type": 2, "permission": True})
    for uid in deny_users:     perms.append({"id": uid, "type": 2, "permission": False})
    for cid in allow_channels: perms.append({"id": cid, "type": 3, "permission": True})
    for cid in deny_channels:  perms.append({"id": cid, "type": 3, "permission": False})

    target_id = command_id if command_id is not None else application_id
    await state.http.edit_application_command_permissions(
        application_id, guild_id, target_id, {"permissions": perms},
    )


# ---------------------------------------------------------------------------
# 高层：扫描所有 requires_capability 标记的命令，写入 overrides
# ---------------------------------------------------------------------------

def _iter_commands_with_capability(bot: "RoleBot"):
    """遍历 ``bot.tree`` 中挂着 ``_required_capability`` 的所有 Command / ContextMenu。"""
    # walk_commands() 覆盖 Group 下的所有子命令（不含 ContextMenu）
    for cmd in bot.tree.walk_commands():
        cap = getattr(cmd, "_required_capability", None)
        if cap is not None:
            yield cmd, cap

    # ContextMenu 存在 tree._context_menus 里（discord.py 内部 dict）
    ctx_menus = getattr(bot.tree, "_context_menus", {}) or {}
    for cmd in ctx_menus.values():
        cap = getattr(cmd, "_required_capability", None)
        if cap is not None:
            yield cmd, cap


async def _sync_one_guild(bot: "RoleBot", guild_id: int) -> None:
    """对单个 guild 写入所有 requires_capability 命令的 overrides。

    注意：这里**不依赖** ``bot.get_guild(guild_id)``，因为 sync_all_command_permissions
    既可能在 setup_hook（bot 未 ready）阶段跑，也可能在 on_ready 阶段跑。
    guild 对象只用于日志美化——非必需。
    """
    maintainer_ids = list(config.MAINTAINER_USER_IDS)

    # 按 capability 聚合，避免每个命令都单独 PUT
    by_capability: dict[config.Capability, list[app_commands.Command]] = {}
    for cmd, cap in _iter_commands_with_capability(bot):
        by_capability.setdefault(cap, []).append(cmd)

    if not by_capability:
        logger.info(f"[guild {guild_id}] 没有 requires_capability 标记的命令，跳过")
        return

    for cap, cmds in by_capability.items():
        role_ids = config.CAPABILITIES.get(cap, set())
        if not role_ids and not maintainer_ids:
            logger.warning(
                f"Capability {cap!s} 既没配置身份组，也没维护者，跳过"
            )
            continue

        for cmd in cmds:
            cmd_id = getattr(cmd, "id", None)
            if cmd_id is None:
                logger.warning(
                    f"命令 {cmd.qualified_name} 没有 ID（可能未 sync），跳过"
                )
                continue
            try:
                await set_command_permissions(
                    bot, guild_id, cmd_id,
                    allow_roles=role_ids,
                    allow_users=maintainer_ids,
                )
                logger.info(
                    f"[guild {guild_id}] {cmd.qualified_name} → {cap!s} → "
                    f"roles {sorted(role_ids)} users {sorted(maintainer_ids)}"
                )
            except discord.HTTPException as e:
                logger.error(
                    f"[guild {guild_id}] 写入 {cmd.qualified_name} permissions 失败: {e}"
                )


async def sync_all_command_permissions(bot: "RoleBot") -> None:
    """遍历所有配置的 guild，把 requires_capability 命令的 overrides 写入 Discord 端。

    **必须在 ``bot.tree.sync()`` 拿到 command.id 之后调用**。
    """
    for guild_id in config.GUILD_IDS:
        try:
            await _sync_one_guild(bot, guild_id)
        except Exception as e:
            logger.error(
                f"sync_all_command_permissions 在 guild {guild_id} 失败: {e}",
                exc_info=True,
            )