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

def _build_capability_index(bot: "RoleBot") -> dict[str, config.Capability]:
    """建立 ``qualified_name → capability`` 的索引，方便用 synced 结果反查。

    qualified_name 形如 ``李曦曦丨活跃 刷屏黑名单-添加``。
    """
    index: dict[str, config.Capability] = {}
    seen: set[int] = set()

    # 全局
    for cmd in bot.tree.walk_commands():
        if id(cmd) in seen:
            continue
        seen.add(id(cmd))
        cap = getattr(cmd, "_required_capability", None)
        if cap is not None:
            index[cmd.qualified_name] = cap

    # 每个 guild（copy_global_to 复制到这里的）
    for guild_id in config.GUILD_IDS:
        for cmd in bot.tree.walk_commands(guild=discord.Object(id=guild_id)):
            if id(cmd) in seen:
                continue
            seen.add(id(cmd))
            cap = getattr(cmd, "_required_capability", None)
            if cap is not None:
                index[cmd.qualified_name] = cap

    # ContextMenu
    ctx_menus = getattr(bot.tree, "_context_menus", {}) or {}
    for cmd in ctx_menus.values():
        if id(cmd) in seen:
            continue
        seen.add(id(cmd))
        cap = getattr(cmd, "_required_capability", None)
        if cap is not None:
            index[cmd.qualified_name] = cap

    return index


async def sync_permissions_for_guild(
    bot: "RoleBot",
    guild_id: int,
    synced_commands: list,
) -> None:
    """对单个 guild，把 requires_capability 命令的 overrides 写入 Discord 端。

    Args:
        synced_commands: ``await bot.tree.sync(guild=guild)`` 返回的 AppCommand 列表。
            包含 Discord 端实际分配的命令 ID（tree 内的原 Command 对象没有 id）。
    """
    maintainer_ids = list(config.MAINTAINER_USER_IDS)
    cap_index = _build_capability_index(bot)

    if not cap_index:
        logger.info(f"[guild {guild_id}] 没有 requires_capability 标记的命令，跳过")
        return

    # 把 synced_commands 按 (type, name) 索引，方便反查
    # AppCommand 没有 type 字段，但从 _context_menus / walk_commands 能区分
    # 简化：直接遍历 synced_commands，按 name 查 cap_index
    written = 0
    skipped = 0
    for app_cmd in synced_commands:
        cap = cap_index.get(app_cmd.name)
        if cap is None:
            continue

        role_ids = config.CAPABILITIES.get(cap, set())
        if not role_ids and not maintainer_ids:
            skipped += 1
            continue

        try:
            await set_command_permissions(
                bot, guild_id, app_cmd.id,
                allow_roles=role_ids,
                allow_users=maintainer_ids,
            )
            logger.info(
                f"[guild {guild_id}] {app_cmd.name} (id={app_cmd.id}) → {cap!s} → "
                f"roles {sorted(role_ids)} users {sorted(maintainer_ids)}"
            )
            written += 1
        except discord.HTTPException as e:
            logger.error(
                f"[guild {guild_id}] 写入 {app_cmd.name} permissions 失败: {e}"
            )

    logger.info(f"[guild {guild_id}] 完成：写入 {written} 条 overrides，跳过 {skipped} 条")


async def sync_all_command_permissions(bot: "RoleBot") -> None:
    """遍历所有配置的 guild，把 requires_capability 命令的 overrides 写入 Discord 端。

    **必须在 ``bot.tree.sync()`` 拿到 AppCommand 列表之后调用**。
    """
    for guild_id in config.GUILD_IDS:
        try:
            # 重新 sync 一次拿到 AppCommand（含 id）
            synced = await bot.tree.sync(guild=discord.Object(id=guild_id))
            await sync_permissions_for_guild(bot, guild_id, synced)
        except Exception as e:
            logger.error(
                f"sync_all_command_permissions 在 guild {guild_id} 失败: {e}",
                exc_info=True,
            )