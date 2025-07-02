# src/role_manager/utility/auth.py
from __future__ import annotations

import discord

import config


def is_role_dangerous(role: discord.Role) -> bool:
    """检查一个身份组是否包含危险权限。"""
    if role.permissions.administrator:
        return True
    for perm_name, has_perm in role.permissions:
        if has_perm and perm_name in config.DANGEROUS_PERMISSIONS:
            return True
    return False