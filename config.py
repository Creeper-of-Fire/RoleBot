from config_data import GUILD_CONFIGS

# ===================================================================
# 模块 (Cogs) 配置
# ===================================================================

# 在这里控制加载哪些模块
COGS = {
    "role_manager": {
        "enabled": True,
    },
    # 未来可以添加更多模块
    # "another_cog": {
    #     "enabled": False,
    # }
}

# ===================================================================
# 其他配置
# ===================================================================

# 从GUILD_CONFIGS中提取所有服务器ID，用于命令同步
GUILD_IDS = list(GUILD_CONFIGS.keys())

# 机器人状态
STATUS_TEXT = "发放身份组中"  # 显示在机器人状态上的文字
# 状态类型: 'playing', 'watching', 'listening'
STATUS_TYPE = 'watching'

# ===================================================================
# 新增：权限控制
# ===================================================================
# 定义一组被认为是“危险”或“敏感”的权限。
# 机器人将阻止用户通过自助服务获取包含这些权限的身份组。
# 'administrator' 权限总是被视为危险，无论是否在此列表中。
# 这些是 discord.Permissions 对象的属性名 (字符串形式)。
DANGEROUS_PERMISSIONS = {
    "manage_channels",      # 管理频道
    "manage_guild",         # 管理服务器
    "manage_roles",         # 管理身份组 (创建/编辑/删除低于此身份组的身份组)
    "manage_webhooks",      # 管理 Webhook
    "manage_emojis_and_stickers", # 管理表情符号和贴纸
    "manage_events",        # 管理活动
    "kick_members",         # 踢出成员
    "ban_members",          # 封禁成员
    "moderate_members",     # 对成员进行定罪 (例如禁言)
    "mention_everyone",     # @everyone, @here 和所有身份组
    "mute_members",         # 使成员在语音频道中静音
    "deafen_members",       # 使成员在语音频道中闭麦
    "move_members",         # 移动语音频道中的成员
    # "manage_messages",      # 管理消息 (删除他人消息、置顶)
    # "manage_nicknames",     # 管理他人昵称
    # "view_audit_log",     # 查看审计日志 (通常被认为是安全的，除非特定场景)
    # "change_nickname",    # 更改自己昵称 (通常安全)
}
