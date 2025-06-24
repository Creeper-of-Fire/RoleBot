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
