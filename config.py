import os
import typing

from dotenv import load_dotenv

from config_data import GUILD_CONFIGS, FASHION_CONFIG, ROLE_SYNC_CONFIG

load_dotenv()
# ===================================================================
# 核心配置
# ===================================================================

# 你的机器人 Token
# 现在优先从环境变量 'DISCORD_BOT_TOKEN' 获取，如果环境变量不存在，则使用空字符串）
TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")

# 代理设置 (如果不需要，设为 None)
# 优先从环境变量 'DISCORD_BOT_PROXY' 获取，如果环境变量不存在，则使用 None
PROXY = os.getenv("DISCORD_BOT_PROXY", None)

FORCE_REFRESH_COMMAND = False
# ===================================================================
# 模块 (Cogs) 配置
# ===================================================================

# 在这里控制加载哪些模块
COGS = {
    "core": {
        "enabled": True,
    },
    "backup": {
        "enabled": True,
    },
    "self_service": {
        "enabled": True,
    },
    "fashion": {
        "enabled": True,
    },
    "model_fan_roles": {
        "enabled": True,
    },
    "role_jukebox": {
        "enabled": True,
    },
    "timed_role": {
        "enabled": False,
    },
    "role_sync": {
        "enabled": True,
    },
    "role_viewer":{
        "enabled": True,
    },
    "role_application": {
        "enabled": True,
    },
    "track_activity": {
        "enabled": True,
    },
    "honor_system": {
        "enabled": True,
    },
    "timed_honor": {
        "enabled": True,
    },
    "heartbeat_information": {
        "enabled": True,
    },
}

# ===================================================================
# 其他配置
# ===================================================================

# 从GUILD_CONFIGS中提取所有服务器ID，用于命令同步
GUILD_IDS = set(list(GUILD_CONFIGS.keys()) + list(FASHION_CONFIG.keys()) + list(ROLE_SYNC_CONFIG.keys()))

# 机器人状态
STATUS_TEXT = "用户的身份组发放请求"  # 显示在机器人状态上的文字
# 状态类型: 'playing', 'watching', 'listening'
STATUS_TYPE = 'watching'

# CoreCog的CommandGroup
COMMAND_GROUP_NAME = "李曦曦"

# ===================================================================
# Timed Honor 配置
# ===================================================================
TIMED_HONOR_ADMIN_USER_IDS: typing.Set[int] = {
    942388408800669707,
}

# 用户至少拥有以下身份组之一才允许进入领取流程（可为空表示不限制）
TIMED_HONOR_ELIGIBLE_ROLE_IDS: typing.Set[int] = set()

# 允许管理员发送“升级面板”的频道（None 或空集合表示不限制）
TIMED_HONOR_PANEL_CHANNEL_IDS: typing.Optional[typing.Set[int]] = None

# 过期检查任务间隔（分钟）
TIMED_HONOR_EXPIRE_CHECK_INTERVAL_MINUTES: int = 5

# ===================================================================
# 新增：权限控制
# ===================================================================
# 定义一组被认为是“危险”或“敏感”的权限。
# 机器人将阻止用户通过自助服务获取包含这些权限的身份组。
# 'administrator' 权限总是被视为危险，无论是否在此列表中。
# 这些是 discord.Permissions 对象的属性名 (字符串形式)。
DANGEROUS_PERMISSIONS = {
    "manage_channels",  # 管理频道
    "manage_guild",  # 管理服务器
    "manage_roles",  # 管理身份组 (创建/编辑/删除低于此身份组的身份组)
    "manage_webhooks",  # 管理 Webhook
    "manage_emojis_and_stickers",  # 管理表情符号和贴纸
    "manage_events",  # 管理活动
    "kick_members",  # 踢出成员
    "ban_members",  # 封禁成员
    "moderate_members",  # 对成员进行定罪 (例如禁言)
    "mention_everyone",  # @everyone, @here 和所有身份组
    "mute_members",  # 使成员在语音频道中静音
    "deafen_members",  # 使成员在语音频道中闭麦
    "move_members",  # 移动语音频道中的成员
    # "manage_messages",      # 管理消息 (删除他人消息、置顶)
    # "manage_nicknames",     # 管理他人昵称
    # "view_audit_log",     # 查看审计日志 (通常被认为是安全的，除非特定场景)
    # "change_nickname",    # 更改自己昵称 (通常安全)
}

# 是否要测试幻化的失效
CHECK_FASHION_ROLE_VALIDITY = False

# ===================================================================
# 新增：身份组管理器的功能性配置
# ===================================================================
ROLE_MANAGER_CONFIG = {
    # 每日限时身份组可用的总时长的默认值（单位：小时）
    "daily_limit_hours": 2,

    # 每日重置时间的时区（UTC+8，即北京/上海/台北时间）
    "reset_hour_utc8": 15,  # 16点代表下午4点

    # 用户私有管理面板的超时时间（单位：分钟）
    "private_panel_timeout_minutes": 3,
}

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# --- 新增活动追踪模块配置 ---
# 你可以把这个字典放在 config.py 或 config_data.py
ACTIVITY_TRACKER_CONFIG = {
    "guild_configs": {
        # ---类脑---
        1134557553011998840: {  # 你的服务器ID
            "report_channel_id": 1313410500876566578,  # 替换为 Bot 应该输出回填报告的频道ID
            "target_role_id": 1383835973384802396,  # 社区助力者角色ID
            "message_threshold": 200,  # 消息数量阈值
            "days_window": 7,  # 时间窗口（天）
            "ignored_channels": [  # 忽略这些频道的消息
                1134565363506483352,  # 欢迎频道
                1379264757189705748, 1381148770351452170, 1235867354060034068, 1235987938353877053,  # 四个档案馆
                1380109002515546122,  # 化粪池
                # ... 其他频道ID ...
            ],
            "ignored_categories": [  # 忽略这些【频道类别】
                1388149585767305367,  # 工作区
                1387275888068137060,  # ticket区
                1383826169182556280,  # 老区归档
                1290305190448336916,  # 创作者议会归档
            ],
            "data_retention_days": 30  # Redis中数据保留90天
        },
        # 你可以为其他服务器添加配置
    },
    # "startup_backfill": {
    #     "enabled": False,  # 设置为 True 以启用启动时自动回填
    #     "duration_minutes": 2,  # 回填过去 10 分钟的消息
    #
    #     "guild_id": 1134557553011998840  # 替换为 Bot 应该在启动时回填哪个服务器的ID
    # }
}

# --- 备份配置 ---
# 备份文件和状态更新将发送到这个频道
BACKUP_CHANNEL_ID = 1313410500876566578  # 替换为你的备份通知频道ID
# 备份功能将在这个服务器上运行
BACKUP_GUILD_ID = 1134557553011998840  # 替换为你的服务器ID

# 是否启用身份组自动备份功能
ENABLE_ROLE_BACKUPS = True
# --- 身份组自动备份周期 (小时) ---
# 每隔多少小时进行一次“轻量”备份（不刷新成员缓存）
LIGHT_BACKUP_INTERVAL_HOURS = 1
# 每隔多少小时进行一次“重量”备份（刷新成员缓存）
FULL_BACKUP_INTERVAL_HOURS = 6

# --- 权限配置 ---
# 在这里硬编码拥有权限的用户和角色ID

# 超级管理员：拥有所有权限，通常是机器人所有者或最高决策者。
# 可以执行如“删除数据”等最高风险操作。
SUPER_ADMIN_USER_IDS: typing.Set[int] = {
    942388408800669707,  # 我
}

# 管理员：拥有大部分管理权限，但可能无法执行最危险的操作。
# 例如，可以发送面板、刷新缓存、获取数据备份，但不能删除数据。
# 注意：这里包含角色ID和特定的用户ID。
ADMIN_ROLE_IDS: typing.Set[int] = {
    1396831061643755520,  # 赛事委员会
}

ADMIN_USER_IDS: typing.Set[int] = {
    942388408800669707,  # 我
    # 如果某个管理员没有特定角色，也可以在这里单独添加他们的用户 ID
}

# ===================================================================
#                      身份组点歌机 (Jukebox) 配置
# ===================================================================
# 这是一个按服务器ID组织的配置字典。
# 只有在这里配置了的服务器才会启用点歌机功能。
# -------------------------------------------------------------------
JUKEBOX_GUILD_CONFIGS = {
    # --- 示例服务器 1 ---
    1134557553011998840: {
        "enabled": True,
        # 在服务器中预先创建好的、供机器人修改的身份组ID
        "general_queue_role_ids": [
            1416384494143012957,
            1416384629782478908,
            1416384635348455444,
        ],  # 普通队列
        "vip_queue_role_ids": [
            1416384667497664553,
            1416384784988373095,
        ],  # VIP专属队列

        # 拥有这些身份组之一的成员被视为VIP
        "vip_user_role_ids": [
            1398946010947915776,  # 秘书组
            # 1400842195749044428,  # 蛙人
            # 1354043091757305911,  # 答疑组
            # 1389641433153142804,  # BOT 组
            1337450755791261766,  # 管理组
            1419302053674745927,  # 褪色者
        ],

        "lock_duration_hours": 1,  # 每次点歌的锁定时长（小时）
        "max_general_presets": 20,  # 管理员可设置的通用预设上限
        "max_vip_presets_per_user": 2,  # VIP用户可设置的个人预设上限

        # 用于存储预设图标的频道ID
        "icon_storage_channel_id": 1313410500876566578,
    }
}
