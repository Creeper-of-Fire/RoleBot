# main.py
import asyncio
import logging
from typing import Dict, List, Type

import discord
from discord.ext import commands

import config
# 导入我们的配置和模块
import config_data
from activity_tracker.cog import TrackActivityCog
from core.cog import CoreCog
from core.embed_link.embed_manager import EmbedLinkManager
from fashion.cog import FashionCog
from honor_system.cog import HonorCog
from information.cog import HeartbeatInformationCog
from role_application.cog import RoleApplicationCog
from role_sync.cog import RoleSyncCog
from self_service.cog import SelfServiceCog
from timed_role.cog import TimedRolesCog

# ===================================================================
# 日志设置
# ===================================================================
# 配置一个基础的日志记录器，将信息输出到控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 输出到控制台
    ]
)
# 获取我们自己的日志记录器实例，方便后续使用
logger = logging.getLogger('role_bot')


# ===================================================================
# Bot 主类定义
# ===================================================================
class RoleBot(commands.Bot):
    """机器人的主类，继承自 commands.Bot"""

    def __init__(self, **kwargs):
        # 设置机器人需要监听的意图 (Intents)
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix='!', intents=intents, **kwargs)
        # 将 logger 实例正确地附加到 bot 对象上
        self.logger: logging.Logger = logger

    async def on_ready(self):
        """当机器人成功登录并准备就绪时调用"""
        self.logger.info(f"以 {self.user} 身份登录成功!")

        # 根据 config_data.py 中的配置设置机器人的在线状态
        status_type_str = config.STATUS_TYPE.lower()
        activity = None
        if status_type_str == 'playing':
            activity = discord.Game(name=config.STATUS_TEXT)
        elif status_type_str == 'watching':
            activity = discord.Activity(type=discord.ActivityType.watching, name=config.STATUS_TEXT)
        elif status_type_str == 'listening':
            activity = discord.Activity(type=discord.ActivityType.listening, name=config.STATUS_TEXT)

        if activity:
            await self.change_presence(activity=activity)
            self.logger.info(f"机器人状态已设置为: {status_type_str} {config.STATUS_TEXT}")

    async def setup_hook(self):
        """在机器人登录前执行的异步设置。"""
        await EmbedLinkManager.initialize_all_managers()
        await cog_manager.load_all_enabled()
        self.logger.info("开始同步应用命令...")

        if config.FORCE_REFRESH_COMMAND:
            self.logger.warning("<<<<< 正在执行一次性命令缓存强制刷新！>>>>>")

            # 遍历配置中的所有服务器ID
            for guild_id in config.GUILD_IDS:
                try:
                    guild = discord.Object(id=guild_id)

                    # 1. 清空这个服务器上的所有旧命令
                    self.tree.clear_commands(guild=guild)
                    self.logger.info(f"已向 Discord 发送清空服务器 {guild_id} 命令的请求。")

                    # 2. 等待清空指令生效（可选但推荐）
                    await self.tree.sync(guild=guild)
                    self.logger.info(f"服务器 {guild_id} 的旧命令已确认清空。")

                    # 3. 将全局命令（即我们代码中定义的）复制并同步到这个服务器
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)

                    self.logger.info(f"已将 {len(synced)} 个新命令重新同步到服务器 {guild_id}")

                except discord.HTTPException as e:
                    self.logger.error(f"强制刷新服务器 {guild_id} 时失败: {e}")
                except Exception as ex:
                    self.logger.error(f"在处理服务器 {guild_id} 时发生未知错误: {ex}")

            self.logger.warning("<<<<< 命令缓存强制刷新操作完成！>>>>>")
            self.logger.warning("<<<<< 请记得在下次启动前注释掉 setup_hook 中的刷新代码！>>>>>")

        else:
            for guild_id in config.GUILD_IDS:
                guild = discord.Object(id=guild_id)
                self.tree.copy_global_to(guild=guild)
                try:
                    synced = await self.tree.sync(guild=guild)
                    self.logger.info(f"已同步 {len(synced)} 个命令到服务器 {guild_id}")
                except discord.HTTPException as e:
                    self.logger.error(f"同步命令到服务器 {guild_id} 失败: {e}")


# ===================================================================
# Cog 管理器
# ===================================================================
class CogManager:
    """Cog管理器，负责根据配置动态加载、卸载和重载模块"""

    def __init__(self, bot: commands.Bot, config_module):
        self.bot = bot
        # 修复：直接存储 config 模块本身，而不是尝试将其当作字典
        self.config = config_module
        # 定义一个 cog 名称到其类定义的映射，方便动态加载
        self.cog_map: Dict[str, Type[commands.Cog] | List[Type[commands.Cog]]] = {
            "core": CoreCog,
            "self_service": SelfServiceCog,
            "fashion": FashionCog,
            "timed_role": TimedRolesCog,
            "role_sync": RoleSyncCog,
            "role_application": RoleApplicationCog,
            "track_activity": TrackActivityCog,
            "honor_system": HonorCog,
            "heartbeat_information": HeartbeatInformationCog,
        }

    async def load_all_enabled(self):
        """加载所有在 config_data.py 中启用的 Cog"""
        # 修复：现在可以正确地通过 self.config.COGS 访问配置
        for cog_name, cog_config in config.COGS.items():
            if cog_config.get('enabled', False):
                if cog_name in self.cog_map:
                    await self.load_module(cog_name)
                else:
                    self.bot.logger.warning(f"模块 {cog_name} 在配置中启用但未在 cog_map 中注册")

    async def load_module(self, module_name: str):
        """
        加载一个功能模块，该模块可能包含一个或多个Cog。
        """
        cog_or_cogs = self.cog_map.get(module_name)
        if not cog_or_cogs:
            return

        cogs_to_load = cog_or_cogs if isinstance(cog_or_cogs, list) else [cog_or_cogs]

        self.bot.logger.info(f"开始加载模块 '{module_name}'...")
        for cog_class in cogs_to_load:
            try:
                cog_instance_name = cog_class.__name__
                if self.bot.get_cog(cog_instance_name) is not None:
                    self.bot.logger.warning(f"Cog '{cog_instance_name}' 已加载，跳过。")
                    continue

                cog_instance = cog_class(self.bot)
                await self.bot.add_cog(cog_instance)

                self.bot.logger.info(f"  -> 已加载子Cog: {cog_instance_name}")

            except Exception as e:
                self.bot.logger.error(f"加载子Cog {cog_class.__name__} (属于模块 {module_name}) 失败: {e}", exc_info=True)


# ===================================================================
# 主程序入口
# ===================================================================
async def main():
    """主异步函数，负责初始化和启动机器人"""
    # 根据配置决定是否使用代理
    if config.PROXY:
        logger.info(f"检测到代理配置，将通过 {config.PROXY} 初始化机器人")
        bot = RoleBot(proxy=config.PROXY)
    else:
        logger.info("未配置代理，直接初始化机器人")
        bot = RoleBot()
    global cog_manager
    cog_manager = CogManager(bot, config_data)
    try:
        await bot.start(config.TOKEN)
    except discord.LoginFailure:
        logger.error("机器人 Token 无效，请检查环境中的 TOKEN 设置。")
    except Exception as e:
        logger.critical(f"机器人运行时发生致命错误: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("机器人被手动停止。")
