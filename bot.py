# main.py
import asyncio
import logging

import discord
from discord.ext import commands

import config
# 导入我们的配置和模块
import config_data
import env_token
from role_manager.cog import RoleManagerCog

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
        await cog_manager.load_all_enabled()
        self.logger.info("开始同步应用命令...")
        # 遍历配置中的所有服务器ID，并将命令逐个同步过去
        # 这种方式比全局同步快得多，通常是即时生效
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
        self.cog_map = {
            "role_manager": RoleManagerCog,
        }

    async def load_all_enabled(self):
        """加载所有在 config_data.py 中启用的 Cog"""
        # 修复：现在可以正确地通过 self.config.COGS 访问配置
        for cog_name, cog_config in config.COGS.items():
            if cog_config.get('enabled', False):
                if cog_name in self.cog_map:
                    await self.load_cog(cog_name)
                else:
                    self.bot.logger.warning(f"模块 {cog_name} 在配置中启用但未在 cog_map 中注册")

    async def load_cog(self, cog_name: str):
        """加载指定的 Cog"""
        try:
            cog_class = self.cog_map[cog_name]
            # 调用 discord.py 的原生方法添加 Cog 实例
            await self.bot.add_cog(cog_class(self.bot))
            self.bot.logger.info(f"已加载模块: {cog_name}")
        except Exception as e:
            # 如果加载失败，打印详细的错误信息
            self.bot.logger.error(f"加载模块 {cog_name} 失败: {e}", exc_info=True)


# ===================================================================
# 主程序入口
# ===================================================================
async def main():
    """主异步函数，负责初始化和启动机器人"""
    # 根据配置决定是否使用代理
    if config_data.PROXY:
        logger.info(f"检测到代理配置，将通过 {config_data.PROXY} 初始化机器人")
        bot = RoleBot(proxy=config_data.PROXY)
    else:
        logger.info("未配置代理，直接初始化机器人")
        bot = RoleBot()
    global cog_manager
    cog_manager = CogManager(bot, config_data)
    try:
        await bot.start(env_token.TOKEN)
    except discord.LoginFailure:
        logger.error("机器人 Token 无效，请检查 env_token.py 中的 TOKEN 设置。")
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