# utility/embed_link_manager.py

from __future__ import annotations

import asyncio
import json
import logging
import re
import typing
from typing import Dict, List, Optional, Type

import discord

if typing.TYPE_CHECKING:
    from main import RoleBot

CONFIG_FILE_PATH = "./data/embed_links.json"


class EmbedLinkManager:
    """
    一个高内聚的组合式模块，用于管理来自特定Discord消息的Embed。
    它自我管理配置的加载、更新和持久化。

    设计:
    - 类级别注册表 (_registry) 跟踪所有实例。
    - 类级别配置缓存 (_configs) 从JSON加载，并由实例更新。
    - 实例负责从配置中刷新自己的状态 (embed, url)。
    """
    _logger = logging.getLogger("EmbedLinkManager")

    # --- Class-level state ---
    _registry: Dict[str, EmbedLinkManager] = {}
    configs: Optional[Dict[str, Dict[str, int]]] = None
    _is_initialized: bool = False
    _lock = asyncio.Lock()  # 异步锁，用于保护文件I/O和初始化

    def __init__(self, key: str, bot: RoleBot, default_embed: discord.Embed):
        """
        私有构造函数，请使用 get_or_create() 工厂方法创建实例。
        """
        self.key = key
        self.bot = bot
        self.default_embed = default_embed
        self._embed: Optional[discord.Embed] = None
        self._url: Optional[str] = None

        # 注册实例
        self.__class__._registry[self.key] = self

    # --- Public Properties ---
    @property
    def embed(self) -> discord.Embed:
        """获取当前缓存的Embed。如果缓存为空，则返回默认Embed。"""
        return self._embed.copy() if self._embed else self.default_embed

    @property
    def url(self) -> Optional[str]:
        """获取当前缓存的消息跳转URL。"""
        return self._url

    # --- Public Class Methods (API for other parts of the bot) ---
    @classmethod
    def get_or_create(
            cls: Type[EmbedLinkManager],
            key: str,
            bot: RoleBot,
            default_embed: discord.Embed
    ) -> EmbedLinkManager:
        """
        获取或创建并注册一个EmbedLinkManager实例。
        假定 initialize_all_managers() 已经被调用。
        """
        if cls.configs is None:
            # 这个警告帮助开发者发现他们是否忘记了在 setup_hook 中初始化
            cls._logger.critical("EmbedLinkManager 在未初始化的情况下被调用！请在 Bot.setup_hook 中调用 initialize_all_managers()。")

        if key in cls._registry:
            return cls._registry[key]

        return cls(key=key, bot=bot, default_embed=default_embed)

    @classmethod
    def get_manager(cls: Type[EmbedLinkManager], key: str) -> Optional[EmbedLinkManager]:
        """根据key获取一个已注册的管理器。"""
        return cls._registry.get(key)

    @classmethod
    def get_registered_keys(cls: Type[EmbedLinkManager]) -> List[str]:
        """获取所有已注册管理器的键列表，用于自动补全。"""
        return list(cls._registry.keys())

    @classmethod
    def get_all_managers(cls: Type[EmbedLinkManager]) -> List[EmbedLinkManager]:
        """获取所有已注册管理器的实例列表。"""
        return list(cls._registry.values())

    # --- Public Instance Methods (for commands and tasks) ---
    async def set_from_url(self, url: str) -> None:
        """
        通过Discord消息URL来更新和持久化此管理器的配置。

        Args:
            url (str): 指向目标消息的完整URL。

        Raises:
            ValueError: 如果URL格式不正确。
        """
        match = re.search(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)', url)
        if not match:
            raise ValueError("无效的Discord消息URL格式。")

        guild_id, channel_id, message_id = map(int, match.groups())

        new_config = {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "post_id": message_id
        }

        self.__class__.configs[self.key] = new_config
        await self.__class__._save_configs()

        self._logger.info(f"管理器 '{self.key}' 的配置已更新。正在立即刷新...")
        await self.refresh_from_config()  # 立即应用新配置

    async def clear_config(self) -> None:
        """清除此管理器的配置并持久化。"""
        if self.key in self.__class__.configs:
            del self.__class__.configs[self.key]
            await self.__class__._save_configs()
            self._logger.info(f"管理器 '{self.key}' 的配置已清除。")
            await self.refresh_from_config()  # 刷新将使其回退到默认值

    async def refresh_from_config(self) -> None:
        """
        根据当前类中存储的配置来刷新此实例的embed和url。
        此方法应由CoreCog的定时任务调用。
        """
        config = self.__class__.configs.get(self.key)
        if not config:
            # 如果配置不存在，确保状态被重置
            self._logger.error(f"管理器 '{self.key}' 的配置文件不存在")
            if self._embed is not None or self._url is not None:
                self._embed = None
                self._url = None
            return

        try:
            guild = self.bot.get_guild(config["guild_id"]) or await self.bot.fetch_guild(config["guild_id"])
            channel = await guild.fetch_channel(config["channel_id"])
            if not isinstance(channel, (discord.TextChannel, discord.ForumChannel, discord.Thread)):
                self._logger.error(f"[{self.key}] 配置的频道ID {config['channel_id']} 不是有效的文本类型频道。")
                return

            message = await channel.fetch_message(config["post_id"])

            new_embed = None
            if message.embeds:
                new_embed = message.embeds[0]
            elif message.content:
                new_embed = discord.Embed(title=f"{channel.name}", description=message.content, color=discord.Color.blue())

            if new_embed:
                self._embed = new_embed
                self._url = message.jump_url
                # self._logger.info(f"成功为 '{self.key}' 刷新了Embed。") # 在定时任务中会产生过多日志，注释掉
            else:
                self._logger.warning(f"为 '{self.key}' 获取的消息 (ID: {config['post_id']}) 内容为空。")
                self._embed = None
                self._url = None

        except (discord.NotFound, discord.Forbidden, ValueError, AttributeError) as e:
            self._logger.error(f"为 '{self.key}' 刷新Embed时发生错误: {e}")
            self._embed = None
            self._url = None

    # --- Private Class Methods (Internal Logic) ---
    @classmethod
    async def initialize_all_managers(cls: Type[EmbedLinkManager]) -> None:
        """
        (只应运行一次)
        从JSON文件加载所有配置。
        """
        async with cls._lock:
            if cls.configs is not None:  # 防止重复初始化
                return

            try:
                loop = asyncio.get_running_loop()
                content = await loop.run_in_executor(None, cls._read_file_sync)
                if content:
                    cls.configs = json.loads(content)
                    cls._logger.info(f"已成功从 {CONFIG_FILE_PATH} 加载 {len(cls.configs)} 条链接配置。")
                else:
                    cls.configs = {}
            except FileNotFoundError:
                cls._logger.info(f"配置文件 {CONFIG_FILE_PATH} 未找到，将创建新文件。")
                cls.configs = {}
            except json.JSONDecodeError:
                cls._logger.error(f"无法解析 {CONFIG_FILE_PATH}。将使用空配置。")
                cls.configs = {}

    @classmethod
    def _read_file_sync(cls) -> str:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            return f.read()

    @classmethod
    async def _save_configs(cls: Type[EmbedLinkManager]) -> None:
        """将当前配置异步写入JSON文件。"""
        async with cls._lock:
            loop = asyncio.get_running_loop()
            # 使用 to_thread 避免阻塞事件循环
            await loop.run_in_executor(
                None,
                lambda: json.dump(cls.configs, open(CONFIG_FILE_PATH, 'w', encoding='utf-8'), indent=4)
            )
