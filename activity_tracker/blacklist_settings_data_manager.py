from pydantic import BaseModel

from utility.base_data_manager import AsyncGuildDataManager


DEFAULT_ADVICE_MESSAGE = "请去聊天区随便聊聊，不要在资源区无故唤起或占用活跃线程。"


class BlacklistSettings(BaseModel):
    advice_message: str = DEFAULT_ADVICE_MESSAGE


class BlacklistSettingsDataManager(AsyncGuildDataManager[BlacklistSettings]):
    DATA_FILENAME = "activity_blacklist_settings"
    GUILD_MODEL = BlacklistSettings

    def get_advice_message(self, guild_id: int) -> str:
        settings = self.get_guild(guild_id)
        return settings.advice_message if settings else DEFAULT_ADVICE_MESSAGE

    async def set_advice_message(self, guild_id: int, message: str) -> None:
        settings = self.ensure_guild(guild_id)
        settings.advice_message = message
        await self.save_data()
