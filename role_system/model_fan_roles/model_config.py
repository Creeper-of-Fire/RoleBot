from typing import Dict, List, Optional

from pydantic import BaseModel


class ModelRoleConfig(BaseModel):
    name: str
    display_name: str
    role_id: int
    emoji: Optional[str] = None


# 格式: { 服务器ID: [ ModelRoleConfig(...), ... ] }

MODEL_ROLES_CONFIG: Dict[int, List[ModelRoleConfig]] = {
    1134557553011998840: [
        ModelRoleConfig(name="Gemini", display_name="哈基米", role_id=1444246888512753764),
        ModelRoleConfig(name="Claude", display_name="小克", role_id=1444248769494384666),
        ModelRoleConfig(name="DeepSeek", display_name="小鲸鱼", role_id=1444248821079998554),
        ModelRoleConfig(name="ChatGPT", display_name="ChatGPT", role_id=1444248662292304035),
        ModelRoleConfig(name="Grok", display_name="Grok, is this true?", role_id=1444259773100068864),
        ModelRoleConfig(name="Mimo", display_name="Mimo同学", role_id=1506269290855268532),
        ModelRoleConfig(name="豆包", display_name="豆包豆包，", role_id=1506271728496672828),
        ModelRoleConfig(name="GLM", display_name="GLM", role_id=1506271442054938725),
        ModelRoleConfig(name="Kimi", display_name="月之Kimi", role_id=1506269804863160430),
    ],
}
