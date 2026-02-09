from typing import Dict, List, TypedDict, Optional


class ModelRoleConfig(TypedDict):
    name: str
    role_id: int
    emoji: Optional[str]  # 标记为可选


# 格式: { 服务器ID: [ { "name": 显示名称, "role_id": 身份组ID, "emoji": 图标 }, ... ] }

MODEL_ROLES_CONFIG: Dict[int, List[ModelRoleConfig]] = {
    1134557553011998840: [  # 示例服务器 ID
        {"name": "哈基米", "role_id": 1444246888512753764, "emoji": None},
        {"name": "小克", "role_id": 1444248769494384666},
        {"name": "小鲸鱼", "role_id": 1444248821079998554},
        {"name": "ChatGPT", "role_id": 1444248662292304035},
        {"name": "Grok, is this true?", "role_id": 1444259773100068864},
        # 可以添加更多...
    ],
    # 其他服务器...
}
