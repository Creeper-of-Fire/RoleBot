"""DEPRECATED 桩文件。

2026-08 model_fan_roles 迁 toml 后，旧的硬编码 ``MODEL_ROLES_CONFIG`` 字典已经删除。
``ModelRoleConfig`` pydantic class 也已迁移到：

    role_system.model_fan_roles.model_fan_roles_config_models

请更新 import 路径。任何继续从本模块 import ``MODEL_ROLES_CONFIG`` 或
``ModelRoleConfig`` 的代码都会触发 ``ImportError``——这是有意为之，避免
"真相撕裂"（旧 dict 和新 toml 双轨运行，bot 行为不确定）。

如果你看到这条消息，说明你正在触碰一个已经迁完的模块。
"""

__all__: list[str] = []  # 故意空——任何 import 都会失败
