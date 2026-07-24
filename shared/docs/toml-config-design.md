# TOML 配置体系设计（discord-bots workspace）

> 状态：设计阶段（2026-07-23）
> 适用范围：三个 bot 仓库（role_bot / odysseia_ticket_bot / news_bot）
> 配套文档：`subtree-workflow.md`

---

## 背景

社区快四周年，配置改得越来越频繁（加幻化、加纪念身份组、调阈值）。
老的 `config.py` + `config_data.py` 纯 Python dict 模式越来越难维护：

- 嵌套深，`HONOR_CONFIG` 五层嵌套，改一个字段肉眼成本高
- 零验证：role_id 写错、UUID 拼错要等运行时崩
- 改完必须重启 bot
- 跨字段引用（fashion_map 里 base_role_id 是否真实存在）全靠肉眼

`odysseia_ticket_bot` 在 `complaint/` 子系统已经有一套相对成熟的 toml +
pydantic 配置方案（见 `complaint/config/loader.py` + `models.py`）。考虑把
其中的通用 IO 框架抽到 `_shared/`，给所有 bot 共用。

## 要抽到 `_shared/` 的清单

| 模块 | 形态 | 给谁用 |
|---|---|---|
| `_shared/config/toml_manager.py` | `TomlConfigManager[T]` 泛型类（持 `doc_path`） | ticket_bot + role_bot |
| `_shared/config/toml_merge.py` | `TomlMergeAsTableList()` pydantic 字段注解 + 通用合并算法 | 同上 |
| `_shared/config/toml_command.py` | 三个赤裸 handler（`handle_toml_download` / `handle_toml_upload` / `handle_toml_view_hash`） | 业务 cog 自己挂装饰器 + 调 handler；不提供一键注册 |

不抽的（避免 YAGNI）：

- **hash 工具函数** —— `hashlib` 标准库够用，不在 `_shared/` 再包一层
- **一键 command 注册** —— 业务自己挂装饰器 + 调 handler
- **doc.md 内容生成** —— 业务自己写 doc.md，路径注册到 manager，handler 读到就附加

## 决策（2026-07-23）

### ✅ 第一波范围

- **role_bot 只迁 `HONOR_CONFIG`** 到 toml。理由：嵌套最深、改动最频繁、
  多人协作最痛。其他层（FASHION_CONFIG、GUILD_CONFIGS、ROLE_SYNC_CONFIG
  等）暂不动。
- **ticket_bot 把现有 complaint loader 切到新通用框架**，作为第一个
  验证用户。验证通用 API 对得上既有特化逻辑。

### ❌ 暂时不做

- **redis 不进配置层** —— role_bot 的 redis 只承担活动追踪等热数据。
  任何"配置 stage / 暂存 / apply 状态机"的想法本轮不实施。
- **运行时多人协作的并发安全** —— 暂用 git PR + 内容指纹兜底；不上
  redis、不做 state machine。
- **FASHION_CONFIG 等其他配置层** —— 不动，留 python dict。
- **CAPABILITIES / COGS / DANGEROUS_PERMISSIONS**
  —— 这些是"逻辑依赖"不是"数据"，继续留在 role_bot `config.py`。
  详见下文「范围决策」一节。
- **JUKEBOX_GUILD_CONFIGS** —— 不动，虽然看似涉及逻辑，但是也有数据部分，而且比较不敏感，可以toml化，但是现在不动。

### 📐 范围决策：toml = per-guild

**toml 配置默认都带 `guild_id`**。`TomlConfigManager` 的所有方法
（`load` / `save` / `read_raw` / `validate_and_save` / `content_hash`）
都把 `guild_id` 当成**强制参数**，不支持"全局 toml 文件"模式。

理由：

- role_bot 的多数配置天然按服务器差异（幻化映射、活动阈值、纪念日）
- 多服务器共享的配置不进 toml——它们要么是高危险逻辑依赖
  （`CAPABILITIES` / `DANGEROUS_PERMISSIONS`），要么是后续会演化成
  per-guild 的东西（`ROLE_MANAGER_CONFIG`），要么就是基本上不会变动的全局配置 （`COGS`）。把它们放进
  toml 等于提前定型，违反"延后决策"
- 反向测试：未来想"X 要不要走 toml？" → 答"X 跟 guild 有关系吗？"
  → 没关系的就不走
- 最重要的一点：这样子可以简化API，简化类型系统，简化设计……全部简化。

实施细节：

- API 不接受 optional `guild_id`；任何想写"全局 toml"的尝试用错
  类型在运行期挂掉
- 不实现 `global.toml` / `shared.toml` 等聚合文件
- 业务代码 `import config_data` 的全局常量（CAPABILITIES 等）
  继续留在 python 代码里，不动

### 🔧 内容指纹机制

- 用 **sha256(toml_bytes)** 做内容指纹，简单稳定。
- 上传时回带指纹，与当前文件指纹比对；不匹配则拒绝写入。
- 这把"上传后暂存 → 用户确认"的 state machine 砍掉了，改为"前置拒绝"。
  减少代码复杂度。

## API 草图（待实现）

```python
# _shared/config/toml_manager.py
from pathlib import Path
from typing import Callable, Generic, TypeVar

import tomlkit
import tomllib
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class TomlConfigManager(Generic[T]):
    """通用 TOML + pydantic 配置管理器。

    设计目标：
    - 读：tomllib（标准库，快、严格）
    - 写：tomlkit（保留注释、键顺序）
    - 验证：pydantic（schema 化、嵌套结构 + 跨字段约束）
    - 唯一性：内容指纹 sha256 用于乐观锁
    """

    def __init__(
        self,
        data_dir: Path,
        filename_pattern: str,        # e.g. "honor_{guild_id}.toml"
        model_class: type[T],
        *,
        doc_path: Path | str | None = None,        # 可选：AI 友好的 doc.md 路径
        merge_strategy: Callable | None = None,    # 可选：自定义 tomlkit 合并
    ) -> None: ...

    def path_for(self, guild_id: int) -> Path: ...
    def load(self, guild_id: int) -> T: ...               # 不存在则用默认值创建并落盘
    def save(self, config: T, guild_id: int) -> None: ... # 用 tomlkit 写，保留注释
    def read_raw(self, guild_id: int) -> bytes | None: ...
    def read_doc(self) -> str | None: ...                # 读 doc.md（doc_path 为 None 或文件不存在 → None）
    def validate_and_save(self, raw: bytes, guild_id: int) -> T: ...
    def content_hash(self, guild_id: int) -> str | None: ...  # sha256 hex of bytes
```

### handler 语义约定（小白友好 + 乐观锁）

**`handle_toml_upload` 的 hash_file 校验逻辑**（写在 handler 里，业务不重复实现）：

| 本地状态 | hash_file 行为 | 原因 |
|---|---|---|
| 本地**无配置**（首次初始化 / 文件丢失） | 允许省略 → 警告"未做版本校验"+ 二次确认 | 没有"现版本"可对照，没法验 |
| 本地**有配置** | 必须传 hash_file → 与 `content_hash()` 比对 | 乐观锁：版本不一致拒绝覆盖 |
| 本地有配置 + hash_file 缺失 | 拒绝 + 提示"请先 /下载配置 拿到 .sha256 后再上传" | 强制走乐观锁，不允许盲写 |
| 本地有配置 + hash 匹配 | 走 `validate_and_save` | 通过 |

**embed / description 友好化**：

- 三条 handler 返回的 embed 都包含"为什么需要 sha256"解释段（跨业务通用，给小白用户看）
- 业务 cog 的 `cmd_hash` 装饰器 description 建议包含："查看当前 toml 的 SHA-256（上传时用来防止覆盖别人版本）"

**给 AI 的 doc.md**（业务自己写，handler docstring 里有提示），建议：

- 路径放在仓库 `docs/` 下（**不是 `data/`**）—— 跟代码一起进 git + 被 IDE 索引；
  doc 是给人/AI 读的说明文档，不是运行时数据。`data/` 只放运行时配置（toml / db）
- 命名建议 `<config-name>-doc.md`（如 `honor-doc.md` / `complaint-doc.md`）
- 包含以下小节：

- 当前 schema 字段解释（pydantic 模型里各字段含义）
- 哈希机制：什么是 sha256、上传/下载流程、回滚方式
- 常用修改场景示例（加纪念身份组、加幻化的具体步骤）
- 注意事项（哪些字段关联什么角色、不要轻易改哪些值）

### `_merge_into_doc` 通用化策略

ticket_bot 现有 `_merge_into_doc` 写死了 `types`（list-of-dict）和
`role_groups`（dict-of-dict）两个特化路径。通用化思路：

- 业务 pydantic 模型用 `Annotated[..., TomlMergeAsTableList()]` 之类的
  字段标记，告诉 manager 这个 list-of-dict 该按"tomlkit item"合并。
- 不带标记的字段走通用 dict 合并回 tomlkit table。
- 通过边界测试：list 中间项修改、追加、删除三种情况都覆盖。

## 不进入 toml 体系的配置清单（globals）

按本轮决策，role_bot 现有配置里**不会**迁到 toml 的部分：

| 配置 | 留在 | 理由 |
|---|---|---|
| `TOKEN` / `PROXY` / `STATUS_*` | `config.py` | bot 启动级，跟 guild 无关 |
| `COGS` | `config.py` | bot 行为开关，跨 guild 共享 |
| `COMMAND_GROUP_NAME` | `config.py` | bot 命令分组名 |
| `ROLE_IDS` / `CAPABILITIES` / `MAINTAINER_USER_IDS` | `config.py` | 装饰器消费，逻辑依赖 |
| `DANGEROUS_PERMISSIONS` | `config.py` | 高危险安全策略 |
| `SUPER_ADMIN_USER_IDS` / `ADMIN_ROLE_IDS` / `ADMIN_USER_IDS` | `config.py` | 权限判断逻辑依赖 |
| `REDIS_*` | `config.py` | 基础设施 |
| `ROLE_MANAGER_CONFIG` | `config.py` | 全局参数 |
| `BACKUP_*` / `ENABLE_ROLE_BACKUPS` / `*_BACKUP_INTERVAL_HOURS` | `config.py` | 单 bot 备份目标 |
| `CHECK_FASHION_ROLE_VALIDITY` | `config.py` | 全局开关 |

后续要不要迁某个配置的判断标准：**它跟 guild 关系是否够强到值得
per-guild 区分**。当前清单里没有模糊地带；如果将来
`JUKEBOX_GUILD_CONFIGS` / `ACTIVITY_TRACKER_CONFIG` /
`FASHION_CONFIG` 等被识别为需要 per-guild 差异化，再走 toml。

`config_data.py` 里 per-guild 的配置处置：

- `HONOR_CONFIG` → 第一波迁 toml
- `GUILD_CONFIGS` / `FASHION_CONFIG` / `ROLE_SYNC_CONFIG` → 暂留
  python dict，等第一波跑稳再排期

## TODO（后续）

### EmbedLinkManager 迁移

`role_bot/core/embed_link/embed_manager.py` 在新体系下需要重新评估：

- 类级别 `_registry` / `configs` 是进程内存单例，bot 重启 / 多 worker 场景失效
- 配置文件是裸 JSON（`./data/embed_links.json`），没有 schema 验证
- 与即将落地的 toml + pydantic 体系双轨并行，配置来源分裂
- 仅 `role_bot` 真在用：
  - `FashionCog.fashion_guide`
  - `SelfServiceCog.self_service_guide`
  - `HonorCog.honor_celebrate_guide`
- `ticket_bot` / `news_bot` 只引入了基础设施但无业务 cog 调用，可视为占位

**处理建议**（待定，本轮不动）：迁到 toml 体系，或重构成
"每次引用重新 fetch"的无状态设计。本模块并非天然 per-guild
（它是 per-cog-key），迁 toml 会打破上文的"per-guild 红线"，
需要评估如何映射到虚拟 guild 概念或破例。

## 波及面（实现阶段预计）

| 文件 | 改动 | 备注 |
|---|---|---|
| `_shared/config/toml_manager.py` | 新增 | 通用 toml IO 框架 |
| `odysseia_ticket_bot/complaint/config/loader.py` | 改用新框架 | 第一个验证用户 |
| `odysseia_ticket_bot/complaint/config/models.py` | 微调 | 加 merge 标记（如需） |
| `role_bot/honor_system/honor_def_models.py` | 新增 | pydantic schema |
| `role_bot/config_data.py` | 移除 `HONOR_CONFIG` 块 | 移到 toml |
| `role_bot/honor_system/*Cog.py` | 改 load 入口 | 替代 `import config_data` |
| `role_bot/data/honor_{guild_id}.toml` | 新增 | 实际配置数据 |
| `role_bot/docs/honor-doc.md` | 新增 | AI 友好说明文档（handler 通过 `manager.read_doc()` 读；放 `docs/` 而非 `data/`，跟代码一起进 git + 被 IDE 索引） |

> 改 `_shared/` 后要走 git subtree pull/push 流程（见 `subtree-workflow.md`），
> 三个 bot 仓库才能拿到新文件。