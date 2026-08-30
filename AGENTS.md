# AGENTS.md — role_bot

> AI agent 进入此仓库前必读。本文件是给 AI（和我自己回来时）看的。
> workspace 顶层 `discord-bots/AGENTS.md` 是跨 bot 的工作流原则（deploy / subtree / 通用 lesson）。
> **本文件是 role_bot 特有的设计原则**——加新模块 / 加新功能前必读。

---

## 项目定位

role_bot 是 Discord 多 bot 系统中的**身份组管理 bot**。本职：

- 身份组自助领取 / 限时身份组 / 幻化身份组 / 申请面板
- 荣誉系统（永久 record + 杯赛头衔 + 周年纪念 + claimable）
- 角色同步（role_sync）
- 活动面板嵌入（在主面板加按钮）

**它不**做：

- 投票（管理组的通用投票 bot 负责）
- 作品评判 / 身份组设计（赛委手工）
- 论坛帖自动识别 + 标题匹配（用户主动操作）

---

## bot 设计原则 —— 可组合性 + 模块边界决策（2026-08-28）
Type: workflow

**反面教材**：2026-08-28 创作合战系统设计时，agent 把"投票功能"和"身份组设计"揽到了本 bot，但实际上：

- 投票由管理组的通用投票 bot 负责（**不**归本 bot 管）
- 身份组设计由赛委手工完成（**不需要** bot 自动投票）
- 自由设计身份组的结果由 toml 预定义（bot 只 add 不投票）

agent 设计了 `CreativeBattleVoteCog` / 私密子区 / 候选投稿面板 / 投票按钮 / 复杂的 json 数据结构……**这些全都不该做**。用户原话：

> "你最基本的设计和清晰度都没有做到，我不认为我们应该到这一步，你太多的东西没有搞懂，做出了复杂、重复、无效的设计。"

### 核心原则：unix 哲学（每个程序做一件事）

#### 1. 模块边界决策三问（每个新功能必问）

- **职责**：这功能属于"身份组管理"还是别的领域？
- **重复**：是不是已经有现成的 bot / 系统在做这件事？
- **简化**：能不能不做？（如果不做，会发生什么？多数情况：什么都不发生。）

#### 2. 本 bot 的本职（明确范围）

- ✅ 身份组发放（按钮面板 / 申请 / 限时）
- ✅ 身份组同步（role_sync）
- ✅ 永久记录（honor 系统）
- ✅ 活动面板嵌入（按需在主面板加按钮）

#### 3. 明确不该做的事（即使"看起来应该做"）

- ❌ 投票功能（交给管理组的通用投票 bot）
- ❌ 身份组设计 / 作品评判（赛委手工 / 用户自己）
- ❌ 论坛监听 + 标题前缀匹配（用户主动操作，不需要 bot 猜）
- ❌ 自动写 toml 的 `[[definitions]]` 块（admin 单一控制源，bot 不分裂真相）
- ❌ 自动调用 `grant_honor` / 批量 `remove_roles`（靠 `role_sync_honor=true` 反向 sync；身份组回收由 admin 手动）

#### 4. 赛季 / 活动数据永久存在

跟 honor 系统一致——所有赛季记录**永不删除**，json 文件只增不改。这与"归档清理"是反模式：

- 备份链路保留越多历史越好（损坏时能恢复更多）
- 管理员可查任意赛季
- 代码逻辑更简单（不需要 archive / cleanup 动作）

#### 5. 配置变更走人工复用

> 用户原话（2026-08-28）：
> "不需要手动集成，让管理组自己去改 honor 系统的配置就行了，否则真相会撕裂——杯赛有自己的特殊性，其他的没有必要。"
> "也就是说，设计上复用，而复用渠道就是人工。"

- **适用范围**：所有 `[[definitions]]` / `[[fashion_map]]` / `[[models]]` / 任何 toml 顶层 array-of-tables。
- **不适用范围**：`cup_honor`（杯赛）是特例——它有 `expiration_date` 字段需要自动同步到 `cup_honors.json`，自动改 honor toml 的 `notification` 配置是合理的。
- **复用 honor 系统的渠道**：`role_sync_honor=true` + 手工绑定 UUID。
  - admin 在 honor toml 加 `[[definitions]]`，role_id 绑 Discord role，`role_sync_honor=true`
  - admin 把 UUID 填到调用方的 toml
  - bot 通过 UUID 拿 honor def，**不调** `grant_honor`——靠 `role_sync` 模块反向 sync 到 honor.db
- **UUID 生成**：admin 在手机上生成 UUID 不方便，`HonorConfigCog` 提供 `/荣誉头衔丨配置丨生成UUID` 命令，一键复制。

### How to apply

- 接到新功能需求时，先做"模块边界决策三问"，跟用户对一次"这功能归不归本 bot"
- 跟用户原话对一次——别过度推断（agent 容易把"管理员手动维护"理解成"bot 帮管理员做"）
- 数据建模时遵循"赛季永久存在"——不写 cleanup / archive 逻辑
- toml 是 admin 单一控制源，bot 不写任何 `[[xxx]]` 块

### 不适用

- 纯代码重构 / bug fix（按 workspace 顶层 AGENTS.md 的通用 lesson 即可）
- 跨项目通用经验（写到 `~/.minimax/agents/mavis/memory/MEMORY.md`）

---

## 现有模块

- `core/` — 主面板 / 核心 cog（CoreCog、main_panel_view、embed_guides、role_backup）
- `role_system/` — 身份组子模块
  - `self_service/` — 自助身份组
  - `timed_role/` — 限时身份组
  - `fashion/` — 幻化（已迁 toml）
  - `role_jukebox/` — 角色点唱机
  - `role_viewer/` — 身份组查看
  - `model_fan_roles/` — 模型粉丝（已迁 toml）
- `honor_system/` — 荣誉系统
  - `HonorCog.py` / `HonorConfigCog.py` — 主 cog + toml 配置命令
  - `module/` — anniversary / claimable / role_sync
  - `cup_honor/` — 杯赛（特例：bot 自动改 toml）
  - `data_manager/` — SQLite + JSON
- `role_application/` — 申请面板（社区建设者等）
- `information/` — 信息查询 / 心跳
- `activity_tracker/` — 活动追踪（Redis 热数据）
- `creative_battle/` — 创作合战（v1 设计中，参考 `docs/creative-battle-design.md`）
- `utility/` — 工具类（auth / helpers / feature_cog / cached_toml_config_manager / role_service）
- `shared/` — `_shared/` 的 subtree 副本

---

## 实施新功能的标准流程

1. **决策**：做"模块边界决策三问"，跟用户确认归不归本 bot
2. **设计**：写设计文档（如适用，放 `docs/<feature>-design.md`），明确 toml / json / db 划分
3. **schema 优先**：先写 pydantic 模型 + manager，再写 cog
4. **真起 bot 跑按钮**：UI 路径必须真测（AGENTS.md lesson 1）
5. **grep 改 dict-style 访问**：pydantic 重构后必须全量 grep（AGENTS.md lesson 2）
6. **admin doc**：按 lesson 4，**禁止**代码级操作指南
7. **本文件更新**：如果加新模块，§"现有模块"加一行；如果新原则浮现，加到 §"bot 设计原则"