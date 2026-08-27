# 模型阵营 toml 配置 — admin 使用手册

> 受众：Discord 服务器管理员（admin / manage_roles 权限）
> 配置位置：每个 guild 一份 toml：`data/model_fan_roles_{guild_id}.toml`
> 入口命令组：`/模型阵营丨配置`（下载配置 / 上传配置 / 查看配置哈希）

---

## 这是什么

「大模型阵营」是用户在大语言模型粉丝身份组里互斥单选——一个人只能选一个阵营（Gemini 哈基米 / Claude 小克 / DeepSeek 小鲸鱼 ...）。

阵营人气榜（强者羞辱弱者 logic）：bot 启动时按 `role.members` 数排序，人多的排前面。加新模型只需要追加 `[[models]]` 块。

每个 guild 的模型列表互相独立，迁到 toml 后 admin 用命令下载/上传，不再需要联系 BOT 维护者改 Python。

---

## 当前 schema（一个字段：`models` list-of-dict）

```toml
[[models]]
name = "Gemini"                    # 内部名（注释辨识）
display_name = "哈基米"            # 按钮标签（用户可见）
role_id = 1444246888512753764      # Discord 身份组 ID（snowflake int）
emoji = "🦊"                       # 可选，按钮前缀 emoji

[[models]]
name = "Claude"
display_name = "小克"
role_id = 1444248769494384666

# 追加更多...
```

字段说明：

- **`name`** — 内部名（"Gemini" 等），只用作 toml 注释辨识，bot 不展示给用户
- **`display_name`** — 用户可见的按钮标签
- **`role_id`** — Discord 身份组 ID（snowflake int）
- **`emoji`** — 按钮前缀 emoji（可选，不写就空白）

---

## 常用场景

### 加一个新模型（例如 "Qwen"）

1. 在 Discord 服务器手动创建一个身份组（假设 id `1444999999999999999`，名字 "Qwen 阵营"）
2. `/模型阵营丨配置 下载配置` → 拿到 `model_fan_roles_<guild_id>.toml`
3. 末尾追加：
   ```toml
   [[models]]
   name = "Qwen"
   display_name = "通义千问"
   role_id = 1444999999999999999
   emoji = "🐉"
   ```
4. `/模型阵营丨配置 查看配置哈希` → 复制 SHA-256 前 12 字符
5. `/模型阵营丨配置 上传配置` → 附 toml + 粘 hash_str
6. bot 自动热刷新 `safe_model_config_cache`，**无需重启**

### 改某个模型的 display_name / emoji

1. 下载 toml
2. 找到对应 `[[models]]` 块，编辑字段
3. 上传（带新 hash）

### 移除一个模型（用户不再能选它）

**注意**：移除 toml 条目只会让面板不再展示该模型，**已持有该身份组的用户不会自动卸下**。
如果想完全下线：

1. 下载 toml，删除对应 `[[models]]` 块
2. 上传（带 hash）
3. 手动 `Discord 服务器 → 成员 → 批量移除该身份组`（或联系 BOT 维护者写一次性脚本）

### 修改已持有用户的阵营（互斥逻辑）

bot 在用户点按钮时已经做了「卸旧 + 加新」的互斥操作——你不需要改 toml。

---

## SHA-256 哈希机制

**为什么需要 sha256？**
> 上传时回带哈希，跟磁盘上的现版本比对；不一致就拒绝写入。
> 防止"你下载旧版本 → 编辑 → 上传"覆盖别人刚改的新版本。

具体流程：
1. `/模型阵营丨配置 下载配置` → 拿到 toml + 看到当前 SHA-256（前 12 字符足够，全 64 字符也行）
2. 你编辑 toml
3. `/模型阵营丨配置 上传配置`：
   - 粘 toml + hash_str = 你拿到的 SHA-256 前 12 字符
   - bot 比对：hash_str 与磁盘现版本一致 → 通过 pydantic 验证 → 落盘 → 热刷新 cache
   - hash_str 不一致 → 拒绝写入，提示"有人在你之前改了 toml，请先 /下载配置 重拿 hash"

**首次上传（本地无 toml）**：`hash_str` 留空，bot 会要求二次确认。

---

## 注意事项

- **role_id 必须真实存在**——bot 启动时会去 guild 找对应 role，找不到会 log warning；用户层面表现是"按钮不存在"。
- **pydantic 会验证字段**——role_id 错一位、emoji 太长（>64 字符）会立刻报错，不会落盘。
- **不要手改 toml 文件名**——文件名是 `model_fan_roles_{guild_id}.toml`，guild_id 是文件名的一部分。
- **不要删 toml 文件做"重置"**——直接把 `[[models]]` 块全删了（保留空文件），bot 视作"该服未启用模型阵营"。
- **加服只需上传 toml**——不需要改 Python 代码或部署脚本。

---

## 故障排查

- 找不到 `/模型阵营丨配置` 命令 → bot 可能没启用 `model_fan_roles` cog 模块；联系 BOT 维护者
- 上传时报 `ValidationError` → toml 字段类型错（role_id 不是 int、emoji 太长等）；修 toml 重新上传
- 上传时报 `HashMismatch` → 你拿到的 hash 跟磁盘现版本不一致；先 `/下载配置` 拿新 hash 再上传
- 用户报"面板里看不到任何模型" → 该服可能没 toml 或 `models = []`；上传非空 toml 即可

---

## 联系 BOT 维护者

以下事项**不是你（admin）能改的**，请联系 BOT 维护者：

- 加新功能（如"双阵营"、"限时阵营"）——需要改 Python
- 改阵营人气榜排序算法——改 Python
- 修 toml schema（加新字段）——改 pydantic schema
- 部署 / 回滚 toml 历史版本——BOT 维护者用 `BACKUP_CHANNEL_ID` 的 12h zip 备份兜底

请提供：
1. 服务器 guild_id
2. 你想实现的效果
3. 涉及的身份组 ID 和名字
