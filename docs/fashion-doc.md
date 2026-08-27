# 幻化衣橱 toml 配置 — admin 使用手册

> 受众：Discord 服务器管理员（admin / manage_roles 权限）
> 配置位置：每个 guild 一份 toml：`data/fashion_{guild_id}.toml`
> 入口命令组：`/幻化衣橱丨配置`（下载配置 / 上传配置 / 查看配置哈希）

---

## 这是什么

幻化系统让持有"基础身份组"的用户能切换"幻化身份组"（同色不同图），覆盖显示效果。
**幻化组的 Discord 层级必须高于基础组**——这是 Discord 客户端的颜色规则，不在 toml 范围内。

每个 guild 的幻化映射互相独立，迁到 toml 后 admin 用命令下载/上传，不再需要联系 BOT 维护者改 Python。

---

## 当前 schema（一个字段：`fashion_map` list-of-object）

```toml
[[fashion_map]]
base_role_id = 1134611078203052122        # 基础身份组 ID
fashion_role_ids = [1387483565935300708]  # 可幻化身份组 ID 列表
hidden_when_locked = false                # 锁定后是否在 UI 隐藏（必填无默认）

[[fashion_map]]
base_role_id = 1308692985315332126        # Server Boosted
fashion_role_ids = [1388482786503168000, 1388797192587841675]
hidden_when_locked = true                 # 非普通基础组——未 boost 用户看不到
```

每条 entry 三个字段：

- **`base_role_id`** — 基础身份组 ID。持有该身份组的用户可选择佩戴 `fashion_role_ids` 中的任一幻化组。
- **`fashion_role_ids`** — 可幻化身份组 ID 列表。可空（base_role 持有者没有可用幻化）。
- **`hidden_when_locked`** — **必填无默认**。

  当用户**未持有** `base_role_id`、且 `fashion_role_ids` 中任何一个都未持有时，UI 是否隐藏对应的幻化选项。
  - `true`：隐藏——典型用于「Server Boosted / BOT 维护员 / 答疑 AI / 秘书组」等"非普通"基础组
  - `false`：不隐藏——典型用于「创作者 / 助力者 / 破限组」等普通基础组

  **每个 entry 必须显式写**——这是有意的设计，强制 admin 明确 UI 行为，不允许"忘了写 = 默认"这种隐式约定。

### 为什么是 list-of-object？

最初设计是 `fashion_map: dict[int, list[int]]` + 全局 `not_normal_role_ids` 列表——「是否隐藏」靠 set 成员资格判断（过程式 / 数值计算风格）。改成 list-of-object 后：

- 每条 entry 自带 `hidden_when_locked`，UI 行为绑定到数据本身，不再依赖外部全局列表
- 未来加 `display_name` / `priority` / `archived` 等属性都在 entry 上扩展
- 跟 honor 的 `HonorGuildConfig.definitions`（list-of-object）同构

### 同一幻化组被多个 entry 引用时

例如「幻化-Server Booster」（1388797192587841675）在 Server Boosted 和 Server Booster 两个 entry 里都有。UI 隐藏判断采用 AND 语义：
**所有 entry 都是 `hidden_when_locked = true` 才隐藏**——任意 entry 是 false 就不藏。

---

## 常用场景

### 加一组新幻化到现有 base role

例如：你新建了一个「幻化-鉴赏家-月光」身份组（id `1444000000000000000`），想加到「鉴赏家」基础组下。

1. `/幻化衣橱丨配置 下载配置` → 拿到 `fashion_<guild_id>.toml`
2. 编辑文件，找到 `base_role_id = 1387480341400387708` 的 `[[fashion_map]]` 块
3. 在 `fashion_role_ids` 列表末尾加 `, 1444000000000000000`
4. `/幻化衣橱丨配置 查看配置哈希` → 复制 embed 里显示的 SHA-256 前 12 字符
5. `/幻化衣橱丨配置 上传配置` → 附 toml + 粘 hash_str 字段
6. bot 自动热刷新 `safe_fashion_map_cache`，**无需重启**

### 加一组新的「基础身份组 → 幻化组」映射

1. 下载 toml
2. 末尾追加新的 `[[fashion_map]]` 块，三个字段全部填上（**`hidden_when_locked` 必填**）
3. 上传

### 把某个 base role 从「隐藏」改为「不隐藏」（或反之）

1. 下载 toml
2. 找到对应 `[[fashion_map]]` 块，改 `hidden_when_locked` 的值
3. 上传（带新 hash）

### 给神人研究所添加幻化

1. `/幻化衣橱丨配置 下载配置`（在神人研究所服务器触发，guild_id 自动填）→ 拿到 `fashion_1265862009673486408.toml`
2. 编辑同上
3. 上传

如果神人研究所之前没 toml（首次配置）：
- `/上传配置` 时 `hash_str` 字段留空，bot 会提示"未做版本校验"+ 二次确认
- 通过后 toml 落盘，cache 自动填充

---

## SHA-256 哈希机制

**为什么需要 sha256？**
> 上传时回带哈希，跟磁盘上的现版本比对；不一致就拒绝写入。
> 防止"你下载旧版本 → 编辑 → 上传"覆盖别人刚改的新版本。

具体流程：
1. `/幻化衣橱丨配置 下载配置` → 拿到 toml + 看到当前 SHA-256（前 12 字符足够，全 64 字符也行）
2. 你编辑 toml
3. `/幻化衣橱丨配置 上传配置`：
   - 粘 toml + hash_str = 你拿到的 SHA-256 前 12 字符
   - bot 比对：hash_str 与磁盘现版本一致 → 通过 pydantic 验证 → 落盘 → 热刷新 cache
   - hash_str 不一致 → 拒绝写入，提示"有人在你之前改了 toml，请先 /下载配置 重拿 hash"

**首次上传（本地无 toml）**：`hash_str` 留空，bot 会要求二次确认（防止你把空字符串当"无版本校验"误用）。

---

## 注意事项

- **`hidden_when_locked` 必填**——pydantic 拒绝任何缺这个字段的 `[[fashion_map]]` 条目。
- **幻化组的 Discord 层级必须高于基础组**——这是 Discord 客户端的颜色规则，bot 无法帮你检查。
- **role_id 必须真实存在**——bot 启动时会去 guild 找对应 role，找不到会 log warning 但不阻塞；用户层面表现是"幻化按钮不存在"。
- **pydantic 会验证字段**——role_id 错一位（比如把 17 位数写成 16 位）会立刻报错，不会落盘。
- **不要手改 toml 文件名**——文件名是 `fashion_{guild_id}.toml`，guild_id 是文件名的一部分。改文件名 bot 就认不出了。
- **不要删 toml 文件做"重置"**——把 `[[fashion_map]]` 块全删了（保留空文件），bot 视作"该服未启用幻化"。
- **加服只需上传 toml**——不需要改 Python 代码或部署脚本。

---

## 故障排查

- 找不到 `/幻化衣橱丨配置` 命令 → bot 可能没启用 `fashion` cog 模块；联系 BOT 维护者
- 上传时报 `ValidationError` → 字段类型错（role_id 不是 int、缺 `hidden_when_locked` 等）；修 toml 重新上传
- 上传时报 `HashMismatch` → 你拿到的 hash 跟磁盘现版本不一致；先 `/下载配置` 拿新 hash 再上传
- 用户报"幻化面板里看不到某个幻化" → 检查 `hidden_when_locked` 配置——可能设成了 true
- 用户报"幻化面板里看不到任何幻化" → 该服可能没 toml 或 `fashion_map = []`；上传非空 toml 即可

---

## 联系 BOT 维护者

以下事项**不是你（admin）能改的**，请联系 BOT 维护者：

- 加新 cog（如幻化能力要扩展出"限时幻化"）——需要改 Python
- 修改幻化 UI 视觉——改 Python 模板
- 修 toml schema（加新字段、改字段类型）——改 pydantic schema
- 部署 / 回滚 toml 历史版本——BOT 维护者用 `BACKUP_CHANNEL_ID` 的 12h zip 备份兜底

请提供：
1. 服务器 guild_id
2. 你想实现的效果（"加个新幻化"、"修 bug"、"改视觉"等）
3. 涉及的身份组 ID 和名字
