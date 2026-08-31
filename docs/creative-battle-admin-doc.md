# 创作大会 admin 使用手册（简化版）

> 受众：Discord 服务器管理员（admin / manage_roles 权限）
> 配置位置：每个 guild 一份 toml：`data/creative_battle_{guild_id}.toml`
> 入口命令组：`/合战丨配置`（下载配置 / 上传配置 / 查看配置哈希）
> 管理命令组：`/合战丨核心`（**发送面板** + **撤销投稿**——其他全自动）
> 关联命令：`/荣誉头衔丨配置`（投稿成功后 bot 自动 grant_honor 需要 honor toml 配 UUID）

---

## 这是什么

类脑社区每半年一次的「创作大会」：两阵营 PK，参赛者多者获胜。S1 赛季「秋冬大会」时间窗 **2026.9.1 - 2026.12.31**。

### bot 自动做的事（admin 不需要操心）

- 用户点主入口面板 **加入 A 组 / 加入 B 组** 按钮 → bot 自动 add supporter_role
- 用户点 📨 投稿按钮 → 弹 Modal → 用户填标题 → bot 自动 add contributor_role + 写 json + grant_honor
- 主入口 + 分区频道的推广面板 → bot 定时 refresh（**仅投稿期内更新；投稿期结束 → 停止**）
- 投稿期判断 = **if-else**：bot 读 toml 的 `start_date` / `end_date`，admin 改 toml 即可调整时间
- **contributor_role 过期 = 杯赛（cup_honor）模式**——honor toml 配 cup_honor 类型，cup_honor 模块的 24h 轮询自动监控到期 + 推提醒到 notification 频道；admin 看到提醒后手动 remove contributor_role

### admin 需要手动做的事（bot **不**自动）

- 发送主入口 / 分区面板（`/合战丨核心 发送面板`，bot **不**自动发）
- 撤销投稿（`/合战丨核心 撤销投稿 submission_id`——bot 只删 json，不 remove contributor_role 也不撤销 honor）
- 最终身份组发放 → 走人工 7 天投稿 + 7 天投票流程（bot 不参与）
- supporter_role 身份组回收 → admin 手动到 Discord remove（cup_honor 不管这个）
- contributor_role 身份组回收 → 收到 cup_honor 推的过期提醒后到 Discord 手动 remove
- honor toml 的 cup_honor 类型配置 → 管理员一次性配好 `cup_honor.expiration_date` 字段

---

## 1. 准备工作（首次启用一个 guild）

### 1.1 准备频道（admin 在 Discord 端操作）

bot **不管**频道权限配置——所有可见性由 Discord 端管理组设置。

bot 需要的频道：

1. **主入口频道**（所有人可见）—— 用于发主入口面板（A/B 互斥领取）
2. **A 组分区**（仅 A 组支持者成员可见）—— 用于发投稿按钮 + A 组推广面板
3. **B 组分区**（仅 B 组支持者成员可见）—— 用于发投稿按钮 + B 组推广面板

频道权限示例（A 组分区）：

```
@everyone       → ❌ view_channel
@A 组支持者身份组  → ✅ view_channel
```

> ⚠️ **可见性是 Discord 端的事**：bot 只发面板消息，不管频道权限。如果 admin 不设分区权限，所有人（包括对方阵营）都能看到分区频道——这是 admin 的责任。

### 1.2 创建身份组（admin 在 Discord 端操作）

bot 不会自动创建身份组。admin 需要提前在 Discord 端创建：

| 身份组 | 数量 | 说明 |
|---|---|---|
| A 组支持者身份组 | 1 | 用户点 A 按钮后 bot add；过期 / 移除由 admin 手动管理 |
| B 组支持者身份组 | 1 | 同上 |
| A 组参赛者身份组 | 1 | 用户投 A 后 bot add；过期 / 移除由 admin 手动管理 |
| B 组参赛者身份组 | 1 | 同上 |

记下每个身份组的 Discord ID（snowflake int），填到 toml。

### 1.3 通知配置（admin 在 Discord 端操作）

bot 在状态切换时（在通知频道发公告，@ 公告身份组）：

- 通知频道 ID（建议用现有的管理组频道）
- 公告身份组 ID（公告要 @ 的）

### 1.4 创建 honor 头衔 UUID（投稿后 bot 自动 grant_honor 需要）

bot 在投稿成功后会自动调 honor 系统的 `grant_honor(member.id, contributor_honor_uuid)`。
UUID 在 **honor toml** 配，creative_battle toml **引用**这个 UUID。

**强烈建议**配成 **cup_honor 类型**——这样 cup_honor 模块的 24h 轮询会自动监控到期 + 推提醒到 notification 频道（详见 §5）。

**步骤**：

1. 用 `/荣誉头衔丨配置` 下载 `honor_{guild_id}.toml`
2. 加两个 `[[definitions]]` 块（每个阵营一个），配 cup_honor 字段：

```toml
[[definitions]]
uuid = "<UUID v4 即可，或用 /荣誉头衔丨配置 里'生成UUID'命令>"
name = "🍃 A 组之贡献者"
description = "参与创作大会·A 组投稿"
role_id = 222222222            # = creative_battle toml 的 A 组 contributor_role_id
hidden_until_earned = true
expiration_date = "2026-12-31T23:59:59+08:00"   # ★ 顶层字段——HonorExpirationCog 监控过期 + 推提醒
```

> 💡 **UUID 命令只是方便工具**——任何恰当熵池（UUID v4 通过 `uuid.uuid4()` 即可）都行，不会跟现有 record 冲突。

3. 上传 honor toml 回 bot
4. 把上面的 UUID 填到 creative_battle toml 的 `factions[*].contributor_honor_uuid`，并配 `contributor_role_expire_at = 上面的 expiration_date`（详见 §2.1）

> **如果不配 `contributor_honor_uuid`**：bot 不会 grant_honor——投稿流程仍然正常完成（add role + 写 json），只是不记录永久贡献者荣誉。
> **如果不配 `cup_honor` 字段**：honor 永久保留，**不会有过期提醒**——admin 自己判断何时手动 remove contributor_role。

---

## 2. 配置 creative_battle toml

通过 Discord 命令组 `/合战丨配置`：

| 命令 | 用途 |
|---|---|
| `/合战丨配置 下载配置` | 下载当前 toml（附件）+ 当前 SHA-256（前 12 字符） |
| `/合战丨配置 上传配置` toml_file hash_str | 上传修改后的 toml；本地有配置时必须把 SHA-256 粘到 hash_str |
| `/合战丨配置 查看配置哈希` | 仅查看 SHA-256 |

### 2.1 toml 字段详解

完整示例：

```toml
[meta]
season_label = "第零赛季 秋冬"
season_id = "S0-2026-autumn-winter"     # ★ 用作 json 文件名后缀
theme = "秋冬"
start_date = "2026-09-01"              # 投稿期开始（含）；早于此日期 bot 拒绝投稿
end_date = "2026-12-31"                # 投稿期结束（含）；晚于此日期 bot 拒绝投稿

[promotion]
main_channel_id = 1134557553011998840    # 主入口频道 ID
refresh_minutes = 5                    # 推广面板刷新频率（投稿期内才更新）
random_count_per_faction = 2           # 每个分区推广 random 展示几个投稿

[[factions]]
key = "faction_a"
display_name = "A 组"
emoji = "🅰️"
supporter_role_id = 111111111          # A 组支持者身份组 ID
contributor_role_id = 222222222        # ★ A 组参赛者身份组 ID（honor toml 对应 honor 的 role_id + cup_honor 类型）
submission_channel_id = 444444444      # A 组分区频道 ID

# ★ 杯赛模式：contributor_role 过期时间
#   = honor toml 对应 honor 的 cup_honor.expiration_date
#   cup_honor.expiration_check_loop 24h 轮询检查这个时间，到时推提醒到 notification.channel_id
#   admin 看到提醒后手动到 Discord remove contributor_role
contributor_role_expire_at = "2026-12-31T23:59:59+08:00"

# ★ 简化版新增：per-faction 黑/白名单
#   - 黑名单：持任一角色即拒绝加入/投稿 A
#   - 白名单：非空时必须持任一角色才允许投稿 A
#   - 黑名单优先于白名单
#   - 留空 [] = 不限制
#   ⚠️ 这俩字段**仅作用于投稿路径**；加入路径不读，靠身份组 supporter_role 互斥检查
submission_blacklist_role_ids = []     # 例：[999999, 888888] 表示这俩角色不能投稿 A
submission_whitelist_role_ids = []     # 例：[111111] 表示必须有 111111 才能投稿 A

# ★ 简化版新增：投稿成功后 bot 调 grant_honor 用的 UUID（从 honor toml 引用）
#   留空（""或 null）= 不 grant_honor
#   ⚠️ 强烈建议配成 cup_honor 类型 honor（见 §5）—— 这样有自动过期提醒
contributor_honor_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

[[factions]]
key = "faction_b"
display_name = "B 组"
emoji = "🅱️"
supporter_role_id = 555555555
contributor_role_id = 666666666
submission_channel_id = 888888888
contributor_role_expire_at = "2026-12-31T23:59:59+08:00"
submission_blacklist_role_ids = []
submission_whitelist_role_ids = []
contributor_honor_uuid = "11111111-2222-3333-4444-555555555555"

[notification]
channel_id = 999999999                 # 公告频道（bot 在某些事件发公告）
admin_role_id = 000000000              # 公告要 @ 的身份组
```

### 2.2 字段约束（pydantic 自动校验）

- `season_id` 必须非空（用作 json 文件名）
- `start_date` 必须早于 `end_date`
- 必须**恰好** 2 个 faction（A 组 + B 组）
- faction 的 `key` 必须唯一（且小写英文 + 下划线）
- `faction.submission_channel_id` 可以留空——留空则不发该组分区面板
- `promotion.main_channel_id` 可以留空——留空则不发主入口面板
- `contributor_honor_uuid` 可以留空——留空则投稿后不 grant_honor

如果上传时校验失败，bot 会发 embed 列出错误，按提示修正后重传。

### 2.3 上传流程（避免覆盖别人版本）

按 toml 设计规范（`shared/docs/toml-config-design.md`）：

1. 先跑 `/合战丨配置 下载配置`——拿当前 toml + SHA-256
2. 修改 toml 内容
3. 再跑 `/合战丨配置 上传配置` toml_file（修改后的 toml）hash_str（上一步拿到的 SHA-256 前 12 字符）
4. 如果 bot 检测到 hash 不匹配（说明你基于的版本已过期）—— 拒绝写入，提示重新下载

### 2.4 黑/白名单使用示例

**场景 1：限制"管理组不能投稿"**（A 组 + B 组都加）

```toml
[[factions]]
key = "faction_a"
submission_blacklist_role_ids = [123456789012345678]   # 管理组身份组 ID

[[factions]]
key = "faction_b"
submission_blacklist_role_ids = [123456789012345678]   # 同上
```

**场景 2：A 组只允许"VIP 用户"投稿**

```toml
[[factions]]
key = "faction_a"
submission_whitelist_role_ids = [987654321098765432]   # VIP 身份组 ID
submission_blacklist_role_ids = []
```

**场景 3：A 组允许所有人投稿，但禁止"已封禁"角色**

```toml
[[factions]]
key = "faction_a"
submission_blacklist_role_ids = [111222333444555666]   # 已封禁身份组
submission_whitelist_role_ids = []
```

### 2.5 投稿期时间窗调整

投稿期 = `start_date` 到 `end_date`（含）。bot 每次投稿请求都会读 toml 判断：

```
if start_date <= today <= end_date:
    接受投稿
else:
    拒绝
```

**调整方法**：修改 toml 的 `meta.start_date` 或 `meta.end_date`，上传即可——bot 不需要重启，**立即生效**（下一条投稿请求就会用新时间）。

> 这不是状态机——bot 不维护 `status` 字段，也不需要"开/关投稿期"命令。改 toml 是唯一控制方式。

---

## 3. 赛季流程（简化版：admin 只需发面板 + 撤销投稿）

### 3.1 准备阶段（赛季开始前）

1. 准备频道（§1.1）+ 创建身份组（§1.2）+ 配置 honor UUID（§1.4）
2. 在 Discord 端设置分区频道权限（A 组只能 A 支持者看，B 组只能 B 支持者看）
3. 通过 `/合战丨配置` 上传 toml
4. 检查 toml 字段无误（特别是 `start_date` / `end_date` / 各 faction 配置）

### 3.2 启动赛季（admin 手动发面板）

bot **不**自动发面板（设计原则：admin 手动 `/合战丨核心 发送面板`）。

```
/合战丨核心 发送面板 channel_key=main
/合战丨核心 发送面板 channel_key=faction_a
/合战丨核心 发送面板 channel_key=faction_b
```

> `channel_key` 是**自动补全**的——输入 `fac` 会提示 `faction_a` / `faction_b` / `main`。

每个面板都是持久 view（bot 重启后按钮仍能响应）。**投稿期结束 → 推广面板停止更新**（bot 不再 edit embed，但消息本身仍在频道里）。

> **面板自身生命周期 bot 不管**：如果被 admin / 用户删除，下次 `promotion_loop`（仅投稿期内）检测到 NotFound 会自动重发。

### 3.3 投稿期进行中

- 用户点主入口面板按钮 → bot add supporter_role（互斥：用户已选 A 后点 B → bot 拒绝，提示"如需更换请联系管理组"）
- 用户点分区投稿按钮 → bot 弹 Modal → 用户填标题/描述 → bot add contributor_role + 写 json + grant_honor
- 投稿期外用户投稿 → bot 拒绝（ephemeral 提示"当前不是投稿期"）
- 黑/白名单命中 → bot 拒绝（ephemeral 提示）

> ⚠️ **bot 不通知 admin 投稿情况**——admin 自己读 `data/creative_battles_{gid}_{season_id_safe}.json` 看投稿数据。

### 3.4 撤销投稿（admin 命令）

```
/合战丨核心 撤销投稿 submission_id=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

bot 行为：
- 从 json 删除该投稿
- **不** remove contributor_role（admin 自己到 Discord remove）
- **不** 撤销已授予的 honor（admin 自己到 honor 系统处理）

> ⚠️ **撤销投稿不能恢复**——删了就没了。如果只是想改标题/描述，目前只能撤销后重新投稿。

### 3.5 投稿期结束（按 toml 时间自动）

bot 按 `meta.end_date` 自动停止接受投稿。**bot 不做任何身份组回收 / 通知**——admin 自己处理：

1. admin 看 `data/creative_battles_{gid}_{season_id_safe}.json` 决定胜负方
2. admin 走 7 天自由投稿 + 7 天投票 + 设计要求审核的人工流程
3. 设计确定后 admin 手动到 Discord 给获胜方所有 supporter add winner_role
4. 赛季结束后：admin 手动到 Discord remove 所有 supporter / contributor_role（**bot 不参与**）

> 这就是 unix 哲学：bot 提供投稿工具，胜负判断 / 身份组发放 / 回收全是 admin 的事。

### 3.6 后续赛季切换

赛季数据**永久保留**（按 design doc 拍板）。下个赛季：

1. admin 准备新 toml（修改 `season_label` / `season_id` / `start_date` / `end_date` 等）
2. 通过 `/合战丨配置 上传配置` 上传新 toml
3. bot 检测到 `season_id` 变了 → 自动建新 json 文件 → 老 json 文件保留为历史

历史赛季直接读 json 文件查看。

---

## 4. 推广面板机制

bot 每 5 分钟（`promotion.refresh_minutes`）自动 refresh 各频道的推广面板（**仅投稿期内**——`start_date <= today <= end_date` 才更新；投稿期外自动跳过）：

- **主入口面板**（频道 = `promotion.main_channel_id`）：A 组 / B 组 按钮 + 两阵营支持者 / 参赛者计数
- **分区面板**（每个 faction 的 `submission_channel_id`）：📨 投稿按钮 + 该阵营 random 几个投稿

**单一持久 embed**——bot edit 上次消息，没找到则新发。频道里只留 1 条历史。

> **投稿期结束 → bot 停止更新**：到了非投稿期，bot 不再 edit 推广 embed。**面板消息本身仍在频道里**——admin 想清理可以手动删除。

**手动刷新**（仅投稿期内）：删除那条 embed 让 bot 下次 loop 自动重发。

---

## 5. 永久贡献者荣誉 + 自动过期（cup_honor 模式）

> **关键设计**（2026-08-31 用户拍板）：贡献者荣誉在 **honor toml** 配 `expiration_date` 顶层字段。
> HonorExpirationCog 24h 轮询检查这个时间，到时推提醒。
> （cup_honor 类型是另一条路径——杯赛 honor 走 cup_honors.json 自己的 `expiration_date`，不在 toml 里）
> HonorExpirationCog（独立 cog）的 24h 轮询自动监控到期 + 推过期提醒到 notification 频道，
> admin 看到提醒后手动到 Discord remove contributor_role。bot 不调 remove_roles。

> **架构关键**（2026-08-31）：db 是 honor 历史记录表，**不存过期时间**——过期时间只在 toml/json 配置层。
> 普通 honor（无 cup_honor 字段）的身份组**永不过期**，admin 自行手动管理。

**需要 admin 做的事**（**前置**——首次启用前）：

### 步骤 1：在 honor toml 加 `[[definitions]]` + 顶层 `expiration_date` 字段

用 `/荣誉头衔丨配置` 下载 `honor_{guild_id}.toml`，加两个 `[[definitions]]` 块（含顶层 `expiration_date` 字段）。

**关键字段**：`expiration_date`（**顶层**字段，`HonorDefinitionItem` 直接属性）——HonorExpirationCog 24h 轮询检查这个时间，到时推提醒。

**完整 toml 示例**（每个 contributor_role 一条 honor）：

```toml
[[definitions]]
uuid = "<UUID v4 即可，或用 /荣誉头衔丨配置 里'生成UUID'命令>"
name = "🍃 A 组之贡献者"
description = "参与创作大会·A 组投稿"
role_id = 222222222            # = creative_battle toml 的 A 组 contributor_role_id
hidden_until_earned = true

expiration_date = "2026-12-31T23:59:59+08:00"   # ★ 顶层字段（HonorDefinitionItem）——HonorExpirationCog 监控过期 + 推提醒
expiration_date = "2026-12-31T23:59:59+08:00"
lead_days = 3

[[definitions]]
uuid = "<另一个 UUID>"
name = "🍃 B 组之贡献者"
description = "参与创作大会·B 组投稿"
role_id = 666666666
hidden_until_earned = true
expiration_date = "2026-12-31T23:59:59+08:00"   # ★ 顶层字段——HonorExpirationCog 监控过期 + 推提醒
lead_days = 3
```

上传回 bot。

> 💡 **UUID 命令只是方便工具**——任何恰当熵池（UUID v4 通过 `uuid.uuid4()` 即可）都行，不会跟现有 record 冲突。

### 步骤 2：在 creative_battle toml 引用 UUID

```toml
[[factions]]
key = "faction_a"
contributor_role_id = 222222222        # = honor toml 里的 role_id
contributor_honor_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"   # = honor toml 里的 A 组 UUID
contributor_role_expire_at = "2026-12-31T23:59:59+08:00"   # ★ = honor toml 里 cup_honor.expiration_date

[[factions]]
key = "faction_b"
contributor_role_id = 666666666
contributor_honor_uuid = "11111111-2222-3333-4444-555555555555"
contributor_role_expire_at = "2026-12-31T23:59:59+08:00"
```

### 步骤 3：bot 自动处理

赛季内：用户投稿 → bot 自动 `add_roles(contributor_role_id)` + 自动 `grant_honor(member.id, contributor_honor_uuid)`。

> ⚠️ **失败处理**：`grant_honor` 失败（UUID 不存在、DB 错误等）**不阻断投稿**——
> 投稿流程仍正常完成，admin 看到 `honor_granted=false` 后可手动到 honor 系统补。
> json 里 `honor_granted` 字段记录 bot 是否成功调用。

### 步骤 4：过期自动推提醒

到 `expiration_date` 后：

1. HonorExpirationCog 的 `expiration_check_loop`（24h 轮询）检测到 honor 过期
2. 自动推 `ExpiredHonorNoticeView` embed 到 honor toml 的 `cup_honor.notification.channel_id` 频道
3. admin 看到提醒后到 Discord 手动 remove 所有持有 contributor_role 的成员

### 撤销投稿不撤销 honor

bot 的 `/合战丨核心 撤销投稿` 命令**只删 json**，**不撤销已授予的 honor**。
如需撤销，请到 honor 系统的 `/荣誉头衔丨管理 管理持有者` 处理。

### 调整过期时间

改 `honor_*.toml` 的顶层 `expiration_date` 字段，重新上传即可——下次 HonorExpirationCog 轮询会用新时间。

## 6. 身份组回收（HonorExpirationCog 推提醒 + admin 手动）

> **通用机制**：contributor_role 的过期由 HonorExpirationCog（独立 cog）自动监控。
> bot **不调** remove_roles——admin 看到提醒后**手动**到 Discord remove。

### 6.1 contributor_role 过期（HonorExpirationCog 推提醒）

到 honor toml 的顶层 `expiration_date` 后：

1. HonorExpirationCog 的 `expiration_check_loop`（24h 轮询）检测到 honor 过期
2. 自动推 `ExpiredHonorNoticeView` embed 到 honor toml 的 `cup_honor.notification.channel_id` 频道（含 honor 名称 + 过期时间）
3. admin 看到提醒后到 Discord 手动 remove 所有持有 contributor_role 的成员

| 身份组 | 何时过期 / 回收 | 谁推提醒 | 谁 remove |
|---|---|---|---|
| A 组 / B 组 contributor_role | honor toml 的 `expiration_date` 字段 | HonorExpirationCog 自动 | admin 手动 |

## 7. 常见问题

### Q: 上传 toml 时提示"hash 不匹配"

**原因**：你基于的 toml 版本已过期（别人在你之前改了）。

**解决**：先 `/合战丨配置 下载配置` 拿最新版本，重新修改，再次上传。

### Q: bot 启动时找不到频道

**原因**：`promotion.main_channel_id` / `faction.submission_channel_id` / `notification.channel_id` 填错，或者 bot 没在那个频道的权限。

**解决**：

1. 在 Discord 端确认频道存在
2. 检查 toml 里填的 ID（snowflake int）是不是真的那个频道
3. 检查 bot 在该频道是否有 view + send message 权限

### Q: 按 A 组 / B 组按钮没反应

**原因**：可能是 persistent view 没注册（bot 重启后 toml 没加载）。

**解决**：

1. 检查 toml 是否上传成功（`/合战丨配置 查看配置哈希`）
2. 重新跑 `/合战丨核心 发送面板 channel_key=main` 重发面板

### Q: 已选 A 阵营后想换 B 怎么办？

**原因**：bot 设计为互斥——已二选一后不能反过去选，需要 admin 手动 remove A 的 supporter_role 后再点 B。

**解决**：

1. 用户联系 admin（公告身份组的成员）
2. admin 到 Discord 端手动 remove A 的 supporter_role
3. 用户重新到主入口面板点 B

### Q: 投稿时提示"黑名单 / 白名单"被拒

**原因**：用户的身份组触发了 toml 配置的黑/白名单规则。

**解决**：

1. 检查 toml 中对应 faction 的 `submission_blacklist_role_ids` / `submission_whitelist_role_ids` 配置
2. 如需调整，修改 toml 后上传
3. 黑名单优先——即使在白名单中，黑名单命中也拒

### Q: 投稿后没拿到 contributor_role

**原因**：可能是今天不在投稿期（`today < start_date` 或 `today > end_date`）。

**解决**：

1. 检查 toml 的 `start_date` / `end_date` 字段
2. 调整时间窗后上传即可（**立即生效**，bot 不需要重启）
3. 检查 bot 在分区频道是否仍有 add_roles 权限
4. 检查对应 faction 的 `submission_channel_id` 是否正确

### Q: 投稿后没拿到 honor

**原因**：

1. toml 的 `contributor_honor_uuid` 留空（不 grant_honor）—— **预期行为**
2. honor toml 没有对应的 `[[definitions]]` 块
3. UUID 拼写错误 / 多余空格
4. honor 系统 DB 异常（极少见）

**解决**：

1. 检查 creative_battle toml 的 `contributor_honor_uuid` 是否填了正确的 UUID
2. 检查 honor toml 是否有对应的 `[[definitions]]` 块，uuid 字段一致
3. 如有需要，admin 用 honor 系统的 `/荣誉头衔丨管理 授予` / `批量授予` 手动补

### Q: cup_honor 没推过期提醒？

**原因**：

1. honor 没在 cup_honors.json 里配置（cup_honor 走 json，不走 toml）
2. honor toml 的 `cup_honor.expiration_date` 还没到（cup_honor 模块按这个字段判断）
3. honor toml 的 `cup_honor.expiration_date` 跟 creative_battle toml 的 `contributor_role_expire_at` 不一致
4. cup_honor 模块 disabled（`config.py` 的 `honor_system.enabled = False`）
5. 推过提醒了但 admin 没注意——cup_honor 有"已提醒"状态防重复推

**解决**：

1. 检查 cup_honors.json 里是否有对应 uuid 的 cup_honor，且 `cup_honor.expiration_date` 是否正确
2. 检查 `expiration_date` 是否是未来时间
3. 把两个 toml 的时间字段对齐
4. 检查 `config.py` 的 `honor_system.enabled = True`
5. 查 honor 系统的"已通知状态"json——可能要重置

### Q: 怎么撤销投稿？

**答**：用 `/合战丨核心 撤销投稿 submission_id=<投稿 UUID>`。

bot 行为：
- 从 json 删除该投稿（**不可恢复**）
- **不** remove contributor_role（admin 自行到 Discord 处理）
- **不** 撤销已授予的 honor（admin 自行到 honor 系统处理）

### Q: 怎么查看历史赛季？

**答**：json 文件位于 `data/creative_battles_{guild_id}_{season_id_safe}.json`，**永久保留**——admin 想看历史直接读 json 文件。

---

## 8. 限制与边界（按 `role_bot/AGENTS.md`）

**bot 不会做的事**（设计原则——admin 不要期待 bot 做）：

- ❌ 投票
- ❌ 身份组设计 / 作品评判（委员会手工）
- ❌ forum 帖子自动识别（用户主动点按钮）
- ❌ **身份组过期提醒 / 自动回收**——admin 全权负责
- ❌ 身份组 add 后的自动 remove（用户换阵营时 bot 拒绝 + 提示联系管理组）
- ❌ **winner_role 自动发放**——人工 7+7 流程
- ❌ 投稿管理 / 取缔资格（无"管理组"bot 角色）
- ❌ bot 自动发面板（admin 手动 `/合战丨核心 发送面板`）
- ❌ 频道权限配置（Discord 端 admin 自己设）
- ❌ 胜负判断（plain 展示参赛人数，admin 自己看 json 决定）
- ❌ **游客领取面板**——游客由 admin 复用 honor claimable 模块处理

**如果 admin 需要 bot 做以上的事**——请联系 BOT 维护者，**不要**自己改 bot 代码。

---

## 9. 联系 BOT 维护者的情况

按 AGENTS.md lesson 4，以下情况**必须**联系 BOT 维护者（不是 admin 自己改）：

- 需要改 toml schema（新增字段、改字段类型）
- 需要 bot 行为变化（如做投票 / 做论坛监听 / 做批量身份组回收通知）
- 需要把 bot 迁移到其他 guild
- 需要查看 / 修改历史赛季 json 数据

**BOT 维护者联系方式**：（这里填实际的 Discord ID 或用户名）
