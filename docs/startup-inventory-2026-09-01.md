# role_bot 启动期行为盘点（2026-09-01）

> **目的**：把 `role_bot` 在启动期（约 1 分钟，从 `12:27:28` 到 `12:28:37`）发生的所有事件
> 全部列清楚 + 对应到代码位置，方便后续讨论哪些事在互相挤、哪些是 fire-and-forget、
> 哪些是 await 链、哪些是 `tasks.loop` 首跑。
>
> **本文不做任何实施 / 优化建议**——只盘点现状。所有引用格式 `path/file.py:line`。

---

## 0. 全文速读

- **日志来源**：`python ../cof-discord-bot-ctl/discordctl.py logs --tail 5000` 拉取的容器日志（2026-09-01 12:27 起的一段）。
- **进程身份**：Linux 容器内 `/usr/local/lib/python3.12/site-packages/discord/...`（d.py 标准包路径）+ `/app `/app/role_bot/...`（bot 项目代码）。
- **本盘点范围**：从 `setup_hook` 开始（cmd sync）→ 到第一个 user-facing 交互（12:28:58 第一个 404）。
- **21 个 cog 全部在 `12:27:23 ~ 12:27:38` 加载完成**；之后所有启动期事件集中在 12:28:36 ~ 12:28:55 这 19 秒窗口里挤在一起。

---

## 1. 启动期时间线（基于 2026-09-01 实际日志）

下表把日志里出现的关键事件 + 触发它的代码位置一一对应。

| 时间 (UTC+8)         | 日志内容 / 触发动作 | 触发来源（文件:行号）| 类型 |
|----------------------|----------------------|------------------------|------|
| `12:27:23.032`       | `未配置代理，直接初始化机器人` | `main.py:236` `bot = RoleBot()` | 同步 init |
| `12:27:23.033`       | `PyNaCl is not installed, voice will NOT be supported` | d.py 自身 warning | — |
| `12:27:23.034`       | `davey is not installed, voice will NOT be supported` | d.py 自身 warning | — |
| `12:27:23.036`       | `logging in using static token` | d.py 内部 | — |
| `12:27:23.036`       | `Privileged message content intent is missing` | d.py 自身 warning | — |
| `12:27:23.329`       | `开始加载模块 'core'...` | `main.py:99` `await cog_manager.load_all_enabled()` → `load_module("core")` | setup_hook await 链 |
| `12:27:23.331`       | `CoreCog 已加载，正在启动后台任务...` | `core/CoreCog.py:71` `CoreCog.cog_load` | `__init__` 注册 + `cog_load` 启动 tasks |
| `12:27:23.332`       | `已加载子Cog: CoreCog` | `main.py:220` | — |
| `12:27:23.333`       | `EmbedGuidesConfigManager 初始化` | `core/embed_guides/embed_guides_manager.py` 单例 init | 模块顶层 import 触发 |
| `12:27:23.333`       | `指引文案配置 Cog 已加载` | `core/embed_guides/embed_guides_command.py:58` `__init__` | — |
| `12:27:24.335`       | `功能模块 EmbedGuidesConfigCog 已成功注册到 CoreCog。` | `core/CoreCog.py:371` `register_feature_cog` | 由 `utility/feature_cog.py:54` 的 `cog_load` `await asyncio.sleep(1)` 后再注册 |
| `12:27:24.336`       | `模块 EmbedGuidesConfigCog 已注册到 CoreCog` | `utility/feature_cog.py:67` | — |
| `12:27:24.336`       | `已加载子Cog: EmbedGuidesConfigCog` | `main.py:220` | — |
| `12:27:24.336`       | `开始加载模块 'backup'...` | `main.py:195` `load_module("backup")` | — |
| `12:27:24.337`       | `已加载子Cog: BackupCog`（**注意**：`BackupCog.__init__` 里直接 `self.auto_backup_task.start()`，见 `core/role_backup_cog.py:45`） | `core/role_backup_cog.py:45` | tasks.loop 启动（不等 ready） |
| `12:27:24.337`       | `开始加载模块 'self_service'...` | `main.py:195` | — |
| `12:27:25.340`       | `功能模块 SelfService 已成功注册到 CoreCog。`（async sleep(1) 后） | `core/CoreCog.py:371` + `utility/feature_cog.py:54` | — |
| `12:27:25.341`       | `开始加载模块 'fashion'...` | — | — |
| `12:27:26.344`       | `已加载子Cog: FashionCog`（**注意**：`FashionCog.__init__:37` 直接 `check_fashion_role_validity_task.start()`，但 body 是 `pass`） | `role_system/fashion/FashionCog.py:37` | tasks.loop 启动（不等 ready） |
| `12:27:26.345`       | `FashionConfigManager 初始化` | 模块顶层 | — |
| `12:27:26.345`       | `幻化衣橱配置 Cog 已加载` | `role_system/fashion/FashionConfigCog.py:73` | — |
| `12:27:27.349`       | `开始加载模块 'model_fan_roles'...` | — | — |
| `12:27:28.352`       | `已加载子Cog: ModelFanRolesCog` | `role_system/model_fan_roles/ModelFanRolesCog.py:35` `__init__`（无 tasks.loop / 无 create_task） | — |
| `12:27:28.358`       | `ModelFanRolesConfigManager 初始化` | 模块顶层 | — |
| `12:27:28.359`       | `模型阵营配置 Cog 已加载` | `role_system/model_fan_roles/ModelFanRolesConfigCog.py:75` | — |
| `12:27:29.362`       | `已加载子Cog: ModelFanRolesConfigCog` | — | — |
| `12:27:29.362`       | `开始加载模块 'role_jukebox'...` | — | — |
| `12:27:30.371`       | `已加载子Cog: RoleJukeboxCog`（**注意**：`RoleJukeboxCog.__init__:48` 直接 `rotation_task.start()`） | `role_system/role_jukebox/RoleJukeboxCog.py:48` | tasks.loop 启动（不等 ready） |
| `12:27:30.371`       | `开始加载模块 'role_sync'...` | — | — |
| `12:27:31.405`       | `已加载子Cog: RoleSyncCog`（**注意**：`RoleSyncCog.__init__:44` 直接 `daily_sync_task.start()`） | `role_sync/RoleSyncCog.py:44` | tasks.loop 启动（不等 ready） |
| `12:27:31.405`       | `开始加载模块 'role_viewer'...` | — | — |
| `12:27:32.409`       | `已加载子Cog: RoleViewerCog` | — | — |
| `12:27:32.410`       | `开始加载模块 'role_application'...` | — | — |
| `12:27:32.411`       | `已加载子Cog: RoleApplicationCog` | — | — |
| `12:27:32.412`       | `开始加载模块 'track_activity'...` | — | — |
| `12:27:32.431`       | `Cog 'TrackActivity' 加载完成。` | `activity_tracker/TrackActivityCog.py:387` `cog_load`（**注意**：注册 `ActivityRoleView`，但 Redis check / on_ready 派 task 还没触发） | — |
| `12:27:32.433`       | `开始加载模块 'honor_system'...` | — | — |
| `12:27:32.446`       | `HonorConfigManager 初始化` | 模块顶层 | — |
| `12:27:33.459`       | `已加载子Cog: HonorCog`（**注意**：`HonorCog.__init__:41` 立刻 `self.bot.loop.create_task(self.synchronize_all_honor_definitions())`） | `honor_system/HonorCog.py:41` | create_task（fire-and-forget） |
| `12:27:34.463`       | `已加载子Cog: HonorAnniversaryModuleCog` | — | — |
| `12:27:34.465`       | `已加载子Cog: ClaimableHonorModuleCog` | — | — |
| `12:27:34.466`       | `已加载子Cog: CupHonorModuleCog` | — | — |
| `12:27:35.469`       | `已加载子Cog: RoleClaimHonorModuleCog` | — | — |
| `12:27:36.474`       | `已加载子Cog: HonorAdminCog` | — | — |
| `12:27:36.475`       | `已加载子Cog: HonorExpirationCog`（**注意**：`HonorExpirationCog.cog_load:199` 启动 `expiration_check_loop`） | `honor_system/honor_expiration_cog.py:199` | tasks.loop 启动（不等 ready） |
| `12:27:36.476`       | `开始加载模块 'heartbeat_information'...` | — | — |
| `12:27:36.479`       | `已加载子Cog: HeartbeatInformationCog`（**注意**：`cog_load:37` 调用 `_start_heartbeat_task`，但内部目前有 `TODO` 返回，详见 §2） | `information/HeartbeatInformationCog.py:42` | 当前 no-op |
| `12:27:36.479`       | `开始加载模块 'creative_battle'...` | — | — |
| `12:27:37.501`       | `已加载子Cog: CreativeBattleCog`（**注意**：`cog_load:218` 启动 `promotion_loop`） | `creative_battle/CreativeBattleCog.py:218` | tasks.loop 启动（不等 ready） |
| `12:27:37.502`       | `创作大会配置 Cog 已加载` | `creative_battle/CreativeBattleConfigCog.py` | — |
| `12:27:38.506`       | `已加载子Cog: CreativeBattleConfigCog` | — | — |
| `12:27:38.507`       | `开始同步应用命令...` | `main.py:100`（setup_hook 内） | setup_hook await 链 |
| `12:27:38.828`       | `已同步 21 个命令到服务器 1134557553011998840` | `main.py:138` | Discord API round-trip |
| `12:27:39.065`       | `已同步 21 个命令到服务器 1265862009673486408` | `main.py:138` | — |
| `12:27:39.286`       | `已同步 21 个命令到服务器 1380075940285124724` | `main.py:138` | — |
| `12:27:39.673`       | `Shard ID None has connected to Gateway` | d.py 内部 | — |
| `12:28:36.919`       | `Shard ID 0 timed out waiting for chunks for guild_id 1134557553011998840` | d.py 内部 | **chunk 超时**（不是 user 触发） |
| `12:28:36.944`       | `备份Cog已就绪，自动备份任务即将开始。` | `core/role_backup_cog.py:212` `before_auto_backup` | BackupCog 的 `auto_backup_task` 的 `before_loop` 命中 |
| `12:28:36.945`       | `HonorCog: 开始同步所有服务器的荣誉定义...` | `honor_system/HonorCog.py:168` `synchronize_all_honor_definitions` 主体首条 log | create_task `HonorCog.__init__:41` 派出去的 task 进入执行（已 `wait_until_ready`） |
| `12:28:36.973`       | `同步服务器 1134557553011998840 的荣誉...` | `honor_system/HonorCog.py:176` | — |
| `12:28:37.070`       | `HonorCog: 荣誉定义同步完成。` | `honor_system/HonorCog.py:229` | await 链结束（DB 写完成） |
| `12:28:37.071`       | `以 李曦曦•为您而来#2797 身份登录成功!` | `main.py:79` `RoleBot.on_ready` 第 1 条 log | `on_ready` 进入 |
| `12:28:37.073`       | `机器人状态已设置为: watching 用户的身份组发放请求` | `main.py:93` `change_presence` 完成 | — |
| `12:28:37.075`       | `核心模块已就绪，主控制面板持久化视图已注册。` | `core/CoreCog.py:108` `CoreCog.on_ready` 主体 | `on_ready` 第 2 条 |
| `12:28:37.076`       | `备份模块已就绪，目标服务器: '类脑ΟΔΥΣΣΕΙΑ', 目标频道: '#🤖︱𝗕𝗢𝗧监控室'` | `core/role_backup_cog.py:71` `BackupCog.on_ready` | `on_ready` 第 3 条 |
| `12:28:37.079`       | `ClaimableHonorModule: 正在重新注册持久化视图...` | `honor_system/module/claimable_honor_module.py:170` `ClaimableHonorModuleCog.on_ready` | `on_ready` 第 4 条 |
| `12:28:37.083`       | `ClaimableHonorView 已注册。` | `honor_system/module/claimable_honor_module.py:174` | — |
| `12:28:37.533`       | `开始执行自动 LIGHT 备份...` | `core/role_backup_cog.py:176` `auto_backup_task` 主体首条 log（首跑命中） | tasks.loop 首跑 |
| `12:28:37.535`       | `开始在后台线程为服务器 '类脑ΟΔΥΣΣΕΙΑ' 生成身份组备份数据...` | `core/role_backup_cog.py:84`（`_blocking_create_backup_data` 进 executor） | — |
| `12:28:37.593`       | `开始执行每日身份组同步任务...` | `role_sync/RoleSyncCog.py:152` `daily_sync_task` 主体首条 log（首跑命中） | tasks.loop 首跑 |
| `12:28:37.594`       | `每日身份组同步任务完成。` | `role_sync/RoleSyncCog.py:192` | 同步任务 body 极快结束 |
| `12:28:37.595`       | `正在执行 honor 过期检查（toml + cup_honors.json 合并）...` | `honor_system/honor_expiration_cog.py:227` `_perform_expiration_check` 首条 log（首跑命中） | tasks.loop 首跑 |
| `12:28:45.673`       | `CoreCog 已就绪，准备执行首次缓存更新...` | `core/CoreCog.py:219` `_update_all_caches_task.before_loop` 首条 log | tasks.loop `before_loop` 命中 |
| `12:28:45.674`       | `CoreCog 已就绪，准备执行首次缓存更新...`（**第二次**：同一 `before_loop` 被 `_backup_data_task` 复用，因 `CoreCog.py:211-212` `@_backup_data_task.before_loop` 同名叠加） | `core/CoreCog.py:211-212`（**注意**：双装饰器叠加导致 `before_loop` 同时挂在两个 task 上） | tasks.loop `before_loop` 命中 |
| `12:28:45.786`       | `开始执行每小时的全局安全缓存更新...` | `core/CoreCog.py:82` `_update_all_caches_task` 主体首条 log | tasks.loop 首跑 |
| `12:28:45.787`       | `开始执行计划的数据备份任务...` | `core/CoreCog.py:177` `_backup_data_task` 主体首条 log | tasks.loop 首跑（**注意**：12h 间隔，启动后立刻首跑） |
| `12:28:45.806`       | `开始在后台线程中创建数据备份 ZIP 文件...` | `core/CoreCog.py:115` `_blocking_create_backup_zip` 进 executor | — |
| `12:28:47.001`       | `SelfServiceCog: 开始更新安全自助身份组缓存...` | `role_system/self_service/SelfServiceCog.py:48` | CoreCog `_update_all_caches_task` 通过 `asyncio.gather` 并行调 |
| `12:28:47.004`       | `SelfServiceCog: 安全自助身份组缓存更新完毕。` | `role_system/self_service/SelfServiceCog.py:67` | — |
| `12:28:47.004`       | `FashionCog: 开始更新安全幻化身份组缓存...` | `role_system/fashion/FashionCog.py:60` | — |
| `12:28:47.086`       | `FashionCog: 安全幻化身份组缓存更新完毕。` | `role_system/fashion/FashionCog.py:98` | — |
| `12:28:47.087`       | `ModelFanRolesCog: 开始更新模型身份组缓存...` | `role_system/model_fan_roles/ModelFanRolesCog.py:62` | — |
| `12:28:47.092`       | `ModelFanRolesCog: 缓存更新完毕，共加载 1 个服务器的配置。` | `role_system/model_fan_roles/ModelFanRolesCog.py:106` | （**注意**：末尾 `for guild_id in new_cache: self.bot.loop.create_task(self._refresh_stats_background(guild_id))` —— §4 详） |
| `12:28:47.093`       | `RoleSyncCog: 开始更新安全同步身份组缓存...` | `role_sync/RoleSyncCog.py:55` | — |
| `12:28:47.097`       | `RoleSyncCog: 安全同步身份组缓存更新完毕。` | `role_sync/RoleSyncCog.py:106` | — |
| `12:28:47.097`       | `模块 'Honor' 开始更新安全身份组缓存...` | `honor_system/HonorCog.py:58` | — |
| `12:28:47.121`       | `模块 'Honor' 安全缓存更新完毕，共加载 125 个身份组。` | `honor_system/HonorCog.py:77` | — |
| `12:28:48.932`       | `每小时全局安全缓存更新完毕。` | `core/CoreCog.py:97` `_update_all_caches_task` 主体结束 log | gather 全部完成 |
| `12:28:54.879`       | `Can't keep up, shard ID None websocket is 33.1s behind.` | d.py 内部 | **gateway 落后 33 秒** |
| `12:28:55.864`       | `DataManager: 成功连接到 Redis 服务器 (异步客户端)。` | `activity_tracker/TrackActivityCog.py:394` `on_ready` → `data_manager.check_connection` | `on_ready` 命中 |
| `12:28:55.864`       | `DataManager: 后台 flush 任务已启动 (interval=0.2s, threshold=200)` | `activity_tracker/TrackActivityCog.py:402-405` `start_buffer_flusher` | 后台 flush task 启动 |
| `12:28:55.865`       | `Bot is ready. Creating startup incremental sync task...` | `activity_tracker/TrackActivityCog.py:407` `on_ready` 第二条 | `on_ready` 命中 |
| `12:28:58.093`       | `Ignoring exception in view <ActivityRoleView ...> for item <Button ... label='查看我的活跃报告'>` → `discord.errors.NotFound: 404 ... Unknown interaction` | `activity_tracker/views.py:193` `view_report_button` → `interaction.response.defer` 失败 | **第一个 404**（用户点按钮但 token 已过期） |
| `12:28:58.324`       | 同上，但 `label='检查活跃度 & 申领身份组'` → `activity_tracker/views.py:188` `check_activity_button` | — | **第二个 404** |
| `12:29:00.031`       | `服务器 '类脑ΟΔΥΣΣΕΙΑ' 开始历史消息回填任务。内存锁已激活。` | `activity_tracker/TrackActivityCog.py:705` `_backfill_guild_history` 首条 log | create_task 派出去的 backfill 终于进入执行（启动 on_ready 触发 → 内部又 create_task 派出去；实际 API 调用受 `await asyncio.sleep(1)` 节流） |
| `12:29:01.121`       | `服务器 '类脑ΙΛΙΑΣ' 开始历史消息回填任务。内存锁已激活。` | 同上，第二服 | — |
| `12:29:29.660`       | `后台 ZIP 文件创建完毕，共打包 64 个文件。` | `core/CoreCog.py:137` `_backup_data_task` 的 zip 创建完成 | 12h 备份结束（**注意**：从 12:28:45.806 起跑，到 12:29:29.660 完成 = 43.85s） |
| `12:29:32.797`       | `自动数据备份成功，文件已发送至频道 #🤖︱𝗕𝗢𝗧监控室。` | `core/CoreCog.py:197` `_backup_data_task` 主体结束 log | — |
| `12:30:53.043`       | `刷新服务器 类脑ΟΔΥΣΣΕΙΑ 的模型身份组统计数据...` | `role_system/model_fan_roles/ModelFanRolesCog.py:168` `_refresh_stats_background` 终于进 executor | 启动期 cache update 末尾派出去的 background refresh（见 §4） |

---

## 2. 所有 `on_ready` listener 清单

> 仅盘点真实注册的 listener；cog `_init__` / `cog_load` / `tasks.loop.before_loop` 里的
> "等 ready 后做事"逻辑单独算（见 §3 / §4）。

| 文件:行号 | Listener 名 | 触发逻辑 | 启动期会做什么 |
|-----------|-------------|-----------|-----------------|
| `main.py:77` | `RoleBot.on_ready` | d.py 主类继承自 `commands.Bot`，d.py 在 `READY` gateway 事件触发时 dispatch | ① 写"登录成功"日志；② 解析 `config.STATUS_TYPE` / `config.STATUS_TEXT`，调 `self.change_presence(activity=...)` —— **这是 `await` 调用本身**。 |
| `core/CoreCog.py:100` | `CoreCog.on_ready`（`@commands.Cog.listener()` 装饰） | d.py dispatch | `self.bot.add_view(MainPanelView(self))` —— 同步操作（**但 `MainPanelView` 构造内部可能 lazy 创建按钮/embed**），写日志 |
| `core/role_backup_cog.py:50` | `BackupCog.on_ready`（`@commands.Cog.listener()` 装饰） | d.py dispatch | ① `self.backup_guild = self.bot.get_guild(self.guild_id)`；② `self.backup_channel = self.bot.get_channel(self.channel_id)`；③ 若两者都拿到，写"备份模块已就绪"日志；**否则 `auto_backup_task.cancel()`** |
| `activity_tracker/TrackActivityCog.py:391` | `TrackActivityCog.on_ready`（`@commands.Cog.listener()` 装饰） | d.py dispatch | ① `await self.bot.wait_until_ready()`（**注意**：在 `on_ready` 内又等一次 `wait_until_ready`，这是冗余但无害）；② `await self.data_manager.check_connection()` → Redis ping + 写"成功连接"日志；③ 若失败，`self.activity_group.interaction_check = lambda i: False`（禁用所有指令）并 return；④ 成功后 `await self.data_manager.start_buffer_flusher()` → 启动 `_flusher_loop`（后台 200ms / 200 条 flush）；⑤ 写 "Bot is ready. Creating startup incremental sync task..."；⑥ `self.bot.loop.create_task(self._incremental_sync_on_startup())`（**fire-and-forget**，内部又会为每个 guild 派一个 `_backfill_guild_history`） |
| `honor_system/module/claimable_honor_module.py:168` | `ClaimableHonorModuleCog.on_ready`（`@commands.Cog.listener()` 装饰） | d.py dispatch | ① 写 "ClaimableHonorModule: 正在重新注册持久化视图..."；② `self.bot.add_view(ClaimableHonorView(self))`；③ 写 "ClaimableHonorView 已注册" |

**显式 *没有* `on_ready` listener 的 cog**（盘点过程确认）：

- `honor_system/HonorCog.py` —— 用 `__init__` 直接 `create_task(synchronize_all_honor_definitions)`，内含 `await self.bot.wait_until_ready()`
- `honor_system/honor_expiration_cog.py:177` HonorExpirationCog —— 用 `cog_load` 启动 `expiration_check_loop`，loop 自己有 `before_loop` 等 ready
- `honor_system/cup_honor/cup_honor_module.py:29` CupHonorModuleCog —— 无 on_ready
- `honor_system/module/anniversary_module.py:23` HonorAnniversaryModuleCog —— 无 on_ready
- `honor_system/module/role_sync_honor_module.py` RoleClaimHonorModuleCog —— 无 on_ready（待确认行号，但全文 grep 已确认无 `async def on_ready`）
- `honor_system/HonorConfigCog.py:42` HonorConfigCog —— 无 on_ready
- `honor_system/honor_admin_cog.py:67` HonorAdminCog —— 无 on_ready
- `core/embed_guides/embed_guides_command.py` EmbedGuidesConfigCog —— 无 on_ready
- `role_system/self_service/SelfServiceCog.py:21` SelfServiceCog —— 无 on_ready
- `role_system/fashion/FashionCog.py:23` FashionCog —— 无 on_ready
- `role_system/fashion/FashionConfigCog.py:53` FashionConfigCog —— 无 on_ready
- `role_system/model_fan_roles/ModelFanRolesCog.py:32` ModelFanRolesCog —— 无 on_ready
- `role_system/model_fan_roles/ModelFanRolesConfigCog.py:55` ModelFanRolesConfigCog —— 无 on_ready
- `role_system/role_jukebox/RoleJukeboxCog.py:22` RoleJukeboxCog —— 无 on_ready
- `role_system/role_viewer/RoleViewerCog.py:22` RoleViewerCog —— 无 on_ready
- `role_system/timed_role/TimedRolesCog.py:26` TimedRolesCog —— 无 on_ready
- `role_application/RoleApplicationCog.py` RoleApplicationCog —— 无 on_ready
- `information/HeartbeatInformationCog.py:28` HeartbeatInformationCog —— 无 on_ready
- `creative_battle/CreativeBattleCog.py:164` CreativeBattleCog —— 无 on_ready（promotion_loop 自己有 `before_loop`）

---

## 3. 所有 `@tasks.loop` 清单（含首跑时机）

> 列字段说明：
> - **启动调用点**：`.start()` 在哪行被调（`__init__` / `cog_load` / `cog_unload` 之外的地方）
> - **before_loop**：注册的 `before_loop` 是否存在 + 它会做什么
> - **首跑实际时机**：从实际日志看，bot ready 之后第一个 tick 什么时候发生
> - **是否阻塞事件循环**：tasks.loop 本身**不阻塞** on_ready 本身（d.py 在独立 task 里跑），但它的 await 链会**争抢**事件循环时间片

| 文件:行号 | 任务名 | 间隔 | 启动调用点 | before_loop 行为 | 首跑实际时机 | 周期行为 | 是否阻塞事件循环 |
|-----------|--------|------|-----------|------------------|--------------|-----------|------------------|
| `core/CoreCog.py:79` | `_update_all_caches_task` | `hours=1` | `CoreCog.cog_load` @ `core/CoreCog.py:72` | `@_update_all_caches_task.before_loop` @ `core/CoreCog.py:212`（**与 `_backup_data_task.before_loop` 叠加**）→ `await self.bot.wait_until_ready()` + `await asyncio.sleep(5)` + log | 日志 `12:28:45.786`（ready 之后 ~9s） | `asyncio.gather(*[cog.update_safe_roles_cache() for cog in self.feature_cogs], return_exceptions=True)` —— 5 个 FeatureCog 并行更新 cache | **抢**：gather 期间 5 个 cache update 并发跑，但 `update_safe_roles_cache` 多数只动内存 + 部分 `guild.get_role`；HonorCog 那条要 SQLAlchemy 查 DB；RoleSyncCog 那条要遍历 `config_data.ROLE_SYNC_CONFIG`。日志显示整段从 12:28:45.786 到 12:28:48.932 = **3.1s** |
| `core/CoreCog.py:174` | `_backup_data_task` | `hours=12` | `CoreCog.cog_load` @ `core/CoreCog.py:73` | 与上同 `@_backup_data_task.before_loop` @ `core/CoreCog.py:211`（**共用一个 `before_cache_update_task` 函数**） | 日志 `12:28:45.787`（与 cache update 同时首跑） | ① `get_channel(BACKUP_CHANNEL_ID)`；② `run_in_executor(partial(_blocking_create_backup_zip, "data"))` —— 阻塞 IO 在线程池；③ `channel.send(content=..., file=backup_file)` | **抢**：12h 间隔但启动立刻首跑 → ZIP 创建（`12:28:45.806 → 12:29:29.660`）= **43.85s**，期间和 backfill / on_message 等共享事件循环；Discord API send `12:29:29.660 → 12:29:32.797` = **3.14s**（含上传 64 文件 zip） |
| `core/role_backup_cog.py:165` | `auto_backup_task` | `hours=config.LIGHT_BACKUP_INTERVAL_HOURS` (= 1) | `BackupCog.__init__` @ `core/role_backup_cog.py:45` | `@auto_backup_task.before_loop` @ `core/role_backup_cog.py:209` → `await self.bot.wait_until_ready()` + log "备份Cog已就绪，自动备份任务即将开始。" | 日志 `12:28:37.533`（on_ready 一瞬间，**注意**：log "备份Cog已就绪" 是 12:28:36.944，比 on_ready 早 127ms——说明 d.py dispatch on_ready 之前 `before_loop` 已通过 wait_until_ready 拿到 ready 信号） | ① 判断 `current_hour % FULL_BACKUP_INTERVAL_HOURS == 0` 决定 LIGHT/FULL；② FULL 时 `await _perform_member_cache_refresh()`（**会调 `guild.fetch_members(limit=None)`**——这是分钟级阻塞 API）；③ `await _create_backup_data_async(self.backup_guild)` —— `run_in_executor`；④ `await _create_backup_file_async(...)`；⑤ `await self.backup_channel.send(...)` | **抢**：实际 backup 跑在 executor（`_blocking_create_backup_data` 遍历 `guild.roles` 同步收集 member_ids），但 channel.send 仍要 Discord API |
| `role_system/timed_role/TimedRolesCog.py:92` | `daily_reset_task` | `minutes=1` | `TimedRolesCog.__init__` @ `role_system/timed_role/TimedRolesCog.py:42` | `@daily_reset_task.before_loop` + `@check_expired_roles_task.before_loop`（**双装饰器叠加**） @ `role_system/timed_role/TimedRolesCog.py:175-176` → `await self.bot.wait_until_ready()` | （**实际日志未抓到首跑首条**——`daily_reset_task` 首跑发生在某个 :00 重置点之后才会跑实际逻辑，首条 log 是 "已到达每日重置时间..."） | 每分钟判断 `now >= today_reset_time > last_reset`，命中则调 `await self.timed_role_data_manager.daily_reset(self, guilds_to_reset)` | **弱抢**：平时只打 1 分钟一次 + 不命中条件直接 return；命中时调 `daily_reset` 涉及 DB / 可能 Discord API |
| `role_system/timed_role/TimedRolesCog.py:119` | `check_expired_roles_task` | `minutes=1` | `TimedRolesCog.__init__` @ `role_system/timed_role/TimedRolesCog.py:43` | 同上双装饰器 `before_timed_roles_tasks` | （**实际日志未抓到首跑**——每分钟一次） | 遍历 `get_users_with_active_timed_role()`；对过期用户 `await try_get_member`（可能 API）+ `await member.remove_roles(...)`；每 5 个用户 `asyncio.sleep(1)` | **弱抢**：纯 DB 读 + 偶尔 Discord API（remove_roles）；节流 1s/5 user |
| `role_system/fashion/FashionCog.py:100` | `check_fashion_role_validity_task` | `hours=24` | `FashionCog.__init__` @ `role_system/fashion/FashionCog.py:37` | `@check_fashion_role_validity_task.before_loop` @ `role_system/fashion/FashionCog.py:154` → `await self.bot.wait_until_ready()` | **当前 body 是 `pass`**（整段被注释，见 `role_system/fashion/FashionCog.py:100-152`） | **无** | **无**（task 跑空 pass） |
| `role_system/role_jukebox/RoleJukeboxCog.py:312` | `rotation_task` | `seconds=1` | `RoleJukeboxCog.__init__` @ `role_system/role_jukebox/RoleJukeboxCog.py:48` | `@rotation_task.before_loop` @ `role_system/role_jukebox/RoleJukeboxCog.py:391` → `await self.bot.wait_until_ready()` | （**每秒一次**：实际日志 `12:28:39.673` gateway 连接 → 启动后每秒都在跑；正常情况下空轨不会 log，所以启动期不可见） | `await asyncio.to_thread(self.manager.get_due_rotations)` —— 同步内存判断到期；`for ... in actions: await self._apply_preset(...)` —— **每次 `await role.edit(...)`**（Discord API） | **强抢**：1 秒 1 次 tick 是启动期事件循环最频繁的"戳一下"来源；只要有轨道到点，每次都打 Discord API；但空配置下应无 actions |
| `role_sync/RoleSyncCog.py:149` | `daily_sync_task` | `hours=24` | `RoleSyncCog.__init__` @ `role_sync/RoleSyncCog.py:44` | `@daily_sync_task.before_loop` @ `role_sync/RoleSyncCog.py:194` → `await self.bot.wait_until_ready()` | 日志 `12:28:37.593`（**注意**：on_ready 触发后立刻首跑；"开始执行每日身份组同步任务..." → 1ms 后"每日身份组同步任务完成。"——意味着 cache 是空的，body 跑得快） | 遍历 `safe_daily_sync_pairs_cache`（首次首跑时为空 dict，因为 `update_safe_roles_cache` 还没跑），对每个 pair 遍历 `source_role.members` + `await member.add_roles(...)` + `await member.send(...)`；每 10 个用户 `asyncio.sleep(1)` | **强抢**：cache 有内容后会发大量 Discord API（add_roles + send DM）；本次首跑 cache 为空所以秒结束 |
| `honor_system/honor_expiration_cog.py:206` | `expiration_check_loop` | `hours=24` | `HonorExpirationCog.cog_load` @ `honor_system/honor_expiration_cog.py:199` | `@expiration_check_loop.before_loop` @ `honor_system/honor_expiration_cog.py:210` → `await self.bot.wait_until_ready()` | 日志 `12:28:37.595`（与 on_ready 同时首跑） | `await self._perform_expiration_check()`：遍历所有 `honor_*.toml` 的 guild，对每个 guild 检查 `cfg.definitions` 找过期 honor，再遍历 `cup_honors.json`；命中时 `await channel.send(content=..., embed=..., view=...)` | **抢**：每 guild 都要 `guild.get_role` + 可能 `channel.send` |
| `creative_battle/CreativeBattleCog.py:830` | `promotion_loop` | `minutes=5` | `CreativeBattleCog.cog_load` @ `creative_battle/CreativeBattleCog.py:218` | `@promotion_loop.before_loop` @ `creative_battle/CreativeBattleCog.py:873` → `await self.bot.wait_until_ready()` | （**5 分钟一次**：启动后第一次跑要等 5 分钟；不可见） | 遍历 `_iter_configured_guild_ids()`，对每个 guild 刷新主入口 + 每个 faction 的分区面板（`channel.send/edit`） | **弱抢**（5 分钟一次，但 refresh 期间要发多个 Discord API 调用） |
| `information/HeartbeatInformationCog.py:221` | 动态生成的 `tasks.loop` | `seconds=info.update_interval_seconds` | `_start_heartbeat_task` @ `information/HeartbeatInformationCog.py:204`，由 `cog_load` 在 `information/HeartbeatInformationCog.py:42` 遍历现有 heartbeat 调用 | `@new_task.before_loop(before_loop_waiter)` @ `information/HeartbeatInformationCog.py:228` → `await self.bot.wait_until_ready()` | **当前 no-op**：`_start_heartbeat_task` 函数体在 `information/HeartbeatInformationCog.py:215-216` 有 `# TODO 由于速率限制，现在取消实时更新功能，之后转为可发送限时信息` 后立刻 `return`，**不创建 tasks.loop** | **无** | **无** |

---

## 4. 所有 `__init__` / `cog_load` / `on_ready` 里的 `create_task`

> "fire-and-forget" task 派发——不阻塞派发方本身，但会和事件循环抢时间片。

| 文件:行号 | 派出的 coroutine | 何时触发 | 实际行为 |
|-----------|------------------|----------|----------|
| `honor_system/HonorCog.py:41` | `self.synchronize_all_honor_definitions()` | `HonorCog.__init__` 末尾（**注意**：在 `super().__init__` 之后，所以注册到 CoreCog 之前就派 task） | task 内部 `await self.bot.wait_until_ready()` —— 等到 ready 后同步 toml/cup_honors.json 到 SQLAlchemy（db.query + db.commit）。日志显示从派发到完成 = `12:27:33.459 → 12:28:37.070`（含 wait_until_ready + DB 写） |
| `activity_tracker/TrackActivityCog.py:408` | `self._incremental_sync_on_startup()` | `TrackActivityCog.on_ready`（即 12:28:55.865） | 遍历 `config["guild_configs"]`，对每个 guild：① 读 `last_sync_ts`；② `await _resolve_report_channel(...)`；④ 若 `last_sync_ts is None` 写首启动通知；⑤ 否则 `await self._safe_report_send(...)`；⑥ `self.bot.loop.create_task(self._backfill_guild_history(...))` —— **每个 guild 再派一个 task**；⑦ `await asyncio.sleep(1)` —— **节流，避免同时启动多个任务造成拥堵**（见 `TrackActivityCog.py:669`） |
| `activity_tracker/TrackActivityCog.py:663` | `self._backfill_guild_history(guild, target_channel, start_datetime, end_datetime)` | 在 `_incremental_sync_on_startup` 内，对每个 guild 派一个 | ① 加 `_backfill_locks` 内存锁；② `await processor.get_scannable_channels(single_channel)`；③ 遍历 `scannable_channel_ids`：对每个 channel `guild.get_channel(...) or await self.bot.fetch_channel(...)`；④ `async for message in channel.history(limit=None, after=start, before=end)`；⑤ 过滤；⑥ `add_message_to_pipeline` + 每 500 条 `execute_pipeline` + `asyncio.sleep(0.05)`；⑦ 每 5s `progress_message.edit(embed=...)`；⑧ 收尾 `update_sync_timestamp` + `target_channel.send(final_embed)`；⑨ 释放锁。日志显示从派发到完成 = `12:29:00.031 → 12:35:42.391` = **6分42秒**（类脑ΟΔΥΣΣΕΙΑ）；`12:29:01.121 → 12:35:24.906` = **6分23秒**（类脑ΙΛΙΑΣ） |
| `activity_tracker/TrackActivityCog.py:1251-1255` | `self._backfill_guild_history(...)` | 在 `/用户活跃度 手动拉取历史消息` 命令 handler 内（启动期不发生） | 同上 |
| `role_system/model_fan_roles/ModelFanRolesCog.py:111` | `self._refresh_stats_background(guild_id)` | `update_safe_roles_cache` 末尾，对 `new_cache` 每个 guild 派一个 | `await asyncio.to_thread(self._blocking_count_role_members, base_configs, guild)` —— 线程池跑同步 `len(role.members)`（O(K·N)）；写回 `stats_cache` / `stats_last_updated`。日志显示启动期从派发到实际进 executor 完成 = `12:28:47.092 → 12:30:53.043` = **2分6秒**（类脑ΟΔΥΣΣΕΙΑ） |
| `role_system/model_fan_roles/ModelFanRolesCog.py:142` | `self._refresh_stats_background(guild_id)` | `get_ranked_model_data` 内 fire-and-forget（启动期不发生——只有用户点 view 时才进） | 同上 |
| `honor_system/HonorConfigCog.py:77` | `honor_cog.synchronize_all_honor_definitions()` | `cmd_upload_config` 命令末尾（启动期不发生） | 同 §1 HonorCog 的 create_task |
| `role_system/model_fan_roles/ModelFanRolesConfigCog.py:97` | `model_cog.update_safe_roles_cache()` | `cmd_upload_config` 命令末尾（启动期不发生） | 跑 model_fan_roles_{guild_id}.toml 读取 + 缓存刷新 |
| `role_system/fashion/FashionConfigCog.py:95` | `fashion_cog.update_safe_roles_cache()` | `cmd_upload_config` 命令末尾（启动期不发生） | 跑 fashion_{guild_id}.toml 读取 + 缓存刷新 |
| `activity_tracker/data_manager.py:136` | `self._flush_items(items)` | `record_message` 内 fire-and-forget（**启动期不直接发生**——但 `cog_load:402` 启动的后台 `_flusher_loop` 每 200ms 会扫一次 buffer，命中阈值也会派 flush task） | `redis.pipeline()` 批量写 |
| `role_sync/RoleSyncCog.py:213` | `self.daily_sync_task()` | `manual_daily_sync` 命令末尾（启动期不发生） | 复用 tasks.loop 主体 |

---

## 5. 启动期互相"挤在一起"的事

> 按 ready 触发瞬间（12:28:37）前后 18 秒窗口，分门别类看谁在抢事件循环。

### 5.1 `12:28:37` on_ready 触发瞬间（约 500ms 内同时发生）

| 事件 | 类型 | 阻塞？ |
|------|------|-------|
| `RoleBot.on_ready` → `change_presence` | await | 是（HTTP round-trip） |
| `CoreCog.on_ready` → `bot.add_view(MainPanelView(self))` | 同步 | 短 |
| `BackupCog.on_ready` → `bot.get_guild` + `bot.get_channel` | 同步 | 短（cache 命中） |
| `ClaimableHonorModuleCog.on_ready` → `bot.add_view(ClaimableHonorView(self))` | 同步 | 短 |
| `HonorCog.synchronize_all_honor_definitions`（create_task，wait_until_ready 刚返回）继续跑 DB | await | 是（DB 写） |

### 5.2 `12:28:37 ~ 12:28:45` 之间（约 8 秒静默期）

| 事件 | 类型 | 说明 |
|------|------|------|
| `auto_backup_task` 首跑：调 `_perform_member_cache_refresh`（**如果是 FULL_BACKUP 时间**，会调 `guild.fetch_members(limit=None)`） + `_create_backup_data_async`（executor） | tasks.loop 首跑 | 启动时是 LIGHT（`12:28:37.533`），所以没拉成员；只跑 `await _create_backup_data_async(self.backup_guild)` 进 executor |
| `daily_sync_task` 首跑：cache 空，秒结束 | tasks.loop 首跑 | 见 §3 |
| `expiration_check_loop` 首跑：log "正在执行 honor 过期检查..." | tasks.loop 首跑 | 实际 work 短暂 |
| `_update_all_caches_task.before_loop` 在 `await asyncio.sleep(5)` 之后 `12:28:45.673` log | tasks.loop before_loop | **注意**：`await asyncio.sleep(5)` 是阻塞 5 秒，让 cache 有内容（first run 时 CoreCog.feature_cogs 可能还没全注册完——见 `core/CoreCog.py:218`） |

### 5.3 `12:28:45 ~ 12:28:55`（10 秒密集期）

| 事件 | 类型 | 是否阻塞 |
|------|------|---------|
| `_update_all_caches_task` 主体 `asyncio.gather` 5 个 FeatureCog 并行 `update_safe_roles_cache` | await | 是（gather 期间所有 cache 跑完才 log "完毕"） |
| `_backup_data_task` 主体首跑：executor 创建 ZIP + `channel.send` | tasks.loop 首跑 | 是（43.85s 创建 ZIP + 3.14s 上传） |
| `12:28:54.879` d.py 自己 warn "websocket 33.1s behind" | 警告 | — |
| `TrackActivityCog.on_ready`（**延迟**：Redis ping 约 18s 才返回成功，可能因 Redis cold start） | await chain | 是 |
| `TrackActivityCog.on_ready` 内 `start_buffer_flusher()` 启动 `_flusher_loop`（200ms tick） | tasks.loop 启动 | 不阻塞 on_ready 后续 |
| `TrackActivityCog.on_ready` 内 `create_task(_incremental_sync_on_startup)` | fire-and-forget | 不阻塞 on_ready 后续 |

### 5.4 `12:28:58 ~`（第一个 404 已发生）

| 事件 | 类型 |
|------|------|
| 用户按 `ActivityRoleView` 按钮（`ActivityRoleView` 是 `cog_load:388` 注册的 `12:27:32.431`，早就 ready 了），但 `interaction.token` 已经在 on_ready 之前生成 → ready 期间事件循环堆积 → token 过期（Discord 15 分钟 token 生命周期，但**实际上是 bot 自己的事件循环卡死导致 button callback 延误**，见 `activity_tracker/views.py:188-193`） | 用户触发 + d.py 报 404 |
| `_backfill_guild_history` 对类脑ΟΔΥΣΣΕΙΑ 终于进入实际执行（`12:29:00.031`，距 on_ready 23 秒） | create_task 派出去的 backfill 跑起来 |
| `ModelFanRolesCog._refresh_stats_background` 终于进入 executor 完成（`12:30:53.043`，距派发 2 分 6 秒） | — |

### 5.5 await 链 vs fire-and-forget vs tasks.loop 汇总

| 类别 | 谁 | 影响 |
|------|----|-----|
| **会阻塞 on_ready 链**（await chain） | `change_presence` (RoleBot.on_ready:92)、`get_guild`/`get_channel` (BackupCog.on_ready:59-60)、`add_view` (CoreCog/ClaimableHonor on_ready)、HonorCog 的 `db.query + db.commit`、`TrackActivityCog.on_ready` 内 `wait_until_ready` + `data_manager.check_connection` (Redis ping) + `start_buffer_flusher` | 每个 await 都会让 on_ready dispatch 链暂停；用户按按钮的 callback 在队列里等 |
| **fire-and-forget**（`bot.loop.create_task(...)`） | `HonorCog.synchronize_all_honor_definitions`（__init__）、`TrackActivityCog._incremental_sync_on_startup`（on_ready）、`_backfill_guild_history` per guild（in incremental_sync）、`ModelFanRolesCog._refresh_stats_background` per guild（in cache update）、`data_manager._flush_items` per threshold hit | 不阻塞派发方，但会和事件循环抢时间片 |
| **tasks.loop 首跑**（不等 ready 直接 `.start()`） | `auto_backup_task` (BackupCog __init__:45)、`daily_reset_task` + `check_expired_roles_task` (TimedRolesCog __init__:42-43)、`check_fashion_role_validity_task` (FashionCog __init__:37, body 是 `pass`)、`rotation_task` (RoleJukeboxCog __init__:48)、`daily_sync_task` (RoleSyncCog __init__:44)、`expiration_check_loop` (HonorExpirationCog cog_load:199)、`promotion_loop` (CreativeBattleCog cog_load:218) | 派发方立即返回；task 自己在 `wait_until_ready` / `before_loop` 命中后才开始跑主体 |
| **tasks.loop 启动后等待 ready 的方式** | `before_loop` (绝大多数) 或 `await self.bot.wait_until_ready()` 在首条语句 | 影响"ready 信号到达 → 真正跑主体"的延迟 |

---

## 6. 引用相关日志佐证

> 上面 §1 的时间线已经全部来自实际日志（2026-09-01 12:27 起的容器日志）。
> 下面把"日志 → 代码 → 行为"的关联再做一次显式汇总，方便对照。

### 6.1 on_ready listener → 日志对照

| on_ready listener | 产生日志 | 日志内容 |
|-------------------|----------|----------|
| `main.py:77 RoleBot.on_ready` | `12:28:37.071` | `以 李曦曦•为您而来#2797 身份登录成功!` |
| `main.py:77 RoleBot.on_ready` | `12:28:37.073` | `机器人状态已设置为: watching 用户的身份组发放请求` |
| `core/CoreCog.py:100 CoreCog.on_ready` | `12:28:37.075` | `核心模块已就绪，主控制面板持久化视图已注册。` |
| `core/role_backup_cog.py:50 BackupCog.on_ready` | `12:28:37.076` | `备份模块已就绪，目标服务器: '类脑ΟΔΥΣΣΕΙΑ', 目标频道: '#🤖︱𝗕𝗢𝗧监控室'` |
| `honor_system/module/claimable_honor_module.py:168 ClaimableHonorModuleCog.on_ready` | `12:28:37.079` + `12:28:37.083` | `ClaimableHonorModule: 正在重新注册持久化视图...` / `ClaimableHonorView 已注册。` |
| `activity_tracker/TrackActivityCog.py:391 TrackActivityCog.on_ready` | `12:28:55.864` + `12:28:55.864` + `12:28:55.865` | `DataManager: 成功连接到 Redis 服务器 (异步客户端)。` / `DataManager: 后台 flush 任务已启动 (interval=0.2s, threshold=200)` / `Bot is ready. Creating startup incremental sync task...` |

### 6.2 tasks.loop 首跑 → 日志对照

| tasks.loop | 产生日志 | 日志内容 |
|-----------|----------|----------|
| `core/role_backup_cog.py:165 auto_backup_task` | `12:28:37.533` | `开始执行自动 LIGHT 备份...` |
| `core/role_backup_cog.py:165 auto_backup_task` | `12:28:37.535` | `开始在后台线程为服务器 '类脑ΟΔΥΣΣΕΙΑ' 生成身份组备份数据...` |
| `core/role_backup_cog.py:165 auto_backup_task` | `12:31:09.094` ~ `12:31:13.698` | `后台身份组数据生成完毕，共处理了 219 个身份组。` → ... → `自动 LIGHT 备份成功并已发送到频道 🤖︱𝗕𝗢𝗧监控室。` |
| `role_sync/RoleSyncCog.py:149 daily_sync_task` | `12:28:37.593` + `12:28:37.594` | `开始执行每日身份组同步任务...` / `每日身份组同步任务完成。` |
| `honor_system/honor_expiration_cog.py:206 expiration_check_loop` | `12:28:37.595` | `正在执行 honor 过期检查（toml + cup_honors.json 合并）...` |
| `core/CoreCog.py:79 _update_all_caches_task` 的 before_loop | `12:28:45.673` + `12:28:45.674` | `CoreCog 已就绪，准备执行首次缓存更新...` × 2（**注意双装饰器叠加**） |
| `core/CoreCog.py:79 _update_all_caches_task` 主体 | `12:28:45.786` + `12:28:48.932` | `开始执行每小时的全局安全缓存更新...` → ... → `每小时全局安全缓存更新完毕。` |
| `core/CoreCog.py:174 _backup_data_task` 主体 | `12:28:45.787` + `12:29:32.797` | `开始执行计划的数据备份任务...` → ... → `自动数据备份成功，文件已发送至频道 #🤖︱𝗕𝗢𝗧监控室。` |

### 6.3 create_task 派出的子任务 → 日志对照

| create_task 派发点 | 派出去的 task 名 | 产生日志 | 日志内容 |
|--------------------|------------------|----------|----------|
| `honor_system/HonorCog.py:41` (in __init__) | `synchronize_all_honor_definitions()` | `12:28:36.945` + `12:28:36.973` + `12:28:37.070` | `HonorCog: 开始同步所有服务器的荣誉定义...` / `同步服务器 1134557553011998840 的荣誉...` / `HonorCog: 荣誉定义同步完成。` |
| `activity_tracker/TrackActivityCog.py:408` (in on_ready) | `_incremental_sync_on_startup()` | `12:29:00.031` + `12:29:01.121` | `服务器 '类脑ΟΔΥΣΣΕΙΑ' 开始历史消息回填任务。内存锁已激活。` / `服务器 '类脑ΙΛΙΑΣ' 开始历史消息回填任务。内存锁已激活。` |
| `activity_tracker/TrackActivityCog.py:663` (in `_incremental_sync_on_startup`) | `_backfill_guild_history(...)` per guild | `12:35:24.906` + `12:35:42.391` | `服务器 '类脑ΙΛΙΑΣ' 的回填任务结束，内存锁已释放。` / `服务器 '类脑ΟΔΥΣΣΕΙΑ' 的回填任务结束，内存锁已释放。` |
| `role_system/model_fan_roles/ModelFanRolesCog.py:111` (in `update_safe_roles_cache`) | `_refresh_stats_background(guild_id)` per guild | `12:30:53.043` + `12:45:06.881` + `12:46:33.702` + `12:47:35.725` + `12:48:36.732` | `刷新服务器 类脑ΟΔΥΣΣΕΙΑ 的模型身份组统计数据...` × 多条 |

### 6.4 启动期第一个 404 的触发链

| 时间 | 日志 / 行为 | 代码位置 |
|------|-------------|----------|
| `12:27:32.431` | `Cog 'TrackActivity' 加载完成。`（在 `TrackActivityCog.cog_load` 里注册 `ActivityRoleView(self)`） | `activity_tracker/TrackActivityCog.py:388` |
| （启动期） | 用户按 `ActivityRoleView` 上的按钮（"查看我的活跃报告" 或 "检查活跃度 & 申领身份组"） | `activity_tracker/views.py:188-193` |
| `12:28:58.093` | 第一个 404：`discord.errors.NotFound: 404 Not Found (error code: 10062): Unknown interaction`（"查看我的活跃报告" 按钮回调里的 `interaction.response.defer(ephemeral=True, thinking=True)` 失败） | `activity_tracker/views.py:193` `view_report_button` |
| `12:28:58.324` | 第二个 404（"检查活跃度 & 申领身份组" 按钮回调里的 `interaction.response.defer(ephemeral=True, thinking=True)` 失败） | `activity_tracker/views.py:188` `check_activity_button` |
| `12:31:04.310` | 类似 404 但按钮是 `OpenLobbyButton`（"轮播身份组"） | `role_system/role_jukebox/user_view.py:57` `view.show(interaction)` |

> **注**：上面 3 个 404 都是 `interaction.response.defer/send_message` 失败——意味着 button callback 终于被 d.py dispatch 到时，token 已过期（Discord 15 分钟 token 周期内，但 bot 在 on_ready 链上卡了 ~19 秒，导致 callback 延误）。

---

## 7. 关键代码引用速查表

> 把全文出现最多的代码引用再做一次汇总，方便 review。

| 类别 | 路径:行号 | 一句话说明 |
|------|-----------|-----------|
| 主类 on_ready | `main.py:77` | RoleBot.on_ready |
| 主类 change_presence | `main.py:92` | `await self.change_presence(activity=activity)` |
| cog 注册 | `main.py:99` | `await cog_manager.load_all_enabled()` |
| 命令 sync | `main.py:137` | `await self.tree.sync(guild=guild)` |
| FeatureCog 注册 | `utility/feature_cog.py:54` | `async def cog_load` + `await asyncio.sleep(1)` + `core_cog.register_feature_cog(self)` |
| CoreCog.on_ready | `core/CoreCog.py:100` | `self.bot.add_view(MainPanelView(self))` |
| CoreCog.cache loop | `core/CoreCog.py:79` | `_update_all_caches_task` (hours=1) |
| CoreCog.backup loop | `core/CoreCog.py:174` | `_backup_data_task` (hours=12) |
| CoreCog 双 before_loop | `core/CoreCog.py:211-212` | `@_backup_data_task.before_loop` + `@_update_all_caches_task.before_loop` 共享 `before_cache_update_task` |
| CoreCog gather | `core/CoreCog.py:88-91` | `await asyncio.gather(*[cog.update_safe_roles_cache() ...])` |
| BackupCog.__init__ | `core/role_backup_cog.py:32` | 构造函数末尾直接 `.start()` `auto_backup_task` |
| BackupCog.on_ready | `core/role_backup_cog.py:50` | get_guild / get_channel |
| BackupCog auto_backup_task | `core/role_backup_cog.py:165` | (hours=LIGHT_BACKUP_INTERVAL_HOURS=1) |
| TrackActivityCog.on_ready | `activity_tracker/TrackActivityCog.py:391` | Redis check + buffer flusher + create_task incremental_sync |
| TrackActivityCog.create_task | `activity_tracker/TrackActivityCog.py:408` | `self.bot.loop.create_task(self._incremental_sync_on_startup())` |
| TrackActivityCog.backfill dispatch | `activity_tracker/TrackActivityCog.py:663` | 每个 guild 一个 backfill task |
| TrackActivityCog.backfill body | `activity_tracker/TrackActivityCog.py:698` | 核心回填 |
| DataManager.start_buffer_flusher | `activity_tracker/data_manager.py:177` | 启动 200ms tick |
| HonorCog.create_task | `honor_system/HonorCog.py:41` | `__init__` 末尾派 `synchronize_all_honor_definitions` |
| HonorCog.synchronize body | `honor_system/HonorCog.py:166` | `wait_until_ready` + DB sync |
| ClaimableHonorModuleCog.on_ready | `honor_system/module/claimable_honor_module.py:168` | `add_view` |
| HonorExpirationCog.cog_load | `honor_system/honor_expiration_cog.py:198` | 启动 `expiration_check_loop` |
| HonorExpirationCog.expiration_check_loop | `honor_system/honor_expiration_cog.py:206` | (hours=24) |
| HonorExpirationCog._perform_expiration_check | `honor_system/honor_expiration_cog.py:214` | 实际执行体 |
| RoleSyncCog.daily_sync_task | `role_sync/RoleSyncCog.py:149` | (hours=24) |
| RoleSyncCog.__init__ start | `role_sync/RoleSyncCog.py:44` | `daily_sync_task.start()` |
| TimedRolesCog.daily_reset_task | `role_system/timed_role/TimedRolesCog.py:92` | (minutes=1) |
| TimedRolesCog.check_expired_roles_task | `role_system/timed_role/TimedRolesCog.py:119` | (minutes=1) |
| TimedRolesCog.before_loop 双叠加 | `role_system/timed_role/TimedRolesCog.py:175-176` | 两个 task 共用 `before_timed_roles_tasks` |
| FashionCog.check_fashion_role_validity_task | `role_system/fashion/FashionCog.py:100` | (hours=24, body 是 `pass`) |
| RoleJukeboxCog.rotation_task | `role_system/role_jukebox/RoleJukeboxCog.py:312` | (seconds=1) |
| CreativeBattleCog.promotion_loop | `creative_battle/CreativeBattleCog.py:830` | (minutes=5) |
| CreativeBattleCog.cog_load | `creative_battle/CreativeBattleCog.py:202` | 注册 view + 启动 promotion_loop |
| HeartbeatInformationCog._start_heartbeat_task | `information/HeartbeatInformationCog.py:204` | 当前在 line 215-216 有 TODO + `return`，**未实际启动 tasks.loop** |
| ModelFanRolesCog.create_task (cache update 末尾) | `role_system/model_fan_roles/ModelFanRolesCog.py:111` | `_refresh_stats_background(guild_id)` |
| ModelFanRolesCog.create_task (view callback) | `role_system/model_fan_roles/ModelFanRolesCog.py:142` | `_refresh_stats_background(guild_id)` |
| 第一个 404 触发点 | `activity_tracker/views.py:188-193` | `check_activity_button` / `view_report_button` `interaction.response.defer` |

---

## 8. 行为分类摘要（启动期）

| 行为 | 谁负责 | 时机 | 启动期可见表现 |
|------|--------|------|-----------------|
| 21 cog 同步加载 | `main.py:99 cog_manager.load_all_enabled()` | `12:27:23.329 → 12:27:38.506` | 21 条"已加载子Cog: ..." |
| 21 cog 通过 `cog_load` → `await asyncio.sleep(1)` 注册到 CoreCog | `utility/feature_cog.py:54` | 每个 cog 加载后 ~1s | "功能模块 XXX 已成功注册到 CoreCog" |
| 21 个 slash 命令 sync 到 3 个 guild | `main.py:137` | `12:27:38.828 → 12:27:39.286` | "已同步 21 个命令到服务器 ..." × 3 |
| Gateway 连接 | d.py | `12:27:39.673` | "Shard ID None has connected to Gateway" |
| Guild chunk 等待 | d.py | `12:27:39 ~ 12:28:36` | **chunk 超时**（`12:28:36.919`）——这是为什么 ready 推迟 ~57s |
| HonorCog 的 synchronize (create_task) 命中 wait_until_ready 后跑 | `honor_system/HonorCog.py:166` | `12:28:36.945 → 12:28:37.070` | "HonorCog: 开始同步所有服务器的荣誉定义..." → "HonorCog: 荣誉定义同步完成。" |
| 5 个 on_ready listener 全部命中 | 见 §2 | `12:28:37.071 → 12:28:37.083` | 5 条 "已就绪" log（见 §6.1） |
| BackupCog auto_backup_task 首跑 | `core/role_backup_cog.py:165` | `12:28:37.533` | "开始执行自动 LIGHT 备份..." |
| RoleSyncCog daily_sync_task 首跑 | `role_sync/RoleSyncCog.py:149` | `12:28:37.593` | "开始执行每日身份组同步任务..." → 1ms 后"完成" |
| HonorExpirationCog expiration_check_loop 首跑 | `honor_system/honor_expiration_cog.py:206` | `12:28:37.595` | "正在执行 honor 过期检查..." |
| RoleJukebox rotation_task 1 秒 1 tick | `role_system/role_jukebox/RoleJukeboxCog.py:312` | `12:28:39.673` 起每秒 | 无 log（空配置无 actions） |
| CoreCog 5s sleep 后 `_update_all_caches_task.before_loop` | `core/CoreCog.py:218` `await asyncio.sleep(5)` | `12:28:45.673` | "CoreCog 已就绪，准备执行首次缓存更新..." |
| CoreCog `_update_all_caches_task` 主体 gather 5 个 FeatureCog | `core/CoreCog.py:88-91` | `12:28:45.786 → 12:28:48.932` | 5 个 "开始更新..." → "更新完毕" 对 |
| CoreCog `_backup_data_task` 主体 | `core/CoreCog.py:174` | `12:28:45.787 → 12:29:32.797` | "开始执行计划的数据备份任务..." → 43.85s 创建 zip → 3.14s send |
| ModelFanRolesCog `update_safe_roles_cache` 末尾派 `_refresh_stats_background` | `role_system/model_fan_roles/ModelFanRolesCog.py:111` | `12:28:47.092` 派 → `12:30:53.043` 真正进 executor | "刷新服务器 类脑ΟΔΥΣΣΕΙΑ 的模型身份组统计数据..." |
| WebSocket 落后 | d.py | `12:28:54.879` | "Can't keep up, shard ID None websocket is 33.1s behind." |
| TrackActivityCog.on_ready 命中（Redis check + start_buffer_flusher + create_task incremental_sync） | `activity_tracker/TrackActivityCog.py:391` | `12:28:55.864 → 12:28:55.865` | "成功连接到 Redis" / "后台 flush 任务已启动" / "Bot is ready. Creating startup incremental sync task..." |
| 第一个 404（用户按 ActivityRoleView 按钮） | `activity_tracker/views.py:193` | `12:28:58.093` | `discord.errors.NotFound: 404 ... Unknown interaction` |
| _incremental_sync_on_startup 派 _backfill_guild_history per guild（**注意**：sleep(1) 节流） | `activity_tracker/TrackActivityCog.py:663-669` | `12:29:00.031`（类脑ΟΔΥΣΣΕΙΑ）+ `12:29:01.121`（类脑ΙΛΙΑΣ） | "服务器 XXX 开始历史消息回填任务。内存锁已激活。" |
| 12h 自动备份发送完成 | `core/CoreCog.py:197` | `12:29:32.797` | "自动数据备份成功，文件已发送至频道 #🤖︱𝗕𝗢𝗧监控室。" |
| ModelFanRolesCog `_refresh_stats_background` 进 executor | `role_system/model_fan_roles/ModelFanRolesCog.py:168` | `12:30:53.043` | "刷新服务器 类脑ΟΔΥΣΣΕΙΑ 的模型身份组统计数据..." |
| 类脑ΙΛΙΑΣ backfill 完成 | `activity_tracker/TrackActivityCog.py:792` | `12:35:24.906` | "服务器 '类脑ΙΛΙΑΣ' 的回填任务结束，内存锁已释放。" |
| 类脑ΟΔΥΣΣΕΙΑ backfill 完成 | `activity_tracker/TrackActivityCog.py:792` | `12:35:42.391` | "服务器 '类脑ΟΔΥΣΣΕΙΑ' 的回填任务结束，内存锁已释放。" |

---

## 待讨论 / 后续

<!-- 此 section 故意留空，供后续讨论填写 -->