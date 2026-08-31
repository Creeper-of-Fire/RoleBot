# 创作大会系统 — 设计文档（简化版）

> 状态：简化版（unix 哲学 + if-else 投稿期 + per-faction 黑/白名单）
> 受众：开发者 / AI agent（dev doc）
> 用户拍板记录：见文末 §10

---

## 1. 这是什么

类脑社区每半年一次的「创作大会」：两阵营 PK，参赛者多者获胜。S1「秋冬大会」时间窗 **2026.9.1 - 2026.12.31**。

阵营意象按**抽象代号 A 组 / B 组**（具体意象由委员会在最终身份组设计阶段另定）。bot 不绑定具体意象（autumn / winter / etc.），全部走 `cfg.factions` 遍历。

### 1.1 bot 只做（unix 哲学：bot 只做最少的事，身份组发放/移除交给 honor 系统）

- **主入口面板**（发到 `promotion.main_channel_id`）：A 组 / B 组 **互斥**领取按钮
  - 用户已选 A 阵营后点 B → bot 拒绝（**不 remove** A，admin自己管）
- **分区投稿面板**（每个 `submission_channel_id` 各一个）：📨 投稿按钮
  - 点击弹 Modal（标题 + 描述）→ bot add contributor_role + 写 json + grant_honor
- **推广面板**定时 refresh（**仅投稿期内**——投稿期结束 → 停止更新）
- **投稿期判断 = if-else**：bot 读 toml 的 `start_date` / `end_date`，`now in [start_date, end_date]` 就接受投稿；否则拒绝。**不是状态机**。
- **投稿黑/白名单 per-faction**：每个阵营独立配置
  - `blacklist_role_ids`：持任一即拒（黑名单优先于白名单）
  - `whitelist_role_ids`：非空时持任一才允许
- **grant_honor 直接调**：投稿成功后 bot 直接调 honor 系统的 `grant_honor` 接口
  - uuid 在 toml 的 `contributor_honor_uuid` 配置（per-faction）
- **contributor_role 过期 = HonorExpirationCog 通用机制**（honor_system/honor_expiration_cog.py）：
  - **普通 honor**：honor toml `HonorDefinitionItem.expiration_date` 字段（pydantic 配置层，不入 SQLAlchemy db——honor 是永久记录，**只有身份组过期**）
  - **杯赛 honor**：cup_honors.json `CupHonorDetails.expiration_date` 字段（已有，杯赛专属 json 配置）
  - HonorExpirationCog.expiration_check_loop（24h 轮询）**合并遍历** toml + cup_honors.json → 到期推 `ExpiredHonorNoticeView` 到 `cfg.cup_honor.notification.channel_id` 频道
  - **admin 看到提醒后手动到 Discord remove contributor_role**——bot 不调 remove_roles
  - **关键架构**：db 是 honor 历史记录表，**不存过期时间**——过期时间是配置（toml / json），db 列加 `expiration_date` 是反模式（破坏性 schema + 真理撕裂 + 双真相）
- **撤销投稿**（admin 命令 `/合战丨核心 撤销投稿 submission_id`）：从 json 删除投稿记录
  - **不** remove contributor_role——admin 自己到 Discord remove
  - **不** 撤销已授予的 honor——admin 自行到 honor 系统处理
- **plain展示** 参赛人数（按组），**不做胜负判断**

### 1.2 bot 不做（按 `role_bot/AGENTS.md` 红线）

- ❌ 投票（"投票实际上就是不应该存在"——用户原话）
- ❌ 身份组设计（委员会手工，toml 预定义）
- ❌ forum 监听 / 扫描（漏事件 + 性能差）
- ❌ **身份组过期提醒 / 自动回收**（管理组手动；这版彻底删除 ver4 final 的 expire_check_loop）
- ❌ 批量 remove_roles（管理组手动）
- ❌ **winner_role 发放**——人工 7 天投稿 + 7 天投票流程，不进 bot（按原稿 2 拍板）
- ❌ 投稿管理 / 取缔资格（原稿没有"管理组"bot 角色）
- ❌ **状态机 pre_open / open / closed**——投稿期用 if-else 判断，不需要状态字段
- ❌ **状态机自动切换**（`season_loop`）——已经删除，bot 不再切状态
- ❌ 自动发面板（admin 手动 `/合战丨核心 发送面板`）
- ❌ 频道可见性配置（Discord 端管理组管）
- ❌ **游客领取面板**——这是 admin 自己的事，复用 honor claimable 模块即可（按用户最新拍板）

### 1.3 unix 哲学核心

bot 是**最小工具**：
- 投稿面板：每个分区一个，按钮触发投稿流程
- 主入口面板：A/B 互斥领取
- 撤销投稿：删 json 的最小操作
- **不**提供身份组回收、过期通知、状态机、胜负判断、最终身份组发放

admin / 委员会 负责：身份组设计、回收、winner_role 发放、最终身份组结果、胜负判断。

---

## 2. 体系架构

```
┌────────────────────────────────────────────────────────┐
│      CreativeBattleCog (主 cog)                          │
├────────────────────────────────────────────────────────┤
│ - /合战丨核心 发送面板 channel_key                        │
│     → admin 手动触发，发"按钮面板 + 推广 embed"            │
│ - /合战丨核心 撤销投稿 submission_id                     │
│     → 删 json（**不** remove role，**不** 撤销 honor）     │
│ - 投稿 Modal：用户填作品标题 + 描述                        │
│ - promotion_loop：每 N 分钟 edit 推广面板                  │
│     ★ 投稿期内（now in [start_date, end_date]）才更新     │
│ - cog_load：遍历 cfg.factions 注册 FactionPanelView        │
│ - _handle_join：A/B 互斥 + per-faction 黑/白名单           │
│ - _handle_submission：if-else 投稿期 + add role + grant_honor │
└────────────────────────────────────────────────────────┘
         ↓ toml 配置（admin 编辑）           ↓ honor 复用（bot 直接调）
┌────────────────────────────┐     ┌──────────────────────────────┐
│ data/creative_battle_       │     │ data/honor_{gid}.toml        │
│ {gid}.toml                  │     │ [[definitions]]              │
│ factions[*].contributor_honor_uuid 引用 honor 的 uuid  │
└────────────────────────────┘     └──────────────────────────────┘
         ↓ 运行时数据（per-season，永久累积）
┌──────────────────────────────────────────────────────────┐
│ data/creative_battles_{gid}_{season_id_safe}.json              │
│ 单赛季数据；json 文件永久保留                             │
└──────────────────────────────────────────────────────────┘
```

---

## 3. toml 配置

`data/creative_battle_{guild_id}.toml` —— admin 通过 `/合战丨配置` 命令组维护（仿 `HonorConfigCog`，复用 `_shared/config/toml_command.py`）。

### 3.1 完整示例

```toml
[meta]
season_label = "第零赛季 秋冬"
season_id = "S0-2026-autumn-winter"     # ★ 用作 json 文件名后缀
theme = "秋冬"
start_date = "2026-09-01"                # 投稿期开始日期（含）
end_date = "2026-12-31"                  # 投稿期结束日期（含）；now in [start, end] → 接受投稿

[promotion]
main_channel_id = 1134557553011998840    # 主入口频道（发 A/B 互斥面板）
refresh_minutes = 5                      # 投稿期内推广面板刷新频率
random_count_per_faction = 2             # 每个分区推广 random 展示几个投稿

[[factions]]
key = "faction_a"
display_name = "A 组"
emoji = "🅰️"
supporter_role_id = 111111111            # 支持者身份组（用户点 A 按钮后 bot add；互斥由 bot 拒绝实现，**不** remove）
contributor_role_id = 222222222          # ★ 参赛者身份组（投稿后 bot add；过期由 cup_honor 推提醒，admin 手动 remove）
contributor_role_expire_at = 2026-12-31T23:59:59+08:00   # ★ contributor_role 过期时间（unix 哲学：bot 不硬编码规则）
                                                       #   honor toml 对应 honor 的 cup_honor.expiration_date 必须配这个时间
                                                       #   到时 cup_honor.expiration_check_loop 自动推通知
submission_channel_id = 444444444        # A 组分区频道（发投稿面板）

# ★ 简化版新增：per-faction 黑/白名单（OO 一点，admin 自己配）
#   黑名单：持任一即拒；白名单：非空时持任一才允许；黑名单优先于白名单
blacklist_role_ids = []                  # 空 = 不限制；非空 = 持任一角色即拒绝加入/投稿 A
whitelist_role_ids = []                  # 空 = 不限制；非空 = 必须持任一角色才允许加入/投稿 A

# ★ 简化版新增：contributor_honor_uuid（per-faction，从 honor toml 引用）
#   投稿成功后 bot 调 grant_honor(member.id, contributor_honor_uuid)
#   留空（None）= 不 grant_honor
#   ⚠️ **强烈建议配成 cup_honor 类型**——这样 honor 的 cup_honor.expiration_date 会触发
#   cup_honor.expiration_check_loop 自动推过期提醒，admin 不用自己记到期时间
contributor_honor_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

[[factions]]
key = "faction_b"
display_name = "B 组"
emoji = "🅱️"
supporter_role_id = 555555555
contributor_role_id = 666666666
contributor_role_expire_at = 2026-12-31T23:59:59+08:00
submission_channel_id = 888888888
blacklist_role_ids = []
whitelist_role_ids = [123456789]         # 例：B 组要求白名单角色 123456789
contributor_honor_uuid = "11111111-2222-3333-4444-555555555555"

[notification]
channel_id = 999999999
admin_role_id = 000000000
```

### 3.2 pydantic schema

```python
# role_bot/creative_battle/creative_battle_models.py
import datetime as _dt
from typing import Annotated, Optional
from pydantic import BaseModel, Field, field_validator
from shared.config.toml_merge import TomlMergeAsTableList


class CreativeBattleMeta(BaseModel):
    season_label: str = Field(..., max_length=100)
    season_id: str = Field(..., max_length=100)
    theme: str = Field(..., max_length=50)
    start_date: _dt.date        # 投稿期开始（含）
    end_date: _dt.date          # 投稿期结束（含）

    @field_validator("season_id")
    @classmethod
    def _season_id_no_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("season_id 不能为空")
        return v

    @field_validator("end_date")
    @classmethod
    def _end_after_start(cls, v, info):
        start = info.data.get("start_date")
        if start and v <= start:
            raise ValueError("end_date 必须晚于 start_date")
        return v


class FactionConfig(BaseModel):
    key: str = Field(..., pattern=r"^[a-z][a-z0-9_]{0,30}$")
    display_name: str = Field(..., min_length=1, max_length=50)
    emoji: str = Field(..., min_length=1, max_length=64)
    supporter_role_id: int                # bot add；过期由 admin 手动管理
    submission_channel_id: Optional[int]  # 未配置则不发该组分区面板

    # ★ 简化版新增：per-faction 黑/白名单
    blacklist_role_ids: list[int] = Field(default_factory=list)  # 持任一即拒
    whitelist_role_ids: list[int] = Field(default_factory=list)  # 非空时持任一才允许

    # ★ 简化版新增：投稿成功后 grant_honor 的 UUID（从 honor toml 引用）
    contributor_honor_uuid: Optional[str] = None


class PromotionConfig(BaseModel):
    main_channel_id: Optional[int] = Field(None)
    refresh_minutes: int = Field(5, ge=1, le=1440)
    random_count_per_faction: int = Field(2, ge=1, le=10)


class NotificationConfig(BaseModel):
    channel_id: int
    admin_role_id: int


class CreativeBattleGuildConfig(BaseModel):
    enabled: bool = Field(False)
    meta: CreativeBattleMeta
    promotion: PromotionConfig = Field(default_factory=PromotionConfig)
    factions: Annotated[list[FactionConfig], TomlMergeAsTableList()] = Field(default_factory=list)
    notification: NotificationConfig

    @field_validator("factions")
    @classmethod
    def _exactly_two_factions(cls, v):
        if len(v) != 2:
            raise ValueError("创作大会必须恰好定义两个组（A/B）")
        keys = [f.key for f in v]
        if len(set(keys)) != 2:
            raise ValueError(f"组 key 必须唯一，发现重复：{keys}")
        return v
```

### 3.3 config manager

```python
# role_bot/creative_battle/creative_battle_config_manager.py
from pathlib import Path
from utility.cached_toml_config_manager import CachedTomlConfigManager


class CreativeBattleConfigManager(CachedTomlConfigManager[CreativeBattleGuildConfig]):
    """per-guild toml 配置 + cache + 单例。"""

    def __init__(self) -> None:
        super().__init__(
            data_dir=Path("data"),
            filename_pattern="creative_battle_{guild_id}.toml",
            model_class=CreativeBattleGuildConfig,
            doc_path=Path("docs") / "creative-battle-admin-doc.md",
        )
```

---

## 4. json 状态（per-season 独立文件）

### 4.1 文件命名

`data/creative_battles_{guild_id}_{season_id_safe}.json`

- `season_id_safe`：把 `season_id` 里非 `[A-Za-z0-9_.-]` 的字符替换成 `_`，长度截断到 80
- 同一 `(guild_id, season_id)` 不会重复建文件

### 4.2 数据结构（简化版：不维护 status 字段）

```python
# role_bot/creative_battle/creative_battle_season_models.py
import datetime as _dt
import uuid as _uuid_lib
from typing import Optional
from pydantic import BaseModel, Field


class ParticipantEntry(BaseModel):
    """支持者（点主入口面板'加入阵营'按钮）。"""
    user_id: int
    faction: str
    joined_at: _dt.datetime
    supporter_role_granted: bool = False


class SubmissionEntry(BaseModel):
    """参赛者（点投稿按钮 + 填 Modal）。"""
    submission_id: str = Field(default_factory=lambda: str(_uuid_lib.uuid4()))
    user_id: int
    faction: str
    title: str
    description: Optional[str] = None
    submitted_at: _dt.datetime = Field(default_factory=lambda: _dt.datetime.now())
    contributor_role_granted: bool = False       # bot 是否成功 add contributor_role
    honor_granted: bool = False                  # ★ 简化版新增：bot 是否成功 grant_honor（撤销投稿不撤销 honor）


class SeasonState(BaseModel):
    """一个赛季的运行时状态——永久保留。

    简化版**不维护 status / started_at / ended_at**——投稿期判断 = if-else。
    """
    season_id: str
    supporters: dict[int, ParticipantEntry] = Field(default_factory=dict)
    submissions: dict[str, SubmissionEntry] = Field(default_factory=dict)
    promotion_message_ids: dict[str, int] = Field(default_factory=dict)


class GuildSeasonData(BaseModel):
    guild_id: int
    season: SeasonState
```

---

## 5. 关键流程

### 5.1 主入口面板：A/B 互斥领取

```python
class MainPanelView(ui.View):
    """主入口面板：动态生成 A 组 / B 组互斥领取按钮。"""

    def __init__(self, cog, factions):
        super().__init__(timeout=None)
        for faction in factions:
            btn = ui.Button(
                label=f"{faction.emoji} 加入 {faction.display_name}",
                style=discord.ButtonStyle.primary,
                custom_id=f"cb:main:join:{faction.key}",
            )
            btn.callback = self._make_join_callback(faction.key)
            self.add_item(btn)

    def _make_join_callback(self, faction_key: str):
        async def _cb(interaction: discord.Interaction) -> None:
            await self.cog._handle_join(interaction, faction_key=faction_key)
        return _cb
```

### 5.2 `_handle_join` 流程（互斥 + 黑/白名单）

```python
async def _handle_join(self, interaction, faction_key):
    cfg = self.config_mgr.get(interaction.guild.id)
    if cfg is None or not cfg.enabled:
        await interaction.response.send_message("❌ 当前服务器未启用创作大会。", ephemeral=True)
        return

    faction = next((f for f in cfg.factions if f.key == faction_key), None)
    if faction is None:
        await interaction.response.send_message(f"❌ 阵营 '{faction_key}' 未配置。", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.response.send_message("❌ 找不到成员。", ephemeral=True)
        return

    # ★ 互斥检查：用户已持其他阵营 supporter_role → 拒绝（**不** remove）
    other_supporter_ids = {f.supporter_role_id for f in cfg.factions if f.key != faction.key}
    held_other = [r.id for r in member.roles if r.id in other_supporter_ids]
    if held_other:
        held_faction = next((f for f in cfg.factions if f.supporter_role_id in held_other), None)
        await interaction.response.send_message(
            f"❌ 你已在 {held_faction.emoji if held_faction else '其他'}"
            f"{held_faction.display_name if held_faction else '阵营'}。"
            f"如需更换阵营，请联系管理组手动移除原身份组后再点击。",
            ephemeral=True,
        )
        return

    # ★ 黑/白名单检查（per-faction，黑名单优先）
    reject_msg = self._check_blacklist_whitelist(member, faction)
    if reject_msg:
        await interaction.response.send_message(reject_msg, ephemeral=True)
        return

    # add supporter_role（幂等）
    await member.add_roles(faction.supporter_role_id, reason=f"创作大会 加入 {faction.display_name}")

    state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)
    state.season.supporters[interaction.user.id] = ParticipantEntry(
        user_id=interaction.user.id, faction=faction_key,
        joined_at=_dt.datetime.now(UTC8), supporter_role_granted=True,
    )
    await self.state_mgr.save_data(state)
    await interaction.response.send_message(f"✅ 你已加入 {faction.emoji} {faction.display_name}！", ephemeral=True)


def _check_blacklist_whitelist(self, member, faction):
    """per-faction 黑/白名单检查（unix 哲学：黑名单优先于白名单）。"""
    if faction.blacklist_role_ids and self._member_holds_any_role(member, faction.blacklist_role_ids):
        return f"❌ 你持有的某个身份组不允许加入/投稿 {faction.display_name}（黑名单）。"
    if faction.whitelist_role_ids and not self._member_holds_any_role(member, faction.whitelist_role_ids):
        return f"❌ 你未持有加入/投稿 {faction.display_name} 所需的身份组（白名单）。"
    return None
```

### 5.3 分区投稿面板 + Modal

```python
class FactionPanelView(ui.View):
    """分区面板：📨 投稿按钮。"""
    def __init__(self, cog, faction_key):
        super().__init__(timeout=None)
        submit_btn = ui.Button(label="📨 提交作品", style=discord.ButtonStyle.primary,
                               custom_id=f"cb:submit:{faction_key}")
        submit_btn.callback = self._on_submit_click
        self.add_item(submit_btn)


class SubmissionModal(ui.Modal, title="提交作品"):
    def __init__(self, cog, faction_key):
        super().__init__(title="提交作品")
        self.cog = cog
        self.faction_key = faction_key
        self.title_input = ui.TextInput(label="作品标题", max_length=100, required=True)
        self.description_input = ui.TextInput(label="作品描述 / 链接（可选）", max_length=500, required=False)
        self.add_item(self.title_input)
        self.add_item(self.description_input)

    async def on_submit(self, interaction):
        await self.cog._handle_submission(
            interaction, faction_key=self.faction_key,
            title=self.title_input.value, description=self.description_input.value or None,
        )
```

### 5.4 `_handle_submission` 流程（if-else + 黑/白名单 + add role + grant_honor）

```python
async def _handle_submission(self, interaction, faction_key, title, description):
    cfg = self.config_mgr.get(interaction.guild.id)
    if cfg is None or not cfg.enabled:
        await interaction.response.send_message("❌ 当前服务器未启用创作大会。", ephemeral=True)
        return

    # ★ 投稿期 if-else（**不是状态机**）
    today = _dt.datetime.now(UTC8).date()
    if not (cfg.meta.start_date <= today <= cfg.meta.end_date):
        await interaction.response.send_message(
            f"❌ 当前不是投稿期（投稿期：{cfg.meta.start_date} ~ {cfg.meta.end_date}）。",
            ephemeral=True,
        )
        return

    faction = next((f for f in cfg.factions if f.key == faction_key), None)
    if faction is None:
        await interaction.response.send_message(f"❌ 阵营 '{faction_key}' 未配置。", ephemeral=True)
        return

    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        await interaction.response.send_message("❌ 找不到成员。", ephemeral=True)
        return

    # 投稿者也走黑/白名单检查（per-faction）
    reject_msg = self._check_blacklist_whitelist(member, faction)
    if reject_msg:
        await interaction.response.send_message(reject_msg, ephemeral=True)
        return

    # 写 json（先写状态——即使后续 grant_honor 失败也保留提交记录）
    state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)
    submission = SubmissionEntry(
        user_id=interaction.user.id, faction=faction_key,
        title=title, description=description,
        submitted_at=_dt.datetime.now(UTC8),
        contributor_role_granted=False, honor_granted=False,
    )
    state.season.submissions[submission.submission_id] = submission
    await self.state_mgr.save_data(state)

    # add contributor_role
    await member.add_roles(faction.contributor_role_id, reason=f"创作大会投稿 {title[:30]}")
    submission.contributor_role_granted = True

    # ★ grant_honor（按 toml contributor_honor_uuid 配置；可选）
    if faction.contributor_honor_uuid:
        granted_def = self.honor_data_manager.grant_honor(member.id, faction.contributor_honor_uuid)
        if granted_def:
            submission.honor_granted = True

    await self.state_mgr.save_data(state)
    await interaction.response.send_message(f"✅ 作品已提交！\n标题：{title}", ephemeral=True)
```

### 5.5 撤销投稿（admin 命令）

```python
@admin_group.command(name="撤销投稿", description="从 json 删除指定投稿（**不** remove role——请到 Discord 手动 remove）")
@app_commands.describe(submission_id="投稿 UUID")
async def cmd_revoke_submission(self, interaction, submission_id):
    cfg = self.config_mgr.get(interaction.guild.id)
    state = self.state_mgr.ensure_season(interaction.guild.id, cfg.meta.season_id)
    sub = state.season.submissions.get(submission_id)
    if sub is None:
        await interaction.response.send_message(f"❌ 找不到 submission_id={submission_id}。", ephemeral=True)
        return

    # 撤销：只删 json entry，**不** remove contributor_role，**不** 撤 honor
    del state.season.submissions[submission_id]
    await self.state_mgr.save_data(state)

    await interaction.response.send_message(
        f"✅ 投稿已从 json 撤销（{submission_id[:8]}…，《{sub.title}》）。\n"
        f"⚠️ bot 没有 remove contributor_role——请到 Discord 手动 remove。\n"
        f"⚠️ bot 没有撤销 grant_honor——如需撤销请到 honor 系统处理。",
        ephemeral=True,
    )
```

### 5.6 推广面板（promotion_loop，**仅投稿期内**）

```python
@tasks.loop(minutes=5)
async def promotion_loop(self):
    today = _dt.datetime.now(UTC8).date()
    for guild_id in self._iter_configured_guild_ids():
        cfg = self.config_mgr.get(guild_id)
        if cfg is None or not cfg.enabled:
            continue
        # ★ if-else（不是状态机）：仅投稿期内刷新
        if not (cfg.meta.start_date <= today <= cfg.meta.end_date):
            continue

        state = self.state_mgr.ensure_season(guild_id, cfg.meta.season_id)
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            continue

        if cfg.promotion.main_channel_id:
            await self._refresh_promotion(guild, state, "main",
                cfg.promotion.main_channel_id, embed=self._build_main_embed(cfg, state.season))
        for faction in cfg.factions:
            if not faction.submission_channel_id:
                continue
            await self._refresh_promotion(guild, state, faction.key,
                faction.submission_channel_id, embed=self._build_faction_embed(cfg, state.season, faction))

        await self.state_mgr.save_data(state)
```

---

## 6. cog 命令清单

| 命令 | 触发 | 描述 |
|---|---|---|
| `/合战丨核心 发送面板 channel_key` | admin | 发按钮面板到对应频道（main=主入口；任意 faction key=分区投稿面板） |
| `/合战丨核心 撤销投稿 submission_id` | admin | 从 json 删除投稿（**不** remove role，**不** 撤 honor） |
| `/合战丨配置 下载配置` | admin | 下载当前 toml + SHA-256 |
| `/合战丨配置 上传配置` toml_file hash_str | admin | 上传修改后的 toml；本地有配置时必须粘 SHA-256 |
| `/合战丨配置 查看配置哈希` | admin | 仅查看 SHA-256 |

**简化版删除的命令**（ver3 / ver4 残留）：
- ❌ `/合战丨结束合战`——投稿期由 toml 时间自动判断，不需要结束命令
- ❌ `/合战丨投稿管理`——原稿没有"管理组"bot 角色

### 6.1 自动补全

```python
async def channel_key_autocomplete(self, interaction, current):
    cfg = self.config_mgr.get(interaction.guild.id)
    if cfg is None:
        return []
    options = ["main"] + [f.key for f in cfg.factions]
    return [
        app_commands.Choice(name=f"{opt}（{_describe(opt, cfg)}）", value=opt)
        for opt in options
        if not current or current.lower() in opt.lower()
    ]
```

### 6.2 cog_load（注册 view + 启动 tasks）

```python
async def cog_load(self):
    await super().cog_load()

    # ★ 遍历所有 guild 的 toml，注册对应 faction 的 view（不硬编码 faction key）
    for guild_id in iter_guild_ids_from_toml_files(Path("data"), "creative_battle_"):
        cfg = self.config_mgr.get(guild_id)
        if cfg is None:
            continue
        for faction in cfg.factions:
            self.bot.add_view(FactionPanelView(self, faction_key=faction.key))

    # 启动 tasks（**没有** season_loop / expire_check_loop——简化版已删除）
    self.promotion_loop.start()
```

---

## 7. 文件清单（简化版 — 5 个文件）

```
role_bot/creative_battle/
├── __init__.py
├── creative_battle_models.py              # toml pydantic schema（含 blacklist/whitelist/honor_uuid per faction）
├── creative_battle_config_manager.py      # CachedTomlConfigManager[T] 子类
├── creative_battle_season_models.py       # json pydantic schema（简化版：删 status / started_at / ended_at）
├── creative_battle_state_manager.py       # per-season 文件 manager
├── CreativeBattleCog.py                   # 主 cog：主入口 + 投稿面板 + 撤销投稿 + 互斥 + grant_honor
└── CreativeBattleConfigCog.py             # /合战丨配置 命令组（仿 HonorConfigCog）

role_bot/utility/
└── toml_filename_utils.py                 # 公用 utility
```

> **简化版 vs ver4 final 变化**：
> - ❌ 删除 `creative_battle_expire_notification_state_manager.py`
> - ❌ 删除 `CreativeBattleCogExpireView.py`
> - ❌ 删除 `CreativeBattleExpireLoop.py`
> - ✅ `creative_battle_models.py` 简化（删 expire_at × 3 + winner_role_id，加 blacklist/whitelist/honor_uuid）
> - ✅ `creative_battle_season_models.py` 简化（删 status / started_at / ended_at，加 honor_granted）
> - ✅ `CreativeBattleCog.py` 重写（删 season_loop / expire_check_loop / _close_season / 撤销投稿命令 / 互斥逻辑 / grant_honor）

---

## 8. 与现有系统集成

### 8.1 永久贡献者荣誉 → bot 直接调 `grant_honor`

> **简化版拍板**：bot 在投稿成功后**直接调** honor 系统的 `grant_honor` 接口。
> uuid 在 toml 的 `factions[*].contributor_honor_uuid` 配置（per-faction，从 honor toml 引用）。

**前置步骤**：admin 在 `data/honor_{guild_id}.toml` 加 `[[definitions]]`：

```toml
[[definitions]]
uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"  # = creative_battle toml faction_a.contributor_honor_uuid
name = "🍃 A 组之贡献者"
description = "参与创作大会·A 组投稿"
role_id = 222222222            # = creative_battle toml faction_a 期望的 contributor_role_id
hidden_until_earned = true
# 注意：simplified 版不需要 role_sync_honor——bot 直接 grant_honor，不靠 role 反向 sync
```

**bot 行为**：投稿成功后
```python
if faction.contributor_honor_uuid:
    granted_def = self.honor_data_manager.grant_honor(member.id, faction.contributor_honor_uuid)
```

**失败处理**：`grant_honor` 失败（UUID 不存在、DB 错误等）不阻断投稿流程——
admin 可后续通过 `/合战丨核心 撤销投稿` 撤销投稿，或直接到 honor 系统手动补。

### 8.2 最终身份组（winner_role）→ 人工 7+7 流程，bot 不参与

> 跟简化版无关——这版彻底删除了 winner_role 相关字段。

按原稿 2 拍板，最终身份组的获取流程是 **人工的 7 天自由投稿 + 7 天投票 + 设计要求审核**。
**bot 不参与**——不会自动给获胜方 supporter add winner_role。admin 看 json 决定胜负方后
走 7+7 人工流程，最终设计确定后手动到 Discord 给获胜方所有 supporter add winner_role。

### 8.3 主面板集成

`CreativeBattleCog` 继承 `FeatureCog`，`get_main_panel_entries()` 返回 `None`
（主面板按钮留 v2——按 design doc 推迟）。

### 8.4 备份 / 还原

- toml 走 `/上传配置` + discord 备份 zip
- per-season json 是 docker volume 文件，备份机制由 `BACKUP_CHANNEL_ID` 12h zip 兜底
- json 历史只增不改——损坏时 zip 备份能恢复更多

---

## 9. 风险与回滚

- **按钮事件丢失**：persistent view 自动重连，cog_load 时已注册
- **per-season json 损坏**：备份 zip 兜底
- **角色权限不足**：admin 手动调整 `manage_roles` 权限
- **Modal submit 失败**：`add_roles` 抛 `discord.Forbidden` 时记 warning，json 仍写（标记 `contributor_role_granted=False`）
- **`grant_honor` 失败**：记 warning，json 标记 `honor_granted=False`，不阻断投稿
- **推广消息被删**：下次 loop 检测到 NotFound 重发

---

## 10. 用户拍板记录

| # | 决策 | 拍板时间 |
|---|---|---|
| 1 | 模块位置：独立 cog（`role_bot/creative_battle/`），不混入 honor_system | 2026-08-28 |
| 2 | 活动名改为「创作大会」（ver3 文档） | 2026-08-28 |
| 3 | 时间窗：2026.9.1 - 2026.12.31（按推广文案原稿 2） | 2026-08-28 |
| 4 | 不做 forum 监听 / 扫描（漏事件 + 性能差） | 2026-08-28 |
| 5 | 投稿走 Modal 表单（用户主动填）+ 投稿数据由按钮面板承载 | 2026-08-28 |
| 6 | 不做胜负逻辑判断——plain展示，admin 自己看 + 自己决策 | 2026-08-28 |
| 7 | 删管理组投稿管理面板 + 取缔资格（原稿没有"管理组"bot 角色） | 2026-08-28 |
| 8 | 永久贡献者荣誉走 honor 系统（v3 用 role_sync_honor=true + 批量 sync） | 2026-08-28 |
| 9 | 最终身份组 → 人工 7 天投稿 + 7 天投票，bot 不参与 | 2026-08-28 |
| 10 | per-season 独立 json + 赛季永久保留 | 2026-08-28 |
| 11 | HonorCog 加 UUID 生成命令 | 2026-08-28 |
| 12 | 改 `role_bot/AGENTS.md`（bot 设计原则：可组合性 + 模块边界决策 + unix 哲学） | 2026-08-28 |
| 13 | 推广入口：分区频道（per faction + 可选 main）+ 单一持久 embed | 2026-08-28 |
| 14 | 单一 `发送面板` 命令 + channel_key 参数（自动补全） | 2026-08-28 |
| 15 | 模板代码（re.match 文件名 pattern）抽 utility | 2026-08-28 |
| 16 | bot 不硬编码任何 faction key——遍历 `cfg.factions` | 2026-08-28 |
| 17 | 自动补全必须有 description | 2026-08-28 |
| 18 | 身份组回收 = 管理组手动 | 2026-08-28 |
| 19 | HonorCog 加 `/荣誉头衔丨核心 同步角色荣誉` 批量命令 | 2026-08-28 |
| 20 | 取消 `contributor_honor_uuid` 字段——honor 系统靠 `role_sync_honor=true` 标记 | 2026-08-28 |
| 21 | ver4：投稿期按 `start_date` / `end_date` 自动切状态机 `pre_open → open → closed` | 2026-08-28 |
| 22 | ver4：admin 手动 `/合战丨发送面板` 发面板，bot 不自动发 | 2026-08-28 |
| 23 | ver4：投稿期结束 → bot 停止更新推广面板 | 2026-08-28 |
| 24 | ver4：失败方 contributor_role 延后 `loser_contributor_keep_months` 月回收（bot 自动） | 2026-08-28 |
| 25 | ver4：bot 不管面板自身生命周期 | 2026-08-28 |
| 26 | ver4：阵营意象按抽象代号（A 组 / B 组） | 2026-08-28 |
| 27 | ver4 final（unix 哲学）：删 `loser_contributor_keep_months`，加 `expire_at` × 3 字段 | 2026-08-29 |
| **28** | **简化版**：投稿期判断 = **if-else**，**不是状态机**（`start_date <= today <= end_date`） | 2026-08-29 |
| **29** | **简化版**：删 `season_loop` / `expire_check_loop` / `_close_season` / `_announce_season_close` / `_check_guild_for_expired_roles` / 全部 `expire_at` 字段 | 2026-08-29 |
| **30** | **简化版**：删 `winner_role_id`（最终身份组 bot 完全不参与） | 2026-08-29 |
| **31** | **简化版**：删 `supporter_role` 单独管理——主入口面板管"加入阵营" | 2026-08-29 |
| **32** | **简化版**：bot **直接调** `grant_honor`（per-faction `contributor_honor_uuid`），不再依赖 `role_sync_honor=true` 反向 sync | 2026-08-29 |
| **33** | **简化版**：删"管理组" / "5 个身份组过期提醒" / "自动 remove" 整套机制 | 2026-08-29 |
| **34** | **简化版**：互斥 = 拒绝加入（bot **不 remove** 旧身份组） | 2026-08-29 |
| **35** | **简化版**：黑/白名单 **per-faction**（OO 一点，黑名单优先） | 2026-08-29 |
| **36** | **简化版**：撤销投稿（admin 命令）= 删 json + **不** remove role + **不** 撤销 honor | 2026-08-29 |
| **37** | **简化版**：bot 只发**两个面板**——主入口（互斥领取）+ 投稿面板（每分区一个）。**不**做游客领取面板——游客由 admin 复用 honor claimable 模块 | 2026-08-29 |

---

## 11. 未来可能的想法（**不实现**，仅记录）

> 本节是设计抽象的**潜在方向**，按当前简化版原则（unix 哲学 + KISS）**不实施**。
> 触发实现的信号：≥2 个不同的 cog 出现相同的模式时再考虑抽象。

### 11.1 "黑/白名单 + grant_honor + 提醒"通用组件

**观察**：本设计里多个流程有内在共性——

| 流程 | 触发 | 检查 | 动作 |
|---|---|---|---|
| 加入阵营 | 主入口按钮 | 互斥 + 黑/白名单 | `add_roles` |
| 投稿 | 分区按钮 | 黑/白名单 + 投稿期 if-else | `add_roles` + `grant_honor` |
| （未来）其他荣誉领取 | 任意按钮 | 黑/白名单 | `grant_honor` |

**潜在抽象**：抽成一个"领取按钮"通用组件，任何 `button` / `View` 可以"attach"它，组件统一负责：
- 黑/白名单检查
- `grant_honor` 调用
- 拒绝原因 + 成功 embed

类似模式：

```python
# 伪代码——不实施
class ClaimableButton(ui.Button):
    def __init__(self, *, blacklist, whitelist, honor_uuid, ...):
        ...
    async def callback(self, interaction):
        if reject := self.check_black_white(interaction.user):
            await interaction.response.send_message(reject, ephemeral=True)
            return
        if self.honor_uuid:
            self.data_manager.grant_honor(interaction.user.id, self.honor_uuid)
```

**为何不实施**：

- **抽象层级不好定**：互斥、投稿、领取是三个不同动作，参数差异大（投稿需要 Modal，互斥需要动态算"对方 supporter"）
- **装饰器模式需要规定参数**：`@check_blacklist(...)` 必须传 black/white 列表，耦合度变高
- **当前只有一个使用场景**（创作大会），抽象收益 = 0，复杂度反而上升

**触发重构的信号**：第二个 cog 也需要"黑/白名单 + grant_honor"流程时（比如未来有"勋章领取面板"），再考虑抽。

### 11.2 互斥 vs 黑名单的等价性

**观察**：本设计的"用户已选 A 后点 B → bot 拒绝"逻辑，**本质上**是"对方阵营 supporter_role 在黑名单里"。如果把"其他阵营的 supporter_role_id"动态加到本阵营的 `blacklist_role_ids`，互斥检查和黑名单检查可以走同一路径。

**当前实现**：`_handle_join` 里显式检查 `other_supporter_role_ids`（通过 `_other_supporter_role_ids()` 辅助函数），独立于 `_check_blacklist_whitelist()` 的黑/白名单逻辑。

```python
# 当前——两套独立检查
other_supporter_ids = {f.supporter_role_id for f in cfg.factions if f.key != faction.key}
if held_other: return "已在 X 阵营"  # 互斥

reject_msg = self._check_blacklist_whitelist(member, faction)  # 黑/白名单
if reject_msg: return reject_msg
```

**潜在优化**：在 `FactionConfig` 加载或 `_handle_join` 里，把 `other_supporter_ids` 临时扩展进 `faction.blacklist_role_ids` 再调 `_check_blacklist_whitelist`。

**为何不实施**：

- **toml 字段语义混淆**：admin 看 toml 里的 `blacklist_role_ids`，不知道里面是否被 bot 自动注入了"其他阵营 supporter_role"
- **互斥语义独立清晰**：admin doc §3.4 "已选 A 后想换 B 怎么办"明确解释互斥——和黑名单概念分开讲更好懂
- **实现已经够清晰**：当前两套独立路径加起来才 30 行代码，重构收益低


**观察**：本设计之前考虑过两种方式给 contributor_role 加"过期 + 推提醒"：

| 方案 | 维护方 | 状态 |
|---|---|---|
| creative_battle 自建 `expire_check_loop`（ver4 final） | creative_battle | 删了（ver4 final 残留被本轮清理） |
| ~~委托 cup_honor：honor toml 加 `cup_honor.expiration_date`~~ | ~~cup_honor 模块~~ | ❌ 错——cup_honor 是杯赛专用，不应被复用为通用机制 |
| **HonorExpirationCog 通用机制**：honor toml 加 `expiration_date` 字段（不限 cup_honor） | HonorExpirationCog | **当前采用** |

**正确架构**（用户拍板）：

- **HonorCog**：通用 honor CRUD（grant / revoke / 批量 sync role ↔ honor）
- **CupHonorModuleCog**：杯赛 honor 的特殊逻辑（**只管杯赛**，不应该被复用为通用机制）
- **HonorExpirationCog**：通用 honor 身份组过期机制——`expiration_check_loop`（24h 轮询）+ `ExpiredHonorNoticeView` 推提醒（**合并处理** honor toml `HonorDefinitionItem.expiration_date` + cup_honors.json `CupHonorDetails.expiration_date`；**db 不存过期**）

**优势**：

- 不污染 HonorCog（保持通用 honor CRUD 干净）
- 不复用 cup_honor（避免架构错误）
- creative_battle 不用自己写过期逻辑——honor toml 配 expiration_date 即可
- admin 改 toml 的 expiration_date 即可调整过期时间

### 11.3 已被撤销（"A → B 升级"反设计命令）

**说明**：早期文档提出"给持有 honor_from 的成员同时 grant_honor(honor_to) + add_role(role_to)"的命令，命名 `/荣誉头衔丨核心 upgrade`。

**撤销理由**（2026-08-31 用户拍板）：
- **honor 是永久记录**——`upgrade` 命令的语义"升级到永久"逻辑错误
- **身份组才是会过期的**——所谓"升级"实际上是"新增另一组（永久）honor + 另一组（临时）role"，这是普通 grant + add role 命令能做到的事
- **不应该有专门的命令**——管理员用 `/荣誉头衔丨管理 授予` / `批量授予` 已能完全替代

### 11.4 通用 honor 身份组过期机制（**已实现 2026-08-31**）

**观察**：bot 不调 remove_roles（unix 哲学），但 admin 需要在身份组过期时收到提醒——以便手动 remove。

**机制**（HonorExpirationCog，honor_system/honor_expiration_cog.py）：
- `HonorExpirationCog.expiration_check_loop` 24h 轮询
- **合并遍历两个配置源**（**不**读 db）：
  - 普通 honor：`HonorConfigManager.get(guild_id).definitions[i].expiration_date`（honor toml pydantic 字段，2026-08-31 新加）
  - 杯赛 honor：`CupHonorJsonManager.get_all_cup_honors()[i].cup_honor.expiration_date`（cup_honors.json，杯赛专属 json 配置）
- 到期 → 推 `ExpiredHonorNoticeView` 到 `cfg.cup_honor.notification.channel_id` 频道（每个 guild 单独推）
- 防重复通知：`NotificationStateManager`（cup_honor 已有，json 存已通知列表，全局共享）

**关键架构决策**（2026-08-31 用户拍板）：
- **db 不存过期时间**——SQLAlchemy `HonorDefinition` 表是 honor 历史记录，**没有** `expiration_date` 列
- **过期时间是配置**——只在 toml / json 里，不入 db（避免破坏性 schema + 真理撕裂 + 双真相）
- **cup_honor自己不再有循环**——cup_honor_module.py 撤了 `expiration_check_loop` + `ExpiredHonorNoticeView` + `_perform_expiration_check`（合并到 HonorExpirationCog）

**View 字限控制**：
- embed **不列 user list**（创作大会 contributor 持有人多，列出会爆 Discord 1024 char 字段限制）
- 只给 summary（"N 个成员仍持有 role"）
- `refill_by_role` 按钮：扫 role 持有者 → grant_honor(uuid)（按需触发，不预设列表）

**admin doc**：见 `creative-battle-admin-doc.md` §5。
