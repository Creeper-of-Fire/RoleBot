# embed_guides 使用手册

本手册面向管理员（通常丢给 AI 助手使用），覆盖 Discord bot 中"幻化 / 自助身份组 / 荣誉活动"三类引导文案的 toml 配置。

## 概述

`embed_guides_{guild_id}.toml` 是 **per-guild 单文件**（跟 `honor_{guild_id}.toml` 同 pattern），每个 guild 独立一份：

```
data/embed_guides_1134557553011998840.toml      ← 类脑
data/embed_guides_1265862009673486408.toml      ← 神人研究所（如果配置了）
data/honor_1134557553011998840.toml
data/honor_1265862009673486408.toml
...
```

每个文件里 3 个 section（共用同一份 schema）：

- **`[fashion_guide]`**：幻化衣橱面板顶部展示的入门指引（用户点主面板的"幻化衣橱"按钮看到的 embed）
- **`[self_service_guide]`**：自助身份组面板的入门指引（"为避免频繁@全体成员..."那段）
- **`[honor_celebrate_guide]`**：荣誉活动面板里第二条 embed（"🎊 当前进行中的荣誉获取活动"）

通过 Discord 命令 `/指引文案丨配置` 下的子命令组操作 toml（下载/上传/查看哈希）。

> ⚠️ **历史背景**：旧版用 Discord 帖子做文案存储（`/配置embed链接` 命令配 URL → bot 拉 Discord 消息 → 15 分钟刷新一次）。新版直接 toml 写文案 + Discord 的上传命令做热重载——文案与 Discord 帖子解耦，bot 不再依赖 Discord 帖子可访问。

> ⚠️ **跟 honor toml 同 pattern**：本 toml **不是**全局单文件。每个 guild 独立一份，跟 `honor_{guild_id}.toml` 完全对称。`TomlConfigManager` 的 API 把 `guild_id` 强制要求（详见 `shared/docs/toml-config-design.md` 的 "toml = per-guild" 红线）。

> ⚠️ **跳转链接已删除**：之前三个面板里有"跳转到原帖"按钮（点过去看 Discord 帖子）。新版以 toml 文案为 source of truth，没有 Discord 帖子可跳——按钮去掉。如果将来真的需要"看完整版"，可以在 section 里加一个可选 `reference_url` 字段（目前未实现）。

## 字段详解

每个 section 用同一个 schema：

```toml
[fashion_guide]
title = "👗 幻化身份入门指引"
content = """
先看这里！幻化身份组是 Discord 身份组层级的产物——
拥有"基础身份组"才能选择对应的"幻化身份组"。
幻化身份组在服务器设置里要放在基础身份组**上面**才能覆盖颜色。
"""
color = 0xFFA500  # Optional；默认 0xFFA500 (orange)
```

| 字段 | 必填 | 类型 | 说明 |
|------|------|------|------|
| `title` | ❌ | str (≤256) | embed 标题。省略 → 用默认（`"指引加载中"`） |
| `content` | ❌ | str | embed 正文（description），支持 Discord markdown。省略 → 用默认（"管理员尚未配置此指引，或指引正在加载中。"） |
| `color` | ❌ | int (hex) | embed 颜色。例：`0xFFA500` (orange)、`0x3498DB` (blue)。省略 → 默认 `0xFFA500` |

**所有字段都有默认值**——admin 可以只覆盖一两个字段，其它字段用默认。整份 toml 缺失或某个 section 缺失时，cog 拿到的也是有效 `EmbedGuideSection` 实例，直接 `cfg.fashion_guide.to_embed()` 即可，**不需要 None 检查**。

## 常用操作

### 修改某个 guild 的某个 guide 文案

```powershell
# 1. 下载当前 toml
/指引文案丨配置 下载配置

# 2. 编辑 embed_guides_<guild_id>.toml（用编辑器 / 丢给 AI）

# 3. 上传修改后的 toml
/指引文案丨配置 上传配置
file: embed_guides_1134557553011998840.toml
hash_str: <step 1 给的 SHA-256 前 12 字符>
```

> ⚠️ **首次上传**（本地还没有 toml）`hash_str` 可省略——会弹二次确认按钮。
> **后续上传**必须提供 `hash_str`，否则 bot 拒绝（防 TOCTOU 覆盖）。
> 如果忘记 / 粘错 / 编辑期间别人更新过，重新 `/下载配置` 拿新 hash。

### 只覆盖 title（content 留默认）

```toml
[fashion_guide]
title = "👗 幻化必读"
# content 字段省略 → 用默认文案
```

这样能改标题但保持默认 body。适合小修小补。

### 删除某个 guide

直接把 toml 里的 `[xxx_guide]` section 整段删掉就行。下次访问 `cfg.xxx_guide` 会用默认 `EmbedGuideSection`（cog 直接 `.to_embed()` 即可，无 None 检查）。

### 添加新 guide 类型（**这不是你（管理员）能改的**）

如果你需要一种新的指引文案类型（比如欢迎语、活动公告、规则速览等），**这不是 Discord 管理员能独立完成的**——它需要改 BOT 代码 + 部署新版本（你只有 toml 的修改权限，没有 repo / 服务器密钥 / 部署权限）。

正确的做法：

1. **联系 BOT 维护者**，描述需求：
   - 用途（什么场景下展示这个文案）
   - 大致内容 / 调性
   - 哪个 cog 需要这个 guide（用于命名 section key）
2. BOT 维护者会：
   - 在 `core/embed_guides/embed_guides_models.py` 的 `EmbedGuidesConfig` 加 `xxx_guide: EmbedGuideSection = Field(default_factory=...)`
   - 在同文件顶部加一个 `_default_xxx_section()` 辅助函数
   - 在新 cog 里读：`EmbedGuidesConfigManager.get_instance().get(guild_id).xxx_guide.to_embed()`
   - 部署新版本
3. **部署完成后**，你才能在 `/指引文案丨配置` 看到新 section 并配置它

> 注：现有 3 个 guide（`fashion_guide` / `self_service_guide` / `honor_celebrate_guide`）已经覆盖核心场景。如果只是想调整文案或删除某个 section，按上面"修改文案"/"删除某个 guide"做就行，不要联系 BOT 维护者。

## 注意事项

- **三 section 是同一个 pydantic 类型**——加新字段时三 section 都会受影响（`title` / `content` / `color`）。
- **Section key 命名约定**：`{cog_name}_guide`，跟 cog 在 Discord 的命令分组保持一致，方便 debug 时一眼对应。
- **重启 bot 之后**：cache 失效（`HonorConfigManager._cache` 跟 `EmbedGuidesConfigManager._cache` 都跟进程绑定）。下次访问会重新从磁盘读，**但 toml 已经在 data 目录**——不会丢数据。
- **跟 honor toml 完全独立**：本 toml 只管文案，不管身份组 / 数据。修改引导文案**不会**触发 HonorCog 的 DB 同步（不像 honor toml 上传后会 `synchronize_all_honor_definitions`）。
- **每个 guild 一份 toml**：要改两个 guild 的引导文案就得 `/上传配置` 两次——这是 per-guild 的代价，换来的是 guild 间不会互相污染 + 跟 honor toml 同样的设计契约。