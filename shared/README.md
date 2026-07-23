# discord-bot-shared

> ⚠️ **本文件**位于 `_shared/` 仓库的根目录；通过 `git subtree --prefix shared` 被引入到当前 bot 仓库的 `shared/` 子目录中。它不是当前 bot 项目本身的文档——bot 项目说明请看仓库根目录的 `README`。

---

三个 Discord Bot 仓库（NewsBot / OdysseiaTicketBot / RoleBot）共享的基础设施代码的 source of truth。

不是 PyPI 包、不是 git submodule、不是 monorepo —— 只是一个版本化的独立仓库，配合 `git subtree` 让三个 bot 仓库各持有一份本地副本。

如果你看到 `shared/` 目录出现在了某 bot 仓库下，说明这个 bot 已经接入了 subtree。`shared/` 里的所有文件都是普通的本地副本，可以独立编辑（push 不会被自动同步，要双向流动请显式 `git subtree push`）。

## 设计目标

- fork 用户 clone bot 仓库后无需任何额外操作，`shared/` 目录就在那
- 共享代码的演化历史独立于任一 bot 仓库
- 双向流动：_shared 改了推 bot 仓库；bot 仓库临时改了也能推回 _shared
- 所有同步动作（pull / push）由 Git 自动 commit，无需人工写 sync 类型 commit

## 当前包含

- `data/json_manager.py` — `AsyncJsonDataManager` 等异步 JSON 数据持久化
- `ui/paginated_view.py` — `PaginatedView` 分页视图基类
- `ui/views.py` — `ConfirmationView` 通用确认按钮

## 不包含

- 各 bot 的 `main.py` / `config.py` / `config_data.py`
- 各 bot 的 `CoreCog`（职责已分叉，不强行抽象）
- 各 bot 的业务 Cog（honor、ticket、role_jukebox 等）
- deploy 脚本（各 bot 保留自己的 Dockerfile / docker-compose.yml）

## 工作流

详细见 `docs/subtree-workflow.md`。

### 首次接入（在每个 bot 仓库做一次）

```powershell
cd <bot-repo>
git subtree add --prefix shared <_shared-remote-url> main --squash
```

### _shared 改了，同步到 bot 仓库

```powershell
cd <bot-repo>
git subtree pull --prefix shared <_shared-remote-url> main --squash
```

### bot 仓库的 shared/ 改了，推回 _shared

```powershell
cd <bot-repo>
git subtree push --prefix shared <_shared-remote-url> main
```

## fork 用户

fork 用户 clone bot 仓库时，`shared/` 目录作为普通文件存在，跟其他代码无区别。无需 `git submodule init`、无需 `pip install`、无需任何额外步骤。

## 添加新的共享内容

1. 在 `_shared/` 仓库修改 / 新增文件
2. 三个 bot 仓库各自 `git subtree pull`
3. 在三个 bot 仓库把 import 路径从 `from utility.X import Y` 改为 `from shared.X import Y`