# Subtree 工作流

本仓库通过 `git subtree` 让三个 bot 仓库各持有一份本地副本。**不要用 git submodule**（clone 体验差），**不要用 PyPI 包**（增加部署负担）。

## 仓库关系

```text
_shared/  (本仓库，独立 repo，独立 history)
   │
   ├── subtree pull ──→ bot-A/shared/   (bot-A 仓库 history 多一个 merge commit)
   ├── subtree pull ──→ bot-B/shared/
   └── subtree pull ──→ bot-C/shared/
```

任何方向都允许：

- _shared 改 → push → bot 仓库 pull
- bot 仓库的 shared/ 改 → push → _shared
- 三种流向都合法

## 首次接入

每个 bot 仓库只在第一次接 subtree 时跑一次 `add`，之后永远只需要 `pull` 或 `push`。

### 1. 创建远端 _shared 仓库

在 GitHub（或其他 Git 平台）创建一个独立仓库，例如：

```text
https://github.com/Creeper-of-Fire/discord-bot-shared.git
```

### 2. 把本地 _shared 推到远端

```powershell
cd D:\Dev\Workspace\discord-bots\_shared
git remote add origin https://github.com/Creeper-of-Fire/discord-bot-shared.git
git add -A
git commit -m "Initial shared infra: data + ui"
git push -u origin main
```

### 3. 在 bot 仓库接入

每个 bot 仓库都要跑一次：

```powershell
cd D:\Dev\Workspace\discord-bots\role_bot

# 先把当前所有未保存改动 commit 或 stash
git status  # 必须干净

# 拉 _shared 的 main 分支到本地 shared/ 目录
git subtree add --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main --squash
```

`--squash` 让 bot 仓库只多一个 merge commit，不会把 _shared 的所有 history 都灌进来。

重复对 `news_bot` 和 `odysseia_ticket_bot` 跑一遍。

## 日常：_shared 改了

```powershell
# 1. 在 _shared 改完并 commit
cd D:\Dev\Workspace\discord-bots\_shared
# 改 shared/ 下文件
git add shared
git commit -m "fix PaginatedView edge case on empty items"
git push

# 2. 三个 bot 仓库各自 pull
cd ..\role_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main --squash

cd ..\news_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main --squash

cd ..\odysseia_ticket_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main --squash
```

每个 bot 仓库自动产生一个 merge commit，**不需要写"sync"类 message**。

## 日常：bot 仓库临时改了 shared/

```powershell
cd D:\Dev\Workspace\discord-bots\role_bot
# 改 shared/ui/paginated_view.py
git add shared
git commit -m "暂时在 RoleBot 加个分页补丁"
git push  # 推到 RoleBot 远端

# 把这个改动同步到 _shared（可选）
git subtree push --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main
```

推送后 _shared 多一个 commit。其他 bot 仓库再各自 `subtree pull` 即可。

## 验证 fork 用户体验

在 Linux 容器（模拟 Linux fork 用户）：

```bash
git clone https://github.com/Creeper-of-Fire/RoleBot.git
cd RoleBot
python main.py
```

`shared/` 目录应该已经在那，不需要 `git submodule update`，不需要 `pip install` 任何包。

## 常见误区

### "要不要把所有 bot 仓库都设成 _shared 的 remote？"

不要。subtree 不是 submodule，不需要持续持有远端指针。pull 时显式指定远端 URL 即可。

### "bot 仓库里 shared/ 文件能不能直接编辑？"

能。subtree 的设计目标就是让文件看起来是本地的，编辑自由。push 是可选的。

### "pull 的时候冲突怎么办？"

罕见（除非两边都改了同一个文件）。如果冲突：

1. 解决冲突
2. `git add`
3. `git commit`（subtree 会自动接管剩下的 merge 步骤）

### "能不能反过来，bot 仓库改完不 push 回 _shared？"

可以。shared/ 在 bot 仓库就是普通文件，可以跟 _shared 不一致——只是下次 pull 会要求 merge。

## 跟 fork 用户的兼容性

- fork 用户 clone bot 仓库：`git clone <fork-url>`
- `shared/` 目录已经在仓库里，是普通文件
- 跑 `python main.py` 正常
- **不会**有任何 Git 警告或额外步骤要求

## 进一步演化

未来如果共享代码量很大（>5000 行），可以考虑：

- 发 PyPI 包（但要让 fork 用户零感知需要 wrapper 技巧）
- monorepo（但要等三个 bot 同步 release 节奏之后）

当前规模远未到那一步。subtree 工作流可以撑很久。