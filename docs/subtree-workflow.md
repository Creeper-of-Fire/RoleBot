# Subtree 工作流

本仓库通过 `git subtree` 让三个 bot 仓库各持有一份本地副本。**不要用 git submodule**（clone 体验差），**不要用 PyPI 包**（增加部署负担）。

## 核心原则：commit 全部保留，**不要 `--squash`**

`_shared` 仓库的每个 commit 都会作为**独立 commit**进入 bot 仓库 history。原 commit 的 subject、author、date 全部保留。

**原因**：
- 三个 bot 仓库的 `shared/` history 完整镜像 _shared，没有撕裂
- 出问题时能精确追溯到 _shared 上的具体 commit
- 不需要编造"sync"/"merge"型 commit message

**绝对不要 `--squash`**：squash 会把 _shared 的多个 commit 合并成一个 squash，丢失所有原始 commit 信息。

## 仓库关系

```text
_shared/  (本仓库，独立 repo，独立 history)
   │
   ├── subtree add  ──→ bot-A/shared/   (bot-A 完整接收 _shared 历史)
   ├── subtree pull ──→ bot-A/shared/   (新 commit 也完整接收)
   ├── subtree push ──→ _shared/        (bot 仓库的 commit 反向流入)
   ...
```

任何方向都允许。

## 首次接入

每个 bot 仓库只在第一次接 subtree 时跑一次 `add`，之后只需要 `pull` 或 `push`。

### 1. 创建远端 _shared 仓库

在 GitHub（或其他 Git 平台）创建一个独立仓库：

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

每个 bot 仓库都要跑一次。注意**不带 `--squash`**：

```powershell
cd <bot-repo>

# 先确保 working tree 干净
git status  # 必须干净

# 拉 _shared 的 main 分支，完整保留所有 commit
git subtree add --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main
```

重复对 `news_bot` 和 `odysseia_ticket_bot` 跑一遍。

完成后 bot 仓库 git log 会显示：

```text
Add 'shared/' from commit '<_shared-HEAD-hash>'     ← subtree 合入点
<... _shared 上每个原始 commit 都进来了 ...>
<bot 仓库原 HEAD>
```

## 日常：_shared 改了

```powershell
# 1. 在 _shared 改完并 commit（高频，可能是多次 commit）
cd D:\Dev\Workspace\discord-bots\_shared
git add .
git commit -m "fix PaginatedView edge case on empty items"
git push

# 2. 三个 bot 仓库各自 pull（不需要 squash）
cd ..\role_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main

cd ..\news_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main

cd ..\odysseia_ticket_bot
git subtree pull --prefix shared https://github.com/Creeper-of-Fire/discord-bot-shared.git main
```

每个 bot 仓库的 pull 都会把 `_shared` 上**从上次同步点到现在之间的所有 commit**作为独立 commits 拉进来。

## 日常：bot 仓库临时改了 shared/

```powershell
cd D:\Dev\Workspace\discord-bots\role_bot
# 改 shared/ui/paginated_view.py
git add shared
git commit -m "暂时在 RoleBot 加个分页补丁"
git push  # 推到 RoleBot 远端

# 反向推回 _shared（可选，但保留双向流动能力）
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

### "要不要给每个 bot 仓库都加一个 _shared 的 remote？"

不要。subtree 不是 submodule，不需要持续持有远端指针。pull 时显式指定远端 URL 即可。

### "bot 仓库里 shared/ 文件能不能直接编辑？"

能。subtree 的设计目标就是让文件看起来是本地的，编辑自由。push 是可选的。

### "pull 的时候冲突怎么办？"

罕见（除非 bot 仓库本地也改了 `shared/` 下同一文件，且 _shared 也改了）。如果冲突：

1. 解决冲突
2. `git add`
3. `git commit`（subtree 会自动接管剩下的 merge 步骤）

### "subtree commit 看着很丑（'Add shared/ from commit X'）"

是的，但这就是 subtree 的正常工作方式。message 含 _shared 的 HEAD commit hash，作为"同步溯源点"。**不要用 `-m` 改它**，改了会丢掉这个关键信息。

要查"这次拉了哪些 commit"，在 `_shared` 仓库跑 `git log <hash>..HEAD`（hash 就是 subtree merge commit message 里的那个）。

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