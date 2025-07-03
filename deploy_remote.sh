#!/bin/bash
# deploy_remote.sh
# 这是一个在远程服务器上执行的部署脚本

set -e # 任何命令失败立即退出，避免不一致状态

# --- 配置变量 (这些变量由外部脚本传递或在本地设置) ---
GITHUB_REPO="https://github.com/Creeper-of-Fire/RoleBot.git"
REMOTE_PROJECT_DIR="/root/RoleBot" # 机器人代码在服务器上的存放位置
CONTAINER_NAME="rolebot"           # docker-compose.yml 中定义的服务名
MAIN_BRANCH="master"                 # 你的GitHub仓库主分支名 (main 或 master)

echo "--- [Remote] 确保项目目录存在: $REMOTE_PROJECT_DIR ---"
mkdir -p "$REMOTE_PROJECT_DIR" || { echo "Error: Failed to create project directory."; exit 1; }
cd "$REMOTE_PROJECT_DIR" || { echo "Error: Failed to change directory to project root."; exit 1; }

echo "--- [Remote] 检查并更新代码 ---"
# 检查是否是一个Git仓库
if [ ! -d ".git" ]; then
    echo "--- [Remote] 项目不是一个Git仓库，正在清理目录并进行初次克隆... ---"
    # 彻底清空当前目录（包括隐藏文件和目录，但不包括 '.' 和 '..'）
    find . -maxdepth 1 -mindepth 1 -exec rm -rf {} + || true
    git clone "$GITHUB_REPO" . --branch "$MAIN_BRANCH" || { echo "Error: Git clone failed."; exit 1; }
else
    echo "--- [Remote] 项目已存在，正在拉取最新代码... ---"
    git fetch --all --tags || { echo "Error: Git fetch failed."; exit 1; }
    git checkout "$MAIN_BRANCH" || { echo "Error: Git checkout failed."; exit 1; }
    git reset --hard "origin/$MAIN_BRANCH" || { echo "Error: Git reset failed."; exit 1; }
    # 更简单的方式是 git pull，但 reset --hard 更能保证服务器代码与远程完全一致
    # git pull origin "$MAIN_BRANCH" || { echo "Error: Git pull failed."; exit 1; }
fi

echo "--- [Remote] 正在使用 Docker Compose 进行最小化停机部署... ---"

# --- 核心优化点 ---
# 1. 先构建新镜像 (如果需要)。--no-cache 确保我们总是从最新代码构建。
#    这一步在旧容器仍在运行时执行，不影响服务。
echo "--- [Remote] 正在后台构建新镜像 (无缓存)... ---"
docker-compose build --no-cache || { echo "Error: Docker Compose build failed."; exit 1; }

# 2. 然后用 'up' 命令来完成切换。
#    'up -d' 会自动停止并替换同名旧容器，这个切换过程非常快。
echo "--- [Remote] 正在启动新容器并替换旧容器... ---"
docker-compose up -d || { echo "Error: Docker Compose up failed."; exit 1; }

echo "--- [Remote] 正在清理无用的 Docker 镜像... ---"
# prune 命令现在可以安全地清理掉被新镜像替换下来的旧镜像
docker image prune -a -f || { echo "Error: Docker image prune failed."; exit 1; }

echo "--- [Remote] 部署完成！---"