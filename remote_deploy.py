#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =======================================================
# RoleBot 远程部署执行脚本 (Python版)
# 职责：只负责在容器内运行数据库迁移（Alembic）
# =======================================================

import os
import subprocess
import sys
from pathlib import Path

# --- 配置 ---
# 这个路径现在是容器内部的路径，即你将服务器项目目录挂载到的容器内路径
CONTAINER_APP_DIR = Path("/app")  # 更改变量名以更明确其作用域
DOCKER_CONTAINER_SERVICE_NAME = "rolebot"  # 指向 docker-compose.yml 中的服务名


def run_command(command: list[str], cwd: Path = None, check: bool = True):
    """通用函数，用于执行系统命令并实时打印输出。"""
    print(f"▶️ Executing: {' '.join(command)}", flush=True)
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip(), flush=True)

        return_code = process.poll()

        if check and return_code != 0:
            print(f"❌ Command failed with exit code {return_code}", file=sys.stderr, flush=True)
            sys.exit(return_code)

        print(f"✅ Command successful.", flush=True)
        return return_code

    except FileNotFoundError:
        print(f"❌ Error: Command not found: {command[0]}", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr, flush=True)
        sys.exit(1)


def main():
    """部署脚本主逻辑"""
    print("--- [Remote Python Script] Starting Alembic migrations ---", flush=True)

    # 1. 确保在正确的容器内部目录中
    os.chdir(CONTAINER_APP_DIR)
    print(f"Working directory set to: {os.getcwd()}", flush=True)

    # 2. 动态查找并运行所有数据库迁移 (Alembic)
    print("\n--- Running Alembic database migrations... ---", flush=True)
    alembic_configs = list(CONTAINER_APP_DIR.glob("**/alembic.ini"))

    if not alembic_configs:
        print("No alembic.ini files found, skipping migration.", flush=True)
    else:
        for config_path in alembic_configs:
            # 获取相对于 /app 的路径，例如 "honor_system/alembic"
            workdir_rel_path = config_path.parent.relative_to(CONTAINER_APP_DIR)
            print(f"---> Found Alembic config in: {workdir_rel_path}", flush=True)

            # 直接在当前容器内执行 alembic 命令
            # 因为 remote_deploy.py 已经在这个容器内运行了，
            # 并且其工作目录已经设置到了 /app，
            # 所以 alembic 命令可以直接使用相对路径。
            run_command([
                "alembic", "upgrade", "head"
            ], cwd=workdir_rel_path)  # 设置 alembic 命令的工作目录

        print("All Alembic migrations completed.", flush=True)

    print("--- [Remote Python Script] Alembic migrations finished. ---", flush=True)


if __name__ == "__main__":
    main()