#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""远程容器内执行的脚本：跑 alembic 数据库迁移。

由 shared.scripts.deploy 通过 `docker compose run ... python3 /app/remote_deploy.py`
调用。

设计：
- 在容器内运行（CWD=/app）
- 自动发现所有 alembic.ini 配置并跑 `alembic upgrade head`
- 找不到 alembic.ini 时安静跳过（bot 没有数据库迁移需求）

每个 bot 仓库保留一个 5 行 wrapper：

    # bot_repo/remote_deploy.py
    from shared.scripts.remote_deploy import main
    main()
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CONTAINER_APP_DIR = Path("/app")


def run_command(command: list[str], cwd: Path | None = None, check: bool = True) -> int:
    """执行系统命令并实时打印输出。

    Args:
        command: 命令 + 参数列表。
        cwd: 命令工作目录。
        check: 非零退出码是否终止脚本（True → sys.exit(return_code)）。
    """
    print(f"▶️ Executing: {' '.join(command)}", flush=True)
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        while True:
            output = process.stdout.readline()
            if output == "" and process.poll() is not None:
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


def main() -> None:
    """主入口：发现并运行所有 alembic 迁移。"""
    print("--- [Remote Python Script] Starting Alembic migrations ---", flush=True)
    print(f"Working directory set to: {CONTAINER_APP_DIR}", flush=True)

    # 动态查找所有 alembic.ini（双保险：调用方也可以选择不调 main）
    alembic_configs = list(CONTAINER_APP_DIR.glob("**/alembic.ini"))

    if not alembic_configs:
        print("No alembic.ini files found, skipping migration.", flush=True)
    else:
        print("\n--- Running Alembic database migrations... ---", flush=True)
        for config_path in alembic_configs:
            workdir_rel_path = config_path.parent.relative_to(CONTAINER_APP_DIR)
            print(f"---> Found Alembic config in: {workdir_rel_path}", flush=True)
            run_command(["alembic", "upgrade", "head"], cwd=workdir_rel_path)
        print("All Alembic migrations completed.", flush=True)

    print("--- [Remote Python Script] Alembic migrations finished. ---", flush=True)


if __name__ == "__main__":
    main()