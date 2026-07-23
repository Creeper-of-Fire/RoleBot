#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RoleBot 一键部署脚本（Python 版）

行为对齐原 deploy.ps1，差异：
- 打包使用标准库 zipfile（不依赖 7-Zip）。
- SSH heredoc 通过 ``subprocess.run(..., input=text.encode("utf-8"))`` 传入，
  根除 deploy.ps1 的 UTF-8 BOM 让远程 bash 误把 BOM 当命令名的 bug。
- 默认遇到错误即终止（与 ``$ErrorActionPreference = "Stop"`` 等价）。

用法：
    python deploy.py            # 仅部署，不跟踪日志
    python deploy.py --follow   # 部署完后实时跟踪容器日志
"""
from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

from rich.console import Console


ROOT_DIR = Path(__file__).resolve().parent
DOCKER_CONTAINER_NAME = "rolebot"
REMOTE_PROJECT_NAME = "RoleBot"


# --- 输出辅助（rich.console 统一处理颜色与 Windows VT） ---------------

# force_terminal=None 时 rich 自动探测：交互终端走彩色、重定向走 plain，
# 与 deploy.ps1 的 "Write-Host -ForegroundColor" 行为一致。
_CONSOLE = Console(highlight=False)
_ERR_CONSOLE = Console(stderr=True, highlight=False)


def info(text: str) -> None:
    _CONSOLE.print(f"[cyan]{text}[/cyan]")


def warn(text: str) -> None:
    _CONSOLE.print(f"[yellow]{text}[/yellow]")


def ok(text: str) -> None:
    _CONSOLE.print(f"[green]{text}[/green]")


def err(text: str) -> None:
    _ERR_CONSOLE.print(f"[red]{text}[/red]")


def die(text: str, code: int = 1) -> None:
    err(f"❌ {text}")
    sys.exit(code)


# --- 1. 加载配置 ------------------------------------------------------

def load_env(path: Path) -> dict[str, str]:
    if not path.exists():
        die(f"无法读取 '{path}' 文件。请确保它存在且格式正确。")
    cfg: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        cfg[key.strip()] = value.strip()
    return cfg


# --- 2. 本地文件检查 --------------------------------------------------

def check_local(ssh_key_path: str, compose_path: Path) -> None:
    info("🔍 正在检查本地SSH私钥和Docker Compose文件...")
    if not Path(ssh_key_path).exists():
        die(
            f"SSH 私钥文件未在 '{ssh_key_path}' 找到。\n"
            "   请检查 deploy.env 中的 SSH_PRIVATE_KEY_PATH 配置。"
        )
    if not compose_path.exists():
        die(f"必需的 'docker-compose.yml' 文件不存在于当前目录：{compose_path}")
    ok("✅ 本地文件检查通过。")


# --- 3. 打包本地项目文件 ---------------------------------------------

EXCLUDE_PATTERNS = {
    "*.pyc", "__pycache__", ".git", ".gitignore", ".venv", ".idea",
    "*.db", "deploy.env",
    "deploy.py", "deploy.ps1", "fetch_log.ps1",
    "*.zip", "*.log", "*.tmp",
}


def _is_excluded(rel_path: str) -> bool:
    parts = rel_path.split("/")
    # 既要在每一个路径片段里查，也要在完整路径里查（兼容 *.zip 这类模式）
    for part in parts:
        for pat in EXCLUDE_PATTERNS:
            if Path(part).match(pat):
                return True
    name = rel_path.rsplit("/", 1)[-1]
    for pat in EXCLUDE_PATTERNS:
        if name == pat or Path(name).match(pat):
            return True
    return False


def package_project(root: Path) -> Path:
    info("📦 正在打包本地项目文件...")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"rolebot_deploy_{timestamp}.zip"
    zip_path = root / zip_name

    file_count = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if _is_excluded(rel):
                continue
            # zipfile.write 自动写到 zip 里的正斜杠路径，无需手动转换
            zf.write(path, arcname=rel)
            file_count += 1

    ok(f"✅ 项目文件打包成功（{file_count} 个文件 → {zip_path.name}）。")
    return zip_path


# --- 4. 传输压缩包 ----------------------------------------------------

def run(cmd: list[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """统一的 subprocess 调用。check=True 时非零退出码抛出。"""
    _CONSOLE.print(f"   [grey50]$ {' '.join(shlex.quote(c) for c in cmd)}[/grey50]")
    return subprocess.run(cmd, check=check, **kwargs)


def scp(local: Path, remote_target: str, ssh_key_path: str) -> None:
    info(f"🚀 正在向服务器传输压缩包…")
    try:
        run(["scp", "-i", ssh_key_path, str(local), remote_target])
        ok("✅ 压缩包传输成功。")
    except subprocess.CalledProcessError as exc:
        die(f"传输压缩包失败（exit={exc.returncode}）。请检查SSH连接、权限或路径。")


# --- 5. 远程执行部署 --------------------------------------------------

def build_remote_script(remote_project_dir: str, remote_zip_name: str) -> str:
    """构造远程 bash 脚本。纯 ascii 开头，绝不带 BOM。"""
    lines = [
        "set -e",
        f'mkdir -p "{remote_project_dir}"',
        f'cd "{remote_project_dir}"',
        "",
        "echo '--- [Remote] 1/6 : 解压新文件...'",
        f'unzip -o "{remote_zip_name}" -d .',
        "",
        "echo '--- [Remote] 2/6 : 构建 Docker 镜像...'",
        "docker compose build",
        "",
        "echo '--- [Remote] 3/6 : 运行所有数据库迁移 (Alembic)...'",
        # 用 < /dev/null 让容器内的 remote_deploy.py 不读 stdin，与原 PS 脚本一致
        f"docker compose run -T --rm -v $(pwd):/app {DOCKER_CONTAINER_NAME} "
        "python3 /app/remote_deploy.py < /dev/null",
        "",
        "echo '--- [Remote] 4/6 : 启动新容器并替换旧容器...'",
        "docker compose up -d --remove-orphans",
        "",
        "echo '--- [Remote] 5/6 : 清理无用的 Docker 镜像...'",
        "docker image prune -a -f",
        "",
        "echo '--- [Remote] 6/6 : 清理临时文件...'",
        f'rm -f "{remote_zip_name}"',
        "rm -f remote_deploy.py",
        "",
        "echo '--- [Remote] 部署成功完成！---'",
    ]
    # LF-only；前面绝对不放 BOM
    return "\n".join(lines) + "\n"


def ssh_run_script(ssh_target: str, ssh_key_path: str, script: str) -> None:
    info("🔧 正在连接到服务器并执行部署命令...")
    cmd = ["ssh", "-T", "-i", ssh_key_path, ssh_target, "bash -s"]
    # 关键：通过 bytes 喂入，避免任何 BOM 泄漏
    try:
        subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        die(f"在服务器上执行部署命令时失败（exit={exc.returncode}）。")


def ssh_stream_logs(ssh_target: str, ssh_key_path: str) -> None:
    info("📋 正在实时跟踪 Docker 容器日志 (按 Ctrl+C 退出)...")
    try:
        subprocess.run(
            ["ssh", "-i", ssh_key_path, ssh_target, f"docker logs -f {DOCKER_CONTAINER_NAME}"],
            check=False,
        )
    except KeyboardInterrupt:
        print()


# --- 入口 ------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RoleBot 一键部署脚本")
    parser.add_argument(
        "--follow", action="store_true",
        help="部署完后实时跟踪容器日志（Ctrl+C 退出）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    warn("⚙️  正在加载部署配置...")
    cfg = load_env(ROOT_DIR / "deploy.env")
    ssh_host = cfg.get("SSH_HOST", "")
    ssh_user = cfg.get("SSH_USER", "")
    ssh_key_path = cfg.get("SSH_PRIVATE_KEY_PATH", "")
    if not (ssh_host and ssh_user and ssh_key_path):
        die("deploy.env 缺少 SSH_HOST / SSH_USER / SSH_PRIVATE_KEY_PATH 配置。")

    if ssh_user == "root":
        remote_base = "/root"
    else:
        remote_base = f"/home/{ssh_user}"
    remote_project_dir = f"{remote_base}/{REMOTE_PROJECT_NAME}"
    info(f"ℹ️  远程项目目录将被设置为: {remote_project_dir}")

    check_local(ssh_key_path, ROOT_DIR / "docker-compose.yml")
    zip_path = package_project(ROOT_DIR)

    ssh_target = f"{ssh_user}@{ssh_host}"
    remote_zip_path = f"{remote_base}/{zip_path.name}"
    try:
        scp(zip_path, f"{ssh_target}:{remote_zip_path}", ssh_key_path)
    finally:
        # 任何情况下都清本地 zip
        try:
            zip_path.unlink()
            info(f"🗑️  已删除本地临时压缩包: {zip_path}")
        except FileNotFoundError:
            pass

    script = build_remote_script(remote_project_dir, zip_path.name)
    ssh_run_script(ssh_target, ssh_key_path, script)
    ok("🎉 部署成功完成！RoleBot 已在服务器上更新并启动。")

    if args.follow:
        ssh_stream_logs(ssh_target, ssh_key_path)
    else:
        info("ℹ️  部署已完成。如需查看日志，可使用 fetch_log.ps1（或 python fetch_log.py ——见后续迁移）。")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        err("\n⏹️  部署被用户中断。")
        sys.exit(130)
