#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bot 一键部署脚本（共享版）。

行为对齐原 role_bot/deploy.py Python 版，差异：
- 参数化支持多 bot：`extra_excludes` + `run_alembic`
- 所有 bot-specific 配置通过 bot 仓库的 `deploy.py` wrapper 注入
- DOCKER_CONTAINER_NAME / REMOTE_PROJECT_NAME 从 `deploy.env` 读

用法（bot 仓库 deploy.py wrapper）：

    # role_bot/deploy.py — 有 alembic 迁移（remote_deploy 跑实际工作）
    import sys
    from shared.scripts.deploy import main as _main
    if __name__ == "__main__":
        sys.exit(_main())   # run_remote_deploy=True 默认

    # ticket_bot/deploy.py — 没有 remote_deploy 内容（remote_deploy.py 在但不会被调）
    import sys
    from shared.scripts.deploy import main as _main
    if __name__ == "__main__":
        sys.exit(_main(run_remote_deploy=False))

    # news_bot/deploy.py — 同上
    ...

`deploy.env` 必填字段：
- SSH_HOST
- SSH_USER
- SSH_PRIVATE_KEY_PATH
- DEPLOY_DOCKER_CONTAINER_NAME
- DEPLOY_REMOTE_PROJECT_NAME
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console


# 默认排除模式（共享）。三个 bot 完全一致，不提供 bot-specific 注入。
#
# - `data/`: 运行时数据目录（db / json / toml）。三个 bot 的容器都用 docker
#   命名卷映射到 `/app/data`，不依赖宿主机 `~/.../data/`——deploy 不应
#   覆盖它（早期部署可能由不同用户创建，权限不一致会导致解压失败）。
#   bot 端用 `/下载` `/上传` 等指令改的是命名卷里的副本。
DEFAULT_EXCLUDE_PATTERNS: set[str] = {
    "*.pyc", "__pycache__", ".gitignore",
    "*.db", "deploy.env",
    "deploy.py",                          # 本地 deploy 脚本，不打包
    "data/",                              # runtime data（仅顶层，不影响 shared/data/）
    ".venv", ".git", ".idea",             # 顶层目录
    "*.zip", "*.log", "*.tmp",
}

# 7z 可执行文件查找顺序（Windows 优先，因为本地 deploy 环境都是 Windows）。
# Linux/macOS fork 用户在 PATH 里装 7z 也走得到。
_SEVEN_ZIP_CANDIDATES = [
    r"C:\Program Files\7-Zip\7z.exe",
    r"C:\Program Files (x86)\7-Zip\7z.exe",
    "7z.exe",
    "7z",
]

_CONSOLE = Console(highlight=False)
_ERR_CONSOLE = Console(stderr=True, highlight=False)


# --- 输出辅助 ---


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


# --- 1. 加载配置 ---


def load_env(path: Path) -> dict[str, str]:
    """加载 KEY=VALUE 格式的 env 文件。"""
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


# --- 2. 本地文件检查 ---


def check_local(ssh_key_path: str, compose_path: Path) -> None:
    """检查本地必需文件（SSH 私钥 + docker-compose.yml）。"""
    info("🔍 正在检查本地SSH私钥和Docker Compose文件...")
    if not Path(ssh_key_path).exists():
        die(
            f"SSH 私钥文件未在 '{ssh_key_path}' 找到。\n"
            "   请检查 deploy.env 中的 SSH_PRIVATE_KEY_PATH 配置。"
        )
    if not compose_path.exists():
        die(f"必需的 'docker-compose.yml' 文件不存在于当前目录：{compose_path}")
    ok("✅ 本地文件检查通过。")


# --- 3. 打包 ---


def _is_excluded(rel_path: str, exclude_patterns: set[str]) -> bool:
    """检查相对路径（posix 风格，用 `/`）是否匹配任一排除模式。

    - 顶层目录（data/、.venv、.git、.idea）只匹配**第一段**，避免把 `shared/data/` 这种
      代码路径误杀——`Path("data").match("data/") == True` 会让 `shared/data/x.py`
      也被排除，导致运行时 `ModuleNotFoundError: No module named 'shared.data'`。
    - 文件 glob（*.pyc、deploy.py 等）匹配任意段。
    - 之所以 Python 端决定 exclude，而不是 7z `-xr!` 模式：实测 7z 的 `-xr!data`
      会全局匹配 `shared/data/` 等嵌套同名目录，没法限定顶层。Python 端按段匹配
      行为确定。
    """
    # 顶层目录模式——只对第一段做 match
    top_level_patterns = {"data/", ".venv", ".git", ".idea"}
    first_part = rel_path.split("/", 1)[0]
    for pat in top_level_patterns & exclude_patterns:
        if Path(first_part).match(pat):
            return True

    # 文件/glob 模式——匹配任意段和文件名
    parts = rel_path.split("/")
    name = parts[-1]
    for part in parts + [name]:
        for pat in exclude_patterns - top_level_patterns:
            if part == pat or Path(part).match(pat):
                return True
    return False


def _find_seven_zip() -> str:
    """查找 7z 可执行文件路径。找不到就 die。"""
    for cand in _SEVEN_ZIP_CANDIDATES:
        p = Path(cand)
        if p.is_absolute() and p.exists():
            return str(p)
        found = shutil.which(cand)
        if found:
            return found
    die(
        "❌ 找不到 7-Zip（7z.exe）。\n"
        "deploy 用 7z 打 zip 是为了**正确处理 UTF-8 文件名**——Python 的 zipfile "
        "在某些 Windows locale 下会把中文路径写成 MBCS，导致 Linux 端 unzip "
        "解压后文件名乱码（实测：`荣誉系统使用手册.md` 被解压成 mojibake）。\n"
        "   → 安装 7-Zip for Windows：https://www.7-zip.org/\n"
        "   → 或确保 `7z.exe` 在 PATH 中。\n"
        "   → 不做 Python zipfile 兜底是故意的：它会踩同样的 UTF-8 坑。"
    )
    raise RuntimeError("unreachable")  # die() 已 sys.exit


def package_project(
    root: Path,
    exclude_patterns: set[str],
    zip_prefix: str,
) -> Path:
    """打包本地项目到 zip：用 Python 做 exclude，把文件列表显式喂给 7z。

    为什么不用 7z 的 `-xr!` exclude：
    7z 的 `-xr!data` 会全局匹配所有名为 `data` 的目录（包括 `shared/data/` 这种
    关键代码路径），没有"只顶层"的语义。所以排除逻辑由 Python 端按段匹配（确定），
    7z 只负责压缩——通过把每个要保留的文件路径显式传给 7z。

    为什么用 7z 而不直接用 Python zipfile：
    Python zipfile 在 Windows 某些 locale 下会把中文路径按 MBCS 编码，导致 Linux
    端 unzip 6.00 解码后文件名变 mojibake，曾让 `荣誉系统使用手册.md` 被解读成乱码、
    bot 找不到 doc 文件。7z 实测跨 Windows/Linux 解码稳定。

    Returns:
        zip 文件路径。
    """
    info("📦 正在打包本地项目文件（Python 端 exclude + 7z 压缩）...")
    seven_zip = _find_seven_zip()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"{zip_prefix}_deploy_{timestamp}.zip"
    zip_path = root / zip_name

    # Python 端决定哪些文件进 zip（exclude 行为确定：顶层 / 任意段 glob）
    files_to_pack: list[str] = []   # 这里存的是 root 内的相对路径（posix 风格）
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if _is_excluded(rel, exclude_patterns):
            continue
        files_to_pack.append(rel)

    if not files_to_pack:
        die(f"打包失败：没有可打包的文件（exclude 后列表为空，请检查 {root} 是否有内容）")

    # 7z a -tzip -mx=9 <archive> <file1> <file2> ...
    # 关键：cwd 必须设到 root，并传**相对路径**——否则 7z 只存 basename,
    # 同名不同目录的文件会撞车（例如 role_bot 下多个 important.py）。
    cmd = [seven_zip, "a", "-tzip", "-mx=9", str(zip_path), *files_to_pack]
    info(f"   $ 7z a -tzip -mx=9 {zip_path.name} <{len(files_to_pack)} files>")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root))
    if result.returncode != 0:
        die(
            f"7z 打包失败（exit={result.returncode}）。\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )

    ok(f"✅ 项目文件打包成功（{len(files_to_pack)} 个文件 → {zip_path.name}）。")
    return zip_path


# --- 4. 传输 ---


def run(cmd: list[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """统一的 subprocess 调用。check=True 时非零退出码抛出。"""
    _CONSOLE.print(f"   [grey50]$ {' '.join(shlex.quote(c) for c in cmd)}[/grey50]")
    return subprocess.run(cmd, check=check, **kwargs)


def scp(local: Path, remote_target: str, ssh_key_path: str) -> None:
    """SCP 传输 zip 到远程。"""
    info(f"🚀 正在向服务器传输压缩包…")
    try:
        run(["scp", "-i", ssh_key_path, str(local), remote_target])
        ok("✅ 压缩包传输成功。")
    except subprocess.CalledProcessError as exc:
        die(f"传输压缩包失败（exit={exc.returncode}）。请检查SSH连接、权限或路径。")


# --- 5. 远程执行 ---


def build_remote_script(
    remote_project_dir: str,
    remote_zip_path: str,           # 绝对路径（含 remote_base 前缀）
    docker_container_name: str,
    run_remote_deploy: bool,
) -> str:
    """构造远程 bash 脚本。纯 ascii 开头，绝不带 BOM。

    步骤数根据 run_remote_deploy 动态：
    - True:  6 步（解压 → build → remote_deploy → launch → prune → cleanup）
    - False: 5 步（解压 → build → launch → prune → cleanup）

    `remote_deploy` 是步骤命名（远程容器内启动后跑的脚本），其内部
    当前主要做 alembic 迁移，但可能扩展（健康检查、缓存预热等）。

    `remote_zip_path` 必须是绝对路径，因为 zip 落在 `~/`（home dir）但
    unzip 跑在 `~/<project_dir>/`，用相对名会找不到。
    """
    total = 6 if run_remote_deploy else 5
    lines: list[str] = [
        "set -e",
        f'mkdir -p "{remote_project_dir}"',
        f'cd "{remote_project_dir}"',
        "",
        f"echo '--- [Remote] 1/{total} : 解压新文件...'",
        f'unzip -o "{remote_zip_path}" -d .',
        "",
        f"echo '--- [Remote] 2/{total} : 构建 Docker 镜像...'",
        "docker compose build",
    ]
    if run_remote_deploy:
        lines.extend([
            "",
            f"echo '--- [Remote] 3/{total} : 远程部署脚本 (remote_deploy.py)...'",
            f"docker compose run -T --rm -v $(pwd):/app {docker_container_name} "
            "python3 /app/remote_deploy.py < /dev/null",
        ])
        next_step = 4
    else:
        next_step = 3

    lines.extend([
        "",
        f"echo '--- [Remote] {next_step}/{total} : 启动新容器并替换旧容器...'",
        "docker compose up -d --remove-orphans",
        "",
        f"echo '--- [Remote] {next_step + 1}/{total} : 清理无用的 Docker 镜像...'",
        "docker image prune -a -f",
        "",
        f"echo '--- [Remote] {next_step + 2}/{total} : 清理临时文件...'",
        f'rm -f "{remote_zip_path}"',
        "rm -f remote_deploy.py",
        "",
        "echo '--- [Remote] 部署成功完成！---'",
    ])
    return "\n".join(lines) + "\n"


def ssh_run_script(ssh_target: str, ssh_key_path: str, script: str) -> None:
    """通过 SSH 在远程执行 bash 脚本（关键：通过 bytes 喂入，避免 BOM 泄漏）。"""
    info("🔧 正在连接到服务器并执行部署命令...")
    cmd = ["ssh", "-T", "-i", ssh_key_path, ssh_target, "bash -s"]
    try:
        subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        die(f"在服务器上执行部署命令时失败（exit={exc.returncode}）。")


def ssh_stream_logs(ssh_target: str, ssh_key_path: str, container_name: str) -> None:
    """实时跟踪远程 Docker 容器日志（Ctrl+C 退出）。"""
    info("📋 正在实时跟踪 Docker 容器日志 (按 Ctrl+C 退出)...")
    try:
        subprocess.run(
            ["ssh", "-i", ssh_key_path, ssh_target, f"docker logs -f {container_name}"],
            check=False,
        )
    except KeyboardInterrupt:
        print()


# --- 入口 ---


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bot 一键部署脚本")
    parser.add_argument(
        "--follow", action="store_true",
        help="部署完后实时跟踪容器日志（Ctrl+C 退出）",
    )
    return parser.parse_args()


def main(
    *,
    run_remote_deploy: bool = True,
    zip_prefix: str | None = None,
) -> int:
    """部署入口。

    Args:
        run_remote_deploy: 是否在远程调 `remote_deploy.py` 跑容器内启动后脚本
            （当前主要做 alembic 迁移，但可能扩展）。没有数据库迁移需求的 bot
            传 False 跳过该步骤。
        zip_prefix: zip 文件名前缀（默认用 project_name 小写）。

    Returns:
        退出码（0 = 成功）。
    """
    args = parse_args()

    cfg = load_env(Path("deploy.env"))
    ssh_host = cfg.get("SSH_HOST", "")
    ssh_user = cfg.get("SSH_USER", "")
    ssh_key_path = cfg.get("SSH_PRIVATE_KEY_PATH", "")
    container_name = cfg.get("DEPLOY_DOCKER_CONTAINER_NAME", "")
    project_name = cfg.get("DEPLOY_REMOTE_PROJECT_NAME", "")

    if not (ssh_host and ssh_user and ssh_key_path):
        die("deploy.env 缺少 SSH_HOST / SSH_USER / SSH_PRIVATE_KEY_PATH 配置。")
    if not (container_name and project_name):
        die("deploy.env 缺少 DEPLOY_DOCKER_CONTAINER_NAME / DEPLOY_REMOTE_PROJECT_NAME 配置。")

    remote_base = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
    remote_project_dir = f"{remote_base}/{project_name}"
    info(f"ℹ️  远程项目目录将被设置为: {remote_project_dir}")

    check_local(ssh_key_path, Path("docker-compose.yml"))
    prefix = zip_prefix or project_name.lower()
    zip_path = package_project(Path.cwd(), DEFAULT_EXCLUDE_PATTERNS, prefix)

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

    script = build_remote_script(
        remote_project_dir=remote_project_dir,
        remote_zip_path=remote_zip_path,    # 绝对路径（zip 在 ~/，unzip 跑在 ~/<project>）
        docker_container_name=container_name,
        run_remote_deploy=run_remote_deploy,
    )
    ssh_run_script(ssh_target, ssh_key_path, script)
    ok(f"🎉 部署成功完成！{project_name} 已在服务器上更新并启动。")

    if args.follow:
        ssh_stream_logs(ssh_target, ssh_key_path, container_name)
    else:
        info(f"ℹ️  部署已完成。如需查看日志，运行 `docker logs -f {container_name}`。")

    return 0


if __name__ == "__main__":
    # 允许 `python -m shared.scripts.deploy` 直接运行（用默认参数）
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        err("\n⏹️  部署被用户中断。")
        sys.exit(130)