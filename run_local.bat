@echo off
REM ============================================================
REM RoleBot 本地启动脚本
REM 用法：在 PowerShell / CMD 里 .\run_local.bat
REM
REM 行为：
REM   1. 检查 .venv 是否存在
REM   2. 检查 .env 里是否有 DISCORD_BOT_TOKEN
REM   3. 用 .venv 里的 python 启动 main.py
REM ============================================================

setlocal

REM 切到脚本所在目录
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] .venv\Scripts\python.exe 不存在。请先创建虚拟环境并 pip install -r requirements.txt
    exit /b 1
)

if not exist ".env" (
    echo [ERROR] .env 不存在。请从"配置文件示例（请放在项目根目录）"复制并填入 DISCORD_BOT_TOKEN
    exit /b 1
)

echo [INFO] 使用 .venv 启动 RoleBot（本地开发模式）
echo [INFO] 实时日志会直接打印到当前终端。按 Ctrl+C 停止
echo.

".venv\Scripts\python.exe" main.py %*

endlocal