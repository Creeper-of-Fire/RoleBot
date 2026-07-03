# fetch_log.ps1
# ==============================================================================
# RoleBot 远程日志拉取脚本（非阻塞版）
#
# 与 deploy.ps1 的区别：
#   - deploy.ps1 自带 `docker logs -f`，会**一直阻塞**到 Ctrl+C
#   - fetch_log.ps1 默认只拉**最近 200 行**，看完就退出
#
# 用法：
#   .\fetch_log.ps1              # 拉最近 200 行
#   .\fetch_log.ps1 -Lines 1000  # 拉最近 1000 行
#   .\fetch_log.ps1 -Follow      # 持续跟踪（等同于 docker logs -f）
# ==============================================================================

param(
    [int]$Lines = 200,
    [switch]$Follow
)

$ErrorActionPreference = "Stop"

# --- 1. 加载配置 ---
Write-Host "⚙️ 正在加载部署配置..." -ForegroundColor Yellow

$config = @{}
try {
    Get-Content ".\deploy.env" | ForEach-Object {
        if ($_ -match '^(.*?)=(.*)') {
            $key = $Matches[1].Trim()
            $value = $Matches[2].Trim()
            $config[$key] = $value
        }
    }
} catch {
    Write-Host "❌ 错误: 无法读取 'deploy.env' 文件。" -ForegroundColor Red
    exit 1
}

$sshHost     = $config["SSH_HOST"]
$sshUser     = $config["SSH_USER"]
$sshKeyPath  = $config["SSH_PRIVATE_KEY_PATH"]
$container   = "rolebot"

if (-not (Test-Path $sshKeyPath)) {
    Write-Host "❌ 错误: 私钥文件 '$sshKeyPath' 不存在。" -ForegroundColor Red
    exit 1
}

# --- 2. SSH 拉日志 ---
if ($Follow) {
    Write-Host "📋 正在持续跟踪 '$container' 日志 (Ctrl+C 退出)..." -ForegroundColor Magenta
    ssh -i $sshKeyPath "$sshUser@$sshHost" "docker logs -f $container"
} else {
    Write-Host "📋 正在拉取 '$container' 最近 $Lines 行日志..." -ForegroundColor Cyan
    ssh -i $sshKeyPath "$sshUser@$sshHost" "docker logs --tail $Lines $container"
}