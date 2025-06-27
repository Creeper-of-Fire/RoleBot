# deploy_rolebot.ps1
# ==============================================================================
# RoleBot 一键部署脚本 (优化版)
#
# 功能:
# 1. 从 deploy.env 加载远程服务器的连接配置。
# 2. 检查本地必需的配置文件 (.env, config_data.py, deploy_remote.sh) 是否存在。
# 3. 使用 SCP 将 .env, config_data.py 和 deploy_remote.sh 安全地传输到远程服务器。
# 4. SSH 连接到远程服务器，并执行上传的 deploy_remote.sh 脚本，完成部署。
#
# 使用方法:
# 1. 确保已在 deploy.env 和 .env 文件中填写正确的配置。
# 2. 确保 config_data.py 和 deploy_remote.sh 是你想要部署的版本。
# 3. 在 PowerShell 中，导航到此脚本所在的目录。
# 4. 运行: .\deploy_rolebot.ps1
#
# ==============================================================================

# --- 脚本配置 ---
$ErrorActionPreference = "Stop" # 遇到任何错误就停止脚本

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
}
catch {
    Write-Host "❌ 错误: 无法读取 'deploy.env' 文件。请确保它存在且格式正确。" -ForegroundColor Red
    exit 1
}

# 从配置中提取变量
$sshHost = $config["SSH_HOST"]
$sshUser = $config["SSH_USER"]
$sshKeyPath = $config["SSH_PRIVATE_KEY_PATH"]
$remoteProjectDir = "/root/RoleBot" # 机器人代码在服务器上的存放位置

# --- 2. 本地文件检查 ---
Write-Host "🔍 正在检查本地必需文件..." -ForegroundColor Cyan

if (-not (Test-Path $sshKeyPath)) {
    Write-Host "❌ 错误: SSH 私钥文件未在 '$sshKeyPath' 找到。" -ForegroundColor Red
    Write-Host "   请检查 deploy.env 中的 SSH_PRIVATE_KEY_PATH 配置。" -ForegroundColor Gray
    exit 1
}

# 增加了 deploy_remote.sh 到需要复制的文件列表
$localFilesToCopy = @(".\.env", ".\config.py", ".\config_data.py", ".\deploy_remote.sh")
foreach ($file in $localFilesToCopy) {
    if (-not (Test-Path $file)) {
        Write-Host "❌ 错误: 必需的配置文件 '$file' 不存在。" -ForegroundColor Red
        exit 1
    }
}

Write-Host "✅ 本地文件检查通过。" -ForegroundColor Green

# --- 3. 传输配置文件和远程部署脚本 ---
Write-Host "🚀 正在向服务器 ($sshHost) 传输文件..." -ForegroundColor Cyan

try {
    # 创建远程项目目录（如果不存在）
    ssh -i $sshKeyPath "$($sshUser)@$($sshHost)" "mkdir -p $remoteProjectDir"

    # 循环传输文件
    foreach ($file in $localFilesToCopy) {
        Write-Host "   -> 正在传输 $file..." -ForegroundColor Gray
        scp -i $sshKeyPath $file "$($sshUser)@$($sshHost):$remoteProjectDir"
    }

    # 确保 deploy_remote.sh 有执行权限
    ssh -i $sshKeyPath "$($sshUser)@$($sshHost)" "chmod +x $remoteProjectDir/deploy_remote.sh"

    Write-Host "✅ 文件传输成功，并设置 deploy_remote.sh 为可执行。" -ForegroundColor Green
}
catch {
    Write-Host "❌ 错误: 传输文件失败。请检查SSH连接、权限或路径是否正确。" -ForegroundColor Red
    $_ | Out-String # 打印完整的错误信息
    exit 1
}

# --- 4. 在远程服务器上执行部署脚本 ---
Write-Host "🔧 正在连接到服务器并执行远程部署脚本..." -ForegroundColor Cyan

try {
    # 直接执行远程服务器上的脚本
    # 注意：这里不再需要管道，因为是直接在远程执行脚本文件
    ssh -i $sshKeyPath "$($sshUser)@$($sshHost)" "$remoteProjectDir/deploy_remote.sh"

    Write-Host "🎉 部署成功完成！RoleBot 已在服务器上更新并启动。" -ForegroundColor Green
}
catch {
    Write-Host "❌ 错误: 在服务器上执行部署命令时失败。" -ForegroundColor Red
    $_ | Out-String # 打印完整的错误信息
    exit 1
}