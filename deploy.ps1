# deploy.ps1
# ==============================================================================
# RoleBot 一键部署脚本 (本地文件传输模式)
#
# 功能:
# 1. 从 deploy.env 加载远程服务器的连接配置。
# 2. 检查本地必需的文件 (.env, config_data.py, docker-compose.yml 等)。
# 3. 将整个项目目录打包成一个临时 zip 文件。
# 4. 使用 SCP 将 zip 文件安全地传输到远程服务器。
# 5. SSH 连接到远程服务器，执行以下操作:
#    a. 创建/清空远程项目目录。
#    b. 解压传输的 zip 文件到项目目录。
#    c. 执行 Docker Compose 构建、数据库迁移和启动容器。
#    d. 清理远程临时文件。
# 6. (可选) 实时查看 Docker 容器日志。
#
# 使用方法:
# 1. 确保已在 deploy.env 和 .env 文件中填写正确的配置。
# 2. 确保你的本地项目目录包含所有机器人所需的代码和配置文件。
# 3. 在 PowerShell 中，导航到此脚本所在的目录 (通常是项目根目录)。
# 4. 运行: .\deploy.ps1
#
# ==============================================================================

# --- 脚本配置 ---
$ErrorActionPreference = "Stop" # 遇到任何错误就停止脚本
$dockerContainerName = "rolebot"
$remoteProjectBaseDir = "/root" # 远程服务器上项目存放的父目录
$remoteProjectName = "RoleBot"  # 远程服务器上项目目录的名称

# --- 1. 加载配置 ---
Write-Host "⚙️ 正在加载部署配置..." -ForegroundColor Yellow

$config = @{ }
try
{
    Get-Content ".\deploy.env" | ForEach-Object {
        if ($_ -match '^(.*?)=(.*)')
        {
            $key = $Matches[1].Trim()
            $value = $Matches[2].Trim()
            $config[$key] = $value
        }
    }
}
catch
{
    Write-Host "❌ 错误: 无法读取 'deploy.env' 文件。请确保它存在且格式正确。" -ForegroundColor Red
    exit 1
}

# 从配置中提取变量
$sshHost = $config["SSH_HOST"]
$sshUser = $config["SSH_USER"]
$sshKeyPath = $config["SSH_PRIVATE_KEY_PATH"]

# 远程项目完整路径
$remoteProjectDir = "$remoteProjectBaseDir/$remoteProjectName"
Write-Host "ℹ️ 远程项目目录将被设置为: $remoteProjectDir" -ForegroundColor DarkCyan

# --- 2. 本地文件检查 ---
Write-Host "🔍 正在检查本地SSH私钥和Docker Compose文件..." -ForegroundColor Cyan

if (-not (Test-Path $sshKeyPath))
{
    Write-Host "❌ 错误: SSH 私钥文件未在 '$sshKeyPath' 找到。" -ForegroundColor Red
    Write-Host "   请检查 deploy.env 中的 SSH_PRIVATE_KEY_PATH 配置。" -ForegroundColor Gray
    exit 1
}

# 确保 docker-compose.yml 存在
if (-not (Test-Path ".\docker-compose.yml"))
{
    Write-Host "❌ 错误: 必需的 'docker-compose.yml' 文件不存在于当前目录。" -ForegroundColor Red
    exit 1
}

Write-Host "✅ 本地文件检查通过。" -ForegroundColor Green

# --- 3. 打包本地项目文件 ---
Write-Host "📦 正在打包本地项目文件..." -ForegroundColor Cyan

# 临时 zip 文件名和路径
$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$zipFileName = "rolebot_deploy_$timestamp.zip"
$zipFilePath = Join-Path $PSScriptRoot $zipFileName

# 创建要排除的文件/目录列表 (例如，Python虚拟环境、git相关文件、本部署脚本本身)
$excludeList = @(
    "*.pyc",
    "__pycache__",
    ".git",
    ".gitignore",
    ".venv",
    ".idea",
    "*.db",
    "deploy.env", # 敏感信息，不应该打包进去
    $zipFileName, # 排除自身
    "*.zip", # 排除万一没有清理掉的zip
    "deploy.ps1", # 排除自身
    "*.log" # 如果有日志文件
)

# ===============================================================
# ✨ 使用 7z.exe 替代 Compress-Archive ✨
# ===============================================================
try
{
    Write-Host "   -> 正在创建 ZIP 文件: $zipFilePath" -ForegroundColor Gray

    # 7-Zip 命令的基本结构: 7z a <archive_name> <files_to_add> -xr!<exclude_pattern>
    # 转换为 PowerShell 语法
    $sourceDir = $PSScriptRoot # 当前脚本所在的目录作为源目录

    # 构建 7z 的排除参数
    # 注意：7z的排除模式需要绝对路径或者相对于当前工作目录的路径。
    # 我们这里使用相对路径，且需要确保它们不包含根目录部分。
    $sevenZipExcludeArgs = $excludeList | ForEach-Object { "-xr!$_" } # 这里的 $_ 已经是字符串

    # 7z.exe 命令 (a: add to archive, -tzip: zip format, -r: recurse subdirectories)
    # `$sourceDir\*` 表示打包源目录下的所有文件和子目录
    # -mx=9 表示最大压缩级别，可选

    # **注意:** 确保 '7z.exe' 在你的系统 PATH 中。
    # 我们将命令和参数作为一个数组传递给 Start-Process，这样更可靠。
    $7zArguments = @(
        "a", # add to archive
        "-tzip", # output format is zip
        "`"$zipFilePath`"", # archive name (quoted for spaces)
        "`"$sourceDir\*`"", # files/directories to add (all contents of sourceDir, preserves structure)
        "-r", # recurse subdirectories
        "-mx=9"                        # maximum compression
    ) + $sevenZipExcludeArgs           # add all exclude arguments

    # 打印将执行的命令（方便调试）
    Write-Host "   -> 执行命令: 7z.exe $( $7zArguments -join ' ' )" -ForegroundColor DarkGray
    $sevenZipExePath = "C:\Program Files\7-Zip\7z.exe"
    # 执行 7z.exe
    $process = Start-Process -FilePath $sevenZipExePath -ArgumentList $7zArguments -NoNewWindow -PassThru -ErrorAction Stop -Wait
    $process.WaitForExit()

    Write-Host "✅ 项目文件打包成功。" -ForegroundColor Green
}
catch
{
    Write-Host "❌ 错误: 打包项目文件失败。请确保 7-Zip 已安装且 '7z.exe' 在系统 PATH 中。" -ForegroundColor Red
    $_ | Out-String
    exit 1
}
# ===============================================================


# --- 4. 传输压缩包到远程服务器 ---
Write-Host "🚀 正在向服务器 ($sshHost) 传输压缩包..." -ForegroundColor Cyan

$remoteZipPath = "$remoteProjectBaseDir/$zipFileName"

try
{
    Write-Host "   -> 正在传输 $zipFilePath 到 ${sshHost}:$remoteZipPath..." -ForegroundColor Gray
    scp -i $sshKeyPath $zipFilePath "$( $sshUser )@$( $sshHost ):$remoteZipPath"
    Write-Host "✅ 压缩包传输成功。" -ForegroundColor Green
}
catch
{
    Write-Host "❌ 错误: 传输压缩包失败。请检查SSH连接、权限或路径是否正确。" -ForegroundColor Red
    $_ | Out-String
    exit 1
}
finally
{
    # 传输完成后，删除本地的临时zip文件
    Remove-Item $zipFilePath -Force
    Write-Host "🗑️ 已删除本地临时压缩包: $zipFilePath" -ForegroundColor DarkGray
}

# --- 5. 在远程服务器上执行部署逻辑 ---
Write-Host "🔧 正在连接到服务器并执行部署命令..." -ForegroundColor Cyan

$remoteCommands = @"
set -e
mkdir -p "$remoteProjectDir"
cd "$remoteProjectDir"

echo '--- [Remote] 1/6 : 解压新文件...'
unzip -o "$remoteProjectBaseDir/$zipFileName" -d .

echo '--- [Remote] 2/6 : 构建 Docker 镜像...'
# 直接在宿主机上运行 docker-compose build
docker-compose build

echo '--- [Remote] 3/6 : 运行所有数据库迁移 (Alembic)...'
# 通过 docker-compose run 启动一个临时容器来执行 remote_deploy.py
# remote_deploy.py 会在容器内执行 alembic upgrade head
docker-compose run -T --rm -v `$(pwd)`:/app $dockerContainerName python3 /app/remote_deploy.py < /dev/null

echo '--- [Remote] 4/6 : 启动新容器并替换旧容器...'
# 直接在宿主机上运行 docker-compose up
docker-compose up -d --remove-orphans

echo '--- [Remote] 5/6 : 清理无用的 Docker 镜像...'
# 直接在宿主机上运行 docker image prune
docker image prune -a -f

echo '--- [Remote] 6/6 : 清理临时文件...'
rm -f "$remoteProjectBaseDir/$zipFileName"
rm -f remote_deploy.py

echo '--- [Remote] 部署成功完成！---'
"@

try
{
    $OutputEncoding = [System.Text.Encoding]::UTF8
    $linuxCompatibleCommands = $remoteCommands.Replace("`r`n", "`n")

    $linuxCompatibleCommands | ssh -T -i $sshKeyPath "$( $sshUser )@$( $sshHost )" "bash -s"

    Write-Host "🎉 部署成功完成！RoleBot 已在服务器上更新并启动。" -ForegroundColor Green

    # 实时查看 Docker 容器日志
    Write-Host "📋 正在实时查看 Docker 容器日志 (按 Ctrl+C 退出)..." -ForegroundColor Magenta
    ssh -i $sshKeyPath "$( $sshUser )@$( $sshHost )" "docker logs -f $dockerContainerName"
}
catch
{
    Write-Host "❌ 错误: 在服务器上执行部署命令时失败。" -ForegroundColor Red
    $_ | Out-String
    exit 1
}