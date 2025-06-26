<#
.SYNOPSIS
    一键部署RoleBot到阿里云ECS。
    该脚本在本地构建Docker镜像，打包后上传到服务器，并远程执行部署命令。
.DESCRIPTION
    执行流程:
    1. 可选: 在本地使用 docker-compose.yml 构建镜像。
    2. 可选: 使用 docker save 将镜像打包成 .tar 文件。
    3. 在远程服务器上创建部署目录。
    4. 使用 scp 上传：
        - 镜像 .tar 文件 (如果执行了构建)。
        - .env 文件。
        - docker-compose.deploy.yml (并重命名为 docker-compose.yml)。
    5. 远程执行 docker load 加载镜像 (如果上传了新的镜像文件)。
    6. 远程执行 docker-compose up -d 启动服务。
    7. 清理本地和远程的临时文件。
#>

# ===================================================================
# ========================  请配置以下变量  ========================
# ===================================================================

# 示例: 加载 deploy.env 文件
$deployEnvPath = Join-Path $PSScriptRoot "deploy.env" # 假设 deploy.env 和脚本在同一目录
if (Test-Path $deployEnvPath) {
    Get-Content $deployEnvPath | ForEach-Object {
        if ($_ -match "^([^#=]+?)=(.*)$") {
            $varName = $Matches[1].Trim()
            $varValue = $Matches[2].Trim()
            # 移除引号，如果存在
            if ($varValue -match '^"(.*)"$' -or $varValue -match "^'(.*)'$") {
                $varValue = $Matches[1]
            }
            Write-Verbose "Setting environment variable: $varName = $varValue"
            Set-Item -Path Env:$varName -Value $varValue
        }
    }
} else {
     Write-Host "警告: 未找到 deploy.env 文件，将尝试使用已设置的环境变量。" -ForegroundColor Yellow
}

# --- SSH 和服务器配置 ---
$sshHost            = $env:SSH_HOST
$sshUser            = $env:SSH_USER
$privateKeyPath     = $env:SSH_PRIVATE_KEY_PATH

# --- 本地项目路径配置 ---
# !!! 重要: 请将此路径修改为你本地RoleBot项目的【绝对路径】 !!!
$localProjectPath   = "C:\Users\Creeper10\Desktop\ProjectForFun\role_bot" # <---- 修改这里！！！

# --- 远程部署路径配置 ---
$remoteDeployPath   = "/opt/rolebot"

# --- Docker 配置 ---
# `docker-compose.yml`中定义的服务名，默认为'rolebot'
$dockerServiceName      = "rolebot"
# 本地构建出的镜像名称，格式为`项目文件夹名-服务名`
# 你的项目文件夹是 "role_bot"，服务名是 "rolebot"，所以镜像名是 "role_bot-rolebot"
$dockerImageName        = "role_bot-rolebot"
# 打包后的镜像文件名
$dockerImageArchiveName = "rolebot_image.tar"
$localArchiveFullPath = Join-Path $localProjectPath $dockerImageArchiveName # 定义打包后的完整路径

# ===================================================================
# ========================  脚本主体，无需修改  ========================
# ===================================================================

# 函数：打印彩色标题
function Write-SectionHeader {
    param (
        [string]$Title
    )
    Write-Host "`n"
    Write-Host "================================================================" -ForegroundColor Green
    Write-Host "  $Title" -ForegroundColor Green
    Write-Host "================================================================" -ForegroundColor Green
}

# 函数：执行本地命令并检查错误
function Invoke-LocalCommand {
    param (
        [scriptblock]$Command,
        [string]$Description
    )
    if ($Description) {
        Write-Host ">> $Description" -ForegroundColor Cyan
    } else {
        Write-Host ">> 正在执行: $($Command.ToString())" -ForegroundColor Cyan
    }

    & $Command # 使用 & 运算符确保参数正确传递
    if ($LASTEXITCODE -ne 0) {
        Write-Host "错误: 上一步操作失败，退出脚本。" -ForegroundColor Red
        exit 1
    }
    Write-Host ">> 操作成功。" -ForegroundColor Gray
}

# 函数：执行远程 SSH 命令并检查错误 (使用参数数组更健壮)
function Invoke-RemoteCommand {
    param (
        [string]$Command,
        [string]$Description,
        [string]$RemoteHost,
        [string]$User,
        [string]$PrivateKey
    )
    if ($Description) {
        Write-Host ">> $Description" -ForegroundColor Cyan
    } else {
        Write-Host ">> 正在执行远程命令: $($Command)" -ForegroundColor Cyan
    }

    # 使用参数数组构建 ssh 命令，避免特殊字符问题
    $sshArgs = @("$User@$RemoteHost", "-i", "$PrivateKey", "--", $Command)
    & ssh @sshArgs # 使用 @sshArgs 将数组元素作为单独参数传递

    if ($LASTEXITCODE -ne 0) {
        Write-Host "错误: 远程命令执行失败，退出脚本。" -ForegroundColor Red
        exit 1
    }
    Write-Host ">> 远程命令操作成功。" -ForegroundColor Gray
}


# --- 脚本开始 ---
Clear-Host
Write-SectionHeader "开始部署 RoleBot"

# 检查必要的环境变量是否已设置
if (-not $sshHost -or -not $sshUser -or -not $privateKeyPath) {
    Write-Host "错误: 必须设置 SSH_HOST, SSH_USER 和 SSH_PRIVATE_KEY_PATH 环境变量或在脚本开头加载它们。" -ForegroundColor Red
    exit 1
}

# 检查本地项目路径是否存在
if (-not (Test-Path $localProjectPath)) {
    Write-Host "错误: 本地项目路径 '$localProjectPath' 不存在。请编辑脚本并设置正确的路径。" -ForegroundColor Red
    exit 1
}

# 检查必要的本地文件是否存在
$localComposeFile = Join-Path $localProjectPath "docker-compose.yml"
$localDeployComposeFile = Join-Path $localProjectPath "docker-compose.deploy.yml"
$localEnvFile = Join-Path $localProjectPath ".env"
$localDockerfile = Join-Path $localProjectPath "Dockerfile"

if (-not (Test-Path $localComposeFile)) { Write-Host "错误: 文件 '$localComposeFile' 不存在。" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localDeployComposeFile)) { Write-Host "错误: 文件 '$localDeployComposeFile' 不存在。" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localEnvFile)) { Write-Host "错误: 文件 '$localEnvFile' 不存在。" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localDockerfile)) { Write-Host "错误: 文件 '$localDockerfile' 不存在。" -ForegroundColor Red; exit 1 }


# --- 交互式询问是否重新构建镜像 ---
$rebuildImage = Read-Host "是否重新构建 Docker 镜像？ (Y/n)"
if ($rebuildImage -eq "" -or $rebuildImage -eq "y" -or $rebuildImage -eq "Y") {
    $needsImageUpload = $true # 需要重新构建，所以也需要上传新镜像
    Write-SectionHeader "步骤 1 & 2: 在本地构建 Docker 镜像并打包"
    Push-Location $localProjectPath # 进入项目目录
    Invoke-LocalCommand -Command { docker-compose -f docker-compose.yml build } -Description "使用 docker-compose.yml 构建镜像..."
    Pop-Location # 返回原目录

    Invoke-LocalCommand -Command { docker save -o "$localArchiveFullPath" "$dockerImageName" } -Description "打包镜像 '$dockerImageName' 为 '$dockerImageArchiveName'..."

} else {
    $needsImageUpload = $false # 不需要构建，假设服务器已有正确镜像
    Write-SectionHeader "步骤 1 & 2: 跳过本地构建和打包镜像"
    Write-Host ">> 已选择跳过本地构建和打包镜像。将使用服务器上现有的镜像。" -ForegroundColor Yellow
    Write-Host ">> 如果服务器上没有该镜像，或者镜像是旧的，本次部署可能会失败或使用旧版本。" -ForegroundColor Yellow
}

# 3. 远程创建部署目录
Write-SectionHeader "步骤 3: 在服务器上创建部署目录"
$sshCommandMkdir = "mkdir -p $remoteDeployPath"
Invoke-RemoteCommand -Command $sshCommandMkdir -Description "在服务器上创建目录 '$remoteDeployPath'..." -Host $sshHost -User $sshUser -PrivateKey $privateKeyPath

# 4. 上传文件到服务器
Write-SectionHeader "步骤 4: 上传部署文件到服务器"

# 总是上传 .env 和 docker-compose.deploy.yml (并重命名)
Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localDeployComposeFile" "$($sshUser)@$($sshHost):$($remoteDeployPath)/docker-compose.yml" } -Description "上传并重命名 docker-compose.deploy.yml -> docker-compose.yml..."
Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localEnvFile" "$($sshUser)@$($sshHost):$($remoteDeployPath)/" } -Description "上传 '.env' 文件..."

# 只有在重新构建时才上传镜像文件
if ($needsImageUpload) {
    Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localArchiveFullPath" "$($sshUser)@$($sshHost):$($remoteDeployPath)/" } -Description "上传新的镜像文件 '$dockerImageArchiveName'..."
} else {
    Write-Host ">> 跳过上传镜像文件，因为它没有重新构建。" -ForegroundColor Yellow
}


# 5. 远程加载镜像并启动服务
Write-SectionHeader "步骤 5: 在服务器上加载镜像并启动服务"
$remoteArchiveFullPath = "$remoteDeployPath/$dockerImageArchiveName"

# 根据是否上传了新镜像，构建不同的远程命令
$remoteCommands = "cd $remoteDeployPath && "

if ($needsImageUpload) {
    # 如果上传了新镜像，先加载它
    $remoteCommands += "echo '>> 正在加载Docker镜像...' && " +
                       "docker load -i $remoteArchiveFullPath && " +
                       "echo '>> 镜像加载成功, ' && "
} else {
     Write-Host ">> 跳过远程加载镜像，因为它没有重新上传。" -ForegroundColor Yellow
}

# 启动服务，并进行清理
$remoteCommands += "echo '>> 正在使用 docker-compose 启动服务...' && " +
                   "docker-compose -f docker-compose.yml up -d --remove-orphans && " + # Added --remove-orphans
                   "echo '>> 服务启动成功, 正在检查容器状态...' && " +
                   "docker-compose ps && " +
                   "echo '>> 正在清理无用的镜像...' && " +
                   "docker image prune -f && "

# 只有在上传了新镜像时才清理远程的 tar 文件
if ($needsImageUpload) {
     $remoteCommands += "echo '>> 正在清理临时.tar文件...' && " +
                       "rm $remoteArchiveFullPath && "
}

$remoteCommands += "echo '>> 服务器端部署完成！'"

Invoke-RemoteCommand -Command $remoteCommands -Description "在服务器上执行部署命令..." -Host $sshHost -User $sshUser -PrivateKey $privateKeyPath

# 6. 清理本地临时文件 (只在生成了文件时清理)
Write-SectionHeader "步骤 6: 清理本地临时文件"
if ($needsImageUpload -and (Test-Path $localArchiveFullPath)) {
    Invoke-LocalCommand -Command { Remove-Item -Path $localArchiveFullPath -Force } -Description "清理本地临时文件 '$dockerImageArchiveName'..."
} else {
    Write-Host ">> 没有本地临时文件需要清理。" -ForegroundColor Yellow
}


Write-SectionHeader "🎉 部署成功完成！🎉"