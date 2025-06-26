<#
.SYNOPSIS
    һ������RoleBot��������ECS��
    �ýű��ڱ��ع���Docker���񣬴�����ϴ�������������Զ��ִ�в������
.DESCRIPTION
    ִ������:
    1. ��ѡ: �ڱ���ʹ�� docker-compose.yml ��������
    2. ��ѡ: ʹ�� docker save ���������� .tar �ļ���
    3. ��Զ�̷������ϴ�������Ŀ¼��
    4. ʹ�� scp �ϴ���
        - ���� .tar �ļ� (���ִ���˹���)��
        - .env �ļ���
        - docker-compose.deploy.yml (��������Ϊ docker-compose.yml)��
    5. Զ��ִ�� docker load ���ؾ��� (����ϴ����µľ����ļ�)��
    6. Զ��ִ�� docker-compose up -d ��������
    7. �����غ�Զ�̵���ʱ�ļ���
#>

# ===================================================================
# ========================  ���������±���  ========================
# ===================================================================

# ʾ��: ���� deploy.env �ļ�
$deployEnvPath = Join-Path $PSScriptRoot "deploy.env" # ���� deploy.env �ͽű���ͬһĿ¼
if (Test-Path $deployEnvPath) {
    Get-Content $deployEnvPath | ForEach-Object {
        if ($_ -match "^([^#=]+?)=(.*)$") {
            $varName = $Matches[1].Trim()
            $varValue = $Matches[2].Trim()
            # �Ƴ����ţ��������
            if ($varValue -match '^"(.*)"$' -or $varValue -match "^'(.*)'$") {
                $varValue = $Matches[1]
            }
            Write-Verbose "Setting environment variable: $varName = $varValue"
            Set-Item -Path Env:$varName -Value $varValue
        }
    }
} else {
     Write-Host "����: δ�ҵ� deploy.env �ļ���������ʹ�������õĻ���������" -ForegroundColor Yellow
}

# --- SSH �ͷ��������� ---
$sshHost            = $env:SSH_HOST
$sshUser            = $env:SSH_USER
$privateKeyPath     = $env:SSH_PRIVATE_KEY_PATH

# --- ������Ŀ·������ ---
# !!! ��Ҫ: �뽫��·���޸�Ϊ�㱾��RoleBot��Ŀ�ġ�����·���� !!!
$localProjectPath   = "C:\Users\Creeper10\Desktop\ProjectForFun\role_bot" # <---- �޸��������

# --- Զ�̲���·������ ---
$remoteDeployPath   = "/opt/rolebot"

# --- Docker ���� ---
# `docker-compose.yml`�ж���ķ�������Ĭ��Ϊ'rolebot'
$dockerServiceName      = "rolebot"
# ���ع������ľ������ƣ���ʽΪ`��Ŀ�ļ�����-������`
# �����Ŀ�ļ����� "role_bot"���������� "rolebot"�����Ծ������� "role_bot-rolebot"
$dockerImageName        = "role_bot-rolebot"
# �����ľ����ļ���
$dockerImageArchiveName = "rolebot_image.tar"
$localArchiveFullPath = Join-Path $localProjectPath $dockerImageArchiveName # �������������·��

# ===================================================================
# ========================  �ű����壬�����޸�  ========================
# ===================================================================

# ��������ӡ��ɫ����
function Write-SectionHeader {
    param (
        [string]$Title
    )
    Write-Host "`n"
    Write-Host "================================================================" -ForegroundColor Green
    Write-Host "  $Title" -ForegroundColor Green
    Write-Host "================================================================" -ForegroundColor Green
}

# ������ִ�б������������
function Invoke-LocalCommand {
    param (
        [scriptblock]$Command,
        [string]$Description
    )
    if ($Description) {
        Write-Host ">> $Description" -ForegroundColor Cyan
    } else {
        Write-Host ">> ����ִ��: $($Command.ToString())" -ForegroundColor Cyan
    }

    & $Command # ʹ�� & �����ȷ��������ȷ����
    if ($LASTEXITCODE -ne 0) {
        Write-Host "����: ��һ������ʧ�ܣ��˳��ű���" -ForegroundColor Red
        exit 1
    }
    Write-Host ">> �����ɹ���" -ForegroundColor Gray
}

# ������ִ��Զ�� SSH ��������� (ʹ�ò����������׳)
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
        Write-Host ">> ����ִ��Զ������: $($Command)" -ForegroundColor Cyan
    }

    # ʹ�ò������鹹�� ssh ������������ַ�����
    $sshArgs = @("$User@$RemoteHost", "-i", "$PrivateKey", "--", $Command)
    & ssh @sshArgs # ʹ�� @sshArgs ������Ԫ����Ϊ������������

    if ($LASTEXITCODE -ne 0) {
        Write-Host "����: Զ������ִ��ʧ�ܣ��˳��ű���" -ForegroundColor Red
        exit 1
    }
    Write-Host ">> Զ����������ɹ���" -ForegroundColor Gray
}


# --- �ű���ʼ ---
Clear-Host
Write-SectionHeader "��ʼ���� RoleBot"

# ����Ҫ�Ļ��������Ƿ�������
if (-not $sshHost -or -not $sshUser -or -not $privateKeyPath) {
    Write-Host "����: �������� SSH_HOST, SSH_USER �� SSH_PRIVATE_KEY_PATH �����������ڽű���ͷ�������ǡ�" -ForegroundColor Red
    exit 1
}

# ��鱾����Ŀ·���Ƿ����
if (-not (Test-Path $localProjectPath)) {
    Write-Host "����: ������Ŀ·�� '$localProjectPath' �����ڡ���༭�ű���������ȷ��·����" -ForegroundColor Red
    exit 1
}

# ����Ҫ�ı����ļ��Ƿ����
$localComposeFile = Join-Path $localProjectPath "docker-compose.yml"
$localDeployComposeFile = Join-Path $localProjectPath "docker-compose.deploy.yml"
$localEnvFile = Join-Path $localProjectPath ".env"
$localDockerfile = Join-Path $localProjectPath "Dockerfile"

if (-not (Test-Path $localComposeFile)) { Write-Host "����: �ļ� '$localComposeFile' �����ڡ�" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localDeployComposeFile)) { Write-Host "����: �ļ� '$localDeployComposeFile' �����ڡ�" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localEnvFile)) { Write-Host "����: �ļ� '$localEnvFile' �����ڡ�" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $localDockerfile)) { Write-Host "����: �ļ� '$localDockerfile' �����ڡ�" -ForegroundColor Red; exit 1 }


# --- ����ʽѯ���Ƿ����¹������� ---
$rebuildImage = Read-Host "�Ƿ����¹��� Docker ���� (Y/n)"
if ($rebuildImage -eq "" -or $rebuildImage -eq "y" -or $rebuildImage -eq "Y") {
    $needsImageUpload = $true # ��Ҫ���¹���������Ҳ��Ҫ�ϴ��¾���
    Write-SectionHeader "���� 1 & 2: �ڱ��ع��� Docker ���񲢴��"
    Push-Location $localProjectPath # ������ĿĿ¼
    Invoke-LocalCommand -Command { docker-compose -f docker-compose.yml build } -Description "ʹ�� docker-compose.yml ��������..."
    Pop-Location # ����ԭĿ¼

    Invoke-LocalCommand -Command { docker save -o "$localArchiveFullPath" "$dockerImageName" } -Description "������� '$dockerImageName' Ϊ '$dockerImageArchiveName'..."

} else {
    $needsImageUpload = $false # ����Ҫ���������������������ȷ����
    Write-SectionHeader "���� 1 & 2: �������ع����ʹ������"
    Write-Host ">> ��ѡ���������ع����ʹ�����񡣽�ʹ�÷����������еľ���" -ForegroundColor Yellow
    Write-Host ">> �����������û�иþ��񣬻��߾����Ǿɵģ����β�����ܻ�ʧ�ܻ�ʹ�þɰ汾��" -ForegroundColor Yellow
}

# 3. Զ�̴�������Ŀ¼
Write-SectionHeader "���� 3: �ڷ������ϴ�������Ŀ¼"
$sshCommandMkdir = "mkdir -p $remoteDeployPath"
Invoke-RemoteCommand -Command $sshCommandMkdir -Description "�ڷ������ϴ���Ŀ¼ '$remoteDeployPath'..." -Host $sshHost -User $sshUser -PrivateKey $privateKeyPath

# 4. �ϴ��ļ���������
Write-SectionHeader "���� 4: �ϴ������ļ���������"

# �����ϴ� .env �� docker-compose.deploy.yml (��������)
Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localDeployComposeFile" "$($sshUser)@$($sshHost):$($remoteDeployPath)/docker-compose.yml" } -Description "�ϴ��������� docker-compose.deploy.yml -> docker-compose.yml..."
Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localEnvFile" "$($sshUser)@$($sshHost):$($remoteDeployPath)/" } -Description "�ϴ� '.env' �ļ�..."

# ֻ�������¹���ʱ���ϴ������ļ�
if ($needsImageUpload) {
    Invoke-LocalCommand -Command { scp -i "$privateKeyPath" "$localArchiveFullPath" "$($sshUser)@$($sshHost):$($remoteDeployPath)/" } -Description "�ϴ��µľ����ļ� '$dockerImageArchiveName'..."
} else {
    Write-Host ">> �����ϴ������ļ�����Ϊ��û�����¹�����" -ForegroundColor Yellow
}


# 5. Զ�̼��ؾ�����������
Write-SectionHeader "���� 5: �ڷ������ϼ��ؾ�����������"
$remoteArchiveFullPath = "$remoteDeployPath/$dockerImageArchiveName"

# �����Ƿ��ϴ����¾��񣬹�����ͬ��Զ������
$remoteCommands = "cd $remoteDeployPath && "

if ($needsImageUpload) {
    # ����ϴ����¾����ȼ�����
    $remoteCommands += "echo '>> ���ڼ���Docker����...' && " +
                       "docker load -i $remoteArchiveFullPath && " +
                       "echo '>> ������سɹ�, ' && "
} else {
     Write-Host ">> ����Զ�̼��ؾ�����Ϊ��û�������ϴ���" -ForegroundColor Yellow
}

# �������񣬲���������
$remoteCommands += "echo '>> ����ʹ�� docker-compose ��������...' && " +
                   "docker-compose -f docker-compose.yml up -d --remove-orphans && " + # Added --remove-orphans
                   "echo '>> ���������ɹ�, ���ڼ������״̬...' && " +
                   "docker-compose ps && " +
                   "echo '>> �����������õľ���...' && " +
                   "docker image prune -f && "

# ֻ�����ϴ����¾���ʱ������Զ�̵� tar �ļ�
if ($needsImageUpload) {
     $remoteCommands += "echo '>> ����������ʱ.tar�ļ�...' && " +
                       "rm $remoteArchiveFullPath && "
}

$remoteCommands += "echo '>> �������˲�����ɣ�'"

Invoke-RemoteCommand -Command $remoteCommands -Description "�ڷ�������ִ�в�������..." -Host $sshHost -User $sshUser -PrivateKey $privateKeyPath

# 6. ��������ʱ�ļ� (ֻ���������ļ�ʱ����)
Write-SectionHeader "���� 6: ��������ʱ�ļ�"
if ($needsImageUpload -and (Test-Path $localArchiveFullPath)) {
    Invoke-LocalCommand -Command { Remove-Item -Path $localArchiveFullPath -Force } -Description "��������ʱ�ļ� '$dockerImageArchiveName'..."
} else {
    Write-Host ">> û�б�����ʱ�ļ���Ҫ����" -ForegroundColor Yellow
}


Write-SectionHeader "�9�5 ����ɹ���ɣ��9�5"