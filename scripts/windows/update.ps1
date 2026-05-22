<#
.SYNOPSIS
    DetectAgent 更新/回滚脚本
.DESCRIPTION
    从指定源下载新版本并执行热更新，支持多种文件源：
    - GitLab Release (如果公司用 GitLab，这是最佳选择，零成本)
    - Jenkins 构建产物 (Jenkins 直接 serve)
    - HTTP 文件服务器 (用 nginx/python 随便搭一个)
    - SMB 网络共享 (最简单，但需网络打通)
    - 直接本地文件 (Jenkins 推送过来后用此脚本执行)
    
    支持 SHA256 校验、备份、回滚和版本记录。
.PARAMETER Version
    目标版本号，例如 "1.3.0"
.PARAMETER Source
    文件源类型: gitlab | jenkins | http | smb | local
.PARAMETER SourceUrl
    源地址，根据 Source 类型不同含义不同
.PARAMETER InstallPath
    Agent 安装路径，默认为 C:\detect-agent
.PARAMETER ServiceName
    Windows 服务名称，默认为 DetectAgent
.PARAMETER Force
    强制更新，即使版本相同也执行
.PARAMETER SkipVerify
    跳过更新后的验证（测试用）
.EXAMPLE
    # GitLab Release (推荐，零成本)
    .\update.ps1 -Version "1.3.0" -Source gitlab -SourceUrl "https://gitlab.gainetics.io/api/v4/projects/123/packages/generic/detect-agent/1.3.0/detect-agent.exe"
    
    # Jenkins 构建产物
    .\update.ps1 -Version "1.3.0" -Source jenkins -SourceUrl "http://jenkins.internal/job/detect-agent/lastSuccessfulBuild/artifact/build/windows/detect-agent.exe"
    
    # HTTP 文件服务器 (nginx/python 随便搭一个)
    .\update.ps1 -Version "1.3.0" -Source http -SourceUrl "http://files.internal/detect-agent/"
    
    # SMB 网络共享
    .\update.ps1 -Version "1.3.0" -Source smb -SourceUrl "\\fileserver\share\detect-agent"
    
    # 本地文件 (Jenkins 直接把 exe 推送过来)
    .\update.ps1 -Version "1.3.0" -Source local -SourceUrl "D:\temp\detect-agent-1.3.0.exe"
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$Version,
    
    [ValidateSet("gitlab", "jenkins", "http", "smb", "local", "")]
    [string]$Source = "",
    
    [string]$SourceUrl = "",
    
    [string]$InstallPath = "C:\detect-agent",
    
    [string]$ServiceName = "DetectAgent",
    
    [switch]$Force,
    
    [switch]$SkipVerify,
    
    # 兼容旧参数名 NexusUrl
    [Alias("NexusUrl")]
    [string]$DownloadUrl = ""
)

$ErrorActionPreference = "Stop"

# 日志
$logDir = "$InstallPath\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = "$logDir\update-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"

$binaryDir = "$InstallPath\bin"
$binaryName = "detect-agent.exe"
$binaryPath = "$binaryDir\$binaryName"
$versionFilePath = "$InstallPath\version.json"

# ========== 工具函数 ==========

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$time [$Level] $Message"
    switch ($Level) {
        "ERROR" { Write-Host $line -ForegroundColor Red }
        "WARN"  { Write-Host $line -ForegroundColor Yellow }
        "OK"    { Write-Host $line -ForegroundColor Green }
        default { Write-Host $line }
    }
    Add-Content -Path $logFile -Value $line -ErrorAction SilentlyContinue
}

function Read-ConfigValue {
    param([string]$Key)
    $configFile = "$InstallPath\config\config.yaml"
    if (Test-Path $configFile) {
        $content = Get-Content $configFile -Raw
        if ($content -match "$Key:\s*(.+)$") {
            return $matches[1].Trim().Trim('"')
        }
    }
    # 也检查 env.json
    $envFile = "$InstallPath\config\env.json"
    if (Test-Path $envFile) {
        try {
            $env = Get-Content $envFile -Raw | ConvertFrom-Json
            if ($env.$Key) { return $env.$Key }
        } catch {}
    }
    return $null
}

function Test-CommandExists {
    param([string]$Command)
    $result = Get-Command $Command -ErrorAction SilentlyContinue
    return $result -ne $null
}

# ========== 文件下载函数 ==========

function Download-FromGitLab {
    param([string]$Url, [string]$OutFile)
    
    Write-Log "从 GitLab Release 下载: $Url"
    
    # 从环境变量或配置文件读取 GitLab Token
    $token = $env:GITLAB_TOKEN
    if (-not $token) {
        $token = Read-ConfigValue "gitlab_token"
    }
    
    $headers = @{}
    if ($token) {
        $headers["PRIVATE-TOKEN"] = $token
        Write-Log "使用 GitLab Token 认证"
    }
    
    # 尝试下载 .sha256 文件并校验
    $shaUrl = "$Url.sha256"
    try {
        if ($token) {
            $shaContent = (Invoke-WebRequest -Uri $shaUrl -Headers $headers -TimeoutSec 30 -UseBasicParsing).Content
        } else {
            $shaContent = (Invoke-WebRequest -Uri $shaUrl -TimeoutSec 30 -UseBasicParsing).Content
        }
        $global:expectedSha256 = $shaContent.Trim().Split()[0]
        Write-Log "找到 SHA256: $global:expectedSha256" "OK"
    } catch {
        Write-Log "无 SHA256 校验文件，跳过校验" "WARN"
        $global:expectedSha256 = $null
    }
    
    # 下载文件
    if ($token) {
        Invoke-WebRequest -Uri $Url -Headers $headers -OutFile $OutFile -TimeoutSec 300 -UseBasicParsing
    } else {
        Invoke-WebRequest -Uri $Url -OutFile $OutFile -TimeoutSec 300 -UseBasicParsing
    }
}

function Download-FromJenkins {
    param([string]$Url, [string]$OutFile)
    
    Write-Log "从 Jenkins 下载: $Url"
    
    # 从环境变量读取 Jenkins Token
    $token = $env:JENKINS_TOKEN
    $user = $env:JENKINS_USER
    
    if ($token -and $user) {
        # Basic Auth
        $pair = "$user`:$token"
        $bytes = [System.Text.Encoding]::ASCII.GetBytes($pair)
        $base64 = [System.Convert]::ToBase64String($bytes)
        $headers = @{ Authorization = "Basic $base64" }
        Invoke-WebRequest -Uri $Url -Headers $headers -OutFile $OutFile -TimeoutSec 300 -UseBasicParsing
    } else {
        # 尝试 .sha256
        $shaUrl = "$Url.sha256"
        try {
            $shaContent = (Invoke-WebRequest -Uri $shaUrl -TimeoutSec 30 -UseBasicParsing).Content
            $global:expectedSha256 = $shaContent.Trim().Split()[0]
        } catch {
            $global:expectedSha256 = $null
        }
        Invoke-WebRequest -Uri $Url -OutFile $OutFile -TimeoutSec 300 -UseBasicParsing
    }
}

function Download-FromHttp {
    param([string]$BaseUrl, [string]$Version, [string]$OutFile)
    
    # 支持两种格式：
    #   http://files.internal/detect-agent/v1.3.0/detect-agent.exe
    #   http://files.internal/detect-agent/detect-agent-1.3.0.exe
    $urlCandidates = @(
        "$BaseUrl/v$Version/detect-agent.exe",
        "$BaseUrl/detect-agent-$Version.exe",
        "$BaseUrl/detect-agent.exe"
    )
    
    $downloaded = $false
    foreach ($url in $urlCandidates) {
        Write-Log "尝试下载: $url"
        try {
            Invoke-WebRequest -Uri $url -OutFile $OutFile -TimeoutSec 300 -UseBasicParsing
            $downloaded = $true
            Write-Log "下载成功: $url" "OK"
            
            # 尝试 SHA256
            try {
                $shaContent = (Invoke-WebRequest -Uri "$url.sha256" -TimeoutSec 15 -UseBasicParsing).Content
                $global:expectedSha256 = $shaContent.Trim().Split()[0]
            } catch {
                $global:expectedSha256 = $null
            }
            break
        } catch {
            Write-Log "失败: $_" "WARN"
        }
    }
    
    if (-not $downloaded) {
        throw "所有下载地址都失败了"
    }
}

function Download-FromSmb {
    param([string]$SharePath, [string]$Version, [string]$OutFile)
    
    Write-Log "从 SMB 共享复制: $SharePath"
    
    $candidates = @(
        "$SharePath\v$Version\detect-agent.exe",
        "$SharePath\detect-agent-$Version.exe",
        "$SharePath\detect-agent.exe"
    )
    
    $copied = $false
    foreach ($src in $candidates) {
        if (Test-Path $src) {
            Write-Log "找到文件: $src"
            Copy-Item $src $OutFile -Force
            $copied = $true
            
            # 尝试 SHA256
            $shaFile = "$src.sha256"
            if (Test-Path $shaFile) {
                $global:expectedSha256 = (Get-Content $shaFile -Raw).Trim().Split()[0]
            } else {
                $global:expectedSha256 = $null
            }
            break
        }
    }
    
    if (-not $copied) {
        throw "SMB 共享中未找到文件: $SharePath"
    }
}

function Copy-FromLocal {
    param([string]$LocalPath, [string]$OutFile)
    
    Write-Log "从本地路径复制: $LocalPath"
    
    if (-not (Test-Path $LocalPath)) {
        throw "本地文件不存在: $LocalPath"
    }
    
    Copy-Item $LocalPath $OutFile -Force
    
    # 尝试 SHA256
    $shaFile = "$LocalPath.sha256"
    if (Test-Path $shaFile) {
        $global:expectedSha256 = (Get-Content $shaFile -Raw).Trim().Split()[0]
    } else {
        $global:expectedSha256 = $null
    }
}

# ========== 主逻辑 ==========

try {
    Write-Log "=========================================="
    Write-Log "DetectAgent 更新开始"
    Write-Log "目标版本: $Version"
    Write-Log "服务器:   $env:COMPUTERNAME"
    Write-Log "操作系统: $(Get-CimInstance Win32_OperatingSystem | Select-Object -ExpandProperty Caption)"
    Write-Log "=========================================="
    
    # 1. 读取当前版本
    $currentVersion = "未知"
    if (Test-Path $versionFilePath) {
        try {
            $versionInfo = Get-Content $versionFilePath -Raw | ConvertFrom-Json
            $currentVersion = $versionInfo.version
        } catch {
            Write-Log "无法解析版本文件，将忽略" "WARN"
        }
    }
    Write-Log "当前版本: $currentVersion"
    
    # 2. 判断是否需要更新
    if ($currentVersion -eq $Version -and -not $Force) {
        Write-Log "当前版本 ($currentVersion) 已是最新，跳过更新" "OK"
        exit 0
    }
    
    # 3. 确定 Source 类型和地址
    if ([string]::IsNullOrEmpty($Source)) {
        # 自动检测
        if ($SourceUrl -match "^https?://gitlab" -or $SourceUrl -match "api/v4/projects") {
            $Source = "gitlab"
        } elseif ($SourceUrl -match "^https?://.*jenkins" -or $SourceUrl -match "/job/") {
            $Source = "jenkins"
        } elseif ($SourceUrl -match "^\\\\") {
            $Source = "smb"
        } elseif (Test-Path $SourceUrl -ErrorAction SilentlyContinue) {
            $Source = "local"
        } elseif ($SourceUrl -match "^https?://") {
            $Source = "http"
        } else {
            $Source = "http"
        }
        Write-Log "自动检测文件源: $Source"
    }
    
    # 如果 SourceUrl 为空，从配置文件读取
    if ([string]::IsNullOrEmpty($SourceUrl)) {
        $configUrl = Read-ConfigValue "update_url"
        if ($configUrl) {
            $SourceUrl = $configUrl
            Write-Log "从配置文件读取更新地址: $SourceUrl"
        } else {
            throw "未指定 SourceUrl，请在参数传入或在 config.yaml 中配置 update_url"
        }
    }
    
    Write-Log "文件源: $Source"
    Write-Log "地址:   $SourceUrl"
    
    # 4. 下载文件
    $newBinaryPath = "$binaryDir\detect-agent.exe.new"
    $global:expectedSha256 = $null
    
    switch ($Source) {
        "gitlab"  { Download-FromGitLab  -Url $SourceUrl -OutFile $newBinaryPath }
        "jenkins" { Download-FromJenkins -Url $SourceUrl -OutFile $newBinaryPath }
        "http"    { Download-FromHttp    -BaseUrl $SourceUrl -Version $Version -OutFile $newBinaryPath }
        "smb"     { Download-FromSmb     -SharePath $SourceUrl -Version $Version -OutFile $newBinaryPath }
        "local"   { Copy-FromLocal       -LocalPath $SourceUrl -OutFile $newBinaryPath }
        default   { throw "不支持的文件源: $Source" }
    }
    
    if (-not (Test-Path $newBinaryPath)) {
        throw "文件下载/复制失败"
    }
    
    $fileSize = (Get-Item $newBinaryPath).Length
    Write-Log "文件大小: $([math]::Round($fileSize / 1MB, 2)) MB" "OK"
    
    # 5. SHA256 校验
    if ($global:expectedSha256) {
        Write-Log "校验文件完整性..."
        $actualHash = (Get-FileHash $newBinaryPath -Algorithm SHA256).Hash.ToLower()
        Write-Log "期望 SHA256: $($global:expectedSha256.ToLower())"
        Write-Log "实际 SHA256: $actualHash"
        
        if ($global:expectedSha256.ToLower() -ne $actualHash) {
            throw "SHA256 校验失败!"
        }
        Write-Log "SHA256 校验通过 ✅" "OK"
    } else {
        Write-Log "跳过 SHA256 校验（无校验文件）" "WARN"
    }
    
    # 6. 备份旧版本
    if (Test-Path $binaryPath) {
        $backupFile = "$InstallPath\backups\detect-agent-$currentVersion.exe"
        Write-Log "备份当前版本到 $backupFile ..."
        Copy-Item $binaryPath $backupFile -Force
        Write-Log "备份完成" "OK"
    }
    
    # 7. 停止服务
    Write-Log "正在停止服务 $ServiceName ..."
    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svc -and $svc.Status -eq 'Running') {
        Stop-Service -Name $ServiceName -Force
        $timeout = 30
        $elapsed = 0
        do {
            Start-Sleep -Seconds 1
            $elapsed++
            $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
            if (-not $svc -or $svc.Status -eq 'Stopped') { break }
        } while ($elapsed -lt $timeout)
        
        if ($svc -and $svc.Status -ne 'Stopped') {
            throw "服务无法在 ${timeout}秒内停止"
        }
        Write-Log "服务已停止" "OK"
    } else {
        Write-Log "服务未运行，跳过停止"
    }
    
    Start-Sleep -Seconds 2
    
    # 8. 替换二进制
    Write-Log "替换二进制文件..."
    $rollbackFile = "$binaryDir\detect-agent.exe.rollback"
    if (Test-Path $binaryPath) {
        Copy-Item $binaryPath $rollbackFile -Force
    }
    Move-Item $newBinaryPath $binaryPath -Force
    Write-Log "二进制替换完成" "OK"
    
    # 9. 记录版本信息
    $newVersionInfo = @{
        version = $Version
        previousVersion = $currentVersion
        source = $Source
        sourceUrl = $SourceUrl
        updatedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        updatedBy = "$env:USERDOMAIN\$env:USERNAME"
        machineName = $env:COMPUTERNAME
        osVersion = (Get-CimInstance Win32_OperatingSystem).Version
        fileSizeBytes = $fileSize
        fileHash = $global:expectedSha256
    }
    
    $updateHistory = @()
    $historyFile = "$InstallPath\update-history.json"
    if (Test-Path $historyFile) {
        try {
            $updateHistory = Get-Content $historyFile -Raw | ConvertFrom-Json
        } catch {}
    }
    $updateHistory += $newVersionInfo
    $updateHistory | ConvertTo-Json -Depth 10 | Out-File $historyFile -Force -Encoding utf8
    
    $newVersionInfo | ConvertTo-Json | Out-File $versionFilePath -Force -Encoding utf8
    Write-Log "版本记录已保存: $Version" "OK"
    
    # 10. 启动服务
    Write-Log "正在启动服务 $ServiceName ..."
    try {
        Start-Service -Name $ServiceName -ErrorAction Stop
        Start-Sleep -Seconds 5
    } catch {
        Write-Log "服务启动失败: $_" "ERROR"
        Write-Log "执行回滚..." "WARN"
        if (Test-Path $rollbackFile) {
            Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
            Move-Item $rollbackFile $binaryPath -Force
            Start-Service -Name $ServiceName -ErrorAction SilentlyContinue
            Write-Log "已回滚到旧版本" "WARN"
        }
        throw "更新失败，已自动回滚"
    }
    
    # 11. 验证
    if (-not $SkipVerify) {
        Write-Log "等待服务稳定运行（10秒）..."
        Start-Sleep -Seconds 10
        
        $svc = Get-Service -Name $ServiceName -ErrorAction Stop
        if ($svc.Status -eq 'Running') {
            Write-Log "✅ 服务运行正常!" "OK"
        } else {
            Write-Log "⚠️ 服务状态异常: $($svc.Status)" "WARN"
        }
        
        $process = Get-Process -Name "detect-agent" -ErrorAction SilentlyContinue
        if ($process) {
            Write-Log "进程 PID: $($process.Id) | 内存: $([math]::Round($process.WorkingSet64 / 1MB, 1)) MB" "OK"
        }
    }
    
    # 清理旧备份（保留最近5个）
    Get-ChildItem "$InstallPath\backups\*.exe" | Sort-Object LastWriteTime -Descending | 
        Select-Object -Skip 5 | ForEach-Object { Remove-Item $_.FullName -Force }
    
    if (Test-Path $rollbackFile) { Remove-Item $rollbackFile -Force }
    
    Write-Log "=========================================="
    Write-Log "✅ 更新完成!"
    Write-Log "   版本: ${currentVersion} → ${Version}"
    Write-Log "   来源: $Source"
    Write-Log "   服务器: $env:COMPUTERNAME"
    Write-Log "=========================================="
    
    # 删除 update.flag（如果有）
    $flagFile = "$InstallPath\update.flag"
    if (Test-Path $flagFile) { Remove-Item $flagFile -Force }
    
    exit 0
    
} catch {
    Write-Log "❌ 更新失败: $_" "ERROR"
    Write-Log "详细日志: $logFile" "ERROR"
    exit 1
}