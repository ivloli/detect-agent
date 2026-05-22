<#
.SYNOPSIS
    安装/更新 DetectAgent Windows 服务
.DESCRIPTION
    在 Windows 服务器上安装或更新 DetectAgent 为 Windows 服务，
    支持自动重启、日志记录和权限配置。
.PARAMETER InstallPath
    Agent 安装路径，默认为 C:\detect-agent
.PARAMETER ConfigPath
    配置文件路径，默认为 C:\detect-agent\config\config.yaml
.PARAMETER ServiceName
    Windows 服务名称，默认为 DetectAgent
.PARAMETER UserName
    服务运行用户（可选），默认使用 LocalSystem
.PARAMETER Password
    服务运行用户密码（可选）
.EXAMPLE
    # 默认安装
    .\install-service.ps1
    
    # 指定路径安装
    .\install-service.ps1 -InstallPath "D:\agent\detect-agent" -ConfigPath "D:\agent\config.yaml"
#>

param(
    [string]$InstallPath = "C:\detect-agent",
    [string]$ConfigPath = "$InstallPath\config\config.yaml",
    [string]$ServiceName = "DetectAgent",
    [string]$UserName = "",
    [string]$Password = ""
)

$ErrorActionPreference = "Stop"
$logFile = "$InstallPath\logs\install-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"

function Write-Log {
    param([string]$Message)
    $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$time $Message"
    Write-Host $line
    Add-Content -Path $logFile -Value $line -ErrorAction SilentlyContinue
}

# 确保目录存在
New-Item -ItemType Directory -Force -Path "$InstallPath\bin" | Out-Null
New-Item -ItemType Directory -Force -Path "$InstallPath\logs" | Out-Null
New-Item -ItemType Directory -Force -Path "$InstallPath\config" | Out-Null
New-Item -ItemType Directory -Force -Path "$InstallPath\backups" | Out-Null

$binaryPath = "$InstallPath\bin\detect-agent.exe"

# 检查二进制文件是否存在
if (-not (Test-Path $binaryPath)) {
    Write-Log "⚠️ 未找到 $binaryPath，服务将安装但无法启动"
    Write-Log "请先部署 detect-agent.exe 到 $binaryPath"
}

Write-Log "=========================================="
Write-Log "安装 DetectAgent Windows 服务"
Write-Log "服务名称:      $ServiceName"
Write-Log "安装路径:      $InstallPath"
Write-Log "配置文件:      $ConfigPath"
Write-Log "二进制文件:    $binaryPath"
Write-Log "=========================================="

# 1. 停止并删除旧服务（如果存在）
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService) {
    Write-Log "发现已有服务 $ServiceName，正在停止并删除..."
    
    if ($existingService.Status -eq 'Running') {
        sc.exe stop $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 3
        Write-Log "服务已停止"
    }
    
    sc.exe delete $ServiceName 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    Write-Log "旧服务已删除"
}

# 2. 构建服务命令行
$serviceCmd = "`"$binaryPath`" --conf `"$ConfigPath`""

# 3. 创建新服务
Write-Log "创建服务 $ServiceName ..."

$scArgs = @(
    "create", $ServiceName,
    "binPath=", $serviceCmd,
    "start=", "auto",
    "displayName=", "DetectAgent - 浏览器检测代理服务",
    "description=", "浏览器自动化检测服务，用于拦截检测任务的调度和执行"
)

if ($UserName -ne "" -and $Password -ne "") {
    Write-Log "使用指定用户运行: $UserName"
    $scArgs += "obj=", $UserName
    $scArgs += "password=", $Password
    sc.exe $scArgs 2>&1 | ForEach-Object { Write-Log $_ }
} else {
    Write-Log "使用 LocalSystem 账户运行"
    sc.exe create $ServiceName `
        binPath= $serviceCmd `
        start= auto `
        displayName= "DetectAgent - 浏览器检测代理服务" `
        description= "浏览器自动化检测服务，用于拦截检测任务的调度和执行" 2>&1 | ForEach-Object { Write-Log $_ }
}

# 4. 配置服务恢复选项（崩溃后自动重启）
Write-Log "配置服务恢复选项..."
sc.exe failure $ServiceName reset=86400 actions=restart/5000/restart/10000/restart/30000 2>&1 | Out-Null
sc.exe failureflag $ServiceName 1 2>&1 | Out-Null
# 配置服务权限（允许与桌面交互，浏览器自动化需要）
sc.exe privs $ServiceName SeChangeNotifyPrivilege 2>&1 | Out-Null

# 5. 记录安装信息
$installInfo = @{
    serviceName = $ServiceName
    installPath = $InstallPath
    configPath = $ConfigPath
    binaryPath = $binaryPath
    installedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    machineName = $env:COMPUTERNAME
}
$installInfo | ConvertTo-Json | Out-File "$InstallPath\install.json" -Force -Encoding utf8

Write-Log "=========================================="
Write-Log "✅ 服务安装完成!"
Write-Log ""
Write-Log "后续操作:"
Write-Log "  1. 确认 detect-agent.exe 已放置到: $binaryPath"
Write-Log "  2. 启动服务: sc start $ServiceName"
Write-Log "     或:     Start-Service -Name $ServiceName"
Write-Log "  3. 查看日志: Get-Content '$InstallPath\logs\detect-agent.log' -Tail 50"
Write-Log "  4. 更新版本: powershell -File '$InstallPath\update.ps1' -Version 'x.y.z'"
Write-Log "=========================================="

# 如果二进制存在则尝试启动
if (Test-Path $binaryPath) {
    Write-Log "检测到二进制文件，正在启动服务..."
    try {
        Start-Service -Name $ServiceName -ErrorAction Stop
        Start-Sleep -Seconds 3
        $svc = Get-Service -Name $ServiceName
        if ($svc.Status -eq 'Running') {
            Write-Log "✅ 服务已成功启动!"
        }
    } catch {
        Write-Log "⚠️ 服务启动失败: $_"
        Write-Log "可手动检查日志后尝试启动"
    }
}