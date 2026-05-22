<#
.SYNOPSIS
    DetectAgent 机器自注册脚本
.DESCRIPTION
    部署在黄金镜像的开机启动中，新机器启动时自动向 CMDB/API
    注册本机信息（IP、主机名、已安装浏览器等），
    并将 IP 信息写入本地文件用于 Jenkins 部署。
.PARAMETER ApiEndpoint
    注册 API 地址，默认从本地配置文件读取
.PARAMETER Region
    服务器区域
.EXAMPLE
    # 开机自启默认运行
    .\register-agent.ps1
    
    # 指定 API 地址
    .\register-agent.ps1 -ApiEndpoint "http://cmdb.internal/agents/register"
#>

param(
    [string]$ApiEndpoint = "",
    [string]$Region = "ap-east-1"
)

$ErrorActionPreference = "Continue"  # 不阻塞开机启动

$logFile = "C:\detect-agent\logs\register-$(Get-Date -Format 'yyyyMMdd').log"

function Write-Log {
    param([string]$Message)
    $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$time $Message"
    Add-Content -Path $logFile -Value $line
    Write-Host $line
}

Write-Log "===== DetectAgent 机器注册开始 ====="
Write-Log "主机名: $env:COMPUTERNAME"

# 1. 收集本机信息
try {
    # IP 地址
    $ipAddresses = (Get-NetIPAddress -AddressFamily IPv4 | 
        Where-Object { 
            $_.InterfaceAlias -notlike "*Loopback*" -and 
            $_.PrefixOrigin -ne "WellKnown" -and
            $_.IPAddress -notmatch "^127\.|^169\.254\."
        } |
        Select-Object -ExpandProperty IPAddress)
    
    $primaryIP = $ipAddresses | Select-Object -First 1
    Write-Log "主 IP: $primaryIP"
    Write-Log "全部 IP: $($ipAddresses -join ', ')"
    
    # 检测已安装的浏览器
    $browsers = @{}
    
    # Chrome
    $chromePaths = @(
        "C:\Program Files\Google\Chrome\Application\chrome.exe",
        "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        "${env:LOCALAPPDATA}\Google\Chrome\Application\chrome.exe"
    )
    foreach ($p in $chromePaths) {
        if (Test-Path $p) {
            $version = (Get-Item $p).VersionInfo.FileVersion
            $browsers["chrome"] = @{ installed = $true; path = $p; version = $version }
            Write-Log "发现 Chrome: $version (路径: $p)"
            break
        }
    }
    
    # Edge
    $edgePaths = @(
        "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        "C:\Program Files\Microsoft\Edge\Application\msedge.exe"
    )
    foreach ($p in $edgePaths) {
        if (Test-Path $p) {
            $version = (Get-Item $p).VersionInfo.FileVersion
            $browsers["edge"] = @{ installed = $true; path = $p; version = $version }
            Write-Log "发现 Edge: $version (路径: $p)"
            break
        }
    }
    
    # Firefox
    $firefoxPaths = @(
        "C:\Program Files\Mozilla Firefox\firefox.exe",
        "C:\Program Files (x86)\Mozilla Firefox\firefox.exe"
    )
    foreach ($p in $firefoxPaths) {
        if (Test-Path $p) {
            $version = (Get-Item $p).VersionInfo.FileVersion
            $browsers["firefox"] = @{ installed = $true; path = $p; version = $version }
            Write-Log "发现 Firefox: $version (路径: $p)"
            break
        }
    }
    
    # 系统信息
    $osInfo = Get-CimInstance Win32_OperatingSystem
    $cpuInfo = Get-CimInstance Win32_Processor | Select-Object -First 1
    $memInfo = Get-CimInstance Win32_ComputerSystem
    
    $memoryGB = [math]::Round($memInfo.TotalPhysicalMemory / 1GB, 1)
    $cpuCores = $cpuInfo.NumberOfCores
    $cpuLogical = $cpuInfo.NumberOfLogicalProcessors
    
    Write-Log "系统: $($osInfo.Caption)"
    Write-Log "CPU: ${cpuCores}核/${cpuLogical}线程"
    Write-Log "内存: ${memoryGB}GB"
    
    # 2. 组装注册数据
    $registrationData = @{
        hostname = $env:COMPUTERNAME
        ip = $primaryIP
        allIPs = $ipAddresses
        region = $Region
        os = "$($osInfo.Caption) ($($osInfo.Version))"
        cpu = "${cpuCores} cores / ${cpuLogical} logical"
        memoryGB = $memoryGB
        browsers = $browsers
        detectAgent = $null
        registeredAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        status = "online"
    }
    
    # 检测 DetectAgent 自身状态
    $svc = Get-Service -Name "DetectAgent" -ErrorAction SilentlyContinue
    $agentExe = "C:\detect-agent\bin\detect-agent.exe"
    $agentVersion = $null
    $versionFile = "C:\detect-agent\version.json"
    
    if (Test-Path $versionFile) {
        try {
            $v = Get-Content $versionFile -Raw | ConvertFrom-Json
            $agentVersion = $v.version
        } catch {}
    }
    
    $registrationData.detectAgent = @{
        serviceStatus = if ($svc) { $svc.Status.ToString() } else { "not_installed" }
        version = $agentVersion
        binaryExists = Test-Path $agentExe
        binarySize = if (Test-Path $agentExe) { (Get-Item $agentExe).Length } else { 0 }
    }
    
    Write-Log "Agent 状态: $($registrationData.detectAgent.serviceStatus) | 版本: $($registrationData.detectAgent.version)"
    
    # 3. 注册到本地 IP 清单文件（用于 Jenkins 读取）
    $localInventoryFile = "C:\detect-agent\config\inventory.json"
    
    $inventory = @{ agents = @() }
    if (Test-Path $localInventoryFile) {
        try {
            $inventory = Get-Content $localInventoryFile -Raw | ConvertFrom-Json
            if (-not $inventory.agents) { $inventory.agents = @() }
        } catch {
            $inventory = @{ agents = @() }
        }
    }
    
    # 更新或添加本机记录
    $inventoryAgents = $inventory.agents | Where-Object { $_.ip -ne $primaryIP -and $_.hostname -ne $env:COMPUTERNAME }
    $inventoryAgents += @{
        hostname = $env:COMPUTERNAME
        ip = $primaryIP
        region = $Region
        online = $true
        lastSeen = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        version = $agentVersion
    }
    $inventory.agents = $inventoryAgents
    
    $inventory | ConvertTo-Json -Depth 10 | Out-File $localInventoryFile -Force -Encoding utf8
    Write-Log "本地 IP 清单已更新: $localInventoryFile"
    
    # 4. 上报到远程 CMDB API（如果配置了）
    if ([string]::IsNullOrEmpty($ApiEndpoint)) {
        $configFile = "C:\detect-agent\config\config.yaml"
        if (Test-Path $configFile) {
            $content = Get-Content $configFile -Raw
            if ($content -match "api_endpoint:\s*(.+)$") {
                $ApiEndpoint = $matches[1].Trim()
            } elseif ($content -match "register_url:\s*(.+)$") {
                $ApiEndpoint = $matches[1].Trim()
            }
        }
    }
    
    if (-not [string]::IsNullOrEmpty($ApiEndpoint)) {
        Write-Log "正在上报到 API: $ApiEndpoint ..."
        try {
            $jsonBody = $registrationData | ConvertTo-Json -Depth 10
            $response = Invoke-RestMethod -Uri $ApiEndpoint `
                -Method Post `
                -Body $jsonBody `
                -ContentType "application/json" `
                -TimeoutSec 15
            Write-Log "API 注册成功: $($response | ConvertTo-Json -Compress)"
        } catch {
            Write-Log "API 注册失败: $_ (不影响本地记录)"
        }
    } else {
        Write-Log "未配置 API 地址，仅保存本地清单"
        Write-Log "可在 config.yaml 中添加 register_url: http://your-api/endpoint"
    }
    
    Write-Log "✅ 机器注册完成" "OK"
    
} catch {
    Write-Log "❌ 注册异常: $_"
}

Write-Log "===== 注册结束 ====="
Write-Log ""