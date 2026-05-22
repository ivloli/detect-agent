<#
.SYNOPSIS
    DetectAgent 轻量 HTTP 更新服务（替代 SSH，零配置）
.DESCRIPTION
    作为一个 Windows 服务运行，监听 HTTP 端口。
    Jenkins 只需要 curl 调用即可触发更新，**不需要 SSH，不需要密钥**。
    新增机器来自黄金镜像，自动带此服务，零额外操作。
    
    支持的更新源:
    - ?source=gitlab   GitLab Release (推荐，使用已有 GitLab，零成本)
    - ?source=jenkins  Jenkins 构建产物
    - ?source=http     任意 HTTP 文件服务器
    - ?source=smb      SMB 网络共享
    - ?source=local    本地文件
    
    安装:
        .\update-server.ps1 -Install
        
    启动:
        Start-Service -Name DetectAgentUpdater
        
    测试:
        curl http://10.0.0.101:18080/update?version=1.3.0&source=gitlab
        curl http://10.0.0.101:18080/status
        
    原理:
        1. Jenkins curl 调用 http://agent:18080/update?version=1.3.0
        2. 此服务收到请求，写入 C:\detect-agent\update.flag
        3. Agent 主进程检测到 update.flag 后执行 update.ps1
        4. 更新完成后删除 update.flag
        
    Jenkins 批量更新（不需要 SSH）:
        for ip in 10.0.0.{101..150}; do
            curl -s "http://$ip:18080/update?version=1.3.0&source=gitlab" || echo "$ip failed"
        done
#>

param(
    [string]$Action = "",        # Install, Uninstall, Start, Stop
    [int]$Port = 18080,
    [string]$ServiceName = "DetectAgentUpdater",
    [string]$AgentPath = "C:\detect-agent"
)

$ErrorActionPreference = "Stop"

# ========== HTTP 服务端实现 ==========

function Start-HttpServer {
    param([int]$Port, [string]$AgentPath)
    
    $logFile = "$AgentPath\logs\update-server.log"
    New-Item -ItemType Directory -Force -Path "$AgentPath\logs" | Out-Null
    
    $listener = New-Object System.Net.HttpListener
    $listener.Prefixes.Add("http://+:$Port/")
    
    try {
        $listener.Start()
        Add-Content $logFile "[$(Get-Date)] Update server started on port $Port"
        
        while ($listener.IsListening) {
            $context = $listener.GetContext()
            $request = $context.Request
            $response = $context.Response
            
            $response.Headers.Add("Content-Type", "application/json; charset=utf-8")
            $response.Headers.Add("Access-Control-Allow-Origin", "*")
            
            $path = $request.Url.AbsolutePath.Trim('/')
            $method = $request.HttpMethod
            
            $result = @{}
            
            if ($path -eq "update" -and ($method -eq "GET" -or $method -eq "POST")) {
                # 读取版本参数
                $version = ""
                if ($method -eq "GET") {
                    $version = $request.QueryString["version"]
                } else {
                    $reader = New-Object System.IO.StreamReader($request.InputStream)
                    $body = $reader.ReadToEnd()
                    try {
                        $json = $body | ConvertFrom-Json
                        $version = $json.version
                    } catch {
                        # 也支持 form 格式
                        if ($body -match "version=([^&]+)") {
                            $version = $matches[1]
                        }
                    }
                }
                
                if ([string]::IsNullOrEmpty($version)) {
                    $result = @{ status = "error"; message = "version is required" }
                    $response.StatusCode = 400
                } else {
                    $nexusUrl = ""
                    if ($request.QueryString["nexus_url"]) {
                        $nexusUrl = $request.QueryString["nexus_url"]
                    }
                    
                    # 写入更新标记
                    $flagFile = "$AgentPath\update.flag"
                    $flagContent = "{`"version`":`"$version`",`"nexus_url`":`"$nexusUrl`",`"triggered_at`":`"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`"}"
                    Set-Content -Path $flagFile -Value $flagContent -Force
                    
                    Add-Content $logFile "[$(Get-Date)] Triggered update to version $version from $($request.RemoteEndPoint.Address)"
                    
                    # 读取当前版本
                    $currentVersion = "unknown"
                    $versionFile = "$AgentPath\version.json"
                    if (Test-Path $versionFile) {
                        try {
                            $v = Get-Content $versionFile -Raw | ConvertFrom-Json
                            $currentVersion = $v.version
                        } catch {}
                    }
                    
                    $result = @{
                        status = "ok"
                        message = "Update triggered, $currentVersion → $version"
                        version = $version
                    }
                }
                
            } elseif ($path -eq "status") {
                # 状态查询
                $svc = Get-Service -Name "DetectAgent" -ErrorAction SilentlyContinue
                $process = Get-Process -Name "detect-agent" -ErrorAction SilentlyContinue
                $version = "unknown"
                $versionFile = "$AgentPath\version.json"
                if (Test-Path $versionFile) {
                    try {
                        $v = Get-Content $versionFile -Raw | ConvertFrom-Json
                        $version = $v.version
                    } catch {}
                }
                $flagExists = Test-Path "$AgentPath\update.flag"
                
                $result = @{
                    status = "ok"
                    hostname = $env:COMPUTERNAME
                    ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notlike "*Loopback*" } | Select-Object -First 1 -ExpandProperty IPAddress)
                    version = $version
                    serviceStatus = if ($svc) { $svc.Status.ToString() } else { "not_found" }
                    processRunning = ($process -ne $null)
                    pendingUpdate = if ($flagExists) { "yes" } else { "no" }
                    memoryMB = if ($process) { [math]::Round($process.WorkingSet64 / 1MB, 1) } else { 0 }
                    uptime = if ($process) { [math]::Round((Get-Date) - $process.StartTime | Select -ExpandProperty TotalMinutes) } else { 0 }
                }
                
            } elseif ($path -eq "health") {
                # 简单健康检查
                $result = @{ status = "UP"; service = "detect-agent-updater" }
                
            } else {
                $result = @{ status = "error"; message = "Unknown path: /$path"; available = @("/update", "/status", "/health") }
                $response.StatusCode = 404
            }
            
            # 返回 JSON
            $jsonBytes = [System.Text.Encoding]::UTF8.GetBytes(($result | ConvertTo-Json))
            $response.ContentLength64 = $jsonBytes.Length
            $response.OutputStream.Write($jsonBytes, 0, $jsonBytes.Length)
            $response.Close()
        }
    } catch {
        Add-Content $logFile "[$(Get-Date)] Server error: $_"
    } finally {
        if ($listener.IsListening) {
            $listener.Stop()
        }
    }
}

# ========== 服务管理 ==========

function Install-Service {
    Write-Host "安装服务 $ServiceName ..."
    
    # 停止并删除旧服务
    $existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($existing) {
        sc.exe stop $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 2
        sc.exe delete $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 2
    }
    
    $scriptPath = "$AgentPath\update-server.ps1"
    
    # 创建包装脚本
    $wrapperScript = @"
`$ErrorActionPreference = "Stop"
`$logFile = "$AgentPath\logs\update-server-bootstrap.log"
try {
    Add-Content `$logFile "[`$(Get-Date)] Starting update server..."
    & "$scriptPath" -Action Run -Port $Port -AgentPath "$AgentPath"
} catch {
    Add-Content `$logFile "[`$(Get-Date)] Fatal error: `$_"
}
"@
    Set-Content -Path "$AgentPath\run-update-server.ps1" -Value $wrapperScript -Force
    
    # 创建服务（使用 powershell 后台运行）
    sc.exe create $ServiceName `
        binPath= "powershell.exe -ExecutionPolicy Bypass -File `"$AgentPath\run-update-server.ps1`"" `
        start= auto `
        displayName= "DetectAgent Updater - HTTP更新服务" `
        description= "接收 Jenkins 的 HTTP 更新请求，触发 DetectAgent 更新" 2>&1 | Out-Null
    
    # 配置服务恢复
    sc.exe failure $ServiceName reset=86400 actions=restart/5000/restart/10000/restart/30000 2>&1 | Out-Null
    
    Write-Host "✅ 服务安装完成"
    Write-Host "   服务名: $ServiceName"
    Write-Host "   端口:   $Port"
    Write-Host "   启动:   Start-Service -Name $ServiceName"
    Write-Host ""
    Write-Host "   测试:   curl http://localhost:$Port/health"
    Write-Host "   更新:   curl http://localhost:$Port/update?version=1.3.0"
}

function Uninstall-Service {
    Write-Host "卸载服务 $ServiceName ..."
    
    sc.exe stop $ServiceName 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    sc.exe delete $ServiceName 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    
    # 清理
    if (Test-Path "$AgentPath\run-update-server.ps1") {
        Remove-Item "$AgentPath\run-update-server.ps1" -Force
    }
    
    Write-Host "✅ 服务已卸载"
}

# ========== 主入口 ==========

if ($Action -eq "Install") {
    Install-Service
    exit 0
}

if ($Action -eq "Uninstall") {
    Uninstall-Service
    exit 0
}

if ($Action -eq "Run") {
    # 以服务方式运行
    Start-HttpServer -Port $Port -AgentPath $AgentPath
    exit 0
}

# 默认：在控制台运行（用于调试）
Write-Host "=================================="
Write-Host "DetectAgent HTTP Update Server"
Write-Host "=================================="
Write-Host "Listening on http://0.0.0.0:$Port"
Write-Host "Agent path: $AgentPath"
Write-Host ""
Write-Host "  /update?version=1.3.0   触发更新"
Write-Host "  /status                 查询状态"
Write-Host "  /health                 健康检查"
Write-Host ""
Write-Host "Press Ctrl+C to stop"
Write-Host "=================================="

Start-HttpServer -Port $Port -AgentPath $AgentPath