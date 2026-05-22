# DetectAgent Windows 部署运维手册

## 概述

本文档说明如何在 Windows 服务器上部署和运维 DetectAgent 服务。

### 架构

```
Jenkins (编译 + 推送)
    │
    ├── make build-windows-prod
    │   └── detect-agent-v1.0.0.exe (上传到 Nexus)
    │
    └── Jenkins Pipeline
        ├── Canary: 1台 → 验证
        └── Rolling: 分批更新全部服务器
                │
                ▼
        Windows Agent (N 台)
            ├── C:\detect-agent\
            │   ├── bin\detect-agent.exe
            │   ├── config\config.yaml (仅 Nacos 地址)
            │   ├── update.ps1 (更新脚本)
            │   ├── logs\
            │   └── backups\
            │
            └── Windows Service: DetectAgent
                    │
                    ▼
                Nacos 配置中心 (动态下发配置)
                    │
                    ▼
                Kafka (消费检测任务)
```

---

## 一、制作黄金镜像

### 1.1 预装浏览器

```powershell
# Chrome
winget install Google.Chrome --silent --accept-package-agreements

# Microsoft Edge (自带, 无需安装)
# Firefox
winget install Mozilla.Firefox --silent --accept-package-agreements
```

### 1.2 初始化目录结构

```powershell
# 创建目录
New-Item -ItemType Directory -Force -Path C:\detect-agent\bin
New-Item -ItemType Directory -Force -Path C:\detect-agent\config
New-Item -ItemType Directory -Force -Path C:\detect-agent\logs
New-Item -ItemType Directory -Force -Path C:\detect-agent\backups

# 复制配置文件
Copy-Item config.yaml C:\detect-agent\config\ -Force

# 复制脚本
Copy-Item install-service.ps1 C:\detect-agent\ -Force
Copy-Item update.ps1 C:\detect-agent\ -Force
Copy-Item register-agent.ps1 C:\detect-agent\ -Force
```

### 1.3 安装开机自注册

```powershell
# 添加到开机启动
$startupFolder = "$env:ProgramData\Microsoft\Windows\Start Menu\Programs\StartUp"
Copy-Item C:\detect-agent\register-agent.ps1 $startupFolder\register-agent.ps1 -Force
```

### 1.4 开启 OpenSSH Server（供 Jenkins 远程连接）

```powershell
# 安装 OpenSSH Server
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0

# 启动并设置自动启动
Start-Service sshd
Set-Service -Name sshd -StartupType 'Automatic'

# 防火墙放行 SSH
New-NetFirewallRule -Name 'OpenSSH-Server' -DisplayName 'OpenSSH Server' `
    -Protocol TCP -LocalPort 22 -Action Allow

# SSH 配置：允许密钥认证
$sshDir = "$env:ProgramData\ssh"
$adminPubKey = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ..."  # 替换为 Jenkins 的公钥
Add-Content "$sshDir\administrators_authorized_keys" "`n$adminPubKey"
icacls "$sshDir\administrators_authorized_keys" /inheritance:r /grant "Administrators:F"
```

### 1.5 可选优化

```powershell
# 关闭休眠
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0

# 关闭 Windows 更新（生产服务器）
New-ItemProperty -Path "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU" `
    -Name "NoAutoUpdate" -Value 1 -PropertyType DWORD -Force

# 设置性能模式
powercfg /setactive 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c  # 高性能
```

### 1.6 捕获镜像

```powershell
# 运行 sysprep 后关机
C:\Windows\System32\Sysprep\sysprep.exe /generalize /oobe /shutdown
```

然后从云平台控制台（AWS、Azure、vSphere 等）捕获此实例为黄金镜像。

---

## 二、部署新机器

### 2.1 从黄金镜像启动

1. 在云平台控制台，从黄金镜像创建新实例
2. 分配固定内网 IP（或 DHCP 保留）
3. 启动后 `register-agent.ps1` 会自动运行，完成注册

### 2.2 更新 IP 清单

新机器启动后，IP 会记录在 `C:\detect-agent\config\inventory.json` 中。

你可以选择：

**方式 A：自动收集**（推荐）
在 Jenkins 中通过 SSH 遍历所有 Windows 机器收集 IP：

```bash
# 已知 IP 段扫描
for ip in 10.0.0.{101..150}; do
    ssh administrator@$ip "cat C:\detect-agent\config\inventory.json" 2>/dev/null
done
```

**方式 B：手动维护**
在 `Jekinsfile` 的 `WINDOWS_HOSTS` 列表中直接添加 IP。

**方式 C：自研 CMDB**
在黄金镜像中配置 `register_url`，新机器启动时自动上报到 CMDB，Jenkins 再从 CMDB API 拉取 IP 列表。

---

## 三、Jenkins 部署流水线

### 3.1 Jenkins 配置准备

1. **安装必要插件**：
   - SSH Pipeline Steps
   - Git
   - Credentials Binding

2. **配置凭据**：
   - `nexus-api-key`: Nexus 上传密钥 (String)
   - `windows-agent-key`: Windows SSH 私钥 (SSH Key)

3. **Windows 端配置**：
   - 确保每台 Windows 开启 OpenSSH Server
   - 将 Jenkins 主机的 SSH 公钥添加到 `C:\ProgramData\ssh\administrators_authorized_keys`

### 3.2 执行部署

```bash
# 手动触发 Jenkins Pipeline:
# 1. 选择 Git tag (如 v1.3.0)
# 2. 流水线自动: 编译 → 上传 Nexus → 等待确认 → 推送 Windows 服务器
```

### 3.3 部署策略选择

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| **Canary** | 先更新 1 台，等待人工验证 | 首次部署、大版本更新 |
| **Rolling** | 分批更新，每批 5 台，间隔 2 分钟 | 常规版本更新 |
| **All** | 全部机器同时更新 | 紧急修复、小版本 |

---

## 四、服务管理命令

### 本地执行（在 Windows 服务器上）

```powershell
# 查看服务状态
Get-Service -Name DetectAgent

# 启动服务
Start-Service -Name DetectAgent

# 停止服务
Stop-Service -Name DetectAgent

# 重启服务
Restart-Service -Name DetectAgent

# 查看服务日志
Get-Content C:\detect-agent\logs\detect-agent.log -Tail 100 -Wait

# 查看更新历史
Get-Content C:\detect-agent\update-history.json | ConvertFrom-Json | Format-Table -AutoSize
```

### 远程执行（从 Jenkins/任意 Linux 机器）

```bash
# 查看服务状态
ssh administrator@10.0.0.101 "powershell -Command \"Get-Service DetectAgent\""

# 重启服务
ssh administrator@10.0.0.101 "powershell -Command \"Restart-Service DetectAgent -Force\""

# 执行本地更新脚本
ssh administrator@10.0.0.101 "powershell -ExecutionPolicy Bypass -File C:\detect-agent\update.ps1 -Version 1.3.0"
```

---

## 五、版本更新流程

### 标准流程

```
提交代码 → Git Tag → Jenkins 自动构建 → 
  Canary(1台,等待验证) → 确认 → Rolling(分批) → 全量完成
```

### 手动更新（单台）

```powershell
# 登录到 Windows 服务器
ssh administrator@10.0.0.101

# 执行更新
powershell -ExecutionPolicy Bypass -File C:\detect-agent\update.ps1 -Version "1.3.0"

# 如果有多台，也可以用 for 循环
for /L %i in (101,1,150) do (
    ssh administrator@10.0.0.%i "powershell -File C:\detect-agent\update.ps1 -Version 1.3.0"
)
```

### 回滚

方法一：脚本自动回滚（`update.ps1` 内置回滚逻辑，启动失败时自动执行）

方法二：手动回滚

```powershell
# 1. 停止服务
Stop-Service DetectAgent -Force

# 2. 从备份恢复
Copy-Item C:\detect-agent\backups\detect-agent-1.2.0.exe C:\detect-agent\bin\detect-agent.exe -Force

# 3. 启动服务
Start-Service DetectAgent
```

---

## 六、日常运维

### 6.1 查看版本分布

```bash
# 查看所有机器的版本
for ip in 10.0.0.{101..150}; do
    version=$(ssh administrator@$ip "powershell -Command \"Get-Content C:\detect-agent\version.json | ConvertFrom-Json | Select -ExpandProperty version\"")
    echo "$ip => $version"
done 2>/dev/null | sort -t '>' -k 2
```

### 6.2 批量检查服务状态

```bash
for ip in 10.0.0.{101..150}; do
    status=$(ssh -o ConnectTimeout=5 administrator@$ip "powershell -Command \"(Get-Service DetectAgent).Status\"" 2>/dev/null)
    echo "[$status] $ip"
done 2>/dev/null | sort
```

### 6.3 拉取日志

```bash
ssh administrator@10.0.0.101 "Get-Content C:\detect-agent\logs\detect-agent.log -Tail 200"
# 或拷贝到本地
scp administrator@10.0.0.101:C:\detect-agent\logs\detect-agent.log ./agent-101.log
```

### 6.4 配置更新

通过 **Nacos** 动态下发，**无需重启服务**：

1. 登录 Nacos 控制台
2. 修改 `intercept-detect` 配置
3. 所有 Agent 的 `NacosListener` 自动收到变更
4. 业务配置热更新，无感知

---

## 七、故障排查

### 服务启动失败

```powershell
# 1. 查看 Windows 事件日志
Get-WinEvent -LogName Application -MaxEvents 10 | Where-Object { $_.ProviderName -like "*DetectAgent*" }

# 2. 查看 Agent 日志
Get-Content C:\detect-agent\logs\detect-agent.log -Tail 50

# 3. 检查配置文件
Test-Path C:\detect-agent\config\config.yaml
Get-Content C:\detect-agent\config\config.yaml

# 4. 检查浏览器是否可用
Test-Path "C:\Program Files\Google\Chrome\Application\chrome.exe"

# 5. 手动启动测试（前台运行）
C:\detect-agent\bin\detect-agent.exe --conf C:\detect-agent\config\config.yaml
```

### SSH 连接失败

```powershell
# 在 Windows 上检查 SSH 服务
Get-Service sshd

# 检查防火墙
Get-NetFirewallRule -Name '*SSH*'

# 查看 SSH 日志
Get-Content "$env:ProgramData\ssh\logs\sshd.log" -Tail 20
```

### 更新后版本不符

```powershell
# 检查二进制版本
(Get-Item C:\detect-agent\bin\detect-agent.exe).VersionInfo

# 检查 version.json
Get-Content C:\detect-agent\version.json

# 检查备份目录
Get-ChildItem C:\detect-agent\backups\
```

---

## 八、文件存储源选择指南

`update.ps1` 支持 5 种文件源，根据你们的情况选择最合适的一种：

### 方案 A：GitLab Release（推荐，零成本）

你们已经用 GitLab 管理代码，可以直接用 GitLab 的 Release 或 Package Registry 存放二进制。

**GitLab CI 配置：**

```yaml
# .gitlab-ci.yml 中增加 publish 阶段
publish-windows:
  stage: deploy
  only:
    - tags
  script:
    - make build-windows-prod
    - |
      curl --header "PRIVATE-TOKEN: $CI_JOB_TOKEN" \
        --upload-file build/windows/detect-agent.exe \
        "$CI_API_V4_URL/projects/$CI_PROJECT_ID/packages/generic/detect-agent/$CI_COMMIT_TAG/detect-agent.exe"
    - |
      curl --header "PRIVATE-TOKEN: $CI_JOB_TOKEN" \
        --upload-file build/windows/detect-agent.exe.sha256 \
        "$CI_API_V4_URL/projects/$CI_PROJECT_ID/packages/generic/detect-agent/$CI_COMMIT_TAG/detect-agent.exe.sha256"
```

**触发更新：**

```bash
# Jenkins 批量调用
for ip in 10.0.0.{101..150}; do
  curl -s "http://$ip:18080/update?version=1.3.0&source=gitlab" || echo "$ip failed"
done
```

**优点：** 零额外成本，与 GitLab 深度集成，自动版本管理

### 方案 B：Jenkins 构建产物（零成本）

Jenkins 构建完成后，二进制就在 Jenkins 的构建产物中，直接 serve。

**Jenkinsfile 中生成 URL：**

```groovy
// 构建产物 URL 格式
// http://jenkins.internal/job/detect-agent/123/artifact/build/windows/detect-agent.exe
def buildUrl = "${env.BUILD_URL}artifact/build/windows/detect-agent.exe"
```

**触发更新：**

```bash
curl "http://10.0.0.101:18080/update?version=1.3.0&source=jenkins&source_url=http://jenkins.internal/job/detect-agent/123/artifact/build/windows/detect-agent.exe"
```

**优点：** 零成本，Jenkins 自带，无需额外服务

### 方案 C：HTTP 文件服务器（最简单快速）

用 nginx 或 python 一行命令搭起来：

```bash
# 在 Jenkins 机器上，用 python 搭一个（30秒搞定）
mkdir -p /opt/files/detect-agent
cp build/windows/detect-agent.exe /opt/files/detect-agent/
cd /opt/files && python3 -m http.server 8080

# 或用 nginx
server {
    listen 8080;
    root /opt/files;
}
```

**触发更新：**

```bash
curl "http://10.0.0.101:18080/update?version=1.3.0&source=http&source_url=http://jenkins.internal:8080/detect-agent/"
```

### 方案 D：SMB 网络共享（无需搭服务）

把构建产物放到一个 Windows 共享目录即可：

```powershell
# 在网络共享上创建目录结构
\\fileserver\share\detect-agent\v1.3.0\detect-agent.exe
\\fileserver\share\detect-agent\v1.3.0\detect-agent.exe.sha256
```

**触发更新：**

```bash
curl "http://10.0.0.101:18080/update?version=1.3.0&source=smb&source_url=\\\\fileserver\\share\\detect-agent"
```

**注意：** 需要 Windows 机器能访问到该网络共享

### 方案选择建议

| 场景 | 推荐方案 | 搭建成本 | 运维成本 |
|------|---------|---------|---------|
| 有 GitLab | **GitLab Release** | 0元 | 低 |
| 有 Jenkins | **Jenkins 产物** | 0元 | 低 |
| 想最简单的 | **Python HTTP 服务** | 5分钟 | 低 |
| Windows 网络已打通 | **SMB 共享** | 10分钟 | 低 |
| 有 Nexus/Artifactory | 继续用 Nexus | 已存在 | 低 |

**对于你们的情况，建议先用方案 C（Python 一行命令搭个 HTTP 服务器），后续再切换到方案 A（GitLab Release）。**

---

## 九、文件说明

```
scripts/windows/
├── README-windows.md         # 本文件
├── config.yaml               # Windows 最小配置文件
├── install-service.ps1       # 服务安装脚本
├── update.ps1                # 更新/回滚脚本
├── register-agent.ps1        # 新机器自注册脚本
└── Jenkinsfile               # Jenkins 流水线
```

### 黄金镜像目录结构

```
C:\detect-agent\
├── bin\
│   └── detect-agent.exe       # 主程序（由 update.ps1 更新）
├── config\
│   └── config.yaml            # 配置文件（仅含 Nacos 地址）
├── logs\
│   ├── detect-agent.log        # 服务运行日志
│   └── update-*.log            # 更新日志
├── backups\
│   ├── detect-agent-1.0.0.exe  # 版本备份
│   └── detect-agent-1.1.0.exe
├── version.json               # 当前版本记录
├── update-history.json        # 完整更新历史
├── install.json               # 安装记录
├── update.ps1                 # 更新脚本
└── register-agent.ps1         # 注册脚本