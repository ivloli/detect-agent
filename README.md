# Detect Agent - 浏览器拦截探测Agent

## 📋 项目概述

Detect Agent 是一个基于真实浏览器集群的**网络拦截探测代理服务**。它管理多个 Chromium 内核的浏览器实例（如 Chrome、Edge、360 浏览器等），通过 Chrome DevTools Protocol (CDP) 自动操控真实浏览器访问目标 URL，检测浏览器是否对请求进行了**安全拦截**（如 Safe Browsing 拦截、证书错误拦截等），并将探测结果上报到消息队列。

该项目主要用于大规模 URL 安全监测场景，模拟真实用户浏览行为来验证特定 URL 在各个浏览器下的可访问性。

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Detect Agent Server                       │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  cmd/server/main.go (入口点)                             │ │
│  │  - Nacos 配置加载 & 监听                                 │ │
│  │  - Jaeger Tracing 初始化                                 │ │
│  │  - Kratos App 启动 (HTTP/gRPC/Kafka)                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐ │
│  │   HTTP Server    │  │   gRPC Server    │  │Kafka Server│ │
│  │  (internal/srv)  │  │  (internal/srv)  │  │ (consumer) │ │
│  └────────┬─────────┘  └────────┬─────────┘  └──────┬─────┘ │
│           │                     │                    │        │
│  ┌────────▼─────────────────────▼────────────────────▼─────┐ │
│  │                Service 层 (internal/service)             │ │
│  │                    BatchService                          │ │
│  └────────────────────────────┬────────────────────────────┘ │
│                               │                               │
│  ┌────────────────────────────▼────────────────────────────┐ │
│  │               Biz 层 (internal/biz)                      │ │
│  │                                                          │ │
│  │  ┌──────────────┐     ┌──────────────────┐              │ │
│  │  │ Browser      │     │ BrowserHerd      │              │ │
│  │  │ (单个浏览器)  │◄────│ (浏览器集群)     │              │ │
│  │  └──────────────┘     └────────┬─────────┘              │ │
│  │                                │                        │ │
│  │                    ┌───────────▼───────────┐            │ │
│  │                    │ BrowserShepherd       │            │ │
│  │                    │ (浏览器群管理者)       │            │ │
│  │                    │ - Chrome/Edge/360/UC  │            │ │
│  │                    │   Quark/Sogou 共6种   │            │ │
│  │                    └───────────┬───────────┘            │ │
│  │                                │                        │ │
│  │  ┌─────────────────────────────┼─────────────────────┐  │ │
│  │  │  BatchDetectHandler        │                     │  │ │
│  │  │  (Kafka 消费 + 批量探测)   │                     │  │ │
│  │  └─────────────────────────────┘                      │  │ │
│  │  ┌───────────────────────┐                            │  │ │
│  │  │ NodeReporter          │                            │  │ │
│  │  │ (注册 + 心跳上报)     │                            │  │ │
│  │  └───────────────────────┘                            │  │ │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 核心功能

### 1. 浏览器农场管理
- 管理 **6 种** Chromium 内核浏览器的实例集群：Chrome、Edge、360、UC、Quark、搜狗
- 每个浏览器类型维护一个 **浏览器实例池**（大小可配置）
- 采用 **冷备策略（Standby）**：池中始终维护一个备用实例，当活跃实例达到探测次数上限时无缝替换
- 每 **2 分钟** 自动进行 **全量健康检查**，自动替换不健康的实例

### 2. URL 拦截探测
- 使用 `chromedp` 库通过 CDP 协议操控真实浏览器访问目标 URL
- **400ms 短超时** 探测：利用浏览器安全拦截（如 ERR_BLOCKED）响应极快的特性，不等页面完全加载
- 监听网络事件（Request/Response/LoadingFailed），精准识别拦截行为
- 支持文档、XHR、Fetch 等资源类型的拦截检测

### 3. Kafka 消息驱动
- 从 Kafka 消费 `TaskCreateRequest` 消息进行批量探测
- 探测结果序列化后发送到 `InterceptDetectResult` Topic
- 支持高并发：每个探测任务独立 goroutine 执行，并发处理

### 4. 节点注册与心跳
- 启动时向 Probe Center 上报节点**注册**信息
- 每 **30 秒** 上报一次**心跳**，包含各浏览器集群的实例数量详情

### 5. 动态配置
- 基于 **Nacos** 的配置中心
- 支持运行时动态更新 Kafka 配置、浏览器集群大小等
- 配置变更自动回调，无需重启服务

### 6. 可观测性
- **OpenTelemetry** 链路追踪
- 支持 **Jaeger** 采集上报
- **zap** 结构化日志，支持日志分级和文件输出

---

## 📁 项目结构

```
detect-agent/
├── cmd/server/
│   ├── main.go              # 服务入口：配置初始化、依赖注入、服务启动
│   ├── wire.go               # Wire 依赖注入定义
│   └── wire_gen.go           # Wire 自动生成的依赖注入代码
├── configs/
│   ├── local/                # 本地开发配置
│   └── eks-dev/              # EKS 环境配置
├── internal/
│   ├── biz/                  # 业务逻辑层
│   │   ├── biz.go            # Wire ProviderSet 定义
│   │   ├── browser.go        # 浏览器实例模型 & Chromium 探测逻辑
│   │   ├── browser_herd.go   # 浏览器集群管理（创建、销毁、健康检查）
│   │   ├── browser_shepherd.go # 浏览器群管理者（6种浏览器集群）
│   │   ├── batch_detect_handler.go # Kafka 批量探测处理器
│   │   └── node_reporter.go  # 节点注册 & 心跳上报
│   ├── conf/                 # 配置层
│   │   ├── conf.proto        # Protobuf 配置定义
│   │   ├── conf.pb.go        # 生成配置代码
│   │   ├── data.go           # 全局配置容器（Nacos 动态更新）
│   │   └── provider.go       # 配置 Provider
│   ├── errors/               # 错误码 & 错误处理
│   │   ├── error.go
│   │   └── errorCode.go
│   ├── grpc_client/          # gRPC 客户端
│   │   └── iam_client.go     # IAM 认证客户端
│   ├── pkg/
│   │   ├── franz-kafka/      # Kafka 封装
│   │   │   ├── kafka.go      # Kafka 客户端
│   │   │   ├── producer.go   # 消息生产者
│   │   │   └── batch_detect_consumer.go # 批量探测消费者
│   │   └── utils/            # 工具函数
│   │       ├── cmd_util.go   # 命令执行工具
│   │       ├── time.go       # 时间工具
│   │       └── utils.go      # 通用工具（端口检测等）
│   ├── server/               # 服务层
│   │   ├── server.go         # Wire ProviderSet 定义
│   │   ├── http.go           # HTTP 服务（Batch API）
│   │   ├── grpc.go           # gRPC 服务
│   │   ├── kafka.go          # Kafka 消费者服务
│   │   ├── registry.go       # Nacos 注册中心
│   │   └── response.go       # 统一响应编码器
│   └── service/              # 服务实现层
│       ├── service.go        # Wire ProviderSet
│       └── batch.go          # Batch 服务实现
└── go.mod
```

---

## 🔧 技术栈

| 技术 | 用途 |
|------|------|
| **Go 1.26** | 开发语言 |
| **[Kratos v2](https://github.com/go-kratos/kratos)** | 微服务框架 |
| **[chromedp](https://github.com/chromedp/chromedp)** | Chrome DevTools Protocol 客户端，操控浏览器 |
| **[cdproto](https://github.com/chromedp/cdproto)** | Chrome DevTools Protocol 类型定义 |
| **[franz-go](https://github.com/twmb/franz-go)** | Kafka 客户端 |
| **[Nacos](https://github.com/nacos-group/nacos-sdk-go)** | 配置中心 & 服务注册发现 |
| **[Google Wire](https://github.com/google/wire)** | 依赖注入 |
| **[OpenTelemetry](https://opentelemetry.io/)** | 链路追踪 |
| **[Jaeger](https://www.jaegertracing.io/)** | 分布式追踪后端 |
| **[zap](https://github.com/uber-go/zap)** | 高性能日志库 |
| **Protocol Buffers** | 接口定义 & 数据序列化 |

---

## 🛠️ 快速开始

### 前置条件

- Go 1.26+
- Nacos 服务
- Kafka 集群
- 安装 Chromium 内核浏览器（Chrome/Edge/360/UC/Quark/搜狗）

### 配置

配置文件路径：`configs/local/` 或通过 `-conf` 参数指定：

```bash
# 本地开发
go run cmd/server/main.go -conf ../../configs/local

# 使用 EKS 配置
go run cmd/server/main.go -conf ../../configs/eks-dev
```

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `JAEGER_ENDPOINT` | Jaeger 采集器地址 | 可选，为空则不启用 |

### 构建

```bash
# 构建二进制
go build -ldflags "-X main.Version=1.0.0" -o detect-agent cmd/server/main.go

# 使用 Makefile（如果存在）
make build
```

---

## 🧩 核心模块详解

### Browser - 浏览器实例

单个浏览器实例模型，包含：
- CDP 远程调试连接信息 (`AttachUrl`)
- 健康检查端点 (`HealthCheckUrl`)
- 进程 & 端口管理
- 正在探测/已完成探测的任务计数

**探测流程**：
1. 通过 CDP WebSocket 连接到已有浏览器实例
2. 创建新 Tab（Target）
3. 监听网络事件（Request、Response、LoadingFailed）
4. 导航到目标 URL，设置 400ms 超时
5. 检查是否存在 `ERR_BLOCKED` 错误，判断是否被拦截
6. 返回拦截状态 + 网络事件详情

### BrowserHerd - 浏览器集群

单个浏览器类型的集群管理器：
- **浏览器池**：维护 N+1 个实例（N 为活跃数，1 为冷备）
- **端口分配**：自动从指定端口范围（如 9000-9009）中分配可用端口
- **生命周期**：创建 → 健康检查 → 替换 → 销毁

### BrowserShepherd - 浏览器群管理者

管理所有 6 种浏览器的集群，提供：
- `GetAvailableBrowser(appType)` - 获取一个空闲浏览器实例（随机选取 + 负载均衡）
- `ReleaseBrowser(appType, browser)` - 释放浏览器，更新计数，超限自动替换
- `StartMonitor(ctx)` - 定时健康检查守护协程
- `HerdHealthCheck(herd)` - 对单个集群执行健康检查并自动恢复

### BatchDetectHandler - 批量探测处理器

Kafka 消息驱动的探测引擎：
- 消费批量探测消息，每条消息触发一次 URL 探测
- 并发处理（每个探测任务独立 goroutine）
- 结果回写到 Kafka 结果 Topic

---

## 📡 API 接口

### HTTP / gRPC

| 协议 | 服务 | 说明 |
|------|------|------|
| HTTP | `BatchService` | 批量探测管理 API |
| gRPC | `BatchService` | 与 HTTP 同接口的 gRPC 服务 |
| Kafka | `BatchDetectConsumer` | 消费批量探测任务消息 |

**Kafka 消息流**：

```
探测请求 Topic ──────────► Detect Agent ──────────► 探测结果 Topic
  (TaskCreateRequest)         │                       (NodeMessage)
                              │
                              ├──► Chrome 浏览器探测
                              ├──► Edge 浏览器探测
                              ├──► 360 浏览器探测
                              ├──► UC 浏览器探测
                              ├──► Quark 浏览器探测
                              └──► Sogou 浏览器探测
```

---

## ⚙️ 配置说明

配置通过 Nacos 动态管理，支持热更新：

```yaml
server:
  name: detect-agent
  env: dev
  http:
    addr: :8000
    network: tcp
    timeout: 5
  grpc:
    addr: :9000
    network: tcp
    timeout: 5

kafka:
  brokers: ["localhost:9092"]
  group: detect-agent-group
  batch_detect_topic: batch-detect
  intercept_detect_result_topic: detect-result

browser_herd_size: 3  # 每种浏览器保留的实例数
```

---

## 🤝 依赖说明

项目依赖多个内部 Proto 仓库和共享库：

- `gitlab.gainetics.io/backend-cdn/go-protos/probe-executor` - 探针执行器协议
- `gitlab.gainetics.io/shared/go-common/go-nacos-cli` - Nacos 客户端封装
- `gitlab.gainetics.io/shared/proto-hub/cloud-iam` - IAM 认证协议
- `gitlab.gainetics.io/shared/proto-hub/observable/batch` - 批量探测协议

---

## 📝 License

Internal - Gainetics