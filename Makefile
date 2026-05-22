.PHONY: build build-windows build-windows-prod clean test run help

APP_NAME    := detect-agent
MAIN_PATH   := ./cmd/server
BUILD_DIR   := ./build
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME  := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS     := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"
# 生产构建额外压缩体积（去除符号表和调试信息）
LDFLAGS_PROD := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -s -w"

# 默认构建当前系统
build:
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) $(MAIN_PATH)
	@echo "✅ 构建完成: $(BUILD_DIR)/$(APP_NAME)"

# 交叉编译 Windows (amd64)
build-windows:
	@mkdir -p $(BUILD_DIR)
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME).exe $(MAIN_PATH)
	@echo "✅ Windows 交叉编译完成: $(BUILD_DIR)/$(APP_NAME).exe"

# Windows 生产构建（压缩体积，适合部署）
build-windows-prod:
	@mkdir -p $(BUILD_DIR)/windows
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS_PROD) -o $(BUILD_DIR)/windows/$(APP_NAME).exe $(MAIN_PATH)
	@echo "✅ Windows 生产版本构建完成: $(BUILD_DIR)/windows/$(APP_NAME).exe"
	@echo "📦 文件大小: $$(du -h $(BUILD_DIR)/windows/$(APP_NAME).exe | cut -f1)"
	@sha256sum $(BUILD_DIR)/windows/$(APP_NAME).exe | tee $(BUILD_DIR)/windows/$(APP_NAME).exe.sha256

# 交叉编译 Linux (amd64)
build-linux:
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME)-linux $(MAIN_PATH)
	@echo "✅ Linux 编译完成: $(BUILD_DIR)/$(APP_NAME)-linux"

# 交叉编译 macOS (amd64)
build-darwin:
	@mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME)-darwin $(MAIN_PATH)
	@echo "✅ macOS 编译完成: $(BUILD_DIR)/$(APP_NAME)-darwin"

# 所有平台交叉编译
build-all: build-windows-prod build-linux build-darwin
	@echo "✅ 所有平台编译完成"

# 运行测试
test:
	go test -v -race ./...

# 运行单元测试(不跑集成测试)
test-unit:
	go test -v -race -short ./...

# 运行服务(开发模式)
run:
	go run $(MAIN_PATH) $(ARGS)

# 清理构建产物
clean:
	rm -rf $(BUILD_DIR)
	@echo "🧹 清理完成"

# 查看帮助
help:
	@echo "用法: make <target>"
	@echo ""
	@echo "目标:"
	@echo "  build              构建当前系统可执行文件 (默认)"
	@echo "  build-windows      交叉编译 Windows (amd64) 可执行文件"
	@echo "  build-windows-prod 交叉编译 Windows 生产版本 (压缩, 含校验)"
	@echo "  build-linux        交叉编译 Linux (amd64) 可执行文件"
	@echo "  build-darwin       交叉编译 macOS (amd64) 可执行文件"
	@echo "  build-all          编译所有平台"
	@echo "  test               运行所有测试"
	@echo "  test-unit          运行单元测试"
	@echo "  run                启动服务 (可添加 ARGS 传参)"
	@echo "  clean              清理构建产物"
	@echo "  help               显示此帮助"
	@echo ""
	@echo "示例:"
	@echo "  make build-windows-prod                      # 编译 Windows 生产版本"
	@echo "  make run ARGS='-conf ../../configs/local'    # 本地开发启动"