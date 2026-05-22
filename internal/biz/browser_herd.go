package biz

import (
	"detect-agent/internal/pkg/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

const (
	BrowserMaxDetectNum    = 100000 // 单个浏览器最多在探测到这么多个url后强制重启
	DefaultBrowserHerdSize = 1      // 单个浏览器默认起几个实例
)

type BrowserHerd struct {
	logger            *log.Helper
	mu                sync.Mutex // 当内部浏览器主备切换时，避免并发
	binaryPath        string     // 浏览器二进制地址
	userDataPath      string     // 浏览器配置文件地址
	minPort           int        // 端口范围最小值
	maxPort           int        // 端口范围最大值
	availableBrowsers []*Browser // 可用浏览器列表
	standBy           *Browser   // 备用浏览器
}

func NewChromiumHerd(logger log.Logger, binaryPath, profilePath string, minPort int, maxPort int) *BrowserHerd {
	// 1. 先清理旧浏览器进程
	segments := strings.Split(binaryPath, "\\")
	exeName := segments[len(segments)-1]
	exec.Command("taskkill", "/F", "/IM", exeName).Run()

	herd := &BrowserHerd{
		logger:            log.NewHelper(log.With(logger, "module", "browser_herd/")),
		mu:                sync.Mutex{},
		binaryPath:        binaryPath,
		userDataPath:      profilePath,
		minPort:           minPort,
		maxPort:           maxPort,
		availableBrowsers: make([]*Browser, 0, DefaultBrowserHerdSize),
		standBy:           nil,
	}
	for i := 0; i < DefaultBrowserHerdSize; i++ {
		b, err := herd.CreateChromiumInstance(herd.logger, false)
		if err != nil {
			panic(err)
		}
		herd.availableBrowsers = append(herd.availableBrowsers, b)
	}
	b, err := herd.CreateChromiumInstance(herd.logger, true)
	if err != nil {
		panic(err)
	}
	herd.standBy = b
	return herd
}

// CreateChromiumInstance 创建chromium内核浏览器实例，冷备在实际转正的时候建tab
func (h *BrowserHerd) CreateChromiumInstance(logger *log.Helper, isStandBy bool) (b *Browser, err error) {
	defer func() {
		if err != nil && b != nil {
			h.KillChromiumInstance(b)
		}
	}()
	port := h.getAvailablePort()
	if port == -1 {
		return nil, fmt.Errorf("unable to find available port")
	}

	// 把浏览器配置文件复制一份出来
	segments := strings.Split(h.userDataPath, "\\")
	baseName := segments[len(segments)-1]
	tmpProfile := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d", baseName, time.Now().Unix()))
	copyCmd := exec.Command("robocopy", h.userDataPath, tmpProfile, "/MIR")

	// robocopy比较特殊： 0~7 都算成功
	err = copyCmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code := exitErr.ExitCode()
			if code > 7 {
				panic(fmt.Sprintf("robocopy failed: %v", err))
			}
		} else {
			panic(err)
		}
	}

	cmd := exec.Command(
		h.binaryPath,
		fmt.Sprintf("--remote-debugging-port=%d", port),
		fmt.Sprintf(`--user-data-dir=%s`, tmpProfile),
	)

	err = cmd.Start()
	if err != nil {
		h.logger.Errorf("failed to start chromium instance: %v", err)
		return nil, err
	}

	b = &Browser{
		logger:          logger,
		AttachUrl:       "",
		HealthCheckUrl:  fmt.Sprintf("http://127.0.0.1:%d/json/version", port),
		Process:         cmd.Process,
		Port:            port,
		DetectingJobNum: 0,
		DetectedJobNum:  0,
	}
	// 浏览器启动过程可能会比较慢，这里等15s
	waitBrowserReady := func() error {
		deadline := time.Now().Add(time.Minute)

		for time.Now().Before(deadline) {
			resp, err := http.Get(b.HealthCheckUrl)
			if err == nil && resp.StatusCode == 200 {
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}

		return fmt.Errorf("devtools not ready")
	}
	h.logger.Info("Waiting for Chromium instance to become ready")
	err = waitBrowserReady()
	if err != nil {
		return nil, err
	}
	h.logger.Info("Chromium instance ready")
	attachUrl, err := h.BrowserHealthCheck(b)
	if err != nil {
		return nil, err
	}
	b.AttachUrl = attachUrl
	if !isStandBy {
		err = b.InitTabPool()
	}
	return b, nil
}

// KillChromiumInstance kill掉chromium内核浏览器实例
func (h *BrowserHerd) KillChromiumInstance(b *Browser) error {
	if b == nil {
		return fmt.Errorf("browser instance is nil")
	}
	b.CloseTabPool()
	if b.browserCancel != nil {
		b.browserCancel()
	}
	if b.allocCancel != nil {
		b.allocCancel()
	}
	if b.Process != nil {
		return b.Process.Kill()
	}
	return fmt.Errorf("browser instance process is nil")
}

func (h *BrowserHerd) getAvailablePort() int {
	for i := h.minPort; i < h.maxPort; i++ {
		occupied, err := utils.IsPortOccupiedWindows(i)
		if err != nil {
			h.logger.Errorf("failed to check if port is occupied: %v", err)
			continue
		}
		if !occupied {
			return i
		}
	}
	return -1
}

// BrowserHealthCheck 检查浏览器实例存活
func (h *BrowserHerd) BrowserHealthCheck(b *Browser) (string, error) {
	if b == nil {
		return "", fmt.Errorf("browser instance is nil")
	}
	resp, err := http.Get(b.HealthCheckUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("health check failed, status code: %d", resp.StatusCode)
	}

	var objmap map[string]json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&objmap)
	if err != nil {
		return "", fmt.Errorf("failed to decode health check response: %w", err)
	}

	wsUrl := string(objmap["webSocketDebuggerUrl"])
	// 去除引号
	wsUrl = strings.Trim(wsUrl, "\"")
	return wsUrl, nil
}

// GetIdleBrowser 获取正在探测数最少的浏览器实例
func (h *BrowserHerd) GetIdleBrowser() *Browser {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.availableBrowsers) == 0 {
		return nil
	}
	idle := h.availableBrowsers[0]
	for _, b := range h.availableBrowsers {
		if b.DetectingJobNum < idle.DetectingJobNum {
			idle = b
		}
	}
	return idle
}

func (h *BrowserHerd) ForceRestBrowser(b *Browser) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, bv := range h.availableBrowsers {
		if bv == b {
			err := h.KillChromiumInstance(b)
			if err != nil {
				h.logger.Errorf("failed to kill browser instance: %v", err)
			}
			newB, err := h.CreateChromiumInstance(h.logger, false)
			if err != nil {
				return false
			}
			h.availableBrowsers[i] = newB
			return true
		}
	}
	return false
}