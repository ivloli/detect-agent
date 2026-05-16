package biz

import (
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
)

const (
	BrowserMaxDetectNum = 100000 // 单个浏览器最多在探测到这么多个url后强制重启
)

// Browser 浏览器
type Browser struct {
	AttachUrl       string      // chromedp attach的url
	HealthCheckUrl  string      // 健康检查url
	Process         *os.Process // 浏览器进程
	Port            int         // debug端口
	DetectingJobNum int         // 正在探测的任务数
	DetectedJobNum  int         // 已经探测完成的任务数
}

type BrowserHerd struct {
	logger            *log.Helper
	mu                sync.Mutex
	binaryPath        string     // 浏览器二进制地址
	userDataPath      string     // 浏览器配置文件地址
	minPort           int        // 端口范围最小值
	maxPort           int        // 端口范围最大值
	availableBrowsers []*Browser // 可用浏览器列表
	standBy           *Browser   // 备用浏览器
}

func NewChromiumHerd(logger log.Logger, binaryPath string, minPort int, maxPort int) *BrowserHerd {
	herd := &BrowserHerd{
		logger:            log.NewHelper(log.With(logger, "module", "browser_herd/")),
		mu:                sync.Mutex{},
		binaryPath:        binaryPath,
		minPort:           minPort,
		maxPort:           maxPort,
		availableBrowsers: make([]*Browser, 0, conf.GetData().BrowserHerdSize),
		standBy:           nil,
	}
	browsers := make([]*Browser, 0, conf.GetData().BrowserHerdSize+1)
	for i := 0; i < conf.GetData().BrowserHerdSize+1; i++ {
		b, err := herd.CreateChromiumInstance()
		if err != nil {
			panic(err)
		}
		browsers = append(browsers, b)
	}
	herd.availableBrowsers = browsers[:len(browsers)-1]
	herd.standBy = browsers[len(browsers)-1]
	return herd
}

// CreateChromiumInstance 创建chromium内核浏览器实例
func (h *BrowserHerd) CreateChromiumInstance() (*Browser, error) {
	port := h.getAvailablePort()
	if port == -1 {
		return nil, fmt.Errorf("unable to find available port")
	}
	cmd := exec.Command(
		h.binaryPath,
		fmt.Sprintf("--remote-debugging-port=%d", port),
		fmt.Sprintf(`--user-data-dir=%s`, h.userDataPath),
	)

	err := cmd.Start()
	if err != nil {
		h.logger.Errorf("failed to start chromium instance: %v", err)
		return nil, err
	}

	b := &Browser{
		AttachUrl:       "",
		HealthCheckUrl:  fmt.Sprintf("http://127.0.0.1:%d/json/version", port),
		Process:         cmd.Process,
		Port:            port,
		DetectingJobNum: 0,
		DetectedJobNum:  0,
	}
	attachUrl, err := h.BrowserHealthCheck(b)
	if err != nil {
		return nil, err
	}
	b.AttachUrl = attachUrl
	return b, nil
}

// KillChromiumInstance kill掉chromium内核浏览器实例
func (h *BrowserHerd) KillChromiumInstance(b *Browser) error {
	if b != nil && b.Process != nil {
		return b.Process.Kill()
	}
	return fmt.Errorf("browser instance is nil")
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

// BrowserHealthCheck 对浏览器做健康检查
func (h *BrowserHerd) BrowserHealthCheck(browser *Browser) (string, error) {
	if browser == nil {
		h.logger.Error("browser instance is nil")
		return "", fmt.Errorf("browser instance is nil")
	}
	resp, err := http.Get(browser.HealthCheckUrl)
	if err != nil {
		h.logger.Errorf("failed to get ws debugger url: %v", err)
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		h.logger.Errorf("failed to decode ws debugger url: %v", err)
		return "", err
	}
	return result.WebSocketDebuggerURL, nil
}

// HerdHealthCheck 对浏览器集群做健康检查
func (h *BrowserHerd) HerdHealthCheck() error {
	// 先确保冷备可用
	_, err := h.BrowserHealthCheck(h.standBy)
	if err != nil {
		h.logger.Infof("standby browser unhealthy: %v, try to recover", err)
		b, err := h.CreateChromiumInstance()
		if err != nil {
			panic(err)
		}
		old := h.standBy
		h.mu.Lock()
		h.standBy = b
		h.mu.Unlock()
		err = h.KillChromiumInstance(old)
		if err != nil {
			h.logger.Errorf("failed to kill browser instance: %v", err)
		}
	}
	// 再逐个检查使用中的浏览器健康状况，不健康的浏览器用备用的替换掉，再新建备用，并kill旧的
	for i := 0; i < len(h.availableBrowsers); i++ {
		_, err := h.BrowserHealthCheck(h.availableBrowsers[i])
		if err != nil {
			h.logger.Infof("available browser unhealthy: %v, try to replace with standby", err)
			old := h.availableBrowsers[i]
			h.mu.Lock()
			h.availableBrowsers[i] = h.standBy
			h.standBy = nil
			h.mu.Unlock()
			b, err := h.CreateChromiumInstance()
			if err != nil {
				panic(err)
			}
			h.mu.Lock()
			h.standBy = b
			h.mu.Unlock()
			err = h.KillChromiumInstance(old)
			if err != nil {
				h.logger.Errorf("failed to kill browser instance: %v", err)
			}
		}
	}
	return nil
}
