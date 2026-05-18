package biz

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/go-kratos/kratos/v2/log"
)

const (
	BlockErrKeyword    = "ERR_BLOCKED"          // 浏览器拦截报错关键字
	BlockDetectTimeout = 400 * time.Millisecond // 用比较短的超时来进行探测，浏览器拦截报错很快，不需要等网页完全加载完
)

// Browser 浏览器
type Browser struct {
	logger          *log.Helper
	AttachUrl       string      // chromedp attach的url
	HealthCheckUrl  string      // 健康检查url
	Process         *os.Process // 浏览器进程
	Port            int         // debug端口
	DetectingJobNum int         // 正在探测的任务数
	DetectedJobNum  int         // 已经探测完成的任务数
}

// NetworkEvent captures relevant network data
type NetworkEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	URL       string    `json:"url,omitempty"`
	Method    string    `json:"method,omitempty"`
	Status    int64     `json:"status,omitempty"`
	ErrorText string    `json:"error_text,omitempty"`
	Blocked   bool      `json:"blocked"`
}

func (b *Browser) ChromiumDetect(ctx context.Context, urlStr string) (bool, string, error) {
	// 1. 连接到已有 Chrome 实例
	// 使用 RemoteAllocator 建立到浏览器的 WebSocket 连接
	allocCtx, allocCancel := chromedp.NewRemoteAllocator(ctx, b.AttachUrl)
	defer allocCancel()

	// 2. 创建全新的 Tab (Target)
	// NewContext 在 RemoteAllocator 下会发送 Target.createTarget 命令
	tabCtx, tabCancel := chromedp.NewContext(allocCtx)
	// tabCancel 会发送 Target.closeTarget 命令，显式关闭该 Tab
	defer func() {
		b.logger.Debugf("ChromiumDetect 正在关闭 Tab: %s\n", urlStr)
		tabCancel()
	}()

	var (
		events  []NetworkEvent
		mu      sync.Mutex
		blocked bool
	)

	addEvent := func(ev NetworkEvent) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev)
	}
	// 3. 设置监听
	chromedp.ListenTarget(tabCtx, func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventRequestWillBeSent:
			if e.Type == network.ResourceTypeDocument || e.Type == network.ResourceTypeXHR || e.Type == network.ResourceTypeFetch {
				addEvent(NetworkEvent{
					Timestamp: time.Now(),
					Type:      "Request",
					URL:       e.Request.URL,
					Method:    e.Request.Method,
				})
			}
		case *network.EventResponseReceived:
			addEvent(NetworkEvent{
				Timestamp: time.Now(),
				Type:      "Response",
				URL:       e.Response.URL,
				Status:    e.Response.Status,
			})
		case *network.EventLoadingFailed:
			if strings.Contains(e.ErrorText, BlockErrKeyword) {
				blocked = true
				addEvent(NetworkEvent{
					Timestamp: time.Now(),
					Type:      "LoadingFailed",
					ErrorText: e.ErrorText,
					Blocked:   true,
				})
				b.logger.Debugf("ChromiumDetect url: %s 检测到拦截: %s\n", urlStr, e.ErrorText)
			}
		}
	})

	// 4. 执行探测
	// 设置 400ms 的短超时，Safe Browsing 拦截通常非常快，目前chrome大概130ms拦截，edge200ms
	ctx, cancel := context.WithTimeout(tabCtx, BlockDetectTimeout)
	defer cancel()

	runErr := chromedp.Run(ctx,
		network.Enable(),
		chromedp.Navigate(urlStr),
	)

	// 结果判断
	if runErr != nil && !blocked {
		if strings.Contains(runErr.Error(), context.DeadlineExceeded.Error()) {
			b.logger.Debugf("ChromiumDetect 400ms 超时未拦截，视为正常, url: %s", urlStr)
		} else {
			b.logger.Errorf("ChromiumDetect 探测过程中出错: %v\n", runErr)
		}
	}
	detail := ""
	if len(events) > 0 {
		detailBytes, marshalErr := json.Marshal(events)
		if marshalErr != nil {
			b.logger.Errorf("ChromiumDetect marshal events err: %+v", marshalErr)
		} else {
			detail = string(detailBytes)
		}
	}
	return blocked, detail, runErr
}
