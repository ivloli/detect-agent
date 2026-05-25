package biz

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/go-kratos/kratos/v2/log"
)

const (
	BlockErrKeyword    = "ERR_BLOCKED"          // 浏览器拦截报错关键字
	BlockDetectTimeout = 400 * time.Millisecond // 用比较短的超时来进行探测，浏览器拦截报错很快，不需要等网页完全加载完
	TabPoolSize        = 10                     // 每个浏览器开10个tab复用
)

// Browser 浏览器
type Browser struct {
	logger          *log.Helper
	tabs            []*PooledTab // tab池，避免反复新建销毁
	AttachUrl       string       // chromedp attach的url
	HealthCheckUrl  string       // 健康检查url
	Process         *os.Process  // 浏览器进程
	Port            int          // debug端口
	DetectingJobNum int          // 正在探测的任务数
	DetectedJobNum  int          // 已经探测完成的任务数

	allocCtx      context.Context // cdp 浏览器进程
	allocCancel   context.CancelFunc
	browserCtx    context.Context // cdp 浏览器session
	browserCancel context.CancelFunc
}

type PooledTab struct {
	Ctx      context.Context
	Cancel   context.CancelFunc
	Detector *TabDetector
}

func (b *Browser) InitTabPool() error {
	// 先清理旧的上下文，避免资源泄漏
	if b.allocCancel != nil {
		b.allocCancel()
	}
	if b.browserCancel != nil {
		b.browserCancel()
	}

	// 使用 RemoteAllocator 建立到浏览器的 WebSocket 连接, allocCtx=chrome进程
	b.allocCtx, b.allocCancel = chromedp.NewRemoteAllocator(context.Background(), b.AttachUrl)

	// browserCtx=browser session
	b.browserCtx, b.browserCancel = chromedp.NewContext(b.allocCtx)
	for i := 0; i < TabPoolSize; i++ {
		tabCtx, tabCancel := chromedp.NewContext(b.browserCtx)

		// 初始化 tab, 确保 Target 被成功创建和附加，并启用网络监听
		if err := chromedp.Run(tabCtx, network.Enable()); err != nil {
			b.CloseTabPool()
			if b.browserCancel != nil {
				b.browserCancel()
			}
			if b.allocCancel != nil {
				b.allocCancel()
			}
			return err
		}

		detector := &TabDetector{}
		chromedp.ListenTarget(tabCtx, detector.OnEvent)

		b.tabs = append(b.tabs, &PooledTab{
			Ctx:      tabCtx,
			Cancel:   tabCancel,
			Detector: detector,
		})
	}
	return nil
}

func (b *Browser) CloseTabPool() {
	for _, tab := range b.tabs {
		tab.Cancel()
	}
	b.tabs = nil
}

func (b *Browser) ChromiumDetect(ctx context.Context, tab *PooledTab, urlStr string) (bool, string, error) {
	tab.Detector.Reset()

	// 重置 tab：停止当前正在进行的加载，导航到 about:blank 清除 CDP target 残留状态
	resetCtx, resetCancel := context.WithTimeout(tab.Ctx, 2*time.Second)
	_ = chromedp.Run(resetCtx,
		page.StopLoading(),
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, _, _, _, err := page.Navigate("about:blank").Do(ctx)
			return err
		}),
	)
	resetCancel()

	// 规范化 URL：如果缺少协议前缀则自动补上 https://
	normalizedURL := normalizeURL(urlStr)

	// 使用 tab.Ctx (chromedp 上下文) 作为 chromedp 操作的基础上下文,
	// 并从传入的 ctx 中继承超时/截止时间, 以确保 chromedp 能正确提取 target 信息
	detectCtx := tab.Ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		detectCtx, cancel = context.WithDeadline(tab.Ctx, deadline)
		defer cancel()
	}

	runErr := chromedp.Run(detectCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, _, _, _, err := page.Navigate(normalizedURL).Do(ctx)
			return err
		}),
	)
	if runErr != nil {
		b.logger.Errorf("Chromium detect err: %v", runErr)
		return false, "", runErr
	}

	// 等待最多 400ms，或者被拦截信号提前唤醒
	select {
	case <-tab.Detector.ch:
	case <-time.After(BlockDetectTimeout):
	}

	tab.Detector.mu.Lock()
	blocked := tab.Detector.blocked
	tab.Detector.mu.Unlock()

	detail := ""
	if len(tab.Detector.events) > 0 {
		detailBytes, marshalErr := json.Marshal(tab.Detector.events)
		if marshalErr != nil {
			b.logger.Errorf("ChromiumDetect marshal events err: %+v", marshalErr)
		} else {
			detail = string(detailBytes)
		}
	}
	return blocked, detail, runErr
}

// normalizeURL 规范化 URL：如果缺少协议前缀则自动补上 https://
func normalizeURL(rawURL string) string {
	if rawURL == "" {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" {
		return "https://" + rawURL
	}
	return rawURL
}
