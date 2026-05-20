package biz

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	ctrlplanev1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/control-plane/v1"

	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
)

// BrowserShepherd 浏览器群管理者
type BrowserShepherd struct {
	logger     *log.Helper
	HerdChrome *BrowserHerd
	HerdEdge   *BrowserHerd
	Herd360    *BrowserHerd
	HerdUC     *BrowserHerd
	HerdQuark  *BrowserHerd
	HerdQQ     *BrowserHerd
}

func NewBrowserShepherd(logger log.Logger) *BrowserShepherd {
	bs := &BrowserShepherd{
		logger: log.NewHelper(log.With(logger, "module", "browser_shepherd/")),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdChrome = NewChromiumHerd(logger, "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe", "C:\\browserprofile\\chrome", 9000, 9009)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdEdge = NewChromiumHerd(logger, "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe", "C:\\browserprofile\\edge", 9010, 9019)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.Herd360 = NewChromiumHerd(logger, "C:\\Users\\Administrator\\AppData\\Local\\360ChromeX\\Chrome\\Application\\360ChromeX.exe", "C:\\browserprofile\\360", 9020, 9029)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdUC = NewChromiumHerd(logger, "C:\\Program Files\\UCBrowser\\uc.exe", "C:\\browserprofile\\uc", 9030, 9039)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdQuark = NewChromiumHerd(logger, "C:\\Program Files\\Quark\\quark.exe", "C:\\browserprofile\\quark", 9040, 9049)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdQQ = NewChromiumHerd(logger, "C:\\Program Files\\Tencent\\QQBrowser\\QQBrowser.exe", "C:\\browserprofile\\qq", 9050, 9059)
	}()
	wg.Wait()
	return bs
}

// StartMonitor 定时检查所有浏览器健康状况，自动恢复不健康的实例
func (s *BrowserShepherd) StartMonitor(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go s.HerdHealthCheck(s.HerdChrome)
			go s.HerdHealthCheck(s.HerdEdge)
			go s.HerdHealthCheck(s.Herd360)
			go s.HerdHealthCheck(s.HerdUC)
			go s.HerdHealthCheck(s.HerdQuark)
			go s.HerdHealthCheck(s.HerdQQ)
		}
	}
}

// GetAvailableBrowserTab 获取一个对应类型浏览器tab实例用于探测
func (s *BrowserShepherd) GetAvailableBrowserTab(appType probecomm.InterceptAppType) (*Browser, *PooledTab, error) {
	herd, err := s.getBrowserHerd(appType)
	if err != nil {
		return nil, nil, err
	}
	herd.mu.Lock()
	defer herd.mu.Unlock()
	// 先取一个使用未超限且有空余tab的浏览器
	var targetBrowser *Browser
	for i := 0; i < len(herd.availableBrowsers); i++ {
		targetBrowser = herd.availableBrowsers[i]
		if targetBrowser.DetectedJobNum+targetBrowser.DetectingJobNum < BrowserMaxDetectNum && len(targetBrowser.tabs) > 0 {
			break
		}
	}
	defer func() {

	}()
	if targetBrowser == nil {
		if herd.standBy != nil {
			targetBrowser = herd.standBy
			herd.standBy = nil
			herd.availableBrowsers = append(herd.availableBrowsers, herd.standBy)
			go func() {
				newInstance, err := herd.CreateChromiumInstance(herd.logger)
				if err != nil {
					panic(err)
				}
				herd.mu.Lock()
				defer herd.mu.Unlock()
				herd.standBy = newInstance
			}()
		} else { // 冷备是空，说明在创建中，直接返回错误
			return nil, nil, fmt.Errorf("no free browser available")
		}
	}
	targetBrowser.DetectingJobNum++
	// 如果当前实例已经探测过80%最大次数，启用冷备，异步创建一个新的冷备
	if targetBrowser.DetectedJobNum+targetBrowser.DetectingJobNum >= BrowserMaxDetectNum*0.8 &&
		len(herd.availableBrowsers) == DefaultBrowserHerdSize && herd.standBy != nil {
		herd.availableBrowsers = append(herd.availableBrowsers, herd.standBy)
		herd.standBy = nil
		go func() {
			newInstance, err := herd.CreateChromiumInstance(herd.logger)
			if err != nil {
				panic(err)
			}
			herd.mu.Lock()
			defer herd.mu.Unlock()
			herd.standBy = newInstance
		}()
	}
	tab := targetBrowser.tabs[len(targetBrowser.tabs)-1]
	targetBrowser.tabs = targetBrowser.tabs[:len(targetBrowser.tabs)-1]
	return targetBrowser, tab, nil
}

// ReleaseBrowserTab 探测完成，需要释放浏览器，来维护一些状态
func (s *BrowserShepherd) ReleaseBrowserTab(appType probecomm.InterceptAppType, browser *Browser, tab *PooledTab) error {
	herd, err := s.getBrowserHerd(appType)
	if err != nil {
		return err
	}
	herd.mu.Lock()
	defer herd.mu.Unlock()
	index := -1
	for i := 0; i < len(herd.availableBrowsers); i++ {
		if herd.availableBrowsers[i].Port == browser.Port {
			index = i
			break
		}
	}
	// 如果没找到，可能是因为浏览器实例本身有问题，已经被健康检查任务干掉了，记录日志即可
	if index == -1 {
		s.logger.Infof("browser port %d not found in herd", browser.Port)
		return nil
	}
	browser.DetectedJobNum++
	browser.DetectingJobNum--
	browser.tabs = append(browser.tabs, tab)
	// 如果已经达到最大探测次数，需要把这个浏览器下掉
	if browser.DetectedJobNum >= BrowserMaxDetectNum && browser.DetectingJobNum == 0 {
		newAvailableBrowsers := make([]*Browser, 0)
		for _, b := range herd.availableBrowsers {
			if b.Port != browser.Port {
				newAvailableBrowsers = append(newAvailableBrowsers, b)
			}
		}
		herd.availableBrowsers = newAvailableBrowsers
		// 如果已经提前把备用浏览器补充进来了，那么直接下掉就行了
		if len(herd.availableBrowsers) > DefaultBrowserHerdSize {
			go herd.KillChromiumInstance(browser)
		} else if herd.standBy != nil {
			// 把备用补充进来，并创建新的备用
			herd.availableBrowsers = append(herd.availableBrowsers, herd.standBy)
			herd.standBy = nil
			go func() {
				herd.standBy, err = herd.CreateChromiumInstance(herd.logger)
				if err != nil {
					panic(err)
				}
			}()
		}
	}
	return nil
}

func (s *BrowserShepherd) getBrowserHerd(appType probecomm.InterceptAppType) (*BrowserHerd, error) {
	switch appType {
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME:
		return s.HerdChrome, nil
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE:
		return s.HerdEdge, nil
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_360:
		return s.Herd360, nil
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_UC:
		return s.HerdUC, nil
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QUARK:
		return s.HerdQuark, nil
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_SOGOU:
		return s.HerdQQ, nil
	default:
		return nil, fmt.Errorf("unsupported appType: %v", appType)
	}
}

// GetBrowserDetails 生成当前所有类型浏览器集群详情
func (s *BrowserShepherd) GetBrowserDetails() []*ctrlplanev1.InterceptNodeDetail {
	res := []*ctrlplanev1.InterceptNodeDetail{
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME, AppNum: uint32(len(s.HerdChrome.availableBrowsers))},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE, AppNum: uint32(len(s.HerdEdge.availableBrowsers))},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_360, AppNum: uint32(len(s.Herd360.availableBrowsers))},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_UC, AppNum: uint32(len(s.HerdUC.availableBrowsers))},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QUARK, AppNum: uint32(len(s.HerdQuark.availableBrowsers))},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_SOGOU, AppNum: uint32(len(s.HerdQQ.availableBrowsers))},
	}
	return res
}

// HerdHealthCheck 对浏览器集群做健康检查
func (s *BrowserShepherd) HerdHealthCheck(herd *BrowserHerd) error {
	// 先确保冷备可用
	_, err := herd.BrowserHealthCheck(herd.standBy)
	if err != nil {
		s.logger.Infof("standby browser unhealthy: %v, try to recover", err)
		b, err := herd.CreateChromiumInstance(herd.logger)
		if err != nil {
			panic(err)
		}
		old := herd.standBy
		herd.mu.Lock()
		herd.standBy = b
		herd.mu.Unlock()
		err = herd.KillChromiumInstance(old)
		if err != nil {
			s.logger.Errorf("failed to kill browser instance: %v", err)
		}
	}
	// 再逐个检查使用中的浏览器健康状况，不健康的浏览器用备用的替换掉，再新建备用，并kill旧的
	for i := 0; i < len(herd.availableBrowsers); i++ {
		_, err := herd.BrowserHealthCheck(herd.availableBrowsers[i])
		if err != nil {
			s.logger.Infof("available browser unhealthy: %v, try to replace with standby", err)
			old := herd.availableBrowsers[i]
			herd.mu.Lock()
			herd.availableBrowsers[i] = herd.standBy
			herd.standBy = nil
			herd.mu.Unlock()
			b, err := herd.CreateChromiumInstance(herd.logger)
			if err != nil {
				panic(err)
			}
			herd.mu.Lock()
			herd.standBy = b
			herd.mu.Unlock()
			err = herd.KillChromiumInstance(old)
			if err != nil {
				s.logger.Errorf("failed to kill browser instance: %v", err)
			}
		}
	}
	return nil
}
