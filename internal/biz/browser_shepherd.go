package biz

import (
	"context"
	"fmt"
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
	// ARM Android-only mode: no desktop Chromium herd initialization.
	// Detection runtime is provided by WaydroidAdapter.
	bs.logger.Info("android-only mode enabled: desktop browser herds are disabled")
	return bs
}

// StartMonitor 定时检查所有浏览器健康状况，自动恢复不健康的实例
func (s *BrowserShepherd) StartMonitor(ctx context.Context) {
	if s.HerdChrome == nil && s.HerdEdge == nil && s.Herd360 == nil && s.HerdUC == nil && s.HerdQuark == nil && s.HerdQQ == nil {
		s.logger.Info("no desktop herds initialized, skip monitor loop")
		return
	}
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
	if targetBrowser == nil {
		if herd.standBy != nil {
			err = herd.standBy.InitTabPool()
			if err != nil {
				s.logger.Errorf("failed to init standBy tab pool: %v", err)
				return nil, nil, err
			}
			targetBrowser = herd.standBy
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
		} else { // 冷备是空，说明在创建中，直接返回错误
			return nil, nil, fmt.Errorf("no free browser available")
		}
	}
	targetBrowser.DetectingJobNum++
	// 如果当前实例已经探测过90%最大次数，启用冷备，异步创建一个新的冷备
	if targetBrowser.DetectedJobNum+targetBrowser.DetectingJobNum >= BrowserMaxDetectNum*0.9 &&
		len(herd.availableBrowsers) == DefaultBrowserHerdSize && herd.standBy != nil {
		err = herd.standBy.InitTabPool()
		if err != nil {
			s.logger.Errorf("failed to init standBy tab pool: %v", err)
			return nil, nil, err
		}
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
			err = herd.standBy.InitTabPool()
			if err != nil {
				s.logger.Errorf("failed to init standBy tab pool: %v", err)
				return err
			}
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
	case probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ:
		return s.HerdQQ, nil
	default:
		return nil, fmt.Errorf("unsupported appType: %v", appType)
	}
}

// GetBrowserDetails 生成当前所有类型浏览器集群详情
func (s *BrowserShepherd) GetBrowserDetails() []*ctrlplanev1.InterceptNodeDetail {
	appNum := func(h *BrowserHerd) uint32 {
		if h == nil {
			return 0
		}
		return uint32(len(h.availableBrowsers))
	}
	res := []*ctrlplanev1.InterceptNodeDetail{
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME, AppNum: appNum(s.HerdChrome)},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE, AppNum: appNum(s.HerdEdge)},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_360, AppNum: appNum(s.Herd360)},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_UC, AppNum: appNum(s.HerdUC)},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QUARK, AppNum: appNum(s.HerdQuark)},
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ, AppNum: appNum(s.HerdQQ)},
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
			err = herd.standBy.InitTabPool()
			if err != nil {
				s.logger.Errorf("failed to init standBy tab pool: %v", err)
				return err
			}
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
