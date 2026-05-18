package biz

import (
	"context"
	"detect-agent/internal/conf"
	"fmt"
	"math/rand/v2"
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
	HerdSogou  *BrowserHerd
}

func NewBrowserShepherd(logger log.Logger) *BrowserShepherd {
	bs := &BrowserShepherd{
		logger: log.NewHelper(log.With(logger, "module", "browser_shepherd/")),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdChrome = NewChromiumHerd(logger, "google-chrome", 9000, 9009)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdEdge = NewChromiumHerd(logger, "microsoft-edge-stable", 9010, 9019)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.Herd360 = NewChromiumHerd(logger, "qihoo-360", 9020, 9029)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdUC = NewChromiumHerd(logger, "uc", 9030, 9039)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdQuark = NewChromiumHerd(logger, "quark", 9040, 9049)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bs.HerdSogou = NewChromiumHerd(logger, "sogou", 9050, 9059)
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
			go s.HerdHealthCheck(s.HerdSogou)
		}
	}
}

// GetAvailableBrowser 获取一个对应类型浏览器实例用于探测
func (s *BrowserShepherd) GetAvailableBrowser(appType probecomm.InterceptAppType) (*Browser, error) {
	herd, err := s.getBrowserHerd(appType)
	if err != nil {
		return nil, err
	}
	herd.mu.Lock()
	defer herd.mu.Unlock()
	// 最多取三次，从列表里随机拿浏览器实例出来用。目的是尽可能地取出探测次数不超限制的实例，兜底的情况下也允许超限使用
	randomInstance := herd.availableBrowsers[rand.IntN(len(herd.availableBrowsers))]
	for i := 0; i < 3; i++ {
		if randomInstance.DetectedJobNum+randomInstance.DetectingJobNum < BrowserMaxDetectNum {
			break
		}
		randomInstance = herd.availableBrowsers[rand.IntN(len(herd.availableBrowsers))]
	}
	randomInstance.DetectingJobNum++
	// 如果当前实例已经探测过80%最大次数，启用冷备，异步创建一个新的冷备
	if randomInstance.DetectedJobNum+randomInstance.DetectingJobNum >= BrowserMaxDetectNum*0.8 &&
		len(herd.availableBrowsers) == conf.GetData().BrowserHerdSize && herd.standBy != nil {
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
	return randomInstance, nil
}

// ReleaseBrowser 探测完成，需要释放浏览器，来维护一些状态
func (s *BrowserShepherd) ReleaseBrowser(appType probecomm.InterceptAppType, browser *Browser) error {
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
		if len(herd.availableBrowsers) > conf.GetData().BrowserHerdSize {
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
		return s.HerdSogou, nil
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
		{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_SOGOU, AppNum: uint32(len(s.HerdSogou.availableBrowsers))},
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
