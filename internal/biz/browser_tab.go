package biz

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
)

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

type TabDetector struct {
	mu      sync.Mutex
	blocked bool
	events  []NetworkEvent
	ch      chan struct{}
}

func (d *TabDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blocked = false
	d.events = nil
	d.ch = make(chan struct{}, 1)
}

func (d *TabDetector) OnEvent(ev interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch e := ev.(type) {
	case *network.EventRequestWillBeSent:
		if e.Type == network.ResourceTypeDocument || e.Type == network.ResourceTypeXHR || e.Type == network.ResourceTypeFetch {
			d.events = append(d.events, NetworkEvent{
				Timestamp: time.Now(),
				Type:      "Request",
				URL:       e.Request.URL,
				Method:    e.Request.Method,
			})
		}
	case *network.EventResponseReceived:
		d.events = append(d.events, NetworkEvent{
			Timestamp: time.Now(),
			Type:      "Response",
			URL:       e.Response.URL,
			Status:    e.Response.Status,
		})
	case *network.EventLoadingFailed:
		if strings.Contains(e.ErrorText, BlockErrKeyword) {
			d.blocked = true
			d.events = append(d.events, NetworkEvent{
				Timestamp: time.Now(),
				Type:      "LoadingFailed",
				URL:       e.ErrorText,
				Blocked:   true,
			})
			fmt.Printf("🚫 检测到拦截: %s\n", e.ErrorText)
			select {
			case d.ch <- struct{}{}:
			default:
			}
		}
	}
}
