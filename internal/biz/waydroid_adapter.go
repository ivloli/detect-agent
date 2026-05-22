package biz

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
)

type WaydroidCapability string

const (
	CapabilityCDPFull    WaydroidCapability = "CDP_FULL"
	CapabilitySocketOnly WaydroidCapability = "CDP_SOCKET_ONLY"
	CapabilityNoCDP      WaydroidCapability = "NO_CDP"
)

type WaydroidAdapter struct {
	logger       *log.Helper
	enabled      bool
	serial       string
	portByApp    map[probecomm.InterceptAppType]int
	maxTabsByApp map[probecomm.InterceptAppType]int
	packageByApp map[probecomm.InterceptAppType]string
	mu           sync.Mutex
	socketByApp  map[probecomm.InterceptAppType]string
	busyByApp    map[probecomm.InterceptAppType]map[string]bool
}

func NewWaydroidAdapter(logger log.Logger) *WaydroidAdapter {
	adapter := &WaydroidAdapter{
		logger:       log.NewHelper(log.With(logger, "module", "biz/waydroid_adapter")),
		enabled:      strings.EqualFold(os.Getenv("DETECT_AGENT_WAYDROID_ENABLED"), "1") || strings.EqualFold(os.Getenv("DETECT_AGENT_WAYDROID_ENABLED"), "true"),
		serial:       strings.TrimSpace(os.Getenv("DETECT_AGENT_WAYDROID_SERIAL")),
		portByApp:    map[probecomm.InterceptAppType]int{},
		maxTabsByApp: map[probecomm.InterceptAppType]int{},
		packageByApp: map[probecomm.InterceptAppType]string{},
		socketByApp:  map[probecomm.InterceptAppType]string{},
		busyByApp:    map[probecomm.InterceptAppType]map[string]bool{},
	}

	adapter.packageByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME] = envOrDefault("DETECT_AGENT_WAYDROID_PACKAGE_CHROME", "com.mi.globalbrowser")
	adapter.portByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME] = envIntOrDefault("DETECT_AGENT_WAYDROID_PORT_CHROME", 9222)
	adapter.maxTabsByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME] = envIntOrDefault("DETECT_AGENT_WAYDROID_MAX_TABS_CHROME", 10)

	// Optional extra mapping for QQ topic -> package if needed later.
	adapter.packageByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ] = envOrDefault("DETECT_AGENT_WAYDROID_PACKAGE_QQ", "")
	adapter.portByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ] = envIntOrDefault("DETECT_AGENT_WAYDROID_PORT_QQ", 9322)
	adapter.maxTabsByApp[probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ] = envIntOrDefault("DETECT_AGENT_WAYDROID_MAX_TABS_QQ", 10)

	if adapter.enabled {
		adapter.logger.Infof("waydroid adapter enabled, serial=%s", adapter.serial)
	}

	return adapter
}

func envOrDefault(k, d string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	return v
}

func envIntOrDefault(k string, d int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	var out int
	if _, err := fmt.Sscanf(v, "%d", &out); err != nil || out <= 0 {
		return d
	}
	return out
}

func (a *WaydroidAdapter) Enabled() bool { return a.enabled }

func (a *WaydroidAdapter) Detect(ctx context.Context, appType probecomm.InterceptAppType, targetURL string) (WaydroidCapability, bool, string, error) {
	if !a.enabled {
		return CapabilityNoCDP, false, "", errors.New("waydroid adapter not enabled")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	serial := a.serial
	if serial == "" {
		serial = firstDeviceByADB()
	}
	if serial == "" {
		return CapabilityNoCDP, false, "", errors.New("no adb device found")
	}

	pkg := strings.TrimSpace(a.packageByApp[appType])
	if pkg == "" {
		return CapabilityNoCDP, false, fmt.Sprintf("appType=%v not mapped to waydroid package", appType), nil
	}

	port := a.portByApp[appType]
	if port <= 0 {
		port = 9222
	}
	maxTabs := a.maxTabsByApp[appType]
	if maxTabs <= 0 {
		maxTabs = 10
	}

	// Keep socket fresh before scheduling.
	sock := a.detectSocket(serial, pkg)
	if sock != "" {
		a.socketByApp[appType] = sock
		_ = a.remapPort(serial, port, sock)
	}

	tabs, listErr := a.fetchTabs(port)
	if listErr != nil {
		// still try opening URL directly; capability may be socket-only
		tabs = nil
	}

	strategy := "new"
	reuseTabID := ""
	if len(tabs) > 0 {
		a.syncBusyMap(appType, tabs)
		pageTabs := countPageTargets(tabs)
		if pageTabs >= maxTabs {
			idle := a.pickIdleTab(appType, tabs)
			if idle == "" {
				detail := fmt.Sprintf(`{"capability":"%s","strategy":"fail_no_idle","serial":"%s","socket":"%s","port":%d,"maxTabs":%d,"pageTabs":%d,"url":"%s"}`,
					CapabilityCDPFull, serial, a.socketByApp[appType], port, maxTabs, pageTabs, targetURL)
				return CapabilityCDPFull, false, detail, fmt.Errorf("no idle tab available")
			}
			strategy = "reuse_idle"
			reuseTabID = idle
			a.markBusy(appType, reuseTabID, true)
			defer a.markBusy(appType, reuseTabID, false)
		}
	}

	if reuseTabID != "" {
		if err := a.activateTab(port, reuseTabID); err != nil {
			return CapabilityCDPFull, false, "", fmt.Errorf("reuse tab activate failed: %w", err)
		}
	}

	if err := a.openURL(serial, pkg, targetURL); err != nil {
		return CapabilityNoCDP, false, "", err
	}

	sock = a.detectSocket(serial, pkg)
	if sock == "" {
		return CapabilityNoCDP, false, fmt.Sprintf("package=%s socket not found", pkg), nil
	}
	a.socketByApp[appType] = sock

	if err := a.remapPort(serial, port, sock); err != nil {
		return CapabilitySocketOnly, false, fmt.Sprintf("socket=%s remap failed: %v", sock, err), nil
	}

	tabs, err := a.fetchTabs(port)
	if err != nil {
		return CapabilitySocketOnly, false, fmt.Sprintf("socket=%s list endpoint unavailable: %v", sock, err), nil
	}
	a.syncBusyMap(appType, tabs)

	blocked := false
	detail := fmt.Sprintf(`{"capability":"%s","strategy":"%s","reuseTabId":"%s","serial":"%s","socket":"%s","port":%d,"maxTabs":%d,"tabs":%d,"url":"%s"}`,
		CapabilityCDPFull, strategy, reuseTabID, serial, sock, port, maxTabs, len(tabs), targetURL)
	return CapabilityCDPFull, blocked, detail, nil
}

func (a *WaydroidAdapter) fetchTabs(port int) ([]map[string]any, error) {
	listURL := fmt.Sprintf("http://127.0.0.1:%d/json/list", port)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(listURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list status=%d", resp.StatusCode)
	}
	var tabs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&tabs); err != nil {
		return nil, err
	}
	return tabs, nil
}

func countPageTargets(tabs []map[string]any) int {
	n := 0
	for _, t := range tabs {
		if strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			n++
		}
	}
	return n
}

func (a *WaydroidAdapter) syncBusyMap(appType probecomm.InterceptAppType, tabs []map[string]any) {
	if a.busyByApp[appType] == nil {
		a.busyByApp[appType] = map[string]bool{}
	}
	alive := map[string]struct{}{}
	for _, t := range tabs {
		if !strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			continue
		}
		id := fmt.Sprintf("%v", t["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		alive[id] = struct{}{}
		if _, ok := a.busyByApp[appType][id]; !ok {
			a.busyByApp[appType][id] = false
		}
	}
	for id := range a.busyByApp[appType] {
		if _, ok := alive[id]; !ok {
			delete(a.busyByApp[appType], id)
		}
	}
}

func (a *WaydroidAdapter) pickIdleTab(appType probecomm.InterceptAppType, tabs []map[string]any) string {
	a.syncBusyMap(appType, tabs)
	for _, t := range tabs {
		if !strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			continue
		}
		id := fmt.Sprintf("%v", t["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		if !a.busyByApp[appType][id] {
			return id
		}
	}
	return ""
}

func (a *WaydroidAdapter) markBusy(appType probecomm.InterceptAppType, tabID string, busy bool) {
	if strings.TrimSpace(tabID) == "" {
		return
	}
	if a.busyByApp[appType] == nil {
		a.busyByApp[appType] = map[string]bool{}
	}
	a.busyByApp[appType][tabID] = busy
}

func (a *WaydroidAdapter) activateTab(port int, tabID string) error {
	actURL := fmt.Sprintf("http://127.0.0.1:%d/json/activate/%s", port, tabID)
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Get(actURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("activate status=%d", resp.StatusCode)
	}
	return nil
}

func (a *WaydroidAdapter) openURL(serial, pkg, targetURL string) error {
	if _, err := runADB("adb", "-s", serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", targetURL, pkg); err == nil {
		return nil
	}
	_, err := runADB("adb", "-s", serial, "shell", "monkey", "-p", pkg, "-c", "android.intent.category.LAUNCHER", "1")
	if err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	_, err = runADB("adb", "-s", serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", targetURL, pkg)
	return err
}

func (a *WaydroidAdapter) remapPort(serial string, port int, sock string) error {
	_, _ = runADB("adb", "-s", serial, "forward", "--remove", fmt.Sprintf("tcp:%d", port))
	_, err := runADB("adb", "-s", serial, "forward", fmt.Sprintf("tcp:%d", port), fmt.Sprintf("localabstract:%s", sock))
	return err
}

func (a *WaydroidAdapter) detectSocket(serial, pkg string) string {
	out, _ := runADB("adb", "-s", serial, "shell", "cat", "/proc/net/unix")
	var sockets []string
	scanner := bufio.NewScanner(strings.NewReader(out))
	pkgHint := strings.ToLower(strings.TrimSpace(pkg))
	pkgHint = strings.ReplaceAll(pkgHint, ".", "")
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, "@")
		if idx < 0 {
			continue
		}
		name := strings.TrimSpace(line[idx+1:])
		if matched, _ := regexp.MatchString(`(?i)devtools_remote|webview_devtools_remote`, name); matched {
			sockets = append(sockets, name)
		}
	}
	if len(sockets) == 0 {
		return ""
	}
	sort.Slice(sockets, func(i, j int) bool {
		a := strings.ToLower(sockets[i])
		b := strings.ToLower(sockets[j])
		sa := 0
		sb := 0
		if strings.Contains(a, "globalbrowser") || strings.Contains(a, "browser") || (pkgHint != "" && strings.Contains(a, pkgHint)) {
			sa = 1
		}
		if strings.Contains(b, "globalbrowser") || strings.Contains(b, "browser") || (pkgHint != "" && strings.Contains(b, pkgHint)) {
			sb = 1
		}
		return sa > sb
	})
	return sockets[0]
}

func firstDeviceByADB() string {
	out, _ := runADB("adb", "devices")
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasSuffix(line, "\tdevice") {
			parts := strings.Split(line, "\t")
			if len(parts) > 0 {
				return parts[0]
			}
		}
	}
	return ""
}

func runADB(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	buf, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(buf))
	if err != nil {
		return out, fmt.Errorf("%s %v failed: %w (%s)", name, args, err, out)
	}
	return out, nil
}
