package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	Listen        string
	Serial        string
	Port          int
	MaxTabs       int
	PackageName   string
	SocketPattern string
	AutoReset     bool
}

type service struct {
	cfg    config
	mu     sync.Mutex
	socket string
}

type openReq struct {
	URL   string `json:"url"`
	TabID string `json:"tabId,omitempty"`
}

type closeReq struct {
	TabID string `json:"tabId"`
}

type tabItem struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	Attached bool   `json:"attached"`
	Visible  bool   `json:"visible"`
	State    string `json:"state"`
}

type listResp struct {
	Serial   string           `json:"serial"`
	Socket   string           `json:"socket"`
	Port     int              `json:"port"`
	MaxTabs  int              `json:"maxTabs"`
	PageTabs int              `json:"pageTabs"`
	Tabs     []map[string]any `json:"tabs"`
	Items    []tabItem        `json:"items"`
}

// Mock input shape aligned with kafka consumed TaskCreateRequest (key fields only).
type mockTaskCreateRequest struct {
	TimeoutSec  int             `json:"timeoutSec"`
	Deadline    string          `json:"deadline"`
	Type        string          `json:"type"`
	PayloadJSON string          `json:"payloadJson"`
	TaskMeta    json.RawMessage `json:"taskMeta"`
}

// Inner payload shape aligned with InterceptDetectParam (key field only).
type mockInterceptParam struct {
	URL string `json:"url"`
}

func run(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v failed: %w (%s)", name, args, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func runSafe(name string, args ...string) string {
	out, _ := run(name, args...)
	return out
}

func firstDevice() string {
	out := runSafe("adb", "devices")
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		line := s.Text()
		if strings.HasSuffix(line, "\tdevice") {
			parts := strings.Split(line, "\t")
			if len(parts) > 0 {
				return parts[0]
			}
		}
	}
	return ""
}

func (s *service) detectSocket(serial string) string {
	out := runSafe("adb", "-s", serial, "shell", "cat", "/proc/net/unix")
	re := regexp.MustCompile(s.cfg.SocketPattern)
	var sockets []string
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, "@")
		if idx < 0 {
			continue
		}
		name := strings.TrimSpace(line[idx+1:])
		if re.MatchString(strings.ToLower(name)) {
			sockets = append(sockets, name)
		}
	}
	if len(sockets) == 0 {
		return ""
	}
	// Prefer package-related socket names first.
	prefer := strings.ToLower(strings.ReplaceAll(s.cfg.PackageName, ".", ""))
	sort.Slice(sockets, func(i, j int) bool {
		a := strings.ToLower(sockets[i])
		b := strings.ToLower(sockets[j])
		sa := 0
		sb := 0
		if strings.Contains(a, "globalbrowser") || strings.Contains(a, "browser") || strings.Contains(a, prefer) {
			sa = 1
		}
		if strings.Contains(b, "globalbrowser") || strings.Contains(b, "browser") || strings.Contains(b, prefer) {
			sb = 1
		}
		return sa > sb
	})
	return sockets[0]
}

func (s *service) ensureForwardLocked() error {
	if s.cfg.Serial == "" {
		s.cfg.Serial = firstDevice()
	}
	if s.cfg.Serial == "" {
		return errors.New("no adb device found")
	}
	newSock := s.detectSocket(s.cfg.Serial)
	if newSock == "" {
		return errors.New("no devtools socket found")
	}
	if s.socket == newSock {
		return nil
	}
	if s.cfg.AutoReset {
		_, _ = run("adb", "-s", s.cfg.Serial, "forward", "--remove", fmt.Sprintf("tcp:%d", s.cfg.Port))
	}
	if _, err := run("adb", "-s", s.cfg.Serial, "forward", fmt.Sprintf("tcp:%d", s.cfg.Port), fmt.Sprintf("localabstract:%s", newSock)); err != nil {
		return err
	}
	s.socket = newSock
	return nil
}

func (s *service) cdpURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.cfg.Port)
}

func (s *service) fetchTabsLocked() ([]map[string]any, error) {
	if err := s.ensureForwardLocked(); err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(s.cdpURL() + "/json/list")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cdp list http status=%d", resp.StatusCode)
	}
	var tabs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&tabs); err != nil {
		return nil, err
	}
	return tabs, nil
}

func countPageTabs(tabs []map[string]any) int {
	n := 0
	for _, t := range tabs {
		if strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			n++
		}
	}
	return n
}

func toTabItems(tabs []map[string]any) []tabItem {
	out := make([]tabItem, 0, len(tabs))
	for _, t := range tabs {
		item := tabItem{
			ID:    fmt.Sprintf("%v", t["id"]),
			Type:  fmt.Sprintf("%v", t["type"]),
			Title: fmt.Sprintf("%v", t["title"]),
			URL:   fmt.Sprintf("%v", t["url"]),
			State: "unknown",
		}
		desc := fmt.Sprintf("%v", t["description"])
		if desc != "" && desc != "<nil>" {
			var m map[string]any
			if err := json.Unmarshal([]byte(desc), &m); err == nil {
				if v, ok := m["attached"].(bool); ok {
					item.Attached = v
				}
				if v, ok := m["visible"].(bool); ok {
					item.Visible = v
				}
			}
		}
		if strings.EqualFold(item.Type, "page") {
			item.State = "idle"
		} else {
			item.State = "non_page"
		}
		out = append(out, item)
	}
	return out
}

func hasTabID(tabs []map[string]any, id string) bool {
	for _, t := range tabs {
		if fmt.Sprintf("%v", t["id"]) == id {
			return true
		}
	}
	return false
}

func tabWSURL(tabs []map[string]any, id string) string {
	for _, t := range tabs {
		if fmt.Sprintf("%v", t["id"]) == id {
			return fmt.Sprintf("%v", t["webSocketDebuggerUrl"])
		}
	}
	return ""
}

func (s *service) cdpGet(path string) ([]byte, int, error) {
	if err := s.ensureForwardLocked(); err != nil {
		return nil, 0, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(s.cdpURL() + path)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var payload any
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	body, _ := json.Marshal(payload)
	return body, resp.StatusCode, nil
}

func (s *service) closeTab(tabID string) error {
	_, code, err := s.cdpGet("/json/close/" + url.PathEscape(tabID))
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("close tab http status=%d", code)
	}
	return nil
}

func (s *service) activateTab(tabID string) error {
	_, code, err := s.cdpGet("/json/activate/" + url.PathEscape(tabID))
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("activate tab http status=%d", code)
	}
	return nil
}

func (s *service) navigateByTabID(tabID, targetURL string) error {
	tabs, err := s.fetchTabsLocked()
	if err != nil {
		return err
	}
	if !hasTabID(tabs, tabID) {
		return fmt.Errorf("tab id not found: %s", tabID)
	}
	if err := s.activateTab(tabID); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	return s.openURL(targetURL)
}

func (s *service) openURL(url string) error {
	_, err := run("adb", "-s", s.cfg.Serial, "shell", "am", "start", "-n", s.cfg.PackageName+"/com.android.browser.BrowserActivity", "-a", "android.intent.action.VIEW", "-d", url)
	if err == nil {
		return nil
	}
	// Fallback for browsers without BrowserActivity export.
	_, err2 := run("adb", "-s", s.cfg.Serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", url, s.cfg.PackageName)
	if err2 != nil {
		return fmt.Errorf("open url failed: %v; fallback: %v", err, err2)
	}
	return nil
}

func (s *service) forceStopPackage() error {
	_, err := run("adb", "-s", s.cfg.Serial, "shell", "am", "force-stop", s.cfg.PackageName)
	if err != nil {
		return err
	}
	// socket will likely change after force-stop; clear cached socket.
	s.socket = ""
	return nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.Listen, "listen", ":18080", "http listen address")
	flag.StringVar(&cfg.Serial, "serial", "", "adb serial (optional, auto-pick first device)")
	flag.IntVar(&cfg.Port, "port", 9322, "local forwarded CDP port")
	flag.IntVar(&cfg.MaxTabs, "max-tabs", 0, "max allowed page tabs (0 means unlimited)")
	flag.StringVar(&cfg.PackageName, "package", "com.mi.globalbrowser", "target browser package")
	flag.StringVar(&cfg.SocketPattern, "socket-pattern", "devtools_remote|webview_devtools_remote", "regex for socket detect")
	flag.BoolVar(&cfg.AutoReset, "auto-reset-forward", true, "remove and remap only this local tcp port when socket changes")
	flag.Parse()

	svc := &service{cfg: cfg}
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		err := svc.ensureForwardLocked()
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"serial":  svc.cfg.Serial,
			"socket":  svc.socket,
			"cdpPort": svc.cfg.Port,
		})
	})

	mux.HandleFunc("/tabs/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, listResp{Serial: svc.cfg.Serial, Socket: svc.socket, Port: svc.cfg.Port, MaxTabs: svc.cfg.MaxTabs, PageTabs: countPageTabs(tabs), Tabs: tabs, Items: toTabItems(tabs)})
	})

	mux.HandleFunc("/tabs/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		pageTabs := countPageTabs(tabs)
		writeJSON(w, http.StatusOK, map[string]any{
			"serial":   svc.cfg.Serial,
			"socket":   svc.socket,
			"port":     svc.cfg.Port,
			"maxTabs":  svc.cfg.MaxTabs,
			"pageTabs": pageTabs,
			"canOpen":  svc.cfg.MaxTabs == 0 || pageTabs < svc.cfg.MaxTabs,
		})
	})

	mux.HandleFunc("/tabs/open", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		var req openReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body, url required"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if svc.cfg.Serial == "" {
			svc.cfg.Serial = firstDevice()
		}
		if svc.cfg.Serial == "" {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": "no adb device found"})
			return
		}
		tabsBefore, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if svc.cfg.MaxTabs > 0 && countPageTabs(tabsBefore) >= svc.cfg.MaxTabs {
			if strings.TrimSpace(req.TabID) == "" {
				writeJSON(w, http.StatusConflict, map[string]any{
					"error":    "max tabs reached",
					"maxTabs":  svc.cfg.MaxTabs,
					"pageTabs": countPageTabs(tabsBefore),
					"serial":   svc.cfg.Serial,
					"socket":   svc.socket,
					"port":     svc.cfg.Port,
					"tabs":     tabsBefore,
				})
				return
			}
		}

		if strings.TrimSpace(req.TabID) != "" {
			if !hasTabID(tabsBefore, req.TabID) {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "tab id not found", "tabId": req.TabID})
				return
			}
			if err := svc.navigateByTabID(req.TabID, req.URL); err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": "navigate by tab id failed", "detail": err.Error(), "tabId": req.TabID})
				return
			}
		} else {
			if err := svc.openURL(req.URL); err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
				return
			}
		}
		time.Sleep(1500 * time.Millisecond)
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, listResp{Serial: svc.cfg.Serial, Socket: svc.socket, Port: svc.cfg.Port, MaxTabs: svc.cfg.MaxTabs, PageTabs: countPageTabs(tabs), Tabs: tabs, Items: toTabItems(tabs)})
	})

	mux.HandleFunc("/tabs/reuse", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		var req openReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" || strings.TrimSpace(req.TabID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body, require url and tabId"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		tabsBefore, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if !hasTabID(tabsBefore, req.TabID) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "tab id not found", "tabId": req.TabID})
			return
		}
		if err := svc.navigateByTabID(req.TabID, req.URL); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": "navigate by tab id failed", "detail": err.Error(), "tabId": req.TabID})
			return
		}
		time.Sleep(1200 * time.Millisecond)
		tabsAfter, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"serial":   svc.cfg.Serial,
			"socket":   svc.socket,
			"port":     svc.cfg.Port,
			"maxTabs":  svc.cfg.MaxTabs,
			"pageTabs": countPageTabs(tabsAfter),
			"tabId":    req.TabID,
			"url":      req.URL,
			"tabs":     tabsAfter,
			"items":    toTabItems(tabsAfter),
		})
	})

	mux.HandleFunc("/tabs/close", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		var req closeReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.TabID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body, tabId required"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if !hasTabID(tabs, req.TabID) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "tab id not found", "tabId": req.TabID})
			return
		}
		if err := svc.closeTab(req.TabID); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": "close tab failed", "detail": err.Error(), "tabId": req.TabID})
			return
		}
		time.Sleep(400 * time.Millisecond)
		tabsAfter, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, listResp{Serial: svc.cfg.Serial, Socket: svc.socket, Port: svc.cfg.Port, MaxTabs: svc.cfg.MaxTabs, PageTabs: countPageTabs(tabsAfter), Tabs: tabsAfter, Items: toTabItems(tabsAfter)})
	})

	mux.HandleFunc("/tabs/close-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		svc.mu.Lock()
		defer svc.mu.Unlock()
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		closed := 0
		failed := make([]map[string]any, 0)
		for _, t := range tabs {
			if !strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
				continue
			}
			id := fmt.Sprintf("%v", t["id"])
			if id == "" || id == "<nil>" {
				continue
			}
			if err := svc.closeTab(id); err != nil {
				failed = append(failed, map[string]any{"id": id, "error": err.Error()})
				continue
			}
			closed++
		}
		time.Sleep(400 * time.Millisecond)
		tabsAfter, err := svc.fetchTabsLocked()
		if err != nil {
			// Optional fallback: force-stop browser to guarantee clear.
			forceStop := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("forceStop")), "1") || strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("forceStop")), "true")
			if forceStop {
				_ = svc.forceStopPackage()
				writeJSON(w, http.StatusOK, map[string]any{
					"serial":   svc.cfg.Serial,
					"socket":   svc.socket,
					"port":     svc.cfg.Port,
					"closed":   closed,
					"failed":   failed,
					"maxTabs":  svc.cfg.MaxTabs,
					"pageTabs": 0,
					"tabs":     []any{},
					"items":    []any{},
					"note":     "fallback force-stop applied",
				})
				return
			}
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "hint": "try /tabs/close-all?forceStop=true"})
			return
		}

		if len(failed) > 0 {
			forceStop := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("forceStop")), "1") || strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("forceStop")), "true")
			if forceStop {
				_ = svc.forceStopPackage()
				writeJSON(w, http.StatusOK, map[string]any{
					"serial":   svc.cfg.Serial,
					"socket":   svc.socket,
					"port":     svc.cfg.Port,
					"closed":   closed,
					"failed":   failed,
					"maxTabs":  svc.cfg.MaxTabs,
					"pageTabs": 0,
					"tabs":     []any{},
					"items":    []any{},
					"note":     "partial close failed, fallback force-stop applied",
				})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"serial":   svc.cfg.Serial,
			"socket":   svc.socket,
			"port":     svc.cfg.Port,
			"closed":   closed,
			"failed":   failed,
			"maxTabs":  svc.cfg.MaxTabs,
			"pageTabs": countPageTabs(tabsAfter),
			"tabs":     tabsAfter,
			"items":    toTabItems(tabsAfter),
		})
	})

	// /mock/consume: local endpoint to verify kafka input/output schema without real kafka.
	// input: TaskCreateRequest-like JSON
	// output: NodeMessage-like JSON
	mux.HandleFunc("/mock/consume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}

		var req mockTaskCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid request body", "detail": err.Error()})
			return
		}

		var param mockInterceptParam
		if err := json.Unmarshal([]byte(req.PayloadJSON), &param); err != nil || strings.TrimSpace(param.URL) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid payloadJson, require {\"url\":\"...\"}", "detail": errString(err)})
			return
		}

		svc.mu.Lock()
		defer svc.mu.Unlock()

		startedAt := time.Now().UTC()
		if svc.cfg.Serial == "" {
			svc.cfg.Serial = firstDevice()
		}
		if svc.cfg.Serial == "" {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": "no adb device found"})
			return
		}

		if err := svc.openURL(param.URL); err != nil {
			result := buildMockNodeMessage(req, 500, "open url failed", err.Error(), map[string]any{
				"app":       svc.cfg.PackageName,
				"status":    "FAIL",
				"error":     err.Error(),
				"rawResult": "",
			})
			writeJSON(w, http.StatusOK, result)
			return
		}

		time.Sleep(1500 * time.Millisecond)
		tabs, err := svc.fetchTabsLocked()
		if err == nil && svc.cfg.MaxTabs > 0 && countPageTabs(tabs) > svc.cfg.MaxTabs {
			result := buildMockNodeMessage(req, 500, "max tabs reached", "", map[string]any{
				"app":       svc.cfg.PackageName,
				"status":    "FAIL",
				"error":     "max tabs reached",
				"rawResult": fmt.Sprintf(`{"maxTabs":%d,"pageTabs":%d}`, svc.cfg.MaxTabs, countPageTabs(tabs)),
			})
			writeJSON(w, http.StatusOK, result)
			return
		}
		if err != nil {
			// socket exists but /json/list unusable => capability downgrade
			result := buildMockNodeMessage(req, 200, "", "", map[string]any{
				"app":       svc.cfg.PackageName,
				"status":    "NORMAL",
				"error":     "capability=CDP_SOCKET_ONLY",
				"rawResult": fmt.Sprintf(`{"url":%q,"serial":%q,"socket":%q,"port":%d,"error":%q}`, param.URL, svc.cfg.Serial, svc.socket, svc.cfg.Port, err.Error()),
			})
			writeJSON(w, http.StatusOK, result)
			return
		}

		expanded := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("expanded")), "1") || strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("expanded")), "true")

		// Minimal verdict: if list works, treat as NORMAL and return tabs in rawResult.
		raw, _ := json.Marshal(map[string]any{
			"url":     param.URL,
			"serial":  svc.cfg.Serial,
			"socket":  svc.socket,
			"port":    svc.cfg.Port,
			"tabs":    tabs,
			"started": startedAt.Format(time.RFC3339),
			"done":    time.Now().UTC().Format(time.RFC3339),
		})

		result := buildMockNodeMessage(req, 200, "", "", map[string]any{
			"app":       svc.cfg.PackageName,
			"status":    "NORMAL",
			"error":     "",
			"rawResult": string(raw),
		})

		if expanded {
			if eventData, ok := result["eventData"].(map[string]any); ok {
				if execResult, ok := eventData["execResult"].(map[string]any); ok {
					if outputJSON, ok := execResult["outputJson"].(string); ok {
						var outputObj map[string]any
						if err := json.Unmarshal([]byte(outputJSON), &outputObj); err == nil {
							execResult["outputJsonExpanded"] = outputObj
							if rawResultStr, ok := outputObj["rawResult"].(string); ok {
								var rawObj map[string]any
								if err := json.Unmarshal([]byte(rawResultStr), &rawObj); err == nil {
									execResult["rawResultExpanded"] = rawObj
								}
							}
						}
					}
				}
			}
		}
		writeJSON(w, http.StatusOK, result)
	})

	server := &http.Server{
		Addr:         cfg.Listen,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 20 * time.Second,
	}

	fmt.Printf("waydroid-local listening on %s\n", cfg.Listen)
	fmt.Printf("package=%s cdp-port=%d serial=%s max-tabs=%d\n", cfg.PackageName, cfg.Port, cfg.Serial, cfg.MaxTabs)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func buildMockNodeMessage(in mockTaskCreateRequest, code int, msg, raw string, output map[string]any) map[string]any {
	outJSON, _ := json.Marshal(output)
	return map[string]any{
		"eventType": "EVENT_TYPE_EXEC_RESULT",
		"messageId": fmt.Sprintf("mock-%d-%d", time.Now().UnixMilli(), rand.Intn(100000)),
		"timestamp": time.Now().UnixMilli(),
		"nodeId":    "",
		"taskMeta":  rawOrNull(in.TaskMeta),
		"msgStatus": "MESSAGE_STATUS_COMPLETE",
		"eventData": map[string]any{
			"execResult": map[string]any{
				"in": map[string]any{
					"timeoutSec":  in.TimeoutSec,
					"deadline":    in.Deadline,
					"type":        in.Type,
					"payloadJson": in.PayloadJSON,
				},
				"errorCode":       code,
				"errorMessage":    msg,
				"errorRawMessage": raw,
				"finishedAt":      time.Now().UTC().Format(time.RFC3339),
				"outputJson":      string(outJSON),
			},
		},
	}
}

func rawOrNull(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(v, &out); err != nil {
		return string(v)
	}
	return out
}
