package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
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
	URL string `json:"url"`
}

type listResp struct {
	Serial string           `json:"serial"`
	Socket string           `json:"socket"`
	Port   int              `json:"port"`
	Tabs   []map[string]any `json:"tabs"`
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
		writeJSON(w, http.StatusOK, listResp{Serial: svc.cfg.Serial, Socket: svc.socket, Port: svc.cfg.Port, Tabs: tabs})
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
		if err := svc.openURL(req.URL); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		time.Sleep(1500 * time.Millisecond)
		tabs, err := svc.fetchTabsLocked()
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, listResp{Serial: svc.cfg.Serial, Socket: svc.socket, Port: svc.cfg.Port, Tabs: tabs})
	})

	server := &http.Server{
		Addr:         cfg.Listen,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 20 * time.Second,
	}

	fmt.Printf("waydroid-local listening on %s\n", cfg.Listen)
	fmt.Printf("package=%s cdp-port=%d serial=%s\n", cfg.PackageName, cfg.Port, cfg.Serial)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
