package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type Config struct {
	Listen string

	NacosAddr      string
	NacosScheme    string
	NacosUser      string
	NacosPass      string
	NacosNamespace string
	NacosGroup     string
	NacosDataID    string

	KafkaBrokers          string
	KafkaInBrokers        string
	KafkaOutBrokers       string
	KafkaHeartbeatBrokers string
	KafkaGroup            string
	KafkaInTopic          string
	KafkaOutTopic         string
	KafkaHeartbeatTopic   string
	KafkaUser             string
	KafkaPass             string

	Serial   string
	Package  string
	Port     int
	MaxTabs  int
	TimeoutS int
}

type TaskCreateRequest struct {
	TimeoutSec  int             `json:"timeoutSec"`
	Deadline    string          `json:"deadline"`
	Type        string          `json:"type"`
	PayloadJSON string          `json:"payloadJson"`
	TaskMeta    json.RawMessage `json:"taskMeta"`
}

type InterceptParam struct {
	URL string `json:"url"`
}

type DetectOutput struct {
	App       string `json:"app"`
	Status    string `json:"status"`
	Error     string `json:"error"`
	RawResult string `json:"rawResult"`
}

type ExecResult struct {
	In              TaskCreateRequest `json:"in"`
	ErrorCode       int               `json:"errorCode"`
	ErrorMessage    string            `json:"errorMessage"`
	ErrorRawMessage string            `json:"errorRawMessage"`
	FinishedAt      string            `json:"finishedAt"`
	OutputJSON      string            `json:"outputJson"`
}

type NodeMessage struct {
	EventType string         `json:"eventType"`
	MessageID string         `json:"messageId"`
	Timestamp int64          `json:"timestamp"`
	NodeID    string         `json:"nodeId"`
	TaskMeta  any            `json:"taskMeta"`
	MsgStatus string         `json:"msgStatus"`
	EventData map[string]any `json:"eventData"`
}

type HeartbeatMessage struct {
	ReportType  string           `json:"reportType"`
	NodeType    string           `json:"nodeType"`
	NodeName    string           `json:"nodeName"`
	PublicIPv4  string           `json:"publicIpv4"`
	Timestamp   string           `json:"timestamp"`
	NodeDetails []map[string]any `json:"nodeDetails"`
}

type Runtime struct {
	cfg    *Config
	mu     sync.Mutex
	socket string
	busy   map[string]bool
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

func (rt *Runtime) detectSocket() string {
	out := runSafe("adb", "-s", rt.cfg.Serial, "shell", "cat", "/proc/net/unix")
	re := regexp.MustCompile(`(?i)devtools_remote|webview_devtools_remote`)
	var sockets []string
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		line := s.Text()
		idx := strings.Index(line, "@")
		if idx < 0 {
			continue
		}
		name := strings.TrimSpace(line[idx+1:])
		if re.MatchString(name) {
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
		if strings.Contains(a, "globalbrowser") || strings.Contains(a, "browser") {
			sa = 1
		}
		if strings.Contains(b, "globalbrowser") || strings.Contains(b, "browser") {
			sb = 1
		}
		return sa > sb
	})
	return sockets[0]
}

func (rt *Runtime) ensureForward() error {
	if rt.cfg.Serial == "" {
		rt.cfg.Serial = firstDevice()
	}
	if rt.cfg.Serial == "" {
		return errors.New("no adb device found")
	}
	sock := rt.detectSocket()
	if sock == "" {
		return errors.New("no devtools socket found")
	}
	if rt.socket == sock {
		return nil
	}
	_, _ = run("adb", "-s", rt.cfg.Serial, "forward", "--remove", fmt.Sprintf("tcp:%d", rt.cfg.Port))
	if _, err := run("adb", "-s", rt.cfg.Serial, "forward", fmt.Sprintf("tcp:%d", rt.cfg.Port), fmt.Sprintf("localabstract:%s", sock)); err != nil {
		return err
	}
	rt.socket = sock
	return nil
}

func (rt *Runtime) listTabs() ([]map[string]any, error) {
	if err := rt.ensureForward(); err != nil {
		return nil, err
	}
	u := fmt.Sprintf("http://127.0.0.1:%d/json/list", rt.cfg.Port)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(u)
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

func countPages(tabs []map[string]any) int {
	n := 0
	for _, t := range tabs {
		if strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			n++
		}
	}
	return n
}

func (rt *Runtime) syncBusy(tabs []map[string]any) {
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
		if _, ok := rt.busy[id]; !ok {
			rt.busy[id] = false
		}
	}
	for id := range rt.busy {
		if _, ok := alive[id]; !ok {
			delete(rt.busy, id)
		}
	}
}

func (rt *Runtime) pickIdle(tabs []map[string]any) string {
	rt.syncBusy(tabs)
	for _, t := range tabs {
		if !strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			continue
		}
		id := fmt.Sprintf("%v", t["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		if !rt.busy[id] {
			return id
		}
	}
	return ""
}

func (rt *Runtime) activate(tabID string) error {
	client := &http.Client{Timeout: 4 * time.Second}
	u := fmt.Sprintf("http://127.0.0.1:%d/json/activate/%s", rt.cfg.Port, tabID)
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("activate status=%d", resp.StatusCode)
	}
	return nil
}

func (rt *Runtime) navigateByTabID(tabID, targetURL string) error {
	tabs, err := rt.listTabs()
	if err != nil {
		return err
	}
	var wsURL string
	for _, t := range tabs {
		if fmt.Sprintf("%v", t["id"]) == tabID {
			wsURL = fmt.Sprintf("%v", t["webSocketDebuggerUrl"])
			break
		}
	}
	if wsURL == "" {
		return fmt.Errorf("tab not found: %s", tabID)
	}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := cdpCall(conn, 1, "Page.enable", nil); err != nil {
		return err
	}
	if _, err := cdpCall(conn, 2, "Runtime.enable", nil); err != nil {
		return err
	}
	if _, err := cdpCall(conn, 3, "Page.navigate", map[string]any{"url": targetURL}); err != nil {
		return err
	}
	_ = rt.activate(tabID)
	return nil
}

func cdpCall(conn *websocket.Conn, id int, method string, params map[string]any) (map[string]any, error) {
	if params == nil {
		params = map[string]any{}
	}
	req := map[string]any{"id": id, "method": method, "params": params}
	if err := conn.WriteJSON(req); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(6 * time.Second))
	for {
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			return nil, err
		}
		rawID, ok := msg["id"]
		if !ok {
			continue
		}
		fid, ok := rawID.(float64)
		if !ok || int(fid) != id {
			continue
		}
		if e, ok := msg["error"]; ok {
			return nil, fmt.Errorf("cdp error: %v", e)
		}
		res, _ := msg["result"].(map[string]any)
		if res == nil {
			res = map[string]any{}
		}
		return res, nil
	}
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return strings.TrimPrefix(h, "www.")
}

func pickTargetTabIDByURL(tabs []map[string]any, targetURL string) string {
	reqHost := hostOf(targetURL)
	best := ""
	bestScore := -1
	for _, t := range tabs {
		if !strings.EqualFold(fmt.Sprintf("%v", t["type"]), "page") {
			continue
		}
		id := fmt.Sprintf("%v", t["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		score := 0
		u := fmt.Sprintf("%v", t["url"])
		th := hostOf(u)
		if reqHost != "" && th != "" && strings.Contains(th, reqHost) {
			score += 60
		}
		if strings.HasPrefix(strings.ToLower(u), "http://") || strings.HasPrefix(strings.ToLower(u), "https://") {
			score += 20
		}
		desc := fmt.Sprintf("%v", t["description"])
		if desc != "" && desc != "<nil>" {
			var m map[string]any
			if err := json.Unmarshal([]byte(desc), &m); err == nil {
				if v, ok := m["visible"].(bool); ok && v {
					score += 10
				}
				if v, ok := m["attached"].(bool); ok && v {
					score += 10
				}
			}
		}
		if score > bestScore {
			bestScore = score
			best = id
		}
	}
	return best
}

func detectBlockedOnTab(tabID, wsURL, targetURL string, timeout time.Duration) (bool, map[string]any, error) {
	if strings.TrimSpace(wsURL) == "" {
		return false, nil, fmt.Errorf("empty ws url")
	}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return false, nil, err
	}
	defer conn.Close()

	if _, err := cdpCall(conn, 1, "Page.enable", nil); err != nil {
		return false, nil, err
	}
	if _, err := cdpCall(conn, 2, "Network.enable", nil); err != nil {
		return false, nil, err
	}
	if _, err := cdpCall(conn, 3, "Page.navigate", map[string]any{"url": targetURL}); err != nil {
		return false, nil, err
	}

	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)
	blocked := false
	evidence := map[string]any{
		"tabId": tabID,
		"url":   targetURL,
	}
	for time.Now().Before(deadline) {
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}
		method, _ := msg["method"].(string)
		if method != "Network.loadingFailed" {
			continue
		}
		params, _ := msg["params"].(map[string]any)
		errorText := fmt.Sprintf("%v", params["errorText"])
		blockedReason := fmt.Sprintf("%v", params["blockedReason"])
		evidence["errorText"] = errorText
		evidence["blockedReason"] = blockedReason
		if strings.Contains(strings.ToUpper(errorText), "ERR_BLOCKED") || strings.Contains(strings.ToLower(blockedReason), "blocked") {
			blocked = true
			break
		}
	}
	return blocked, evidence, nil
}

func (rt *Runtime) openURL(targetURL string) error {
	if _, err := run("adb", "-s", rt.cfg.Serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", targetURL, rt.cfg.Package); err == nil {
		return nil
	}
	_, err := run("adb", "-s", rt.cfg.Serial, "shell", "monkey", "-p", rt.cfg.Package, "-c", "android.intent.category.LAUNCHER", "1")
	if err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	_, err = run("adb", "-s", rt.cfg.Serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", targetURL, rt.cfg.Package)
	return err
}

func (rt *Runtime) detect(urlStr string) (bool, string, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	tabs, err := rt.listTabs()
	if err != nil {
		return false, "", err
	}
	pageTabs := countPages(tabs)
	strategy := "new"
	reuseTabID := ""
	if pageTabs >= rt.cfg.MaxTabs {
		idle := rt.pickIdle(tabs)
		if idle == "" {
			detail := fmt.Sprintf(`{"strategy":"fail_no_idle","maxTabs":%d,"pageTabs":%d,"url":%q}`, rt.cfg.MaxTabs, pageTabs, urlStr)
			return false, detail, errors.New("no idle tab available")
		}
		reuseTabID = idle
		strategy = "reuse_idle"
		rt.busy[idle] = true
		defer func() { rt.busy[idle] = false }()
		if err := rt.activate(idle); err != nil {
			return false, "", err
		}
		if err := rt.navigateByTabID(idle, urlStr); err != nil {
			return false, "", err
		}
	} else {
		if err := rt.openURL(urlStr); err != nil {
			return false, "", err
		}
	}
	time.Sleep(1500 * time.Millisecond)
	tabsAfter, err := rt.listTabs()
	if err != nil {
		return false, "", err
	}
	rt.syncBusy(tabsAfter)
	selectedTabID := reuseTabID
	if selectedTabID == "" {
		selectedTabID = pickTargetTabIDByURL(tabsAfter, urlStr)
	}

	blocked := false
	evidence := map[string]any{}
	if strings.TrimSpace(selectedTabID) != "" {
		var wsURL string
		for _, t := range tabsAfter {
			if fmt.Sprintf("%v", t["id"]) == selectedTabID {
				wsURL = fmt.Sprintf("%v", t["webSocketDebuggerUrl"])
				break
			}
		}
		if wsURL != "" {
			b, ev, err := detectBlockedOnTab(selectedTabID, wsURL, urlStr, 4*time.Second)
			if err == nil {
				blocked = b
				evidence = ev
			}
		}
	}

	detailObj := map[string]any{
		"strategy":    strategy,
		"reuseTabId":  reuseTabID,
		"targetTabId": selectedTabID,
		"maxTabs":     rt.cfg.MaxTabs,
		"pageTabs":    countPages(tabsAfter),
		"socket":      rt.socket,
		"port":        rt.cfg.Port,
		"url":         urlStr,
	}
	if len(evidence) > 0 {
		detailObj["blockedEvidence"] = evidence
	}
	raw, _ := json.Marshal(detailObj)
	return blocked, string(raw), nil
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadKafkaFromNacos(cfg *Config) {
	if strings.TrimSpace(cfg.KafkaBrokers) != "" || strings.TrimSpace(cfg.NacosAddr) == "" {
		return
	}
	sc, err := buildServerConfigs(cfg.NacosAddr, cfg.NacosScheme)
	if err != nil || len(sc) == 0 {
		log.Printf("[server-lite] skip nacos load: %v", err)
		return
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(cfg.NacosNamespace),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir("/tmp/nacos/log"),
		constant.WithCacheDir("/tmp/nacos/cache"),
		constant.WithLogLevel("warn"),
		constant.WithUsername(cfg.NacosUser),
		constant.WithPassword(cfg.NacosPass),
	)
	client, err := clients.NewConfigClient(vo.NacosClientParam{ClientConfig: &cc, ServerConfigs: sc})
	if err != nil {
		log.Printf("[server-lite] nacos client init failed: %v", err)
		return
	}
	content, err := client.GetConfig(vo.ConfigParam{DataId: cfg.NacosDataID, Group: cfg.NacosGroup})
	if err != nil {
		log.Printf("[server-lite] nacos get config failed: %v", err)
		return
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(content), &m); err != nil {
		log.Printf("[server-lite] nacos config decode failed: %v", err)
		log.Printf("[server-lite] nacos raw content (first 512): %s", clip(content, 512))
		return
	}
	kv, _ := m["kafka"].(map[string]any)
	if kv == nil {
		return
	}
	if arr, ok := kv["brokers"].([]any); ok {
		bs := make([]string, 0, len(arr))
		for _, x := range arr {
			bs = append(bs, fmt.Sprintf("%v", x))
		}
		cfg.KafkaBrokers = strings.Join(bs, ",")
	}
	if cfg.KafkaGroup == "" {
		cfg.KafkaGroup = fmt.Sprintf("%v", kv["group"])
	}
	if cfg.KafkaInTopic == "" {
		cfg.KafkaInTopic = fmt.Sprintf("%v", kv["intercept_detect_chrome_topic"])
	}
	if cfg.KafkaOutTopic == "" {
		cfg.KafkaOutTopic = fmt.Sprintf("%v", kv["intercept_detect_result_topic"])
	}
	log.Printf("[server-lite] kafka loaded from nacos brokers=%s in=%s out=%s group=%s", cfg.KafkaBrokers, cfg.KafkaInTopic, cfg.KafkaOutTopic, cfg.KafkaGroup)
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func buildServerConfigs(rawAddr, scheme string) ([]constant.ServerConfig, error) {
	if strings.TrimSpace(rawAddr) == "" {
		return nil, fmt.Errorf("empty nacos addr")
	}
	if strings.TrimSpace(scheme) == "" {
		scheme = "http"
	}
	parts := strings.Split(rawAddr, ",")
	out := make([]constant.ServerConfig, 0, len(parts))
	for _, p := range parts {
		h, pt := parseAddr(strings.TrimSpace(p))
		if h == "" || pt <= 0 {
			continue
		}
		sc := constant.NewServerConfig(h, uint64(pt), constant.WithScheme(scheme))
		out = append(out, *sc)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("invalid nacos addr list: %s", rawAddr)
	}
	return out, nil
}

func parseAddr(addr string) (string, int) {
	parts := strings.Split(strings.TrimSpace(addr), ",")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "", 0
	}
	hp := strings.Split(strings.TrimSpace(parts[0]), ":")
	if len(hp) != 2 {
		return "", 0
	}
	var port int
	if _, err := fmt.Sscanf(hp[1], "%d", &port); err != nil {
		return "", 0
	}
	return hp[0], port
}

func buildNodeMessage(in TaskCreateRequest, code int, msg, raw string, output DetectOutput) NodeMessage {
	outJSON, _ := json.Marshal(output)
	return NodeMessage{
		EventType: "EVENT_TYPE_EXEC_RESULT",
		MessageID: fmt.Sprintf("lite-%d-%d", time.Now().UnixMilli(), rand.Intn(100000)),
		Timestamp: time.Now().UnixMilli(),
		NodeID:    "",
		TaskMeta:  rawOrNil(in.TaskMeta),
		MsgStatus: "MESSAGE_STATUS_COMPLETE",
		EventData: map[string]any{
			"execResult": ExecResult{
				In:              in,
				ErrorCode:       code,
				ErrorMessage:    msg,
				ErrorRawMessage: raw,
				FinishedAt:      time.Now().UTC().Format(time.RFC3339),
				OutputJSON:      string(outJSON),
			},
		},
	}
}

func rawOrNil(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(v, &out); err != nil {
		return string(v)
	}
	return out
}

func newKafkaClient(brokers []string, user, pass string, consumeTopic string, group string) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("empty brokers")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	}
	if strings.TrimSpace(consumeTopic) != "" {
		opts = append(opts,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(consumeTopic),
		)
	}
	if user != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
	}
	return kgo.NewClient(opts...)
}

func main() {
	cfg := &Config{}
	flag.StringVar(&cfg.Listen, "listen", ":19080", "http listen")
	flag.StringVar(&cfg.NacosAddr, "nacos-addr", envOr("NACOS_ADDR", ""), "nacos host:port[,host:port]")
	flag.StringVar(&cfg.NacosScheme, "nacos-scheme", envOr("NACOS_SCHEME", "http"), "nacos scheme: http|https")
	flag.StringVar(&cfg.NacosUser, "nacos-user", envOr("NACOS_USER", "nacos"), "nacos username")
	flag.StringVar(&cfg.NacosPass, "nacos-pass", envOr("NACOS_PASS", "nacos"), "nacos password")
	flag.StringVar(&cfg.NacosNamespace, "nacos-namespace", envOr("NACOS_NAMESPACE", "observable-dev"), "nacos namespace")
	flag.StringVar(&cfg.NacosGroup, "nacos-group", envOr("NACOS_GROUP", "boce"), "nacos group")
	flag.StringVar(&cfg.NacosDataID, "nacos-dataid", envOr("NACOS_DATA_ID", "intercept-detect"), "nacos dataId")

	flag.StringVar(&cfg.KafkaBrokers, "kafka-brokers", envOr("KAFKA_BROKERS", ""), "comma-separated brokers")
	flag.StringVar(&cfg.KafkaInBrokers, "kafka-in-brokers", envOr("KAFKA_IN_BROKERS", ""), "comma-separated brokers for input topic")
	flag.StringVar(&cfg.KafkaOutBrokers, "kafka-out-brokers", envOr("KAFKA_OUT_BROKERS", ""), "comma-separated brokers for output topic")
	flag.StringVar(&cfg.KafkaHeartbeatBrokers, "kafka-heartbeat-brokers", envOr("KAFKA_HEARTBEAT_BROKERS", ""), "comma-separated brokers for heartbeat topic")
	flag.StringVar(&cfg.KafkaGroup, "kafka-group", envOr("KAFKA_GROUP", "detect-agent-lite"), "kafka group")
	flag.StringVar(&cfg.KafkaInTopic, "kafka-in-topic", envOr("KAFKA_IN_TOPIC", "intercept_detect_mi"), "kafka input topic")
	flag.StringVar(&cfg.KafkaOutTopic, "kafka-out-topic", envOr("KAFKA_OUT_TOPIC", "task-results"), "kafka output topic")
	flag.StringVar(&cfg.KafkaHeartbeatTopic, "kafka-heartbeat-topic", envOr("KAFKA_HEARTBEAT_TOPIC", "intercept_detect_data_report"), "kafka heartbeat topic")
	flag.StringVar(&cfg.KafkaUser, "kafka-user", envOr("KAFKA_USER", ""), "kafka sasl username")
	flag.StringVar(&cfg.KafkaPass, "kafka-pass", envOr("KAFKA_PASS", ""), "kafka sasl password")

	flag.StringVar(&cfg.Serial, "serial", envOr("WAYDROID_SERIAL", ""), "adb serial")
	flag.StringVar(&cfg.Package, "package", envOr("WAYDROID_PACKAGE", "com.mi.globalbrowser"), "browser package")
	flag.IntVar(&cfg.Port, "port", envInt("WAYDROID_PORT", 9222), "local cdp port")
	flag.IntVar(&cfg.MaxTabs, "max-tabs", envInt("WAYDROID_MAX_TABS", 10), "max page tabs")
	flag.IntVar(&cfg.TimeoutS, "timeout-s", envInt("DETECT_TIMEOUT_S", 10), "detect timeout seconds")
	flag.Parse()

	loadKafkaFromNacos(cfg)
	defaultBrokers := parseBrokers(cfg.KafkaBrokers)
	inBrokers := parseBrokers(cfg.KafkaInBrokers)
	outBrokers := parseBrokers(cfg.KafkaOutBrokers)
	hbBrokers := parseBrokers(cfg.KafkaHeartbeatBrokers)
	if len(inBrokers) == 0 {
		inBrokers = defaultBrokers
	}
	if len(outBrokers) == 0 {
		outBrokers = defaultBrokers
	}
	if len(hbBrokers) == 0 {
		hbBrokers = inBrokers
	}

	if len(inBrokers) == 0 || cfg.KafkaInTopic == "" || cfg.KafkaOutTopic == "" {
		log.Fatalf("kafka config missing: inBrokers=%v inTopic=%q outTopic=%q", inBrokers, cfg.KafkaInTopic, cfg.KafkaOutTopic)
	}

	rt := &Runtime{cfg: cfg, busy: map[string]bool{}}

	inClient, err := newKafkaClient(inBrokers, cfg.KafkaUser, cfg.KafkaPass, cfg.KafkaInTopic, cfg.KafkaGroup)
	if err != nil {
		log.Fatalf("new kafka input client failed: %v", err)
	}
	defer inClient.Close()

	outClient, err := newKafkaClient(outBrokers, cfg.KafkaUser, cfg.KafkaPass, "", "")
	if err != nil {
		log.Fatalf("new kafka output client failed: %v", err)
	}
	defer outClient.Close()

	hbClient, err := newKafkaClient(hbBrokers, cfg.KafkaUser, cfg.KafkaPass, "", "")
	if err != nil {
		log.Fatalf("new kafka heartbeat client failed: %v", err)
	}
	defer hbClient.Close()

	go func() {
		for {
			fetches := inClient.PollFetches(context.Background())
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					log.Printf("kafka poll error: %v", e)
				}
				continue
			}
			fetches.EachRecord(func(r *kgo.Record) {
				var in TaskCreateRequest
				if err := json.Unmarshal(r.Value, &in); err != nil {
					log.Printf("invalid task json: %v", err)
					return
				}
				var p InterceptParam
				if err := json.Unmarshal([]byte(in.PayloadJSON), &p); err != nil || strings.TrimSpace(p.URL) == "" {
					out := buildNodeMessage(in, 500, "invalid payloadJson", errString(err), DetectOutput{App: cfg.Package, Status: "FAIL", Error: "invalid payloadJson", RawResult: ""})
					publishResult(outClient, cfg.KafkaOutTopic, out)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
				_ = ctx
				blocked, detail, err := rt.detect(p.URL)
				cancel()
				if err != nil {
					out := buildNodeMessage(in, 500, "detect failed", err.Error(), DetectOutput{App: cfg.Package, Status: "FAIL", Error: err.Error(), RawResult: detail})
					publishResult(outClient, cfg.KafkaOutTopic, out)
					return
				}
				status := "NORMAL"
				if blocked {
					status = "BLOCKED"
				}
				out := buildNodeMessage(in, 200, "", "", DetectOutput{App: cfg.Package, Status: status, Error: "", RawResult: detail})
				publishResult(outClient, cfg.KafkaOutTopic, out)
			})
		}
	}()

	if strings.TrimSpace(cfg.KafkaHeartbeatTopic) != "" && len(hbBrokers) > 0 {
		go func() {
			tk := time.NewTicker(30 * time.Second)
			defer tk.Stop()
			for {
				hb := HeartbeatMessage{
					ReportType: "INTERCEPT_REPORT_TYPE_HEARTBEAT",
					NodeType:   "INTERCEPT_NODE_TYPE_BROWSER_FARM",
					NodeName:   "server-lite",
					PublicIPv4: "",
					Timestamp:  time.Now().UTC().Format(time.RFC3339),
					NodeDetails: []map[string]any{
						{
							"appName": "INTERCEPT_APP_TYPE_CHROME",
							"appNum":  1,
						},
					},
				}
				b, _ := json.Marshal(hb)
				rec := &kgo.Record{Topic: cfg.KafkaHeartbeatTopic, Key: []byte(fmt.Sprint(time.Now().UnixNano())), Value: b}
				hbClient.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
					if err != nil {
						log.Printf("heartbeat produce failed: %v", err)
					}
				})
				<-tk.C
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		err := rt.ensureForward()
		if err != nil {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
			return
		}
		tabs, _ := rt.listTabs()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":               true,
			"serial":           rt.cfg.Serial,
			"socket":           rt.socket,
			"port":             rt.cfg.Port,
			"maxTabs":          rt.cfg.MaxTabs,
			"pageTabs":         countPages(tabs),
			"inTopic":          cfg.KafkaInTopic,
			"outTopic":         cfg.KafkaOutTopic,
			"heartbeatTopic":   cfg.KafkaHeartbeatTopic,
			"inBrokers":        inBrokers,
			"outBrokers":       outBrokers,
			"heartbeatBrokers": hbBrokers,
		})
	})

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second}
	log.Printf("server-lite up listen=%s package=%s serial=%s port=%d maxTabs=%d", cfg.Listen, cfg.Package, cfg.Serial, cfg.Port, cfg.MaxTabs)
	log.Printf("kafka in=%s out=%s hb=%s", cfg.KafkaInTopic, cfg.KafkaOutTopic, cfg.KafkaHeartbeatTopic)
	log.Printf("kafka brokers in=%v out=%v hb=%v", inBrokers, outBrokers, hbBrokers)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func publishResult(cl *kgo.Client, topic string, msg NodeMessage) {
	b, _ := json.Marshal(msg)
	rec := &kgo.Record{Topic: topic, Key: []byte(fmt.Sprint(time.Now().UnixNano())), Value: b}
	cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			log.Printf("kafka produce failed: %v", err)
		}
	})
}

func envOr(k, d string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	return v
}

func envInt(k string, d int) int {
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

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
