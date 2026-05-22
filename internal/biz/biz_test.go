package biz

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
	ctrlplanev1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/control-plane/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ==================== Test buildResult ====================

func TestBuildResult(t *testing.T) {
	req := &ctrlplanev1.TaskCreateRequest{
		TaskMeta: &ctrlplanev1.TaskMeta{
			TaskId: "task-001",
			Tag:    "test-tag",
		},
		TimeoutSec:  30,
		Type:        ctrlplanev1.BizCommandType(0), // use valid default
		PayloadJson: `{"url":"https://example.com","app":"CHROME"}`,
	}

	result := buildResult(req, CodeSuccess, "", "", `{"status":"normal"}`, probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME)
	if result == nil {
		t.Fatal("buildResult returned nil")
	}
	if result.EventType != ctrlplanev1.EventType_EVENT_TYPE_EXEC_RESULT {
		t.Fatalf("unexpected event type: %v", result.EventType)
	}
	if result.MsgStatus != ctrlplanev1.MessageStatus_MESSAGE_STATUS_COMPLETE {
		t.Fatalf("unexpected message status: %v", result.MsgStatus)
	}
	if result.TaskMeta.GetTaskId() != "task-001" {
		t.Fatalf("unexpected taskId: %s", result.TaskMeta.GetTaskId())
	}
	if result.MessageId == "" {
		t.Fatal("messageId should not be empty")
	}
	if result.Timestamp == 0 {
		t.Fatal("timestamp should not be zero")
	}

	// Verify ExecResult payload
	execResult, ok := result.EventData.(*ctrlplanev1.NodeMessage_ExecResult)
	if !ok {
		t.Fatal("EventData should be ExecResult")
	}
	if execResult.ExecResult.ErrorCode != CodeSuccess {
		t.Fatalf("unexpected error code: %d", execResult.ExecResult.ErrorCode)
	}
	if execResult.ExecResult.In.PayloadJson != req.PayloadJson {
		t.Fatalf("payload mismatch")
	}
}

func TestBuildResult_WithError(t *testing.T) {
	req := &ctrlplanev1.TaskCreateRequest{
		TaskMeta: &ctrlplanev1.TaskMeta{
			TaskId: "task-002",
		},
		TimeoutSec:  10,
		Type:        ctrlplanev1.BizCommandType(0),
		PayloadJson: `{"url":"https://bad.com"}`,
	}

	result := buildResult(req, CodeError, "browser unavailable", "connection refused", `{}`, probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME)
	if result == nil {
		t.Fatal("buildResult returned nil")
	}
	execResult := result.GetExecResult()
	if execResult == nil {
		t.Fatal("expected ExecResult")
	}
	if execResult.ErrorCode != CodeError {
		t.Fatalf("expected code %d, got %d", CodeError, execResult.ErrorCode)
	}
	if execResult.ErrorMessage != "browser unavailable" {
		t.Fatalf("expected error message 'browser unavailable', got '%s'", execResult.ErrorMessage)
	}
	if execResult.ErrorRawMessage != "connection refused" {
		t.Fatalf("expected raw message 'connection refused', got '%s'", execResult.ErrorRawMessage)
	}
}

// ==================== Test Browser Struct ====================

func TestNewNetworkEvent(t *testing.T) {
	ev := NetworkEvent{
		Timestamp: time.Now(),
		Type:      "Request",
		URL:       "https://example.com",
		Method:    "GET",
		Status:    200,
		ErrorText: "",
		Blocked:   false,
	}
	if ev.Type != "Request" || ev.URL != "https://example.com" {
		t.Fatal("NetworkEvent fields not set correctly")
	}

	// Test JSON serialization
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("json.Marshal NetworkEvent failed: %v", err)
	}
	var decoded NetworkEvent
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal NetworkEvent failed: %v", err)
	}
	if decoded.URL != ev.URL {
		t.Fatalf("JSON roundtrip failed: got %s, want %s", decoded.URL, ev.URL)
	}
	if decoded.Blocked != ev.Blocked {
		t.Fatalf("JSON roundtrip blocked field failed")
	}
}

func TestNetworkEvent_Blocked(t *testing.T) {
	ev := NetworkEvent{
		Timestamp: time.Now(),
		Type:      "LoadingFailed",
		ErrorText: "ERR_BLOCKED_BY_CLIENT",
		Blocked:   true,
	}
	if !ev.Blocked {
		t.Fatal("Blocked should be true")
	}
	if ev.ErrorText != "ERR_BLOCKED_BY_CLIENT" {
		t.Fatalf("unexpected error text: %s", ev.ErrorText)
	}
}

// ==================== Test InterceptNodeInfo (proto) ====================

func TestInterceptNodeInfo_Serde(t *testing.T) {
	msg := &ctrlplanev1.InterceptNodeInfo{
		ReportType: ctrlplanev1.InterceptReportType_INTERCEPT_REPORT_TYPE_REGISTER,
		NodeType:   ctrlplanev1.InterceptNodeType_INTERCEPT_NODE_TYPE_BROWSER_FARM,
		NodeName:   "test-node",
		Timestamp:  timestamppb.New(time.Now()),
		NodeDetails: []*ctrlplanev1.InterceptNodeDetail{
			{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME, AppNum: 3},
			{AppName: probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE, AppNum: 3},
		},
	}

	// JSON serialize/deserialize
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal InterceptNodeInfo failed: %v", err)
	}

	var decoded ctrlplanev1.InterceptNodeInfo
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal InterceptNodeInfo failed: %v", err)
	}

	if decoded.ReportType != msg.ReportType {
		t.Fatalf("ReportType mismatch: got %v, want %v", decoded.ReportType, msg.ReportType)
	}
	if decoded.NodeName != msg.NodeName {
		t.Fatalf("NodeName mismatch: got %s, want %s", decoded.NodeName, msg.NodeName)
	}
	if len(decoded.NodeDetails) != 2 {
		t.Fatalf("expected 2 NodeDetails, got %d", len(decoded.NodeDetails))
	}
}

// ==================== Test Biz Constants ====================

func TestBizConstants(t *testing.T) {
	if CodeSuccess != 200 {
		t.Fatalf("CodeSuccess should be 200, got %d", CodeSuccess)
	}
	if CodeError != 500 {
		t.Fatalf("CodeError should be 500, got %d", CodeError)
	}
	if BrowserMaxDetectNum != 100000 {
		t.Fatalf("BrowserMaxDetectNum should be 100000, got %d", BrowserMaxDetectNum)
	}
	if BlockDetectTimeout != 400*time.Millisecond {
		t.Fatalf("BlockDetectTimeout should be 400ms, got %v", BlockDetectTimeout)
	}
	if BlockErrKeyword != "ERR_BLOCKED" {
		t.Fatalf("BlockErrKeyword should be ERR_BLOCKED, got %s", BlockErrKeyword)
	}
}

// ==================== Test TaskCreateRequest Serde ====================

func TestTaskCreateRequestSerde(t *testing.T) {
	// Simulate the deserialization process in ChromiumBatchHandler.HandleBatch
	msg := &ctrlplanev1.TaskCreateRequest{
		TaskMeta: &ctrlplanev1.TaskMeta{
			TaskId: "test-task",
			Tag:    "test-tag",
		},
		TimeoutSec:  30,
		Type:        ctrlplanev1.BizCommandType(0),
		PayloadJson: `{"url":"https://example.com","app":"CHROME"}`,
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal TaskCreateRequest failed: %v", err)
	}

	var decoded ctrlplanev1.TaskCreateRequest
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal TaskCreateRequest failed: %v", err)
	}

	if decoded.GetTaskMeta().GetTaskId() != "test-task" {
		t.Fatalf("TaskId mismatch")
	}
	if decoded.GetTimeoutSec() != 30 {
		t.Fatalf("TimeoutSec mismatch")
	}
}

// ==================== Test InterceptAppType constants ====================

func TestInterceptAppTypeConstants(t *testing.T) {
	// Verify we can reference the constants without panics
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_360
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_UC
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QUARK
	_ = probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ

	t.Log("All InterceptAppType constants accessible")
}

// ==================== Test Context With Timeout ====================

func TestContextWithTimeout(t *testing.T) {
	// Simulate the context creation in HandleBatch
	ctx := context.Background()
	timeoutSec := int64(30)
	newCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	deadline, ok := newCtx.Deadline()
	if !ok {
		t.Fatal("expected deadline to be set")
	}
	if deadline.Before(time.Now()) {
		t.Fatal("deadline should be in the future")
	}
	t.Logf("Context deadline set: %v", deadline)
}

// ==================== Test Wire ProviderSet ====================

func TestProviderSet(t *testing.T) {
	// ProviderSet should not be nil when using Wire
	// This is a compile-time check: if ProviderSet is properly defined, Wire can use it
	if false {
		_ = ProviderSet
	}
	t.Log("ProviderSet is properly defined for Wire injection")
}
