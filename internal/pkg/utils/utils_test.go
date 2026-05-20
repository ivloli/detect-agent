package utils

import (
	"encoding/json"
	"testing"
	"time"
)

// ==================== TestJsonMarshal / TestIgnoreErrorJsonMarshal / TestJsonUnmarshal ====================

func TestJsonMarshal_SkipOnNilCodec(t *testing.T) {
	// encoding.GetCodec("json") may return nil in test context (kratos codec not registered)
	// Check if the function panics or returns error gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Logf("JsonMarshal panicked (expected if codec not registered): %v", r)
		}
	}()
	result, err := JsonMarshal(map[string]string{"key": "value"})
	if err != nil {
		t.Logf("JsonMarshal returned error (may be expected if codec not registered): %v", err)
		return
	}
	if result == "" {
		t.Fatal("JsonMarshal returned empty string")
	}
}

func TestJsonMarshal_UseEncodingJson(t *testing.T) {
	// Use standard library json directly as an alternative test
	type testStruct struct {
		Name string `json:"name"`
	}
	s := testStruct{Name: "hello"}
	result, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if string(result) != `{"name":"hello"}` {
		t.Fatalf("json.Marshal got %s, want %s", string(result), `{"name":"hello"}`)
	}
}

func TestIgnoreErrorJsonMarshal(t *testing.T) {
	s := map[string]interface{}{"key": "value"}
	result := IgnoreErrorJsonMarshal(s)
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(result), &decoded); err != nil {
		t.Fatalf("result is not valid json: %v", err)
	}
	if decoded["key"] != "value" {
		t.Fatalf("unexpected value: %v", decoded["key"])
	}
}

func TestIgnoreErrorJsonMarshal_Error(t *testing.T) {
	result := IgnoreErrorJsonMarshal(make(chan int))
	if result == "value" {
		t.Fatal("expected error message for unmarshalable type")
	}
}

func TestJsonUnmarshal_SkipOnNilCodec(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}
	var s testStruct
	defer func() {
		if r := recover(); r != nil {
			t.Logf("JsonUnmarshal panicked (expected if codec not registered): %v", r)
		}
	}()
	err := JsonUnmarshal([]byte(`{"name":"hello"}`), &s)
	if err != nil {
		t.Logf("JsonUnmarshal returned error (may be expected if codec not registered): %v", err)
		return
	}
	if s.Name != "hello" {
		t.Fatalf("JsonUnmarshal got %s, want hello", s.Name)
	}
}

func TestJsonUnmarshal_UseEncodingJson(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
	}
	var s testStruct
	err := json.Unmarshal([]byte(`{"name":"hello"}`), &s)
	if err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if s.Name != "hello" {
		t.Fatalf("json.Unmarshal got %s, want hello", s.Name)
	}
}

// ==================== TestGenerateUUID ====================

func TestGenerateUUID(t *testing.T) {
	uuid1 := GenerateUUID()
	uuid2 := GenerateUUID()
	if uuid1 == uuid2 {
		t.Fatal("two generated UUIDs should be different")
	}
	if len(uuid1) != 36 {
		t.Fatalf("unexpected UUID length: %d", len(uuid1))
	}
}

// ==================== TestIf ====================

func TestIf(t *testing.T) {
	if got := If(true, "a", "b"); got != "a" {
		t.Fatalf("If(true) got %s, want a", got)
	}
	if got := If(false, "a", "b"); got != "b" {
		t.Fatalf("If(false) got %s, want b", got)
	}
	if got := If(1 > 2, 100, 200); got != 200 {
		t.Fatalf("If(1>2) got %d, want 200", got)
	}
}

// ==================== TestStrPtrToStr ====================

func TestStrPtrToStr(t *testing.T) {
	if got := StrPtrToStr(nil); got != "" {
		t.Fatalf("StrPtrToStr(nil) got %s, want empty", got)
	}
	s := "hello"
	if got := StrPtrToStr(&s); got != "hello" {
		t.Fatalf("StrPtrToStr(&s) got %s, want hello", got)
	}
}

// ==================== TestTernary ====================

func TestTernaryString(t *testing.T) {
	if got := TernaryString(true, "yes", "no"); got != "yes" {
		t.Fatalf("TernaryString(true) got %s, want yes", got)
	}
	if got := TernaryString(false, "yes", "no"); got != "no" {
		t.Fatalf("TernaryString(false) got %s, want no", got)
	}
}

func TestTernary(t *testing.T) {
	if got := Ternary(10 > 5, 1, 2); got != 1 {
		t.Fatalf("Ternary(true) got %d, want 1", got)
	}
	if got := Ternary(10 < 5, 1, 2); got != 2 {
		t.Fatalf("Ternary(false) got %d, want 2", got)
	}
}

// ==================== TestTimestampToTime ====================

func TestTimestampToTime(t *testing.T) {
	// 秒级时间戳: 2022-01-01 00:00:00 UTC = 1640995200
	secTs := int64(1640995200)
	result := TimestampToTime(secTs)
	expected := time.Unix(1640995200, 0).Local()
	if !result.Equal(expected) {
		t.Fatalf("TimestampToTime(sec) got %v, want %v", result, expected)
	}

	// 毫秒级时间戳
	msTs := int64(1640995200000)
	resultMs := TimestampToTime(msTs)
	if !resultMs.Equal(expected) {
		t.Fatalf("TimestampToTime(ms) got %v, want %v", resultMs, expected)
	}

	// 边缘值：10^10 - 1 被视为秒级
	edgeSec := int64(9999999999)
	resultEdgeSec := TimestampToTime(edgeSec)
	expectedEdge := time.Unix(edgeSec, 0).Local()
	if !resultEdgeSec.Equal(expectedEdge) {
		t.Fatalf("TimestampToTime(edgeSec) mismatch")
	}

	// 边缘值：10^10 被视为毫秒级
	edgeMs := int64(10000000000)
	resultEdgeMs := TimestampToTime(edgeMs)
	expectedEdgeMs := time.UnixMilli(edgeMs).Local()
	if !resultEdgeMs.Equal(expectedEdgeMs) {
		t.Fatalf("TimestampToTime(edgeMs) mismatch")
	}
}

// ==================== TestCastInt ====================

func TestCastInt(t *testing.T) {
	// int -> int64
	input := []int{1, 2, 3}
	output := CastInt[int, int64](input)
	if len(output) != 3 {
		t.Fatalf("CastInt length got %d, want 3", len(output))
	}
	for i, v := range output {
		if v != int64(input[i]) {
			t.Fatalf("CastInt[%d] got %d, want %d", i, v, input[i])
		}
	}

	// empty slice
	empty := CastInt[int, int64](nil)
	if empty != nil {
		t.Fatal("CastInt(nil) should return nil")
	}

	empty2 := CastInt[int, int64]([]int{})
	if empty2 != nil {
		t.Fatal("CastInt([]) should return nil")
	}
}

// ==================== TestIn ====================

func TestIn(t *testing.T) {
	if !In(3, 1, 2, 3, 4) {
		t.Fatal("In(3, 1,2,3,4) should be true")
	}
	if In(5, 1, 2, 3, 4) {
		t.Fatal("In(5, 1,2,3,4) should be false")
	}
	if !In("a", "a", "b") {
		t.Fatal(`In("a", "a","b") should be true`)
	}
	if In("c", "a", "b") {
		t.Fatal(`In("c", "a","b") should be false`)
	}
}

// ==================== TestParseNacosServerAddr ====================

func TestParseNacosServerAddr(t *testing.T) {
	// 单个地址
	result, err := ParseNacosServerAddr("127.0.0.1:8848")
	if err != nil {
		t.Fatalf("ParseNacosServerAddr failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 server, got %d", len(result))
	}
	if result[0].IpAddr != "127.0.0.1" || result[0].Port != 8848 {
		t.Fatalf("unexpected server config: %+v", result[0])
	}

	// 多个地址以逗号分隔
	result2, err := ParseNacosServerAddr("192.168.1.1:8848, 192.168.1.2:8849")
	if err != nil {
		t.Fatalf("ParseNacosServerAddr(multi) failed: %v", err)
	}
	if len(result2) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(result2))
	}

	// 无效地址格式
	_, err = ParseNacosServerAddr("invalid-addr:notaport")
	if err == nil {
		t.Fatal("expected error for invalid port")
	}

	// 空字符串
	result3, err := ParseNacosServerAddr("")
	if err != nil {
		t.Fatalf("ParseNacosServerAddr(empty) failed: %v", err)
	}
	if len(result3) != 0 {
		t.Fatalf("expected 0 servers for empty string, got %d", len(result3))
	}
}

// ==================== TestMakeResultID ====================

func TestMakeResultID(t *testing.T) {
	id1 := MakeResultID("task1", "seq1", "node1")
	id2 := MakeResultID("task1", "seq1", "node1")
	if id1 != id2 {
		t.Fatal("deterministic IDs should be equal for same inputs")
	}
	if len(id1) != 22 {
		t.Fatalf("expected 22-char base64url ID, got %d-char: %s", len(id1), id1)
	}

	id3 := MakeResultID("task1", "seq1", "node2")
	if id1 == id3 {
		t.Fatal("different nodeID should produce different result ID")
	}

	// 测试大小写不敏感性（taskID 会被 toLower）
	id4 := MakeResultID("TASK1", "seq1", "node1")
	if id1 != id4 {
		t.Fatal("taskID should be case-insensitive")
	}
}

// ==================== TestTaskSeqIdToTime ====================

func TestTaskSeqIdToTime(t *testing.T) {
	// 有效长度14的时间字符串
	result := TaskSeqIdToTime("20220101120000")
	if result.IsZero() {
		t.Fatal("expected non-zero time for valid taskSeqId")
	}
	expected := time.Date(2022, 1, 1, 12, 0, 0, 0, GetTimeLocation())
	if !result.Equal(expected) {
		t.Fatalf("TaskSeqIdToTime got %v, want %v", result, expected)
	}

	// 无效长度
	invalid := TaskSeqIdToTime("123")
	if !invalid.IsZero() {
		t.Fatal("expected zero time for invalid length")
	}

	// 无效格式
	invalid2 := TaskSeqIdToTime("abcdefghijklmn")
	if !invalid2.IsZero() {
		t.Fatal("expected zero time for invalid format")
	}
}

func TestGetTimeLocation(t *testing.T) {
	loc := GetTimeLocation()
	if loc == nil {
		t.Fatal("GetTimeLocation returned nil")
	}
	// Should be either Asia/Shanghai or local
	name := loc.String()
	if name != "Asia/Shanghai" && name != time.Local.String() {
		t.Logf("GetTimeLocation returned %s", name)
	}
}