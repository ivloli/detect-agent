package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/google/uuid"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"golang.org/x/exp/constraints"
)

func JsonMarshal(t any) (string, error) {
	codec := encoding.GetCodec("json")
	bytes, err := codec.Marshal(t)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func IgnoreErrorJsonMarshal(t any) string {
	bytes, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("marshal err: %v", err)
	}
	return string(bytes)
}

func JsonUnmarshal(b []byte, v any) error {
	codec := encoding.GetCodec("json")
	return codec.Unmarshal(b, v)
}

func GenerateUUID() string {
	return uuid.New().String()
}

func If[T any](b bool, v1 T, v2 T) T {
	if b {
		return v1
	}

	return v2
}

func StrPtrToStr(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

// TernaryString 三元表达式
func TernaryString(condition bool, ifTrue string, ifFalse string) string {
	if condition {
		return ifTrue
	}
	return ifFalse
}

// Ternary 三元表达式
func Ternary[T any](condition bool, ifTrue T, ifFalse T) T {
	if condition {
		return ifTrue
	}
	return ifFalse
}

// TimestampToTime 将时间戳转换为 time.Time
// 支持秒级和毫秒级时间戳的自动识别和转换
// 返回本地时区的时间对象
//
// 参数:
//   - timestamp: 时间戳，int64类型
//   - < 10^10 (100亿): 认为是秒级时间戳
//   - >= 10^10: 认为是毫秒级时间戳
//
// 返回:
//   - time.Time: 转换后的本地时间对象
//
// 示例:
//
//	t1 := TimestampToTime(1640995200)    // 秒级时间戳: 2022-01-01 00:00:00 Local
//	t2 := TimestampToTime(1640995200000) // 毫秒级时间戳: 2022-01-01 00:00:00 Local
func TimestampToTime(timestamp int64) time.Time {
	const threshold = 10000000000 // 10^10

	if timestamp < threshold {
		// 秒级时间戳，使用 Unix() 并转换为本地时间
		return time.Unix(timestamp, 0).Local()
	} else {
		// 毫秒级时间戳，使用 UnixMilli() 并转换为本地时间
		return time.UnixMilli(timestamp).Local()
	}
}

func CastInt[From, To constraints.Integer](f []From) []To {
	if len(f) == 0 {
		return nil
	}

	t := make([]To, 0, len(f))
	for _, v := range f {
		t = append(t, To(v))
	}

	return t
}

func In[T comparable](a T, vs ...T) bool {
	for _, v := range vs {
		if a == v {
			return true
		}
	}

	return false
}

func ParseNacosServerAddr(addr string) ([]constant.ServerConfig, error) {
	var sc []constant.ServerConfig
	for _, v := range strings.Split(addr, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.Contains(v, ":") {
			host, portStr, err := net.SplitHostPort(v)
			if err != nil {
				return nil, fmt.Errorf("invalid address: %s", v)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				return nil, fmt.Errorf("invalid port: %s", portStr)
			}
			sc = append(sc, *constant.NewServerConfig(host, uint64(port)))
		} else {
			sc = append(sc, *constant.NewServerConfig(addr, 0))
		}
	}

	return sc, nil
}

// MakeResultID generates a deterministic 22-char base64url id from (taskID, taskSeqID, nodeID)
// using SHA-256 -> truncate 128-bit -> base64url(no padding).
//
// Properties:
// - Deterministic: same triple => same result_id
// - Very low collision probability (128-bit output)
// - URL-safe and short (22 chars)
func MakeResultID(taskID, taskSeqID, nodeID string) string {
	// Canonicalize to avoid "same meaning, different bytes" producing different IDs.
	// Adjust rules based on your domain semantics (see notes below).
	t := strings.ToLower(strings.TrimSpace(taskID))
	s := strings.TrimSpace(taskSeqID)
	n := strings.TrimSpace(nodeID)

	// Use an unambiguous separator that is extremely unlikely to appear in IDs.
	// Avoid just concatenation to prevent ambiguity: ("ab","c") vs ("a","bc").
	const sep = '\x1f' // Unit Separator

	// Build input bytes without extra allocations
	// (small, but nice for high QPS).
	totalLen := len(t) + 1 + len(s) + 1 + len(n)
	buf := make([]byte, 0, totalLen)
	buf = append(buf, t...)
	buf = append(buf, sep)
	buf = append(buf, s...)
	buf = append(buf, sep)
	buf = append(buf, n...)

	sum := sha256.Sum256(buf)
	// Truncate to 128-bit
	idBytes := sum[:16]

	// base64url without padding gives 22 chars for 16 bytes.
	return base64.RawURLEncoding.EncodeToString(idBytes)
}
