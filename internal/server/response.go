package server

import (
	"encoding/json"
	"fmt"
	"mime"
	httpNet "net/http"
	"path/filepath"
	"regexp"
	"strings"

	iamerror "detect-agent/internal/errors"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/http"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var logger log.Logger

// SetLogger 设置日志记录器
func SetLogger(l log.Logger) {
	logger = l
}

type BaseResponse struct {
	Code    int32       `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type ErrMsg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

func EncoderResponse() http.EncodeResponseFunc {
	return func(w http.ResponseWriter, r *http.Request, v interface{}) error {
		// 特殊处理 HttpBody 类型（用于文件下载等非JSON响应）
		if httpBody, ok := v.(*httpbody.HttpBody); ok {
			// 设置 Content-Type
			contentType := httpBody.GetContentType()
			if contentType == "" {
				// 如果没有指定 Content-Type，默认使用 application/octet-stream
				contentType = "application/octet-stream"
			}
			w.Header().Set("Content-Type", contentType)

			// 对于 CSV 文件，设置 Content-Disposition 头，提示浏览器下载
			if isCSVContent(contentType) {
				filename := resolveCSVFilename(contentType, r)
				w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
			}

			// 直接写入文件内容
			_, err := w.Write(httpBody.GetData())
			return err
		}

		resp := &BaseResponse{
			Code:    httpNet.StatusOK,
			Message: "success",
			Data:    v,
		}

		// 序列化响应数据
		var data []byte
		var err error

		// 全局使用 protojson，输出所有零值字段（包括空数组）
		if protoMsg, ok := v.(proto.Message); ok {
			marshaler := protojson.MarshalOptions{
				EmitUnpopulated: true, // 输出零值字段（空数组、0、false等）
				UseProtoNames:   true, // 使用 proto 字段名
			}

			protoData, err := marshaler.Marshal(protoMsg)
			if err != nil {
				return err
			}

			var protoMap map[string]interface{}
			if err := json.Unmarshal(protoData, &protoMap); err != nil {
				return err
			}

			resp.Data = protoMap
			data, err = json.Marshal(resp)
			if err != nil {
				return err
			}
		} else {
			// 非 proto 消息使用默认序列化
			codec := encoding.GetCodec("json")
			data, err = codec.Marshal(resp)
			if err != nil {
				return err
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(data)
		if err != nil {
			return err
		}
		return nil
	}
}

// isCSVContent 判断 content-type 是否为 csv（包含 charset 等附加参数也能识别）
func isCSVContent(contentType string) bool {
	if contentType == "" {
		return false
	}
	return strings.HasPrefix(contentType, "text/csv") || strings.HasPrefix(contentType, "application/csv")
}

// resolveCSVFilename 从 content-type 或 query 参数中解析导出的 csv 文件名，默认返回 export.csv
func resolveCSVFilename(contentType string, r *http.Request) string {
	const defaultName = "icp_records.csv"

	// 1) 尝试从 content-type 参数中解析（支持 filename / name）
	if mediaType, params, err := mime.ParseMediaType(contentType); err == nil {
		if isCSVContent(mediaType) {
			if name := params["filename"]; name != "" {
				if cleaned := sanitizeFilename(name); cleaned != "" {
					return cleaned
				}
			}
			if name := params["name"]; name != "" {
				if cleaned := sanitizeFilename(name); cleaned != "" {
					return cleaned
				}
			}
		}
	}

	// 2) 尝试从 query 参数读取
	if r != nil {
		if name := r.URL.Query().Get("filename"); name != "" {
			if cleaned := sanitizeFilename(name); cleaned != "" {
				return cleaned
			}
		}
		if name := r.URL.Query().Get("file_name"); name != "" {
			if cleaned := sanitizeFilename(name); cleaned != "" {
				return cleaned
			}
		}
	}

	// 3) 默认
	return defaultName
}

// sanitizeFilename 清理潜在的非法字符并保证后缀为 .csv
func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Trim(name, `"'`)
	if name == "" {
		return ""
	}

	// 仅保留文件名部分，去掉路径
	name = filepath.Base(name)

	// 替换常见非法字符
	illegalChars := regexp.MustCompile(`[<>:"/\\|?*\r\n]`)
	name = illegalChars.ReplaceAllString(name, "_")

	// 限制长度，避免过长
	const maxLen = 150
	if len(name) > maxLen {
		name = name[:maxLen]
	}

	// 确保后缀
	if !strings.HasSuffix(strings.ToLower(name), ".csv") {
		name += ".csv"
	}

	return name
}

// ValidationError 验证错误结构体
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func EncoderError() http.EncodeErrorFunc {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		var em errors.Error
		marshal, e := json.Marshal(err)
		if e != nil {
			em.Message = err.Error()
			em.Reason = "system marshal err"
			em.Code = iamerror.CodeServerError // 使用自定义服务错误码
			goto WRITE
		}

		e = json.Unmarshal(marshal, &em)
		if e != nil {
			// Unmarshal 失败，说明错误类型不是 Kratos errors.Error
			em.Message = err.Error()
			em.Reason = "error type mismatch"
			em.Code = iamerror.CodeServerError
			goto WRITE
		}

		// 检查是否成功解析到了有意义的信息
		// 如果 em.Reason 和 em.Message 都是空的，说明 unmarshal 结果是空的 struct
		if em.Reason == "" && em.Message == "" {
			em.Message = err.Error()
			em.Reason = "empty error"
			em.Code = iamerror.CodeServerError
		}

		// 记录详细错误到日志中
		if logger != nil {
			logger.Log(log.LevelError,
				"reason", em.Reason,
				"message", em.Message,
				"code", em.Code,
				"metadata", em.Metadata,
				"path", r.URL.Path,
				"method", r.Method,
				"error", fmt.Sprintf("%+v", err), // 记录原始错误
			)
		}

		// 处理验证器错误
		if em.Reason == "VALIDATOR" || strings.Contains(em.Reason, "VALIDATOR") {
			reply := &BaseResponse{
				Code:    iamerror.CodeInvalidParams, // 使用参数无效的自定义错误码
				Message: "请求参数无效",                   // message 对应处理后的 message
				Data:    em.Reason,                  // data 对应 kratos 的 reason
			}
			codec := encoding.GetCodec("json")
			data, _ := codec.Marshal(reply)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(httpNet.StatusOK)
			_, _ = w.Write(data)
			return
		}

	WRITE:
		// 获取自定义错误码
		errorCode := em.GetCode()
		if errorCode <= 0 {
			errorCode = iamerror.CodeServerError // 默认使用服务错误码
		}

		reply := &BaseResponse{
			Code:    errorCode,  // 使用自定义错误码
			Message: em.Message, // message 对应 kratos 的 message
			Data:    em.Reason,  // data 对应 kratos 的 reason
		}
		codec := encoding.GetCodec("json")
		data, _ := codec.Marshal(reply)
		w.Header().Set("Content-Type", "application/json")

		// 不设置HTTP状态码，默认200
		_, _ = w.Write(data)
	}
}
