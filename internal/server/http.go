package server

import (
	"bytes"
	"detect-agent/internal/service"
	"io"
	"net/http"
	"time"

	"detect-agent/internal/conf"
	"detect-agent/internal/grpc_client"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/middleware/validate"
	kratoshttp "github.com/go-kratos/kratos/v2/transport/http"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	batchv1 "gitlab.gainetics.io/shared/proto-hub/observable/batch/gen/v1"
)

// requestDecoder 创建请求解码器
func requestDecoder() func(r *http.Request, v interface{}) error {
	return func(r *http.Request, v interface{}) error {
		contentType := r.Header.Get("Content-Type")

		// 如果 Content-Type 为空或没有请求体，直接返回 nil
		if contentType == "" || r.ContentLength == 0 {
			return nil
		}

		// 对于 JSON 请求，使用 protojson 解码器
		if contentType == "application/json" || contentType == "application/json; charset=utf-8" {
			if protoMsg, ok := v.(proto.Message); ok {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return err
				}
				// 重新设置请求体
				r.Body = io.NopCloser(bytes.NewReader(body))

				unmarshaler := protojson.UnmarshalOptions{
					DiscardUnknown: true,
				}
				return unmarshaler.Unmarshal(body, protoMsg)
			}
		}

		return nil
	}
}

// NewHTTPServer new an HTTP server.
func NewHTTPServer(
	batchSrv *service.BatchService,
	iamClient grpc_client.Client,
	logger log.Logger,
) *kratoshttp.Server {

	// 设置日志记录器供EncoderError使用
	SetLogger(logger)

	// 获取Nacos配置
	data := conf.GetData()
	if data == nil {
		panic("nacos data is nil")
	}

	var opts = []kratoshttp.ServerOption{
		kratoshttp.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(),
		),

		// 请求解码器：处理 JSON 请求，支持 camelCase 和 snake_case 两种格式
		kratoshttp.RequestDecoder(requestDecoder()),

		// 统一错误处理
		kratoshttp.ErrorEncoder(EncoderError()),

		// 统一返回
		kratoshttp.ResponseEncoder(EncoderResponse()),
	}

	if data.Server.HTTP.Network != "" {
		opts = append(opts, kratoshttp.Network(data.Server.HTTP.Network))
	}
	if data.Server.Grpc.Addr != "" {
		opts = append(opts, kratoshttp.Address(data.Server.HTTP.Addr))
	}
	if data.Server.Grpc.Timeout > 0 {
		opts = append(opts, kratoshttp.Timeout(time.Duration(data.Server.HTTP.Timeout)*time.Second))
	}

	srv := kratoshttp.NewServer(opts...)
	batchv1.RegisterBatchServiceHTTPServer(srv, batchSrv)
	return srv
}
