package server

import (
	"time"

	"detect-agent/internal/conf"
	"detect-agent/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	batchv1 "gitlab.gainetics.io/shared/proto-hub/observable/batch/gen/v1"
)

// NewGRPCServer new a gRPC server.
func NewGRPCServer(batchSrv *service.BatchService, logger log.Logger) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			tracing.Server(),
		),
	}

	data := conf.GetData()
	if data == nil {
		panic("nacos data is nil")
	}
	if data.Server.Grpc.Network != "" {
		opts = append(opts, grpc.Network(data.Server.Grpc.Network))
	}
	if data.Server.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(data.Server.Grpc.Addr))
	}
	if data.Server.Grpc.Timeout > 0 {
		opts = append(opts, grpc.Timeout(time.Duration(data.Server.Grpc.Timeout)*time.Second))
	}
	srv := grpc.NewServer(opts...)
	batchv1.RegisterBatchServiceServer(srv, batchSrv)
	return srv
}
