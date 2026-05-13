package grpc_client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"detect-agent/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	"google.golang.org/grpc"

	iamv1 "gitlab.gainetics.io/shared/proto-hub/cloud-iam/gen/v1"
)

const (
	defaultDialTimeout    = 3 * time.Second
	defaultRequestTimeout = 2 * time.Second
)

// Client defines the IAM RPC client contract.
type Client interface {
	// TokenParse calls cloud-iam's TokenParse RPC with the provided token.
	TokenParse(ctx context.Context, token string) (*iamv1.TokenParseReply, error)
	// UserInfo fetches IAM user basic information by user IDs.
	UserInfo(ctx context.Context, ids []int64, idType iamv1.UserIdType) ([]*iamv1.UserInfoReplyInfo, error)
}

type client struct {
	rpc           iamv1.AccountRPCServiceClient
	conn          *grpc.ClientConn
	helper        *log.Helper
	requestTimout time.Duration
}

// NewClient constructs a cloud-iam gRPC client based on the runtime configuration.
func NewClient(discovery registry.Discovery, logger log.Logger) (Client, func(), error) {
	cfg, err := loadIamConfig()
	if err != nil {
		return nil, nil, err
	}

	requestTimeout := parseTimeout(cfg.Timeout, defaultRequestTimeout)
	dialTimeout := parseTimeout(cfg.Timeout, defaultDialTimeout)

	opts := []kgrpc.ClientOption{
		kgrpc.WithEndpoint(cfg.Endpoint),
		kgrpc.WithTimeout(dialTimeout),
	}
	if discovery != nil {
		opts = append(opts, kgrpc.WithDiscovery(discovery))
	}

	conn, err := kgrpc.DialInsecure(context.Background(), opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial IAM gRPC server: %w", err)
	}

	helper := log.NewHelper(log.With(logger, "module", "client/iam"))
	helper.Infof("IAM gRPC client initialized, endpoint=%s, dialTimeout=%s, requestTimeout=%s",
		cfg.Endpoint, dialTimeout.String(), requestTimeout.String())

	c := &client{
		rpc:           iamv1.NewAccountRPCServiceClient(conn),
		conn:          conn,
		helper:        helper,
		requestTimout: requestTimeout,
	}

	cleanup := func() {
		if err := conn.Close(); err != nil {
			helper.Errorf("failed to close IAM gRPC conn: %v", err)
		} else {
			helper.Info("closed IAM gRPC client connection")
		}
	}

	return c, cleanup, nil
}

func (c *client) TokenParse(ctx context.Context, token string) (*iamv1.TokenParseReply, error) {
	if token == "" {
		return nil, errors.New("token is empty")
	}

	callCtx := ctx
	if c.requestTimout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, c.requestTimout)
		defer cancel()
	}

	reply, err := c.rpc.TokenParse(callCtx, &iamv1.TokenParseRequest{Token: token})
	if err != nil {
		c.helper.Errorf("TokenParse failed: %v", err)
		return nil, err
	}
	return reply, nil
}

func (c *client) UserInfo(ctx context.Context, ids []int64, idType iamv1.UserIdType) ([]*iamv1.UserInfoReplyInfo, error) {
	if len(ids) == 0 {
		return nil, errors.New("user ids is empty")
	}

	callCtx := ctx
	if c.requestTimout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, c.requestTimout)
		defer cancel()
	}

	req := &iamv1.UserInfoRequest{
		Ids:  ids,
		Type: idType,
	}

	reply, err := c.rpc.UserInfo(callCtx, req)
	if err != nil {
		c.helper.Errorf("UserInfo failed, ids=%v type=%s err=%v", ids, idType.String(), err)
		return nil, err
	}

	return reply.GetInfos(), nil
}

func loadIamConfig() (*conf.GrpcClientConfig, error) {
	data := conf.GetData()
	if data == nil {
		return nil, errors.New("nacos config data is nil")
	}

	if data.GrpcClients == nil || data.GrpcClients.IamService == nil {
		return nil, errors.New("grpc_clients.iam_service config is missing")
	}

	cfg := data.GrpcClients.IamService
	if cfg.Endpoint == "" {
		return nil, errors.New("grpc_clients.iam_service.endpoint is empty")
	}

	return cfg, nil
}

func parseTimeout(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return d
	}
	return fallback
}
