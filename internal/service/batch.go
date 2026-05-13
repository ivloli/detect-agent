package service

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	commonv1 "gitlab.gainetics.io/shared/proto-hub/common/gen/v1"
	pb "gitlab.gainetics.io/shared/proto-hub/observable/batch/gen/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type BatchService struct {
	pb.UnimplementedBatchServiceServer
	helper *log.Helper
}

func NewBatchService(
	logger log.Logger,
) *BatchService {
	return &BatchService{
		helper: log.NewHelper(log.With(logger, "module", "batch_service/")),
	}
}

func (s *BatchService) HealthCheck(ctx context.Context, req *emptypb.Empty) (*commonv1.SuccessReply, error) {
	return &commonv1.SuccessReply{Success: true}, nil
}
