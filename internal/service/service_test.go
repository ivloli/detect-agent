package service

import (
	"context"
	"testing"

	"github.com/go-kratos/kratos/v2/log"
	commonv1 "gitlab.gainetics.io/shared/proto-hub/common/gen/v1"
)

func TestNewBatchService(t *testing.T) {
	logger := log.DefaultLogger
	srv := NewBatchService(logger)
	if srv == nil {
		t.Fatal("NewBatchService returned nil")
	}
	if srv.helper == nil {
		t.Fatal("helper should not be nil")
	}
}

func TestBatchService_HealthCheck(t *testing.T) {
	logger := log.DefaultLogger
	srv := NewBatchService(logger)

	// Test HealthCheck returns success
	reply, err := srv.HealthCheck(context.Background(), nil)
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}
	if reply == nil {
		t.Fatal("HealthCheck reply should not be nil")
	}
	if reply.Success != true {
		t.Fatalf("HealthCheck reply.Success got %v, want true", reply.Success)
	}

	// Verify the reply type is correct
	if _, ok := interface{}(reply).(*commonv1.SuccessReply); !ok {
		t.Fatal("HealthCheck should return *commonv1.SuccessReply")
	}
}