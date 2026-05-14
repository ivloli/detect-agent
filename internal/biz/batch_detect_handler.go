package biz

import (
	"context"
	"detect-agent/internal/pkg/franz-kafka"

	"github.com/go-kratos/kratos/v2/log"
)

// BatchDetectHandler 批量探测处理器
// 负责从Kafka消费批量探测消息并在对应浏览器上执行探测任务
type BatchDetectHandler struct {
	logger   *log.Helper
	producer *franz_kafka.KafkaProducer
}

// NewBatchDetectHandler 创建批量探测处理器
func NewBatchDetectHandler(
	logger log.Logger,
	producer *franz_kafka.KafkaProducer,
) *BatchDetectHandler {
	return &BatchDetectHandler{
		logger:   log.NewHelper(log.With(logger, "module", "biz/batch_detect_handler")),
		producer: producer,
	}
}

// ==================== Chrome 批量处理器 ====================

// ChromeBatchHandler Chrome批量消息处理器
type ChromeBatchHandler struct {
	handler *BatchDetectHandler
}

// NewChromeBatchHandler 创建Chrome批量消息处理器
func NewChromeBatchHandler(handler *BatchDetectHandler) *ChromeBatchHandler {
	return &ChromeBatchHandler{handler: handler}
}

// HandleBatch 批量处理Chrome消息，并发在chrome浏览器上进行探测
func (h *ChromeBatchHandler) HandleBatch(ctx context.Context, keys [][]byte, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	// todo: 协程并行探测，然后各自发kafka
	// 示例：发送探测结果
	// resultTopic := conf.GetData().Kafka.ChromeInterceptResultTopic
	// h.handler.producer.ProduceSync(ctx, resultTopic, key, value)

	return 0, nil
}
