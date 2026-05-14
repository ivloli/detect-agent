package server

import (
	"context"
	"detect-agent/internal/biz"
	"detect-agent/internal/conf"
	franzkafka "detect-agent/internal/pkg/franz-kafka"
	"fmt"
	"net/url"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"go.uber.org/zap"
)

// KafkaServer Kafka服务器
type KafkaServer struct {
	chromeBatchConsumer *franzkafka.BatchDetectConsumer
	batchInsertHandler  *biz.BatchDetectHandler // 批量探测处理器
	logger              *log.Helper
}

// NewKafkaServer 创建Kafka服务器
func NewKafkaServer(
	batchInsertHandler *biz.BatchDetectHandler,
	logger log.Logger,
) *KafkaServer {
	return &KafkaServer{
		batchInsertHandler: batchInsertHandler,
		logger:             log.NewHelper(log.With(logger, "module", "server/kafka")),
	}
}

// NewKafkaProducerConfig 创建Kafka生产者配置
func NewKafkaProducerConfig() *franzkafka.ProducerConfig {
	kafkaConf := conf.GetData().Kafka
	if kafkaConf == nil {
		return nil
	}
	config := &franzkafka.ProducerConfig{
		Brokers: kafkaConf.Brokers,
	}
	if kafkaConf.Sasl != nil && kafkaConf.Sasl.Enable {
		config.Sasl = &franzkafka.SaslConfig{
			Enable:   true,
			Username: kafkaConf.Sasl.Username,
			Password: kafkaConf.Sasl.Password,
		}
	}
	return config
}

// Start 启动Kafka服务器
func (s *KafkaServer) Start(ctx context.Context) error {
	s.logger.Info("启动Kafka服务器")

	// 在启动前最后验证一次Kafka配置
	kafkaConf := conf.GetData().Kafka
	if kafkaConf == nil || len(kafkaConf.Brokers) == 0 {
		return fmt.Errorf("kafka配置未加载，无法启动Kafka消费者")
	}

	// 启动批量写入消费者
	if err := s.startBatchInsertConsumers(ctx, kafkaConf); err != nil {
		s.logger.Errorf("启动批量写入消费者失败: %v", err)
		// 批量写入消费者启动失败不影响主服务，继续运行
	}

	return nil
}

// startBatchInsertConsumers 启动批量写入消费者
func (s *KafkaServer) startBatchInsertConsumers(ctx context.Context, kafkaConf *conf.KafkaConfig) error {

	if s.batchInsertHandler == nil {
		s.logger.Warn("批量插入处理器未设置，跳过批量写入消费者启动")
		return nil
	}

	// 定义要启动的批量任务列表
	tasks := []struct {
		name      string
		topic     string
		startFunc func() error
	}{
		{
			name:  "Chrome",
			topic: kafkaConf.ChromeInterceptDetectTopic,
			startFunc: func() error {
				return s.initDNSBatchConsumer(kafkaConf)
			},
		},
	}

	// 直接启动所有配置了 Topic 的消费者
	for _, task := range tasks {
		if task.topic == "" {
			s.logger.Warnf("%s 批量写入Topic未配置，跳过", task.name)
			continue
		}

		s.logger.Infof("🚀 启动 %s 批量写入消费者 (Topic: %s)", task.name, task.topic)
		if err := task.startFunc(); err != nil {
			s.logger.Errorf("[%s] 启动失败: %v", task.name, err)
		}
	}

	return nil
}

func (s *KafkaServer) initDNSBatchConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:        kafkaConf.Brokers,
		GroupID:        kafkaConf.Group,
		Topic:          kafkaConf.ChromeInterceptDetectTopic,
		MaxBatchSize:   conf.GetData().Kafka.BatchSize,
		BatchTimeoutMs: 1000,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "DNS")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromeBatchHandler(s.batchInsertHandler))
	s.chromeBatchConsumer = consumer
	return consumer.Start()
}

func (s *KafkaServer) applySaslConfig(config *franzkafka.BatchDetectConsumerConfig, kafkaConf *conf.KafkaConfig) {
	if kafkaConf.Sasl != nil && kafkaConf.Sasl.Enable {
		config.Sasl = &franzkafka.SaslConfig{
			Enable:   true,
			Username: kafkaConf.Sasl.Username,
			Password: kafkaConf.Sasl.Password,
		}
	}
}

// Kind 返回服务器类型
func (s *KafkaServer) Kind() transport.Kind {
	return "kafka-consumer"
}

// Endpoint 返回服务器端点
func (s *KafkaServer) Endpoint() (*url.URL, error) {
	return &url.URL{
		Scheme: "kafka",
		Host:   "consumer:9092",
	}, nil
}

// Stop 停止Kafka服务器
func (s *KafkaServer) Stop(ctx context.Context) error {
	s.logger.Info("停止Kafka服务器")

	var errs []error
	if s.chromeBatchConsumer != nil {
		err := s.chromeBatchConsumer.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("stop errors: %v", errs)
	}
	return nil
}

// GracefulStop 优雅停止
func (s *KafkaServer) GracefulStop(ctx context.Context) error {
	return s.Stop(ctx)
}
