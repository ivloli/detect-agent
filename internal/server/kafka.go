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
	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
	"go.uber.org/zap"
)

// KafkaServer Kafka服务器
type KafkaServer struct {
	batchConsumerChrome *franzkafka.BatchDetectConsumer
	batchConsumerEdge   *franzkafka.BatchDetectConsumer
	batchConsumer360    *franzkafka.BatchDetectConsumer
	batchConsumerUC     *franzkafka.BatchDetectConsumer
	batchConsumerQuark  *franzkafka.BatchDetectConsumer
	batchConsumerSogou  *franzkafka.BatchDetectConsumer
	batchDetectHandler  *biz.BatchDetectHandler // 批量探测处理器
	logger              *log.Helper
}

// NewKafkaServer 创建Kafka服务器
func NewKafkaServer(
	batchDetectHandler *biz.BatchDetectHandler,
	logger log.Logger,
) *KafkaServer {
	return &KafkaServer{
		batchDetectHandler: batchDetectHandler,
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

	// 启动批量探测消费者
	if err := s.startBatchDetectConsumers(ctx, kafkaConf); err != nil {
		s.logger.Errorf("启动批量探测消费者失败: %v", err)
	}

	return nil
}

// startBatchDetectConsumers 启动批量探测消费者
func (s *KafkaServer) startBatchDetectConsumers(ctx context.Context, kafkaConf *conf.KafkaConfig) error {

	if s.batchDetectHandler == nil {
		s.logger.Warn("批量探测处理器未设置，跳过批量探测消费者启动")
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
			topic: kafkaConf.InterceptDetectChromeTopic,
			startFunc: func() error {
				return s.initChromeConsumer(kafkaConf)
			},
		},
		{
			name:  "Edge",
			topic: kafkaConf.InterceptDetectEdgeTopic,
			startFunc: func() error {
				return s.initEdgeConsumer(kafkaConf)
			},
		},
		{
			name:  "360",
			topic: kafkaConf.InterceptDetect360Topic,
			startFunc: func() error {
				return s.init360Consumer(kafkaConf)
			},
		},
		{
			name:  "UC",
			topic: kafkaConf.InterceptDetectUCTopic,
			startFunc: func() error {
				return s.initUCConsumer(kafkaConf)
			},
		},
		{
			name:  "Quark",
			topic: kafkaConf.InterceptDetectQuarkTopic,
			startFunc: func() error {
				return s.initQuarkConsumer(kafkaConf)
			},
		},
		{
			name:  "QQ",
			topic: kafkaConf.InterceptDetectQQTopic,
			startFunc: func() error {
				return s.initQQConsumer(kafkaConf)
			},
		},
	}

	// 直接启动所有配置了 Topic 的消费者
	for _, task := range tasks {
		if task.topic == "" {
			s.logger.Warnf("%s 批量探测Topic未配置，跳过", task.name)
			continue
		}

		s.logger.Infof("🚀 启动 %s 批量探测消费者 (Topic: %s)", task.name, task.topic)
		if err := task.startFunc(); err != nil {
			s.logger.Errorf("[%s] 启动失败: %v", task.name, err)
		}
	}

	return nil
}

func (s *KafkaServer) initChromeConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetectChromeTopic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "Chrome")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_CHROME, s.batchDetectHandler))
	s.batchConsumerChrome = consumer
	return consumer.Start()
}

func (s *KafkaServer) initEdgeConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetectEdgeTopic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "Edge")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_EDGE, s.batchDetectHandler))
	s.batchConsumerEdge = consumer
	return consumer.Start()
}

func (s *KafkaServer) init360Consumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetect360Topic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "360")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_360, s.batchDetectHandler))
	s.batchConsumer360 = consumer
	return consumer.Start()
}

func (s *KafkaServer) initUCConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetectUCTopic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "UC")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_UC, s.batchDetectHandler))
	s.batchConsumerUC = consumer
	return consumer.Start()
}

func (s *KafkaServer) initQuarkConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetectQuarkTopic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "Quark")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QUARK, s.batchDetectHandler))
	s.batchConsumerQuark = consumer
	return consumer.Start()
}

func (s *KafkaServer) initQQConsumer(kafkaConf *conf.KafkaConfig) error {
	config := &franzkafka.BatchDetectConsumerConfig{
		Brokers:     kafkaConf.Brokers,
		GroupID:     kafkaConf.Group,
		Topic:       kafkaConf.InterceptDetectQQTopic,
		Concurrency: biz.TabPoolSize,
	}
	s.applySaslConfig(config, kafkaConf)

	logger, _ := zap.NewProduction() // 生产环境建议通过依赖注入传入 logger
	consumer, err := franzkafka.NewBatchDetectConsumer(config, logger, "QQ")
	if err != nil {
		return err
	}
	consumer.SetHandler(biz.NewChromiumBatchHandler(probecomm.InterceptAppType_INTERCEPT_APP_TYPE_QQ, s.batchDetectHandler))
	s.batchConsumerSogou = consumer
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
	if s.batchConsumerChrome != nil {
		err := s.batchConsumerChrome.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if s.batchConsumerEdge != nil {
		err := s.batchConsumerEdge.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if s.batchConsumer360 != nil {
		err := s.batchConsumer360.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if s.batchConsumerUC != nil {
		err := s.batchConsumerUC.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if s.batchConsumerQuark != nil {
		err := s.batchConsumerQuark.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if s.batchConsumerSogou != nil {
		err := s.batchConsumerSogou.Stop()
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
