package franz_kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// ProducerConfig Kafka生产者配置
type ProducerConfig struct {
	Brokers []string    // Kafka brokers地址
	Sasl    *SaslConfig // SASL认证配置
}

// KafkaProducer Kafka生产者封装
type KafkaProducer struct {
	client *kgo.Client
	logger *log.Helper
}

// NewKafkaProducer 创建Kafka生产者
func NewKafkaProducer(config *ProducerConfig, logger log.Logger) (*KafkaProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.AllowAutoTopicCreation(),
	}

	// SASL认证配置
	if config.Sasl != nil && config.Sasl.Enable {
		// 这里可以根据需要添加 SASL 认证逻辑
		// franz-go 的 SASL 配置比较灵活，这里简化处理
		// 实际项目中可能需要使用 kgo.SASL(...)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer client: %w", err)
	}

	// 验证连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping kafka brokers: %w", err)
	}

	return &KafkaProducer{
		client: client,
		logger: log.NewHelper(logger),
	}, nil
}

// ProduceSync 同步发送单条消息
func (p *KafkaProducer) ProduceSync(ctx context.Context, topic string, key, value []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		p.logger.Error("failed to produce message sync",
			zap.String("topic", topic),
			zap.Error(err))
		return err
	}

	return nil
}

// ProduceAsync 异步发送单条消息
func (p *KafkaProducer) ProduceAsync(topic string, key, value []byte, callback func(*kgo.Record, error)) {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	p.client.Produce(context.Background(), record, callback)
}

// Close 关闭生产者
func (p *KafkaProducer) Close() {
	if p.client != nil {
		p.client.Close()
	}
}
