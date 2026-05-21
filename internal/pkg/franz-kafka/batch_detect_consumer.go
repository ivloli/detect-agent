package franz_kafka

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// BatchDetectConsumerConfig 批量探测消费者配置
type BatchDetectConsumerConfig struct {
	Brokers     []string    // Kafka brokers地址
	GroupID     string      // 消费者组ID
	Topic       string      // 消费的主题
	Concurrency int         // 并发处理协程数，对应 TabPoolSize
	Sasl        *SaslConfig // SASL认证配置
}

// MessageHandler 消息处理器接口
type MessageHandler interface {
	// Handle 处理单条消息
	// key: 消息键
	// value: 消息值
	// 返回错误
	Handle(ctx context.Context, key []byte, value []byte) error
}

// BatchDetectConsumer 探测消费者
type BatchDetectConsumer struct {
	client       *kgo.Client
	config       *BatchDetectConsumerConfig
	logger       *zap.Logger
	handler      MessageHandler
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	consumerName string
	msgChan      chan *kgo.Record
}

// NewBatchDetectConsumer 创建探测消费者
func NewBatchDetectConsumer(config *BatchDetectConsumerConfig, logger *zap.Logger, consumerName string) (*BatchDetectConsumer, error) {
	if config.Concurrency <= 0 {
		config.Concurrency = 50
	}

	// 构建客户端选项
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(config.Topic),
		kgo.FetchMaxBytes(50 * 1024 * 1024), // 50MB
		kgo.FetchMinBytes(1),
		kgo.DisableAutoCommit(), // 手动提交偏移量

		// 消费者组协议配置
		kgo.SessionTimeout(30 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
		kgo.RebalanceTimeout(60 * time.Second),
		kgo.RequireStableFetchOffsets(),
	}

	// 如果有 SASL 配置，添加选项
	if config.Sasl != nil && config.Sasl.Enable {
		// 暂无具体实现
	}

	// 创建Kafka客户端
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &BatchDetectConsumer{
		client:       client,
		config:       config,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		consumerName: consumerName,
	}, nil
}

// SetHandler 设置单条消息处理器
func (c *BatchDetectConsumer) SetHandler(handler MessageHandler) {
	c.handler = handler
}

// Start 启动消费者
func (c *BatchDetectConsumer) Start() error {
	c.logger.Info("启动探测消费者",
		zap.String("consumer", c.consumerName),
		zap.Strings("brokers", c.config.Brokers),
		zap.String("groupId", c.config.GroupID),
		zap.String("topic", c.config.Topic),
		zap.Int("concurrency", c.config.Concurrency))

	c.msgChan = make(chan *kgo.Record, c.config.Concurrency)

	// 启动 workers
	for i := 0; i < c.config.Concurrency; i++ {
		c.wg.Add(1)
		go c.workerLoop(i)
	}

	c.wg.Add(1)
	go c.consumeLoop()

	return nil
}

// Stop 停止消费者
func (c *BatchDetectConsumer) Stop() error {
	c.logger.Info("停止探测消费者", zap.String("consumer", c.consumerName))
	c.cancel()
	c.wg.Wait()
	c.client.Close()
	c.logger.Info("探测消费者已停止", zap.String("consumer", c.consumerName))
	return nil
}

// consumeLoop 主消费循环，从Kafka拉取消息发送到 channel
func (c *BatchDetectConsumer) consumeLoop() {
	defer c.wg.Done()
	defer close(c.msgChan)

	c.logger.Info("开始消费循环", zap.String("consumer", c.consumerName))

	for {
		if c.ctx.Err() != nil {
			return
		}

		// 使用 Poll 实现阻塞式拉取
		fetches := c.client.PollFetches(c.ctx)
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for i, fetchErr := range errs {
				c.logger.Error("拉取消息时发生错误",
					zap.String("consumer", c.consumerName),
					zap.Int("error_index", i),
					zap.String("topic", fetchErr.Topic),
					zap.Int32("partition", fetchErr.Partition),
					zap.Error(fetchErr.Err),
				)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			select {
			case <-c.ctx.Done():
				return
			case c.msgChan <- record:
			}
		}
	}
}

// workerLoop 并发处理协程
func (c *BatchDetectConsumer) workerLoop(workerID int) {
	defer c.wg.Done()
	c.logger.Info("Worker started", zap.Int("worker_id", workerID), zap.String("consumer", c.consumerName))
	for {
		select {
		case <-c.ctx.Done():
			return
		case record, ok := <-c.msgChan:
			if !ok {
				return
			}
			c.processRecord(record)
		}
	}
}

// processRecord 处理单条消息并提交偏移量
func (c *BatchDetectConsumer) processRecord(record *kgo.Record) {
	if c.handler == nil {
		c.logger.Error("消息处理器未设置", zap.String("consumer", c.consumerName))
		return
	}

	startTime := time.Now()
	// 调用单条处理器
	err := c.handler.Handle(c.ctx, record.Key, record.Value)
	if err != nil {
		c.logger.Error("处理消息失败",
			zap.String("consumer", c.consumerName),
			zap.Int("partition", int(record.Partition)),
			zap.Int64("offset", record.Offset),
			zap.Error(err))
	} else {
		c.logger.Info("处理消息成功",
			zap.String("consumer", c.consumerName),
			zap.Int("partition", int(record.Partition)),
			zap.Int64("offset", record.Offset),
			zap.Duration("duration", time.Since(startTime)))
	}

	// 提交偏移量
	if err := c.client.CommitRecords(c.ctx, record); err != nil {
		c.logger.Error("提交偏移量失败",
			zap.String("consumer", c.consumerName),
			zap.Error(err))
	}
}
