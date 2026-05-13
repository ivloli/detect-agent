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
	Brokers        []string    // Kafka brokers地址
	GroupID        string      // 消费者组ID
	Topic          string      // 消费的主题
	MaxBatchSize   int         // 批量处理最大消息数量，默认100
	BatchTimeoutMs int         // 批量处理超时时间（毫秒），默认1000
	Sasl           *SaslConfig // SASL认证配置
}

// BatchMessageHandler 批量消息处理器接口
type BatchMessageHandler interface {
	// HandleBatch 批量处理消息
	// keys: 消息键列表（字节数组列表）
	// values: 消息值列表（字节数组列表）
	// 返回成功处理的消息数量和错误
	HandleBatch(ctx context.Context, keys [][]byte, values [][]byte) (int, error)
}

// BatchDetectConsumer 批量探测消费者
type BatchDetectConsumer struct {
	client       *kgo.Client
	config       *BatchDetectConsumerConfig
	logger       *zap.Logger
	handler      BatchMessageHandler
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	consumerName string
}

// NewBatchDetectConsumer 创建批量探测消费者
func NewBatchDetectConsumer(config *BatchDetectConsumerConfig, logger *zap.Logger, consumerName string) (*BatchDetectConsumer, error) {
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 100
	}
	if config.BatchTimeoutMs <= 0 {
		config.BatchTimeoutMs = 1000
	}

	// 构建客户端选项
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(config.Topic),
		kgo.FetchMaxBytes(50 * 1024 * 1024), // 50MB
		// 设置 FetchMaxWait 稍微小于 BatchTimeoutMs，让 Poll 能够更及时返回
		kgo.FetchMaxWait(time.Duration(config.BatchTimeoutMs/2) * time.Millisecond),
		kgo.FetchMinBytes(1),
		kgo.DisableAutoCommit(), // 手动提交偏移量

		// 消费者组协议配置
		kgo.SessionTimeout(30 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
		kgo.RebalanceTimeout(60 * time.Second),
		kgo.RequireStableFetchOffsets(),
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

// SetHandler 设置批量消息处理器
func (c *BatchDetectConsumer) SetHandler(handler BatchMessageHandler) {
	c.handler = handler
}

// Start 启动消费者
func (c *BatchDetectConsumer) Start() error {
	c.logger.Info("启动批量探测消费者",
		zap.String("consumer", c.consumerName),
		zap.Strings("brokers", c.config.Brokers),
		zap.String("groupId", c.config.GroupID),
		zap.String("topic", c.config.Topic),
		zap.Int("maxBatchSize", c.config.MaxBatchSize),
		zap.Int("batchTimeoutMs", c.config.BatchTimeoutMs))

	c.wg.Add(1)
	go c.consumeLoop()

	return nil
}

// Stop 停止消费者
func (c *BatchDetectConsumer) Stop() error {
	c.logger.Info("停止批量探测消费者", zap.String("consumer", c.consumerName))
	c.cancel()
	c.wg.Wait()
	c.client.Close()
	c.logger.Info("批量探测消费者已停止", zap.String("consumer", c.consumerName))
	return nil
}

// consumeLoop 主消费循环 - 优化后的攒批逻辑
func (c *BatchDetectConsumer) consumeLoop() {
	defer c.wg.Done()

	c.logger.Info("开始批量消费循环", zap.String("consumer", c.consumerName))

	var (
		recordsBatch []*kgo.Record
		valuesBatch  [][]byte
		timeout      = time.Duration(c.config.BatchTimeoutMs) * time.Millisecond
		timer        = time.NewTimer(timeout)
	)

	// 初始停止定时器
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	activeTimer := false

	for {
		// 每次进入 Poll 前检查 context
		if c.ctx.Err() != nil {
			if len(valuesBatch) > 0 {
				c.processBatch(recordsBatch, valuesBatch)
			}
			return
		}

		// 使用 Poll 实现阻塞式拉取，并设置一个小的内部超时以便触发定时检查
		pollCtx, cancel := context.WithTimeout(c.ctx, 100*time.Millisecond)
		fetches := c.client.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			return
		}

		// 处理拉取到的消息
		iter := fetches.RecordIter()
		hasNewMessages := !iter.Done()

		for !iter.Done() {
			record := iter.Next()
			recordsBatch = append(recordsBatch, record)
			valuesBatch = append(valuesBatch, record.Value)

			// 检查是否达到批次上限
			if len(valuesBatch) >= c.config.MaxBatchSize {
				c.logger.Debug("达到批处理上限触发探测",
					zap.String("consumer", c.consumerName),
					zap.Int("count", len(valuesBatch)))

				c.processBatch(recordsBatch, valuesBatch)

				// 清理状态
				recordsBatch = nil
				valuesBatch = nil
				if activeTimer {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					activeTimer = false
				}
			}
		}

		// 处理批次逻辑：
		// 1. 如果缓冲区为空且有新消息加入，启动定时器
		// 2. 如果定时器已启动，检查是否超时（通过 select 非阻塞检查）

		if len(valuesBatch) > 0 && !activeTimer {
			// 新批次开始，启动定时器
			timer.Reset(timeout)
			activeTimer = true
		}

		// 检查定时器状态
		if activeTimer {
			select {
			case <-timer.C:
				// 超时触发探测
				c.logger.Debug("批处理超时触发探测",
					zap.String("consumer", c.consumerName),
					zap.Int("count", len(valuesBatch)))

				c.processBatch(recordsBatch, valuesBatch)

				// 重置状态
				recordsBatch = nil
				valuesBatch = nil
				activeTimer = false
			default:
				// 未超时，继续下一轮 Poll
			}
		}

		// 如果没有新消息且没有活跃的定时器（缓冲区已清空），或者还在等待更多消息，继续循环
		if !hasNewMessages && !activeTimer && len(valuesBatch) == 0 {
			// 休眠一小会儿避免 CPU 占用过高，或者直接进入下一轮阻塞 Poll
		}
	}
}

// processBatch 批量处理消息
func (c *BatchDetectConsumer) processBatch(records []*kgo.Record, values [][]byte) {
	if len(values) == 0 {
		return
	}

	if c.handler == nil {
		c.logger.Error("批量消息处理器未设置", zap.String("consumer", c.consumerName))
		return
	}

	// 提取 keys
	keys := make([][]byte, len(records))
	for i, record := range records {
		keys[i] = record.Key
	}

	startTime := time.Now()
	count := len(values)

	// 调用批量处理器（传入 keys 和 values）
	successCount, err := c.handler.HandleBatch(c.ctx, keys, values)
	if err != nil {
		c.logger.Error("批量处理消息失败",
			zap.String("consumer", c.consumerName),
			zap.Int("totalCount", count),
			zap.Int("successCount", successCount),
			zap.Error(err))
	} else {
		c.logger.Info("批量处理消息成功",
			zap.String("consumer", c.consumerName),
			zap.Int("count", count),
			zap.Duration("duration", time.Since(startTime)))
	}

	// 提交偏移量
	if err := c.client.CommitRecords(c.ctx, records...); err != nil {
		c.logger.Error("提交偏移量失败",
			zap.String("consumer", c.consumerName),
			zap.Error(err))
	}
}
