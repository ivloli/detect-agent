package biz

import (
	"context"
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/franz-kafka"
	"fmt"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/oklog/ulid/v2"
	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
	ctrlplanev1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/control-plane/v1"
	localapiv1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/local-api/v1"
	"gitlab.gainetics.io/shared/singularity/kratosx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	CodeSuccess = 200
	CodeError   = 500
)

// BatchDetectHandler 批量探测处理器
// 负责从Kafka消费批量探测消息并在对应浏览器上执行探测任务
type BatchDetectHandler struct {
	logger          *log.Helper
	producer        *franz_kafka.KafkaProducer
	browserShepherd *BrowserShepherd
}

// NewBatchDetectHandler 创建批量探测处理器
func NewBatchDetectHandler(
	logger log.Logger,
	browserShepherd *BrowserShepherd,
) *BatchDetectHandler {
	// 使用BoceBrokers创建内部producer
	producer := newBoceProducer(logger)
	return &BatchDetectHandler{
		logger:          log.NewHelper(log.With(logger, "module", "biz/batch_detect_handler")),
		producer:        producer,
		browserShepherd: browserShepherd,
	}
}

// newBoceProducer 从配置创建使用BoceBrokers的Kafka producer
func newBoceProducer(logger log.Logger) *franz_kafka.KafkaProducer {
	kafkaConf := conf.GetData().Kafka
	if kafkaConf == nil || len(kafkaConf.BoceBrokers) == 0 {
		return nil
	}
	config := &franz_kafka.ProducerConfig{
		Brokers: kafkaConf.BoceBrokers,
	}
	if kafkaConf.Sasl != nil && kafkaConf.Sasl.Enable {
		config.Sasl = &franz_kafka.SaslConfig{
			Enable:   true,
			Username: kafkaConf.Sasl.Username,
			Password: kafkaConf.Sasl.Password,
		}
	}
	producer, err := franz_kafka.NewKafkaProducer(config, logger)
	if err != nil {
		return nil
	}
	return producer
}

// ==================== Chrome 批量处理器 ====================

// ChromiumBatchHandler Chromium批量消息处理器
type ChromiumBatchHandler struct {
	appType probecomm.InterceptAppType
	handler *BatchDetectHandler
}

// NewChromiumBatchHandler 创建Chromium内核批量消息处理器
func NewChromiumBatchHandler(appType probecomm.InterceptAppType, handler *BatchDetectHandler) *ChromiumBatchHandler {
	return &ChromiumBatchHandler{
		appType: appType,
		handler: handler,
	}
}

// Handle 处理单条Chromium消息，在chromium浏览器上进行探测
func (h *ChromiumBatchHandler) Handle(ctx context.Context, key []byte, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	resTopic := conf.GetData().Kafka.InterceptDetectResultTopic
	if len(resTopic) == 0 {
		h.handler.logger.Error("InterceptDetectResult topic is empty")
		return nil
	}

	var msg *ctrlplanev1.TaskCreateRequest
	if err := kratosx.Codec.Unmarshal(value, &msg); err != nil {
		h.handler.logger.Errorf("Handle unmarshal chromium kafka msg to TaskCreateRequest failed: %v", err)
		return err
	}
	output := &localapiv1.InterceptDetectResult{
		App:       h.appType,
		Error:     "",
		RawResult: "",
	}
	var param *localapiv1.InterceptDetectParam
	if err := kratosx.Codec.Unmarshal([]byte(msg.GetPayloadJson()), &param); err != nil {
		h.handler.logger.Errorf("Handle unmarshal chromium payload json to InterceptDetectParam failed: %v", err)
		output.Error = err.Error()
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
		h.sendResult(ctx, msg, output, CodeError, "unmarshal failed", err.Error(), resTopic, h.appType)
		return nil
	}
	if param.Url == "" || !strings.HasPrefix(param.Url, "http://") && !strings.HasPrefix(param.Url, "https://") {
		h.handler.logger.Errorf("Handle input url illegal: %s", param.Url)
		output.Error = fmt.Sprintf("input url illegal: %s", param.Url)
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
		h.sendResult(ctx, msg, output, CodeError, "input url illegal", fmt.Sprintf("input url illegal: %s", param.Url), resTopic, h.appType)
		return nil
	}
	newCtx, cancel := context.WithTimeout(ctx, time.Duration(msg.GetTimeoutSec())*time.Second)
	defer cancel()

	browser, tab, err := h.handler.browserShepherd.GetAvailableBrowserTab(h.appType)
	if err != nil {
		h.handler.logger.Errorf("Handle get available browser failed: %v", err)
		output.Error = err.Error()
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
		h.sendResult(ctx, msg, output, CodeError, "no browser available", err.Error(), resTopic, h.appType)
		return nil
	}
	defer h.handler.browserShepherd.ReleaseBrowserTab(h.appType, browser, tab)

	blocked, detail, err := browser.ChromiumDetect(newCtx, tab, param.Url)
	output.RawResult = detail
	if err != nil {
		h.handler.logger.Errorf("Handle detect browser failed: %v, url: %s, appType: %s", err, param.Url, h.appType.String())
		output.Error = err.Error()
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
		h.sendResult(ctx, msg, output, CodeError, "detect on browser failed", err.Error(), resTopic, h.appType)
		return nil
	}
	if blocked {
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_BLOCKED
	} else {
		output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_NORMAL
	}
	h.sendResult(ctx, msg, output, CodeSuccess, "", "", resTopic, h.appType)
	return nil
}

func (h *ChromiumBatchHandler) sendResult(ctx context.Context, in *ctrlplanev1.TaskCreateRequest,
	out *localapiv1.InterceptDetectResult, code int32, msg, raw, topic string, appType probecomm.InterceptAppType) {
	if h.handler.producer == nil {
		h.handler.logger.Error("sendResult producer is nil, skip sending")
		return
	}
	outJSONBytes, err := kratosx.Codec.Marshal(out)
	if err != nil {
		h.handler.logger.Errorf("sendResult marshal out json failed: %v", err)
		return
	}
	res := buildResult(in, code, msg, raw, string(outJSONBytes), appType)
	resBytes, err := kratosx.Codec.Marshal(res)
	if err != nil {
		h.handler.logger.Errorf("sendResult marshal result failed: %v", err)
		return
	}
	h.handler.logger.Infof("sendResult kafka msg: %s", string(resBytes))
	err = h.handler.producer.ProduceSync(ctx, topic, []byte(fmt.Sprint(time.Now())), resBytes)
	if err != nil {
		h.handler.logger.Errorf("sendResult send to kafka failed: %v", err)
	}
}

func buildResult(in *ctrlplanev1.TaskCreateRequest, code int32, msg, raw, outJSON string, appType probecomm.InterceptAppType) *ctrlplanev1.NodeMessage {
	return &ctrlplanev1.NodeMessage{
		EventType: ctrlplanev1.EventType_EVENT_TYPE_EXEC_RESULT,
		MessageId: ulid.Make().String(),
		Timestamp: time.Now().UnixMilli(),
		NodeId:    appType.String(),
		TaskMeta:  in.GetTaskMeta(),
		MsgStatus: ctrlplanev1.MessageStatus_MESSAGE_STATUS_COMPLETE,
		EventData: &ctrlplanev1.NodeMessage_ExecResult{
			ExecResult: &ctrlplanev1.ExecResultPayload{
				In: &ctrlplanev1.ExecCommandPayload{
					TimeoutSec:  in.GetTimeoutSec(),
					Deadline:    in.GetDeadline(),
					Type:        in.GetType(),
					PayloadJson: in.GetPayloadJson(),
				},
				ErrorCode:       code,
				ErrorMessage:    msg,
				ErrorRawMessage: raw,
				FinishedAt:      timestamppb.Now(),
				OutputJson:      outJSON,
			},
		},
	}
}
