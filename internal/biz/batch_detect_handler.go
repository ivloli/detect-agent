package biz

import (
	"context"
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/franz-kafka"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/oklog/ulid/v2"
	probecomm "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/common/v1"
	ctrlplanev1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/control-plane/v1"
	localapiv1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/local-api/v1"
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
	producer *franz_kafka.KafkaProducer,
	browserShepherd *BrowserShepherd,
) *BatchDetectHandler {
	return &BatchDetectHandler{
		logger:          log.NewHelper(log.With(logger, "module", "biz/batch_detect_handler")),
		producer:        producer,
		browserShepherd: browserShepherd,
	}
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

// HandleBatch 批量处理Chromium消息，并发在chromium浏览器上进行探测
func (h *ChromiumBatchHandler) HandleBatch(ctx context.Context, keys [][]byte, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}
	resTopic := conf.GetData().Kafka.InterceptDetectResultTopic
	if len(resTopic) == 0 {
		h.handler.logger.Error("InterceptDetectResult topic is empty")
		return 0, nil
	}
	var successCnt atomic.Int32
	var wg sync.WaitGroup
	// 反序列化所有消息
	for i := range values {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var msg *ctrlplanev1.TaskCreateRequest
			if err := json.Unmarshal(values[idx], &msg); err != nil {
				h.handler.logger.Errorf("HandleBatch unmarshal chromium kafka msg to TaskCreateRequest failed: %v", err)
				return
			}
			output := &localapiv1.InterceptDetectResult{
				App:       h.appType,
				Error:     "",
				RawResult: "",
			}
			var param *localapiv1.InterceptDetectParam
			if err := json.Unmarshal([]byte(msg.GetPayloadJson()), &param); err != nil {
				h.handler.logger.Errorf("HandleBatch unmarshal chromium payload json to InterceptDetectParam failed: %v", err)
				output.Error = err.Error()
				output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
				h.sendResult(ctx, msg, output, CodeError, "unmarshal failed", err.Error(), resTopic)
				return
			}
			newCtx, cancel := context.WithTimeout(ctx, time.Duration(msg.GetTimeoutSec())*time.Second)
			defer cancel()

			browser, tab, err := h.handler.browserShepherd.GetAvailableBrowserTab(h.appType)
			if err != nil {
				h.handler.logger.Errorf("HandleBatch get available browser failed: %v", err)
				output.Error = err.Error()
				output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
				h.sendResult(ctx, msg, output, CodeError, "no browser available", err.Error(), resTopic)
				return
			}
			defer h.handler.browserShepherd.ReleaseBrowserTab(h.appType, browser, tab)

			blocked, detail, err := browser.ChromiumDetect(newCtx, tab, param.Url)
			output.RawResult = detail
			if err != nil {
				h.handler.logger.Errorf("HandleBatch detect browser failed: %v", err)
				output.Error = err.Error()
				output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_FAIL
				h.sendResult(ctx, msg, output, CodeError, "detect on browser failed", err.Error(), resTopic)
				return
			}
			if blocked {
				output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_BLOCKED
			} else {
				output.Status = localapiv1.InterceptDetectStatus_INTERCEPT_DETECT_STATUS_NORMAL
			}
			successCnt.Add(1)
			h.sendResult(ctx, msg, output, CodeSuccess, "", "", resTopic)
		}(i)
	}
	wg.Wait()

	return int(successCnt.Load()), nil
}

func (h *ChromiumBatchHandler) sendResult(ctx context.Context, in *ctrlplanev1.TaskCreateRequest,
	out *localapiv1.InterceptDetectResult, code int32, msg, raw, topic string) {
	outJSONBytes, err := json.Marshal(out)
	if err != nil {
		h.handler.logger.Errorf("sendResult marshal out json failed: %v", err)
		return
	}
	res := buildResult(in, code, msg, raw, string(outJSONBytes))
	resBytes, err := json.Marshal(res)
	if err != nil {
		h.handler.logger.Errorf("sendResult marshal result failed: %v", err)
		return
	}
	err = h.handler.producer.ProduceSync(ctx, topic, []byte(fmt.Sprint(time.Now())), resBytes)
	if err != nil {
		h.handler.logger.Errorf("sendResult send to kafka failed: %v", err)
	}
}

func buildResult(in *ctrlplanev1.TaskCreateRequest, code int32, msg, raw, outJSON string) *ctrlplanev1.NodeMessage {
	return &ctrlplanev1.NodeMessage{
		EventType: ctrlplanev1.EventType_EVENT_TYPE_EXEC_RESULT,
		MessageId: ulid.Make().String(),
		Timestamp: time.Now().UnixMilli(),
		NodeId:    "", //todo: 待定
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
				FinishedAt:      timestamppb.New(time.Now()),
				OutputJson:      outJSON,
			},
		},
	}
}
