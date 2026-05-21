package biz

import (
	"context"
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/franz-kafka"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	ctrlplanev1 "gitlab.gainetics.io/backend-cdn/go-protos/probe-executor/control-plane/v1"
)

// NodeReporter 向probe-center上报数据
type NodeReporter struct {
	logger          *log.Helper
	browserShepherd *BrowserShepherd
	producer        *franz_kafka.KafkaProducer
}

func NewNodeReporter(logger log.Logger, producer *franz_kafka.KafkaProducer, browserShepherd *BrowserShepherd) *NodeReporter {
	return &NodeReporter{
		logger:          log.NewHelper(logger),
		producer:        producer,
		browserShepherd: browserShepherd,
	}
}

// Start 启动上报，先上报一次注册，再每隔30s上报一次心跳
func (np *NodeReporter) Start(ctx context.Context) {
	heartTk := time.NewTicker(30 * time.Second)
	defer heartTk.Stop()
	msg := np.assembleReportMsg()
	msg.ReportType = ctrlplanev1.InterceptReportType_INTERCEPT_REPORT_TYPE_REGISTER
	err := np.sendMsg(ctx, msg)
	if err != nil {
		panic(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartTk.C:
			msg = np.assembleReportMsg()
			np.sendMsg(ctx, msg)
		}
	}
}

func (np *NodeReporter) assembleReportMsg() *ctrlplanev1.InterceptNodeInfo {
	msg := &ctrlplanev1.InterceptNodeInfo{
		ReportType:  ctrlplanev1.InterceptReportType_INTERCEPT_REPORT_TYPE_HEARTBEAT,
		NodeType:    ctrlplanev1.InterceptNodeType_INTERCEPT_NODE_TYPE_BROWSER_FARM, // todo: 跟移动端共用一套代码的话，这里要从本地配置读
		NodeName:    "",                                                             // todo
		PublicIpv4:  "",                                                             // todo
		Timestamp:   timestamppb.New(time.Now()),
		NodeDetails: np.browserShepherd.GetBrowserDetails(),
	}
	return msg
}

func (np *NodeReporter) sendMsg(ctx context.Context, msg *ctrlplanev1.InterceptNodeInfo) error {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		np.logger.Errorf("sendMsg json marshal intercept node info error: %v", err)
		return err
	}
	topic := conf.GetData().Kafka.HeartbeatReportTopic
	err = np.producer.ProduceSync(ctx, topic, []byte(fmt.Sprint(time.Now())), msgBytes)
	if err != nil {
		np.logger.Errorf("sendMsg json produce intercept node info error: %v", err)
		return err
	}
	return nil
}
