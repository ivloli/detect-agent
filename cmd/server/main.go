package main

import (
	"context"
	"detect-agent/internal/biz"
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/utils"
	"detect-agent/internal/server"
	"flag"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/go-kratos/kratos/v2/transport/http"
	gonacos "gitlab.gainetics.io/shared/go-common/go-nacos-cli"
	zaplog "gitlab.gainetics.io/shared/go-common/zap-kratos-log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs/local", "config path, eg: -conf config.yaml")
}

// setTracerProvider 设置全局tracer provider
func setTracerProvider(url string) error {
	// 创建 Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return err
	}
	tp := tracesdk.NewTracerProvider(
		// 设置采样率
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
		// 记录信息到本地文件
		tracesdk.WithBatcher(exp),
		// 记录信息到jaeger
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(Name),
			semconv.ServiceVersionKey.String(Version),
			attribute.String("environment", conf.Data.Server.Env),
		)),
	)
	otel.SetTracerProvider(tp)
	return nil
}

func newApp(
	logger log.Logger,
	gs *grpc.Server,
	hs *http.Server,
	kafkaSrv *server.KafkaServer,
	r registry.Registrar,
	shepherd *biz.BrowserShepherd,
	reporter *biz.NodeReporter,
) *kratos.App {
	Name = conf.Data.Server.Name
	app := kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{
			"env":       conf.Data.Server.Env,
			"http.addr": conf.Data.Server.HTTP.Addr,
			"grpc.addr": conf.Data.Server.Grpc.Addr,
			"hostname":  id,
			"version":   Version,
		}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			hs,
			kafkaSrv,
		),
		kratos.Registrar(r),
	)
	if shepherd != nil {
		ctx := context.Background()
		go shepherd.StartMonitor(ctx)
	}
	if reporter != nil {
		ctx := context.Background()
		go reporter.Start(ctx)
	}

	return app
}

func main() {
	flag.Parse()
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	// 初始化配置容器
	conf.InitConfigContainer()

	stdlog.Printf("🚀 [DetectAgent服务启动] 正在初始化Nacos配置监听...")
	stdlog.Printf("📡 Nacos连接信息: %s:%d (命名空间: %s, 组: %s, 配置: %s)",
		bc.Nacos.Addr, bc.Nacos.Port, bc.Nacos.NamespaceId, bc.Nacos.GroupId, bc.Nacos.DataId)

	// 创建Nacos客户端
	nacosClient := newNacosClient(&bc)

	// 创建高级配置监听器
	var err error
	conf.NacosListener, err = nacosClient.CreateAdvancedListener(&conf.Data, conf.ConfigChangeCallback)
	if err != nil {
		panic(fmt.Sprintf("创建Nacos配置监听器失败: %v", err))
	}

	stdlog.Printf("⏳ [Nacos监听器] 正在启动配置监听并加载初始配置...")

	// 启动配置监听（包括加载初始配置）
	if err := conf.NacosListener.Start(); err != nil {
		panic(fmt.Sprintf("启动Nacos配置监听失败: %v", err))
	}

	stdlog.Printf("✅ [初始配置加载完成] 当前服务配置:")
	stdlog.Printf("   🏷️  服务名: %s", conf.Data.Server.Name)
	stdlog.Printf("   🌍 环境: %s", conf.Data.Server.Env)
	stdlog.Printf("   🌐 HTTP监听: %s", conf.Data.Server.HTTP.Addr)
	stdlog.Printf("   🔗 gRPC监听: %s", conf.Data.Server.Grpc.Addr)
	stdlog.Printf("🎯 [监听器状态] Nacos配置监听已激活，等待配置变更...")

	// 确保程序退出时停止监听器
	defer func() {
		if conf.NacosListener != nil {
			conf.NacosListener.Stop()
		}
	}()

	// 初始化tracing (可选，根据环境变量或配置决定是否启用)
	tracingEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if tracingEndpoint != "" {
		if err := setTracerProvider(tracingEndpoint); err != nil {
			panic(err)
		}
	}

	app, cleanup, err := wireApp(
		bc.Nacos,
		zaplog.LoggerWithWriterOptions(
			conf.Data.Server.Env,
			zaplog.WithFilename("detect-agent.log"),
			zaplog.WithLogDir("./logs"),
		),
	)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// 等待Kafka配置加载完成
	stdlog.Printf("⏳ [配置验证] 验证Kafka配置是否已从Nacos加载...")
	kafkaConf := conf.GetData().Kafka
	if kafkaConf == nil || len(kafkaConf.Brokers) == 0 {
		stdlog.Printf("⚠️  Kafka配置未加载，等待配置更新...")
		// 等待最多60秒，让配置有机会加载
		for i := 0; i < 60; i++ {
			time.Sleep(time.Second)
			kafkaConf = conf.GetData().Kafka
			if kafkaConf != nil && len(kafkaConf.Brokers) > 0 {
				stdlog.Printf("✅ Kafka配置加载成功 - Brokers: %v, Group: %s",
					kafkaConf.Brokers, kafkaConf.Group)
				break
			}
			if i%10 == 0 && i > 0 {
				stdlog.Printf("⏳ 仍在等待Kafka配置加载... (%d秒)", i)
			}
		}
		if kafkaConf == nil || len(kafkaConf.Brokers) == 0 {
			stdlog.Printf("❌ Kafka配置加载超时，将阻止服务启动")
			panic("Kafka配置未加载，服务无法启动。请检查Nacos配置是否正确")
		}
	} else {
		stdlog.Printf("✅ Kafka配置已加载 - Brokers: %v, Group: %s",
			kafkaConf.Brokers, kafkaConf.Group)
	}

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}

func newNacosClient(bc *conf.Bootstrap) gonacos.NacosClientDO {
	sc, err := utils.ParseNacosServerAddr(bc.Nacos.Addr)
	if err != nil {
		panic(fmt.Sprintf("❌ 解析Nacos服务地址失败: %v", err))
	}
	var si []gonacos.ServerInfo
	for _, v := range sc {
		si = append(si, gonacos.ServerInfo{
			Addr: v.IpAddr,
			Port: v.Port,
		})
	}
	return gonacos.NewNacosClient(gonacos.Conf{
		Servers:     si,
		Username:    bc.Nacos.Username,
		Password:    bc.Nacos.Password,
		NamespaceId: bc.Nacos.NamespaceId,
		GroupId:     bc.Nacos.GroupId,
		DataId:      bc.Nacos.DataId,
	})
}
