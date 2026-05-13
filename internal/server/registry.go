package server

import (
	"detect-agent/internal/conf"
	"detect-agent/internal/pkg/utils"

	"github.com/go-kratos/kratos/contrib/registry/nacos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// createNacosClient 创建 Nacos 命名服务客户端（注册器和发现器共用）
func createNacosClient(data *conf.Nacos, helper *log.Helper) (naming_client.INamingClient, error) {
	// 客户端配置
	cc := constant.ClientConfig{
		NamespaceId:          data.NamespaceId,
		TimeoutMs:            10000, // 超时时间10秒
		NotLoadCacheAtStart:  true,
		Username:             data.Username,
		Password:             data.Password,
		LogDir:               "./logs/nacos",
		CacheDir:             "./cache/nacos",
		LogLevel:             "info", // 生产环境建议使用 info 级别
		UpdateCacheWhenEmpty: true,   // 当服务列表为空时也更新缓存
	}

	// 服务器配置
	sc, err := utils.ParseNacosServerAddr(data.Addr)
	if err != nil {
		helper.Errorf("❌ 解析Nacos服务地址失败: %v", err)
		return nil, err
	}

	helper.Infof("📡 正在连接Nacos: addr=%s:%d, namespace=%s, group=%s",
		data.Addr, data.Port, data.NamespaceId, data.GroupId)

	// 创建客户端
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)

	if err != nil {
		helper.Errorf("❌ 创建Nacos客户端失败: %v", err)
		return nil, err
	}

	helper.Info("✅ Nacos客户端创建成功")
	return client, nil
}

// NewRegistryEngine 创建服务注册器
func NewRegistryEngine(data *conf.Nacos, logger log.Logger) registry.Registrar {
	helper := log.NewHelper(log.With(logger, "module", "server/registry"))
	helper.Info("🔧 [服务注册器] 初始化中...")

	// 创建 Nacos 客户端
	client, err := createNacosClient(data, helper)
	if err != nil {
		panic(err)
	}

	// 使用选项配置注册器
	opts := []nacos.Option{
		nacos.WithGroup(data.GroupId), // 使用配置中的组ID
		// 可选配置：
		// nacos.WithWeight(100),         // 设置权重
		// nacos.WithCluster("DEFAULT"),  // 指定集群
	}

	registrar := nacos.New(client, opts...)
	helper.Info("📝 [服务注册器] 注册器实例已创建，Kratos将在app.Run()时自动注册服务")

	return registrar
}

// NewDiscoveryEngine 创建服务发现器
func NewDiscoveryEngine(data *conf.Nacos, logger log.Logger) registry.Discovery {
	helper := log.NewHelper(log.With(logger, "module", "server/discovery"))
	helper.Info("🔍 [服务发现器] 初始化中...")

	// 创建 Nacos 客户端
	client, err := createNacosClient(data, helper)
	if err != nil {
		panic(err)
	}

	// 使用选项配置发现器（与注册器使用相同的 GroupId）
	opts := []nacos.Option{
		nacos.WithGroup(data.GroupId), // 必须与注册器使用相同的组ID
		// 可选配置：
		// nacos.WithCluster("DEFAULT"),  // 指定集群
	}

	discovery := nacos.New(client, opts...)
	helper.Info("📡 [服务发现器] 发现器实例已创建，可用于服务发现")

	return discovery
}
