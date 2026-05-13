package conf

import (
	"log"

	gonacos "gitlab.gainetics.io/shared/go-common/go-nacos-cli"
)

var (
	// Data 应用配置数据，现在由go-nacos-cli管理
	Data ConfigData

	// ConfigContainer 线程安全的配置容器
	ConfigContainer *gonacos.ThreadSafeConfig

	// NacosListener 高级配置监听器
	NacosListener *gonacos.AdvancedConfigListener
)

// InitConfigContainer 初始化配置容器
func InitConfigContainer() {
	ConfigContainer = gonacos.NewThreadSafeConfig(&Data)
}

// GetData 获取当前配置数据指针
func GetData() *ConfigData {
	if ConfigContainer == nil {
		return &Data
	}

	if config := ConfigContainer.Get(); config != nil {
		if configData, ok := config.(*ConfigData); ok {
			return configData
		}
	}
	return &Data
}

// GetServer 获取服务器配置
func GetServer() *Server {
	data := GetData()
	if data != nil {
		return &data.Server
	}
	// 返回默认的空服务器配置
	defaultConfig := ConfigData{}
	return &defaultConfig.Server
}

// GetDataCopy 获取配置数据的副本
func GetDataCopy() ConfigData {
	data := GetData()
	if data != nil {
		return *data
	}
	return ConfigData{}
}

// ConfigChangeCallback Nacos配置变更回调函数
func ConfigChangeCallback(namespace, group, dataId, data string, target interface{}) error {
	log.Printf("🔄 [Nacos配置监听] 检测到配置变更")

	if configData, ok := target.(*ConfigData); ok {
		// 记录配置变更前的值
		oldServerName := Data.Server.Name
		oldServerEnv := Data.Server.Env
		oldServerPort := Data.Server.HTTP.Addr

		// 通过线程安全容器更新配置
		if ConfigContainer != nil {
			ConfigContainer.Set(configData)
		}

		// 同时更新全局变量（为了向后兼容）
		Data = *configData

		// 打印重新绑定后的配置值
		log.Printf("✅ [配置绑定成功] 重新绑定后的配置值:")
		log.Printf("   🏷️  服务名称: %s (原值: %s)", configData.Server.Name, oldServerName)
		log.Printf("   🌍 环境标识: %s (原值: %s)", configData.Server.Env, oldServerEnv)
		log.Printf("   🌐 HTTP地址: %s (原值: %s)", configData.Server.HTTP.Addr, oldServerPort)

		// 如果有gRPC客户端配置，也打印出来
		if configData.GrpcClients != nil {
			if configData.GrpcClients.IamService != nil {
				log.Printf("   🔐 IAM服务gRPC: %s (超时: %s)",
					configData.GrpcClients.IamService.Endpoint,
					configData.GrpcClients.IamService.Timeout)
			}
		}

		// 打印白名单数量
		if len(configData.WhiteList) > 0 {
			log.Printf("   🛡️  路由白名单: %d个路径", len(configData.WhiteList))
		}

		log.Printf("🎉 [配置更新完成] Nacos配置监听和绑定成功!")
	}
	return nil
}

// ConfigData nacos 上对应配置结构体
type ConfigData struct {
	Server Server `json:"server"`

	// WhiteList 路由白名单
	WhiteList []string `json:"white_list" yaml:"white_list"`

	// GrpcClients 各种gRPC客户端配置
	GrpcClients *GrpcClients `yaml:"grpc_clients" json:"grpc_clients"`

	// Kafka 配置
	Kafka *KafkaConfig `json:"kafka" yaml:"kafka"`
}

// KafkaConfig Kafka配置结构
type KafkaConfig struct {
	// Brokers Kafka集群的broker地址列表
	Brokers []string `json:"brokers" yaml:"brokers"`
	// Group 消费者组名称
	Group string `json:"group" yaml:"group"`
	// SASL认证配置
	Sasl *KafkaSaslConfig `json:"sasl" yaml:"sasl"`
	// TLS配置
	Tls       *KafkaTlsConfig `json:"tls" yaml:"tls"`
	BatchSize int             `json:"batch_size" yaml:"batch_size"`
	// ChromeInterceptDetectTopic Chrome拦截探测topic
	ChromeInterceptDetectTopic string `json:"chrome_intercept_detect_topic" yaml:"chrome_intercept_detect_topic"`
}

// KafkaSaslConfig SASL认证配置
type KafkaSaslConfig struct {
	Enable   bool   `json:"enable" yaml:"enable"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// KafkaTlsConfig TLS配置
type KafkaTlsConfig struct {
	EnableTLS  bool   `json:"enable_tls" yaml:"enable_tls"`
	SkipVerify bool   `json:"skip_verify" yaml:"skip_verify"`
	ServerName string `json:"server_name" yaml:"server_name"`
	CertFile   string `json:"cert_file" yaml:"cert_file"`
	KeyFile    string `json:"key_file" yaml:"key_file"`
	CAFile     string `json:"ca_file" yaml:"ca_file"`
}

type Server struct {
	Name    string `json:"name" yaml:"name"`       // 服务名称
	Version string `json:"version" yaml:"version"` // 服务版本
	Env     string `json:"env" yaml:"env"`         // 环境标识 (development/staging/production)
	HTTP    struct {
		Addr    string `json:"addr" yaml:"addr"`
		Timeout int64  `json:"timeout" yaml:"timeout"`
		Network string `json:"network" yaml:"network"`
	} `json:"http" yaml:"http"`
	Grpc struct {
		Addr    string `json:"addr" yaml:"addr"`
		Timeout int64  `json:"timeout" yaml:"timeout"`
		Network string `json:"network" yaml:"network"`
	} `json:"grpc" yaml:"grpc"`
}

// GrpcClients 定义所有gRPC客户端配置
type GrpcClients struct {
	// IamService IAM服务客户端配置
	IamService *GrpcClientConfig `yaml:"iam_service" json:"iam_service"`
}

// GrpcClientConfig 定义单个gRPC客户端配置
type GrpcClientConfig struct {
	// Endpoint 服务发现地址，例如：discovery:///observable.user.service
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	// Timeout 请求超时时间，格式：10s, 1m 等
	Timeout string `yaml:"timeout" json:"timeout"`
}
