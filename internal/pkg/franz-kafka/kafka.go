package franz_kafka

import "github.com/google/wire"

// ProviderSet is kafka providers.
var ProviderSet = wire.NewSet(NewKafkaProducer)

// SaslConfig SASL认证配置
type SaslConfig struct {
	Enable   bool   // 是否启用SASL认证
	Username string // SASL用户名
	Password string // SASL密码
}
