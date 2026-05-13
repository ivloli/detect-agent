package franz_kafka

// SaslConfig SASL认证配置
type SaslConfig struct {
	Enable   bool   // 是否启用SASL认证
	Username string // SASL用户名
	Password string // SASL密码
}
