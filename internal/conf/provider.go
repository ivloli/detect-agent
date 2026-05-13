package conf

// 配置层不再直接创建Redis客户端，只提供配置数据

// ProvideConfigData returns the latest snapshot of configuration for DI.
func ProvideConfigData() ConfigData {
	return GetDataCopy()
}
