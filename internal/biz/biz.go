package biz

import (
	"github.com/google/wire"
)

// ==================== 依赖注入配置 ====================

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(
	// 批量探测处理器
	NewBatchDetectHandler,
)
