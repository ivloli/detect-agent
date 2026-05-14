package server

import (
	franz_kafka "detect-agent/internal/pkg/franz-kafka"

	"github.com/google/wire"
)

// ProviderSet is server providers.
var ProviderSet = wire.NewSet(
	NewGRPCServer,
	NewHTTPServer,
	NewRegistryEngine,
	NewDiscoveryEngine,
	NewKafkaProducerConfig,
	franz_kafka.ProviderSet,
)
