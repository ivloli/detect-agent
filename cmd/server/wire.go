//go:build wireinject
// +build wireinject

package main

import (
	"detect-agent/internal/biz"
	"detect-agent/internal/conf"
	"detect-agent/internal/grpc_client"
	"detect-agent/internal/server"
	"detect-agent/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

func wireApp(*conf.Nacos, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(
		server.ProviderSet,
		biz.ProviderSet,
		service.ProviderSet,
		grpc_client.NewClient,
		server.NewKafkaServer,
		newApp,
	))
}
