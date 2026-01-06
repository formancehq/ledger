package otlplogs

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/fx"
)

func LoadOTLPLogsGRPCExporter(options ...otlploggrpc.Option) (log.Exporter, error) {
	return otlploggrpc.New(context.Background(), options...)
}

func ProvideOTLPLogsGRPCExporter() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPLogsGRPCExporter, fx.ParamTags(OTLPLogsGRPCOptionsKey), fx.As(new(log.Exporter))),
		),
	)
}

const OTLPLogsGRPCOptionsKey = `group:"_otlpLogsGrpcOptions"`

func ProvideOTLPLogsGRPCOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPLogsGRPCOptionsKey), fx.As(new(otlploggrpc.Option))),
	)
}
