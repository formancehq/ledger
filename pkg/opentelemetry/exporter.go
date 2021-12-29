package opentelemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	ServiceNameKey    = `name:"serviceName"`
	ServiceVersionKey = `name:"version"`
)

func ProvideServiceName(provider interface{}) fx.Option {
	return fx.Provide(fx.Annotate(provider, fx.ResultTags(ServiceNameKey)))
}

func ProvideServiceVersion(provider interface{}) fx.Option {
	return fx.Provide(fx.Annotate(provider, fx.ResultTags(ServiceVersionKey)))
}

func traceSdkExportModule() fx.Option {
	return fx.Options(
		fx.Provide(func(tp *tracesdk.TracerProvider) trace.TracerProvider { return tp }),
		fx.Invoke(func(lc fx.Lifecycle, tracerProvider *tracesdk.TracerProvider) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					otel.SetTracerProvider(tracerProvider)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return tracerProvider.Shutdown(ctx)
				},
			})
		}),
	)
}
