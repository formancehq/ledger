package opentelemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

func commonExporterModule() fx.Option {
	return fx.Options(
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

func traceSdkExportModule() fx.Option {
	return fx.Options(
		fx.Provide(func(tp *tracesdk.TracerProvider) trace.TracerProvider { return tp }),
		commonExporterModule(),
	)
}
