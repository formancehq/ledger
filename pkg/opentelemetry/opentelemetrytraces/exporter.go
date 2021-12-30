package opentelemetrytraces

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	JaegerExporter = "jaeger"
	NoOpExporter   = "noop"
	StdoutExporter = "stdout"
	OTLPExporter   = "otlp"
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
			// set global propagator to tracecontext (the default is no-op).
			otel.SetTextMapPropagator(propagation.TraceContext{})
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
