package otlptraces

import (
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func LoadJaegerTracerExporter(options ...jaeger.CollectorEndpointOption) (*jaeger.Exporter, error) {
	return jaeger.New(jaeger.WithCollectorEndpoint(options...))
}

const (
	JaegerCollectorEndpointGroupKey = `group:"_tracerCollectorEndpointOptions"`
)

func ProvideJaegerTracerCollectorEndpoint(provider any) fx.Option {
	return fx.Provide(fx.Annotate(provider, fx.ResultTags(JaegerCollectorEndpointGroupKey)))
}

func JaegerTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadJaegerTracerExporter, fx.ParamTags(
				JaegerCollectorEndpointGroupKey,
			), fx.As(new(trace.SpanExporter))),
		),
	)
}
