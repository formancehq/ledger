package opentelemetrytraces

import (
	"go.opentelemetry.io/otel/exporters/jaeger"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func LoadJaegerTracerProvider(serviceName string, version string, options ...jaeger.CollectorEndpointOption) (*tracesdk.TracerProvider, error) {
	r, err := newResource(serviceName, version)
	if err != nil {
		return nil, err
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(options...))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(r),
	)
	return tp, nil
}

const (
	JaegerCollectorEndpointGroupKey = `group:"_tracerCollectorEndpointOptions"`
)

func ProvideJaegerTracerCollectorEndpoint(provider interface{}) fx.Option {
	return fx.Provide(fx.Annotate(provider, fx.ResultTags(JaegerCollectorEndpointGroupKey)))
}

func JaegerTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadJaegerTracerProvider, fx.ParamTags(
				ServiceNameKey,
				ServiceVersionKey,
				JaegerCollectorEndpointGroupKey,
			)),
		),
		traceSdkExportModule(),
	)
}
