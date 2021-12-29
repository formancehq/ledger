package opentelemetrytraces

import (
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

func LoadNoOpTracerProvider() trace.TracerProvider {
	return trace.NewNoopTracerProvider()
}

func NoOpTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(LoadNoOpTracerProvider),
	)
}
