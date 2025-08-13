package otlptraces

import (
	"os"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func LoadStdoutTracerProvider() (*stdouttrace.Exporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout),
	)
}

func StdoutTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadStdoutTracerProvider, fx.As(new(trace.SpanExporter))),
		),
	)
}
