package otlpmetrics

import (
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
)

func LoadStdoutMetricsProvider() (sdkmetric.Exporter, error) {
	return stdoutmetric.New()
}

func StdoutMetricsModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadStdoutMetricsProvider, fx.As(new(sdkmetric.Exporter))),
		),
	)
}
