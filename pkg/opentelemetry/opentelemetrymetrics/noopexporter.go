package opentelemetrymetrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
)

func LoadNoOpMeterProvider() metric.MeterProvider {
	return metric.NewNoopMeterProvider()
}

func NoOpMeterModule() fx.Option {
	return fx.Options(
		fx.Provide(LoadNoOpMeterProvider),
		metricsSdkExportModule(),
	)
}
