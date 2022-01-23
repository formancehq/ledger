package opentelemetrymetrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/fx"
)

const (
	NoOpMetricsExporter = "noop"
	OTLPMetricsExporter = "otlp"
)

func metricsSdkExportModule() fx.Option {
	return fx.Options(
		fx.Invoke(func(mp metric.MeterProvider) {
			global.SetMeterProvider(mp)
		}),
	)
}
