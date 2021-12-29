package opentelemetrymetrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
)

func GetMeter(provider metric.MeterProvider) metric.Meter {
	return provider.Meter("numary.com/ledger")
}

func ProvideMeter() fx.Option {
	return fx.Provide(GetMeter)
}
