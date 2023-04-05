package metrics

import (
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
)

type SQLStorageMetricsRegistry struct {
	Latencies instrument.Int64Histogram
}

func RegisterSQLStorageMetrics(schemaName string) (*SQLStorageMetricsRegistry, error) {
	meter := global.MeterProvider().Meter(schemaName)

	latencies, err := meter.Int64Histogram(
		"ledger.storage.sql.time",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Latency of SQL calls"),
	)
	if err != nil {
		return nil, err
	}

	return &SQLStorageMetricsRegistry{
		Latencies: latencies,
	}, nil
}
