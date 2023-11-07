package metrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type GlobalRegistry interface {
	APILatencies() metric.Int64Histogram
	StatusCodes() metric.Int64Counter
	ActiveLedgers() metric.Int64UpDownCounter
}

type globalRegistry struct {
	// API Latencies
	apiLatencies  metric.Int64Histogram
	statusCodes   metric.Int64Counter
	activeLedgers metric.Int64UpDownCounter
}

func RegisterGlobalRegistry(meterProvider metric.MeterProvider) (GlobalRegistry, error) {
	meter := meterProvider.Meter("global")

	apiLatencies, err := meter.Int64Histogram(
		"ledger.api.time",
		metric.WithUnit("ms"),
		metric.WithDescription("Latency of API calls"),
	)
	if err != nil {
		return nil, err
	}

	statusCodes, err := meter.Int64Counter(
		"ledger.api.status",
		metric.WithUnit("1"),
		metric.WithDescription("Status codes of API calls"),
	)
	if err != nil {
		return nil, err
	}

	activeLedgers, err := meter.Int64UpDownCounter(
		"ledger.api.ledgers",
		metric.WithUnit("1"),
		metric.WithDescription("Number of active ledgers"),
	)
	if err != nil {
		return nil, err
	}

	return &globalRegistry{
		apiLatencies:  apiLatencies,
		statusCodes:   statusCodes,
		activeLedgers: activeLedgers,
	}, nil
}

func (gm *globalRegistry) APILatencies() metric.Int64Histogram {
	return gm.apiLatencies
}

func (gm *globalRegistry) StatusCodes() metric.Int64Counter {
	return gm.statusCodes
}

func (gm *globalRegistry) ActiveLedgers() metric.Int64UpDownCounter {
	return gm.activeLedgers
}

type noOpRegistry struct{}

func NewNoOpRegistry() *noOpRegistry {
	return &noOpRegistry{}
}

func (nm *noOpRegistry) APILatencies() metric.Int64Histogram {
	histogram, _ := noop.NewMeterProvider().Meter("ledger").Int64Histogram("api_latencies")
	return histogram
}

func (nm *noOpRegistry) StatusCodes() metric.Int64Counter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64Counter("status_codes")
	return counter
}

func (nm *noOpRegistry) ActiveLedgers() metric.Int64UpDownCounter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64UpDownCounter("active_ledgers")
	return counter
}
