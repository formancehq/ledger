package metrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
)

type GlobalMetricsRegistry interface {
	APILatencies() instrument.Int64Histogram
	StatusCodes() instrument.Int64Counter
	ActiveLedgers() instrument.Int64UpDownCounter
}

type globalMetricsRegistry struct {
	// API Latencies
	aPILatencies  instrument.Int64Histogram
	statusCodes   instrument.Int64Counter
	activeLedgers instrument.Int64UpDownCounter
}

func RegisterGlobalMetricsRegistry(meterProvider metric.MeterProvider) (GlobalMetricsRegistry, error) {
	meter := meterProvider.Meter("global")

	apiLatencies, err := meter.Int64Histogram(
		"ledger.api.time",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Latency of API calls"),
	)
	if err != nil {
		return nil, err
	}

	statusCodes, err := meter.Int64Counter(
		"ledger.api.status",
		instrument.WithUnit("1"),
		instrument.WithDescription("Status codes of API calls"),
	)
	if err != nil {
		return nil, err
	}

	activeLedgers, err := meter.Int64UpDownCounter(
		"ledger.api.ledgers",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of active ledgers"),
	)
	if err != nil {
		return nil, err
	}

	return &globalMetricsRegistry{
		aPILatencies:  apiLatencies,
		statusCodes:   statusCodes,
		activeLedgers: activeLedgers,
	}, nil
}

func (gm *globalMetricsRegistry) APILatencies() instrument.Int64Histogram {
	return gm.aPILatencies
}

func (gm *globalMetricsRegistry) StatusCodes() instrument.Int64Counter {
	return gm.statusCodes
}

func (gm *globalMetricsRegistry) ActiveLedgers() instrument.Int64UpDownCounter {
	return gm.activeLedgers
}

type PerLedgerMetricsRegistry interface {
	CacheMisses() instrument.Int64Counter
	CacheNumberEntries() instrument.Int64UpDownCounter

	QueryLatencies() instrument.Int64Histogram
	QueryInboundLogs() instrument.Int64Counter
	QueryPendingMessages() instrument.Int64Counter
	QueryProcessedLogs() instrument.Int64Counter
}

type perLedgerMetricsRegistry struct {
	cacheMisses        instrument.Int64Counter
	cacheNumberEntries instrument.Int64UpDownCounter

	queryLatencies       instrument.Int64Histogram
	queryInboundLogs     instrument.Int64Counter
	queryPendingMessages instrument.Int64Counter
	queryProcessedLogs   instrument.Int64Counter
}

func RegisterPerLedgerMetricsRegistry(ledger string) (PerLedgerMetricsRegistry, error) {
	// we can now use the global meter provider to create a meter
	// since it was created by the fx
	meter := global.MeterProvider().Meter(ledger)

	cacheMisses, err := meter.Int64Counter(
		"ledger.cache.misses",
		instrument.WithUnit("1"),
		instrument.WithDescription("Cache misses"),
	)
	if err != nil {
		return nil, err
	}

	cacheNumberEntries, err := meter.Int64UpDownCounter(
		"ledger.cache.pending_entries",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of entries in the cache"),
	)
	if err != nil {
		return nil, err
	}

	queryLatencies, err := meter.Int64Histogram(
		"ledger.query.time",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Latency of queries processing logs"),
	)
	if err != nil {
		return nil, err
	}

	queryInboundLogs, err := meter.Int64Counter(
		"ledger.query.inbound_logs",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of inbound logs in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	queryPendingMessages, err := meter.Int64Counter(
		"ledger.query.pending_messages",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of pending messages in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	queryProcessedLogs, err := meter.Int64Counter(
		"ledger.query.processed_logs",
		instrument.WithUnit("1"),
		instrument.WithDescription("Number of processed logs in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	return &perLedgerMetricsRegistry{
		cacheMisses:          cacheMisses,
		cacheNumberEntries:   cacheNumberEntries,
		queryLatencies:       queryLatencies,
		queryInboundLogs:     queryInboundLogs,
		queryPendingMessages: queryPendingMessages,
		queryProcessedLogs:   queryProcessedLogs,
	}, nil
}

func (pm *perLedgerMetricsRegistry) CacheMisses() instrument.Int64Counter {
	return pm.cacheMisses
}

func (pm *perLedgerMetricsRegistry) CacheNumberEntries() instrument.Int64UpDownCounter {
	return pm.cacheNumberEntries
}

func (pm *perLedgerMetricsRegistry) QueryLatencies() instrument.Int64Histogram {
	return pm.queryLatencies
}

func (pm *perLedgerMetricsRegistry) QueryInboundLogs() instrument.Int64Counter {
	return pm.queryInboundLogs
}

func (pm *perLedgerMetricsRegistry) QueryPendingMessages() instrument.Int64Counter {
	return pm.queryPendingMessages
}

func (pm *perLedgerMetricsRegistry) QueryProcessedLogs() instrument.Int64Counter {
	return pm.queryProcessedLogs
}

type NoOpMetricsRegistry struct{}

func NewNoOpMetricsRegistry() *NoOpMetricsRegistry {
	return &NoOpMetricsRegistry{}
}

func (nm *NoOpMetricsRegistry) CacheMisses() instrument.Int64Counter {
	counter, _ := metric.NewNoopMeter().Int64Counter("cache_misses")
	return counter
}

func (nm *NoOpMetricsRegistry) CacheNumberEntries() instrument.Int64UpDownCounter {
	counter, _ := metric.NewNoopMeter().Int64UpDownCounter("cache_number_entries")
	return counter
}

func (nm *NoOpMetricsRegistry) QueryLatencies() instrument.Int64Histogram {
	histogram, _ := metric.NewNoopMeter().Int64Histogram("query_latencies")
	return histogram
}

func (nm *NoOpMetricsRegistry) QueryInboundLogs() instrument.Int64Counter {
	counter, _ := metric.NewNoopMeter().Int64Counter("query_inbound_logs")
	return counter
}

func (nm *NoOpMetricsRegistry) QueryPendingMessages() instrument.Int64Counter {
	counter, _ := metric.NewNoopMeter().Int64Counter("query_pending_messages")
	return counter
}

func (nm *NoOpMetricsRegistry) QueryProcessedLogs() instrument.Int64Counter {
	counter, _ := metric.NewNoopMeter().Int64Counter("query_processed_logs")
	return counter
}

func (nm *NoOpMetricsRegistry) APILatencies() instrument.Int64Histogram {
	histogram, _ := metric.NewNoopMeter().Int64Histogram("api_latencies")
	return histogram
}

func (nm *NoOpMetricsRegistry) StatusCodes() instrument.Int64Counter {
	counter, _ := metric.NewNoopMeter().Int64Counter("status_codes")
	return counter
}

func (nm *NoOpMetricsRegistry) ActiveLedgers() instrument.Int64UpDownCounter {
	counter, _ := metric.NewNoopMeter().Int64UpDownCounter("active_ledgers")
	return counter
}
