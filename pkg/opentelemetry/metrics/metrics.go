package metrics

import (
	"go.opentelemetry.io/otel"
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

type PerLedgerRegistry interface {
	CacheMisses() metric.Int64Counter
	CacheNumberEntries() metric.Int64UpDownCounter

	QueryLatencies() metric.Int64Histogram
	QueryInboundLogs() metric.Int64Counter
	QueryPendingMessages() metric.Int64Counter
	QueryProcessedLogs() metric.Int64Counter
}

type perLedgerRegistry struct {
	cacheMisses        metric.Int64Counter
	cacheNumberEntries metric.Int64UpDownCounter

	queryLatencies       metric.Int64Histogram
	queryInboundLogs     metric.Int64Counter
	queryPendingMessages metric.Int64Counter
	queryProcessedLogs   metric.Int64Counter
}

func RegisterPerLedgerMetricsRegistry(ledger string) (PerLedgerRegistry, error) {
	// we can now use the global meter provider to create a meter
	// since it was created by the fx
	meter := otel.GetMeterProvider().Meter(ledger)

	cacheMisses, err := meter.Int64Counter(
		"ledger.cache.misses",
		metric.WithUnit("1"),
		metric.WithDescription("Cache misses"),
	)
	if err != nil {
		return nil, err
	}

	cacheNumberEntries, err := meter.Int64UpDownCounter(
		"ledger.cache.pending_entries",
		metric.WithUnit("1"),
		metric.WithDescription("Number of entries in the cache"),
	)
	if err != nil {
		return nil, err
	}

	queryLatencies, err := meter.Int64Histogram(
		"ledger.query.time",
		metric.WithUnit("ms"),
		metric.WithDescription("Latency of queries processing logs"),
	)
	if err != nil {
		return nil, err
	}

	queryInboundLogs, err := meter.Int64Counter(
		"ledger.query.inbound_logs",
		metric.WithUnit("1"),
		metric.WithDescription("Number of inbound logs in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	queryPendingMessages, err := meter.Int64Counter(
		"ledger.query.pending_messages",
		metric.WithUnit("1"),
		metric.WithDescription("Number of pending messages in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	queryProcessedLogs, err := meter.Int64Counter(
		"ledger.query.processed_logs",
		metric.WithUnit("1"),
		metric.WithDescription("Number of processed logs in CQRS worker"),
	)
	if err != nil {
		return nil, err
	}

	return &perLedgerRegistry{
		cacheMisses:          cacheMisses,
		cacheNumberEntries:   cacheNumberEntries,
		queryLatencies:       queryLatencies,
		queryInboundLogs:     queryInboundLogs,
		queryPendingMessages: queryPendingMessages,
		queryProcessedLogs:   queryProcessedLogs,
	}, nil
}

func (pm *perLedgerRegistry) CacheMisses() metric.Int64Counter {
	return pm.cacheMisses
}

func (pm *perLedgerRegistry) CacheNumberEntries() metric.Int64UpDownCounter {
	return pm.cacheNumberEntries
}

func (pm *perLedgerRegistry) QueryLatencies() metric.Int64Histogram {
	return pm.queryLatencies
}

func (pm *perLedgerRegistry) QueryInboundLogs() metric.Int64Counter {
	return pm.queryInboundLogs
}

func (pm *perLedgerRegistry) QueryPendingMessages() metric.Int64Counter {
	return pm.queryPendingMessages
}

func (pm *perLedgerRegistry) QueryProcessedLogs() metric.Int64Counter {
	return pm.queryProcessedLogs
}

type noOpRegistry struct{}

func NewNoOpRegistry() *noOpRegistry {
	return &noOpRegistry{}
}

func (nm *noOpRegistry) CacheMisses() metric.Int64Counter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64Counter("cache_misses")
	return counter
}

func (nm *noOpRegistry) CacheNumberEntries() metric.Int64UpDownCounter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64UpDownCounter("cache_number_entries")
	return counter
}

func (nm *noOpRegistry) QueryLatencies() metric.Int64Histogram {
	histogram, _ := noop.NewMeterProvider().Meter("ledger").Int64Histogram("query_latencies")
	return histogram
}

func (nm *noOpRegistry) QueryInboundLogs() metric.Int64Counter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64Counter("query_inbound_logs")
	return counter
}

func (nm *noOpRegistry) QueryPendingMessages() metric.Int64Counter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64Counter("query_pending_messages")
	return counter
}

func (nm *noOpRegistry) QueryProcessedLogs() metric.Int64Counter {
	counter, _ := noop.NewMeterProvider().Meter("ledger").Int64Counter("query_processed_logs")
	return counter
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
