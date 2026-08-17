package ctrl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const (
	historyTemporalityEffective = "effective"
	historyTemporalityInsertion = "insertion"
	historyTemporalityUnknown   = "unknown"

	historyFilterUnfiltered = "unfiltered"
	historyFilterFiltered   = "filtered"
	historyFilterGrouped    = "grouped"

	historyErrorBuilding          = "history_building"
	historyErrorBehind            = "history_behind"
	historyErrorSourceMissing     = "history_source_missing"
	historyErrorCorrupt           = "history_corrupt"
	historyErrorUnsupportedFilter = "unsupported_temporal_filter"
	historyErrorCanceled          = "canceled"
	historyErrorDeadlineExceeded  = "deadline_exceeded"
	historyErrorOther             = "other"
)

type historicalBalanceAggregateMetrics struct {
	requests metric.Int64Counter
	errors   metric.Int64Counter
	duration metric.Int64Histogram
}

type historicalBalanceAggregateObservation struct {
	started time.Time
	attrs   []attribute.KeyValue
}

func newHistoricalBalanceAggregateMetrics(meter metric.Meter) (historicalBalanceAggregateMetrics, error) {
	requests, err := meter.Int64Counter(
		"ctrl.historical_balance.aggregate.requests",
		metric.WithDescription("Historical balance aggregate volume requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return historicalBalanceAggregateMetrics{}, fmt.Errorf("creating historical balance aggregate request counter: %w", err)
	}

	errorsCounter, err := meter.Int64Counter(
		"ctrl.historical_balance.aggregate.errors",
		metric.WithDescription("Historical balance aggregate volume failures by bounded error category"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return historicalBalanceAggregateMetrics{}, fmt.Errorf("creating historical balance aggregate error counter: %w", err)
	}

	duration, err := meter.Int64Histogram(
		"ctrl.historical_balance.aggregate.duration",
		metric.WithDescription("End-to-end historical balance aggregate volume duration"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 5000, 10000, 25000, 50000,
			100000, 250000, 500000, 1000000, 2500000, 5000000,
		),
	)
	if err != nil {
		return historicalBalanceAggregateMetrics{}, fmt.Errorf("creating historical balance aggregate duration histogram: %w", err)
	}

	return historicalBalanceAggregateMetrics{
		requests: requests,
		errors:   errorsCounter,
		duration: duration,
	}, nil
}

func (m historicalBalanceAggregateMetrics) start(
	ctx context.Context,
	span trace.Span,
	selector HistoricalBalanceSelector,
	filter *commonpb.QueryFilter,
	opts query.AggregateOptions,
) historicalBalanceAggregateObservation {
	attrs := []attribute.KeyValue{
		attribute.String("temporality", historicalBalanceTemporalityName(selector.Temporality)),
		attribute.String("filter_shape", historicalBalanceFilterShape(filter, opts)),
	}
	m.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
	span.SetAttributes(
		attribute.String("read.mode", "historical_balance"),
		attribute.String("historical_balance.temporality", attrs[0].Value.AsString()),
		attribute.String("historical_balance.filter_shape", attrs[1].Value.AsString()),
	)

	return historicalBalanceAggregateObservation{
		started: time.Now(),
		attrs:   attrs,
	}
}

func (m historicalBalanceAggregateMetrics) finish(
	ctx context.Context,
	span trace.Span,
	observation historicalBalanceAggregateObservation,
	err error,
) {
	m.duration.Record(ctx, time.Since(observation.started).Microseconds(), metric.WithAttributes(observation.attrs...))
	if err == nil {
		span.SetStatus(codes.Ok, "")

		return
	}

	category := historicalBalanceErrorCategory(err)
	errorAttrs := append([]attribute.KeyValue(nil), observation.attrs...)
	errorAttrs = append(errorAttrs, attribute.String("error_category", category))
	m.errors.Add(ctx, 1, metric.WithAttributes(errorAttrs...))
	span.SetAttributes(attribute.String("historical_balance.error_category", category))
	// Keep the span status description bounded. The underlying error may carry
	// source paths or other operational detail that must not become telemetry
	// dimensions.
	span.SetStatus(codes.Error, category)
}

func historicalBalanceTemporalityName(temporality balancehistorystore.Temporality) string {
	switch temporality {
	case balancehistorystore.TemporalityEffective:
		return historyTemporalityEffective
	case balancehistorystore.TemporalityInsertion:
		return historyTemporalityInsertion
	default:
		return historyTemporalityUnknown
	}
}

func historicalBalanceFilterShape(filter *commonpb.QueryFilter, opts query.AggregateOptions) string {
	if len(opts.GroupByPrefixes) > 0 {
		return historyFilterGrouped
	}
	if filter == nil {
		return historyFilterUnfiltered
	}

	return historyFilterFiltered
}

func historicalBalanceErrorCategory(err error) string {
	if errors.Is(err, context.Canceled) {
		return historyErrorCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return historyErrorDeadlineExceeded
	}

	var building *balancehistorystore.ErrBuilding
	if errors.As(err, &building) {
		return historyErrorBuilding
	}
	var behind *balancehistorystore.ErrBehind
	if errors.As(err, &behind) {
		return historyErrorBehind
	}
	var missing *balancehistorystore.ErrSourceMissing
	if errors.As(err, &missing) {
		return historyErrorSourceMissing
	}
	var corrupt *balancehistorystore.ErrCorrupt
	if errors.As(err, &corrupt) {
		return historyErrorCorrupt
	}
	var unsupportedFormat *balancehistorystore.ErrUnsupportedFormat
	if errors.As(err, &unsupportedFormat) {
		return historyErrorCorrupt
	}
	var unsupportedReducer *balancehistorystore.ErrUnsupportedReducer
	if errors.As(err, &unsupportedReducer) {
		return historyErrorCorrupt
	}
	var unsupportedFilter *balancehistorystore.ErrUnsupportedTemporalFilter
	if errors.As(err, &unsupportedFilter) {
		return historyErrorUnsupportedFilter
	}

	return historyErrorOther
}
