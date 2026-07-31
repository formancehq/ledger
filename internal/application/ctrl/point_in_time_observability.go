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
	pitAxisEffective = "effective"
	pitAxisInsertion = "insertion"
	pitAxisUnknown   = "unknown"

	pitFilterUnfiltered = "unfiltered"
	pitFilterFiltered   = "filtered"
	pitFilterGrouped    = "grouped"

	pitErrorBuilding          = "history_building"
	pitErrorBehind            = "history_behind"
	pitErrorExpired           = "history_expired"
	pitErrorSourceMissing     = "history_source_missing"
	pitErrorCorrupt           = "history_corrupt"
	pitErrorUnsupportedFilter = "unsupported_temporal_filter"
	pitErrorCanceled          = "canceled"
	pitErrorDeadlineExceeded  = "deadline_exceeded"
	pitErrorOther             = "other"
)

type pointInTimeAggregateMetrics struct {
	requests metric.Int64Counter
	errors   metric.Int64Counter
	duration metric.Int64Histogram
}

type pointInTimeAggregateObservation struct {
	started time.Time
	attrs   []attribute.KeyValue
}

func newPointInTimeAggregateMetrics(meter metric.Meter) (pointInTimeAggregateMetrics, error) {
	requests, err := meter.Int64Counter(
		"ctrl.point_in_time.aggregate.requests",
		metric.WithDescription("Point-in-time aggregate volume requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return pointInTimeAggregateMetrics{}, fmt.Errorf("creating PIT aggregate request counter: %w", err)
	}

	errorsCounter, err := meter.Int64Counter(
		"ctrl.point_in_time.aggregate.errors",
		metric.WithDescription("Point-in-time aggregate volume failures by bounded error category"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return pointInTimeAggregateMetrics{}, fmt.Errorf("creating PIT aggregate error counter: %w", err)
	}

	duration, err := meter.Int64Histogram(
		"ctrl.point_in_time.aggregate.duration",
		metric.WithDescription("End-to-end point-in-time aggregate volume duration"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 5000, 10000, 25000, 50000,
			100000, 250000, 500000, 1000000, 2500000, 5000000,
		),
	)
	if err != nil {
		return pointInTimeAggregateMetrics{}, fmt.Errorf("creating PIT aggregate duration histogram: %w", err)
	}

	return pointInTimeAggregateMetrics{
		requests: requests,
		errors:   errorsCounter,
		duration: duration,
	}, nil
}

func (m pointInTimeAggregateMetrics) start(
	ctx context.Context,
	span trace.Span,
	selector PointInTimeSelector,
	filter *commonpb.QueryFilter,
	opts query.AggregateOptions,
) pointInTimeAggregateObservation {
	attrs := []attribute.KeyValue{
		attribute.String("axis", pointInTimeAxisName(selector.Axis)),
		attribute.String("filter_shape", pointInTimeFilterShape(filter, opts)),
	}
	m.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
	span.SetAttributes(
		attribute.String("read.mode", "point_in_time"),
		attribute.String("point_in_time.axis", attrs[0].Value.AsString()),
		attribute.String("point_in_time.filter_shape", attrs[1].Value.AsString()),
	)

	return pointInTimeAggregateObservation{
		started: time.Now(),
		attrs:   attrs,
	}
}

func (m pointInTimeAggregateMetrics) finish(
	ctx context.Context,
	span trace.Span,
	observation pointInTimeAggregateObservation,
	err error,
) {
	m.duration.Record(ctx, time.Since(observation.started).Microseconds(), metric.WithAttributes(observation.attrs...))
	if err == nil {
		span.SetStatus(codes.Ok, "")

		return
	}

	category := pointInTimeErrorCategory(err)
	errorAttrs := append([]attribute.KeyValue(nil), observation.attrs...)
	errorAttrs = append(errorAttrs, attribute.String("error_category", category))
	m.errors.Add(ctx, 1, metric.WithAttributes(errorAttrs...))
	span.SetAttributes(attribute.String("point_in_time.error_category", category))
	// Keep the span status description bounded. The underlying error may carry
	// source paths or other operational detail that must not become telemetry
	// dimensions.
	span.SetStatus(codes.Error, category)
}

func pointInTimeAxisName(axis balancehistorystore.Axis) string {
	switch axis {
	case balancehistorystore.AxisEffective:
		return pitAxisEffective
	case balancehistorystore.AxisInsertion:
		return pitAxisInsertion
	default:
		return pitAxisUnknown
	}
}

func pointInTimeFilterShape(filter *commonpb.QueryFilter, opts query.AggregateOptions) string {
	if len(opts.GroupByPrefixes) > 0 {
		return pitFilterGrouped
	}
	if filter == nil {
		return pitFilterUnfiltered
	}

	return pitFilterFiltered
}

func pointInTimeErrorCategory(err error) string {
	if errors.Is(err, context.Canceled) {
		return pitErrorCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return pitErrorDeadlineExceeded
	}

	var building *balancehistorystore.ErrBuilding
	if errors.As(err, &building) {
		return pitErrorBuilding
	}
	var behind *balancehistorystore.ErrBehind
	if errors.As(err, &behind) {
		return pitErrorBehind
	}
	var expired *balancehistorystore.ErrExpired
	if errors.As(err, &expired) {
		return pitErrorExpired
	}
	var missing *balancehistorystore.ErrSourceMissing
	if errors.As(err, &missing) {
		return pitErrorSourceMissing
	}
	var corrupt *balancehistorystore.ErrCorrupt
	if errors.As(err, &corrupt) {
		return pitErrorCorrupt
	}
	var unsupportedFormat *balancehistorystore.ErrUnsupportedFormat
	if errors.As(err, &unsupportedFormat) {
		return pitErrorCorrupt
	}
	var unsupportedReducer *balancehistorystore.ErrUnsupportedReducer
	if errors.As(err, &unsupportedReducer) {
		return pitErrorCorrupt
	}
	var unsupportedFilter *balancehistorystore.ErrUnsupportedTemporalFilter
	if errors.As(err, &unsupportedFilter) {
		return pitErrorUnsupportedFilter
	}

	return pitErrorOther
}
