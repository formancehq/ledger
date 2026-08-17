package ctrl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

func TestHistoricalBalanceAggregateMetricsUseOnlyBoundedDimensions(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	metrics, err := newHistoricalBalanceAggregateMetrics(provider.Meter("ctrl"))
	require.NoError(t, err)

	ctx := context.Background()
	span := trace.SpanFromContext(ctx)
	effective := metrics.start(ctx, span, HistoricalBalanceSelector{
		Temporality: balancehistorystore.TemporalityEffective,
	}, nil, query.AggregateOptions{})
	metrics.finish(ctx, span, effective, nil)

	insertion := metrics.start(ctx, span, HistoricalBalanceSelector{
		Temporality: balancehistorystore.TemporalityInsertion,
	}, &commonpb.QueryFilter{}, query.AggregateOptions{GroupByPrefixes: []string{"users:"}})
	metrics.finish(ctx, span, insertion, fmt.Errorf("wrapped: %w", &balancehistorystore.ErrBehind{
		Required: 42,
		Current:  41,
	}))

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &resources))

	allowedKeys := map[attribute.Key]struct{}{
		"temporality":    {},
		"filter_shape":   {},
		"error_category": {},
	}
	metricPoints := make(map[string][]attribute.Set)
	for _, scope := range resources.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			switch data := instrument.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					metricPoints[instrument.Name] = append(metricPoints[instrument.Name], point.Attributes)
				}
			case metricdata.Histogram[int64]:
				for _, point := range data.DataPoints {
					metricPoints[instrument.Name] = append(metricPoints[instrument.Name], point.Attributes)
				}
			}
		}
	}

	require.Len(t, metricPoints["ctrl.historical_balance.aggregate.requests"], 2)
	require.Len(t, metricPoints["ctrl.historical_balance.aggregate.duration"], 2)
	require.Len(t, metricPoints["ctrl.historical_balance.aggregate.errors"], 1)

	for _, sets := range metricPoints {
		for _, set := range sets {
			for _, attr := range set.ToSlice() {
				_, ok := allowedKeys[attr.Key]
				require.Truef(t, ok, "unexpected metric attribute %q", attr.Key)
			}
		}
	}

	requestDimensions := attributesAsStrings(metricPoints["ctrl.historical_balance.aggregate.requests"])
	require.ElementsMatch(t, []map[string]string{
		{"temporality": historyTemporalityEffective, "filter_shape": historyFilterUnfiltered},
		{"temporality": historyTemporalityInsertion, "filter_shape": historyFilterGrouped},
	}, requestDimensions)
	require.Equal(t, []map[string]string{{
		"temporality":    historyTemporalityInsertion,
		"filter_shape":   historyFilterGrouped,
		"error_category": historyErrorBehind,
	}}, attributesAsStrings(metricPoints["ctrl.historical_balance.aggregate.errors"]))
}

func TestHistoricalBalanceAggregateTraceUsesOnlyBoundedAttributes(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	metrics, err := newHistoricalBalanceAggregateMetrics(meterProvider.Meter("ctrl"))
	require.NoError(t, err)

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	ctx, span := tracerProvider.Tracer("ctrl").Start(context.Background(), "ctrl.aggregate_volumes")

	observation := metrics.start(ctx, span, HistoricalBalanceSelector{
		At:          123456789,
		Temporality: balancehistorystore.TemporalityEffective,
	}, &commonpb.QueryFilter{}, query.AggregateOptions{})
	metrics.finish(ctx, span, observation, &balancehistorystore.ErrSourceMissing{Detail: "missing audit prefix"})
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, codes.Error, ended[0].Status().Code)
	require.Equal(t, historyErrorSourceMissing, ended[0].Status().Description)

	attrs := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	require.Equal(t, map[string]string{
		"read.mode":                         "historical_balance",
		"historical_balance.temporality":    historyTemporalityEffective,
		"historical_balance.filter_shape":   historyFilterFiltered,
		"historical_balance.error_category": historyErrorSourceMissing,
	}, attrs)
	require.NotContains(t, attrs, "ledger")
	require.NotContains(t, attrs, "historical_balance.requested_at")
}

func TestHistoricalBalanceErrorCategoryIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "building", err: &balancehistorystore.ErrBuilding{}, want: historyErrorBuilding},
		{name: "behind", err: &balancehistorystore.ErrBehind{}, want: historyErrorBehind},
		{name: "source missing", err: &balancehistorystore.ErrSourceMissing{}, want: historyErrorSourceMissing},
		{name: "corrupt", err: &balancehistorystore.ErrCorrupt{}, want: historyErrorCorrupt},
		{name: "quarantined", err: &balancehistorystore.ErrQuarantined{}, want: historyErrorCorrupt},
		{name: "unsupported format", err: &balancehistorystore.ErrUnsupportedFormat{}, want: historyErrorCorrupt},
		{name: "unsupported reducer", err: &balancehistorystore.ErrUnsupportedReducer{}, want: historyErrorCorrupt},
		{name: "unsupported filter", err: &balancehistorystore.ErrUnsupportedTemporalFilter{}, want: historyErrorUnsupportedFilter},
		{name: "canceled", err: context.Canceled, want: historyErrorCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: historyErrorDeadlineExceeded},
		{name: "other", err: errors.New("dynamic detail"), want: historyErrorOther},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, historicalBalanceErrorCategory(fmt.Errorf("wrapped: %w", test.err)))
		})
	}
}

func attributesAsStrings(sets []attribute.Set) []map[string]string {
	result := make([]map[string]string, 0, len(sets))
	for _, set := range sets {
		values := make(map[string]string, set.Len())
		for _, attr := range set.ToSlice() {
			values[string(attr.Key)] = attr.Value.AsString()
		}
		result = append(result, values)
	}

	return result
}
