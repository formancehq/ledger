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

func TestPointInTimeAggregateMetricsUseOnlyBoundedDimensions(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	metrics, err := newPointInTimeAggregateMetrics(provider.Meter("ctrl"))
	require.NoError(t, err)

	ctx := context.Background()
	span := trace.SpanFromContext(ctx)
	effective := metrics.start(ctx, span, PointInTimeSelector{
		Axis: balancehistorystore.AxisEffective,
	}, nil, query.AggregateOptions{})
	metrics.finish(ctx, span, effective, nil)

	insertion := metrics.start(ctx, span, PointInTimeSelector{
		Axis: balancehistorystore.AxisInsertion,
	}, &commonpb.QueryFilter{}, query.AggregateOptions{GroupByPrefixes: []string{"users:"}})
	metrics.finish(ctx, span, insertion, fmt.Errorf("wrapped: %w", &balancehistorystore.ErrBehind{
		Required: 42,
		Current:  41,
	}))

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &resources))

	allowedKeys := map[attribute.Key]struct{}{
		"axis":           {},
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

	require.Len(t, metricPoints["ctrl.point_in_time.aggregate.requests"], 2)
	require.Len(t, metricPoints["ctrl.point_in_time.aggregate.duration"], 2)
	require.Len(t, metricPoints["ctrl.point_in_time.aggregate.errors"], 1)

	for _, sets := range metricPoints {
		for _, set := range sets {
			for _, attr := range set.ToSlice() {
				_, ok := allowedKeys[attr.Key]
				require.Truef(t, ok, "unexpected metric attribute %q", attr.Key)
			}
		}
	}

	requestDimensions := attributesAsStrings(metricPoints["ctrl.point_in_time.aggregate.requests"])
	require.ElementsMatch(t, []map[string]string{
		{"axis": pitAxisEffective, "filter_shape": pitFilterUnfiltered},
		{"axis": pitAxisInsertion, "filter_shape": pitFilterGrouped},
	}, requestDimensions)
	require.Equal(t, []map[string]string{{
		"axis":           pitAxisInsertion,
		"filter_shape":   pitFilterGrouped,
		"error_category": pitErrorBehind,
	}}, attributesAsStrings(metricPoints["ctrl.point_in_time.aggregate.errors"]))
}

func TestPointInTimeAggregateTraceUsesOnlyBoundedAttributes(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	metrics, err := newPointInTimeAggregateMetrics(meterProvider.Meter("ctrl"))
	require.NoError(t, err)

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	ctx, span := tracerProvider.Tracer("ctrl").Start(context.Background(), "ctrl.aggregate_volumes")

	observation := metrics.start(ctx, span, PointInTimeSelector{
		At:   123456789,
		Axis: balancehistorystore.AxisEffective,
	}, &commonpb.QueryFilter{}, query.AggregateOptions{})
	metrics.finish(ctx, span, observation, &balancehistorystore.ErrExpired{Requested: 1, Floor: 2})
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, codes.Error, ended[0].Status().Code)
	require.Equal(t, pitErrorExpired, ended[0].Status().Description)

	attrs := make(map[string]string)
	for _, attr := range ended[0].Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	require.Equal(t, map[string]string{
		"read.mode":                    "point_in_time",
		"point_in_time.axis":           pitAxisEffective,
		"point_in_time.filter_shape":   pitFilterFiltered,
		"point_in_time.error_category": pitErrorExpired,
	}, attrs)
	require.NotContains(t, attrs, "ledger")
	require.NotContains(t, attrs, "point_in_time.requested_at")
}

func TestPointInTimeErrorCategoryIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "building", err: &balancehistorystore.ErrBuilding{}, want: pitErrorBuilding},
		{name: "behind", err: &balancehistorystore.ErrBehind{}, want: pitErrorBehind},
		{name: "expired", err: &balancehistorystore.ErrExpired{}, want: pitErrorExpired},
		{name: "source missing", err: &balancehistorystore.ErrSourceMissing{}, want: pitErrorSourceMissing},
		{name: "corrupt", err: &balancehistorystore.ErrCorrupt{}, want: pitErrorCorrupt},
		{name: "quarantined", err: &balancehistorystore.ErrQuarantined{}, want: pitErrorCorrupt},
		{name: "unsupported format", err: &balancehistorystore.ErrUnsupportedFormat{}, want: pitErrorCorrupt},
		{name: "unsupported reducer", err: &balancehistorystore.ErrUnsupportedReducer{}, want: pitErrorCorrupt},
		{name: "unsupported filter", err: &balancehistorystore.ErrUnsupportedTemporalFilter{}, want: pitErrorUnsupportedFilter},
		{name: "canceled", err: context.Canceled, want: pitErrorCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: pitErrorDeadlineExceeded},
		{name: "other", err: errors.New("dynamic detail"), want: pitErrorOther},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, pointInTimeErrorCategory(fmt.Errorf("wrapped: %w", test.err)))
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
