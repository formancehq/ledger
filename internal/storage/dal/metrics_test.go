package dal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestStore_RegisterMetrics(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	meter := provider.Meter("test")
	registration, err := s.RegisterMetrics(meter)
	require.NoError(t, err)
	require.NotNil(t, registration)
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.NotEmpty(t, collected.ScopeMetrics)
	require.NotEmpty(t, collected.ScopeMetrics[0].Metrics)
	require.NoError(t, registration.Unregister())
}

func TestStore_GetMetrics(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	metrics, ok := s.GetMetrics().(*servicepb.PebbleMetrics)
	require.True(t, ok)
	require.NotNil(t, metrics)
	// The envelope remains compatible; Pebble-only fields are absent.
	require.Zero(t, metrics.GetDiskSpaceUsage())
}
