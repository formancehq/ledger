package dal

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestStatusFromErr(t *testing.T) {
	t.Parallel()

	t.Run("nil error returns ok", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "ok", statusFromErr(nil))
	})

	t.Run("non-nil error returns error", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "error", statusFromErr(errors.New("something")))
	})
}

func TestNewMetricsListener(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	listener := NewMetricsListener(meter)
	require.NotNil(t, listener)
	require.NotNil(t, listener.FlushEnd)
	require.NotNil(t, listener.CompactionEnd)
	require.NotNil(t, listener.WriteStallBegin)
	require.NotNil(t, listener.WriteStallEnd)
}

func TestMetricsListener_Callbacks(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	listener := NewMetricsListener(meter)

	// Exercise FlushEnd callback (should not panic)
	listener.FlushEnd(pebble.FlushInfo{
		Reason:   "test",
		Duration: 42 * time.Millisecond,
	})

	// Exercise FlushEnd with error
	listener.FlushEnd(pebble.FlushInfo{
		Reason: "test-err",
		Err:    errors.New("flush failed"),
	})

	// Exercise CompactionEnd callback
	listener.CompactionEnd(pebble.CompactionInfo{
		Reason:   "test",
		Duration: 100 * time.Millisecond,
	})

	// Exercise CompactionEnd with error
	listener.CompactionEnd(pebble.CompactionInfo{
		Reason: "test-err",
		Err:    errors.New("compact failed"),
	})

	// Exercise WriteStallBegin/End cycle
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "memtable",
	})
	listener.WriteStallEnd()

	// Exercise WriteStallEnd without a preceding Begin
	listener.WriteStallEnd()

	// Exercise nested stall begin (second begin while first is still active)
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "first-stall",
	})
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "second-stall",
	})
	listener.WriteStallEnd()
}

func TestStore_GetMetrics(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	metrics := s.GetMetrics()
	require.NotNil(t, metrics)
}

func TestSinkConfigKey_Bytes(t *testing.T) {
	t.Parallel()

	k := SinkConfigKey{Name: "my-sink"}
	require.Equal(t, []byte("my-sink"), k.Bytes())
}
