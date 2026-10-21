package dal

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestStoreEventListenerFailsOnDataCorruption(t *testing.T) {
	t.Parallel()

	logger := logging.NopZap()
	listener := newStoreEventListener(logger, NewPebbleLogger(logger), noop.NewMeterProvider().Meter("test"), NewWriteStallState())

	require.Panics(t, func() {
		listener.DataCorruption(pebble.DataCorruptionInfo{})
	})
}
