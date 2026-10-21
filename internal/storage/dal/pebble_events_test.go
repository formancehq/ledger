package dal

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type blockingDiskSlowLogger struct {
	logging.Logger

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingDiskSlowLogger) WithFields(map[string]any) logging.Logger { return l }

func (l *blockingDiskSlowLogger) Infof(string, ...any) {
	l.once.Do(func() { close(l.entered) })
	<-l.release
}

func TestDiskSlowLoggingDoesNotBlockPebbleCallback(t *testing.T) {
	t.Parallel()

	logger := &blockingDiskSlowLogger{
		Logger:  logging.NopZap(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer close(logger.release)
	listener := NewPebbleEventLogger(logger, "test")
	listener.DiskSlow(pebble.DiskSlowInfo{Duration: time.Second})
	select {
	case <-logger.entered:
	case <-time.After(time.Second):
		t.Fatal("disk-slow logger was not called")
	}

	returned := make(chan struct{})
	go func() {
		for range 64 { // Also exercise a full, bounded handoff.
			listener.DiskSlow(pebble.DiskSlowInfo{Duration: time.Second})
		}
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("disk-slow callback blocked on logging")
	}
}

func TestStoreEventListenerFailsOnDataCorruption(t *testing.T) {
	t.Parallel()

	logger := logging.NopZap()
	listener := newStoreEventListener(logger, NewPebbleLogger(logger), noop.NewMeterProvider().Meter("test"), NewWriteStallState())

	require.Panics(t, func() {
		listener.DataCorruption(pebble.DataCorruptionInfo{})
	})
}
