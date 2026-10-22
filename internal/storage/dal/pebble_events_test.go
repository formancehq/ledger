package dal

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type blockingPebbleLogger struct {
	logging.Logger

	entered      chan struct{}
	release      chan struct{}
	once         sync.Once
	debugEnabled bool
	logs         chan string
	fields       map[string]any
	errorLogs    chan capturedPebbleErrorLog
}

type capturedPebbleErrorLog struct {
	message string
	fields  map[string]any
}

type recordingPebbleLogger struct {
	logging.Logger

	events       chan string
	debugEnabled bool
}

func (l *recordingPebbleLogger) WithFields(map[string]any) logging.Logger { return l }
func (l *recordingPebbleLogger) Enabled(level logging.Level) bool {
	return level != logging.DebugLevel || l.debugEnabled
}
func (l *recordingPebbleLogger) Infof(format string, args ...any) {
	l.events <- "info: " + fmt.Sprintf(format, args...)
}
func (l *recordingPebbleLogger) Debugf(format string, args ...any) {
	l.events <- "debug: " + fmt.Sprintf(format, args...)
}
func (l *recordingPebbleLogger) Errorf(format string, args ...any) {
	l.events <- "error: " + fmt.Sprintf(format, args...)
}

func TestPebbleEventLogLevelsAndFailureDeduplication(t *testing.T) {
	t.Parallel()

	logger := &recordingPebbleLogger{Logger: logging.NopZap(), events: make(chan string, 10), debugEnabled: true}
	listener := NewPebbleEventLogger(logger, "test")
	listener.CompactionBegin(pebble.CompactionInfo{})
	listener.CompactionBegin(pebble.CompactionInfo{Input: []pebble.LevelInfo{{Tables: []pebble.TableInfo{{Size: 1 << 30}}}}})
	listener.CompactionEnd(pebble.CompactionInfo{})
	listener.CompactionEnd(pebble.CompactionInfo{TotalDuration: 10 * time.Second})
	listener.CompactionEnd(pebble.CompactionInfo{Input: []pebble.LevelInfo{{Tables: []pebble.TableInfo{{Size: 1 << 30}}}}})
	listener.CompactionEnd(pebble.CompactionInfo{Input: []pebble.LevelInfo{{Tables: []pebble.TableInfo{{Size: 1 << 30}}}}, Err: pebble.ErrCancelledCompaction})
	listener.CompactionEnd(pebble.CompactionInfo{Err: errors.New("compaction failure")})
	listener.FlushBegin(pebble.FlushInfo{})
	listener.FlushEnd(pebble.FlushInfo{Err: errors.New("pebble: empty table")})
	listener.FlushEnd(pebble.FlushInfo{TotalDuration: 10 * time.Second})
	listener.FlushEnd(pebble.FlushInfo{Err: errors.New("flush failure")})
	listener.BackgroundError(errors.New("background failure"))

	for _, expected := range []string{
		"debug: Pebble compaction started",
		"info: Pebble compaction started",
		"debug: Pebble compaction completed",
		"info: Pebble compaction completed",
		"info: Pebble compaction completed",
		"info: Pebble compaction cancelled",
		"debug: Pebble flush started",
		"debug: Pebble flush completed",
		"info: Pebble slow flush completed",
		"error: Pebble background error",
	} {
		select {
		case actual := <-logger.events:
			require.Equal(t, expected, actual)
		case <-time.After(time.Second):
			t.Fatalf("missing Pebble log event %q", expected)
		}
	}
}

func (l *blockingPebbleLogger) WithFields(fields map[string]any) logging.Logger {
	l.fields = fields

	return l
}
func (l *blockingPebbleLogger) Enabled(level logging.Level) bool {
	return level != logging.DebugLevel || l.debugEnabled
}

func (l *blockingPebbleLogger) blockf(format string, args ...any) {
	l.once.Do(func() { close(l.entered) })
	<-l.release
	if l.logs != nil {
		l.logs <- fmt.Sprintf(format, args...)
	}
}

func (l *blockingPebbleLogger) Infof(format string, args ...any)  { l.blockf(format, args...) }
func (l *blockingPebbleLogger) Debugf(format string, args ...any) { l.blockf(format, args...) }
func (l *blockingPebbleLogger) Errorf(format string, args ...any) {
	l.blockf(format, args...)
	if l.errorLogs != nil {
		l.errorLogs <- capturedPebbleErrorLog{message: fmt.Sprintf(format, args...), fields: l.fields}
	}
}

func TestPebbleEventLoggingDoesNotBlockCallbacks(t *testing.T) {
	t.Parallel()

	logger := &blockingPebbleLogger{
		Logger:       logging.NopZap(),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		debugEnabled: true,
	}
	defer func() {
		select {
		case <-logger.release:
		default:
			close(logger.release)
		}
	}()
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
			listener.CompactionBegin(pebble.CompactionInfo{})
			listener.CompactionEnd(pebble.CompactionInfo{})
			listener.FlushBegin(pebble.FlushInfo{})
			listener.FlushEnd(pebble.FlushInfo{})
			listener.BackgroundError(errors.New("background failure"))
		}
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Pebble event callback blocked on logging")
	}
}

func TestDisabledPebbleDebugEventsDoNotCrowdErrors(t *testing.T) {
	t.Parallel()

	logger := &blockingPebbleLogger{
		Logger:  logging.NopZap(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		logs:    make(chan string, 2),
	}
	defer func() {
		select {
		case <-logger.release:
		default:
			close(logger.release)
		}
	}()
	listener := NewPebbleEventLogger(logger, "test")
	listener.DiskSlow(pebble.DiskSlowInfo{})
	select {
	case <-logger.entered:
	case <-time.After(time.Second):
		t.Fatal("disk-slow logger was not called")
	}
	for range 64 {
		listener.CompactionBegin(pebble.CompactionInfo{})
		listener.CompactionEnd(pebble.CompactionInfo{})
		listener.FlushBegin(pebble.FlushInfo{})
		listener.FlushEnd(pebble.FlushInfo{})
	}
	listener.BackgroundError(errors.New("background failure"))
	close(logger.release)
	for _, expected := range []string{"Pebble disk operation slow", "Pebble background error"} {
		select {
		case actual := <-logger.logs:
			require.Equal(t, expected, actual)
		case <-time.After(time.Second):
			t.Fatalf("missing Pebble log event %q", expected)
		}
	}
}

func TestPebbleEventQueueReportsDroppedLogs(t *testing.T) {
	t.Parallel()

	logger := &blockingPebbleLogger{
		Logger:    logging.NopZap(),
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
		errorLogs: make(chan capturedPebbleErrorLog, 1),
	}
	defer func() {
		select {
		case <-logger.release:
		default:
			close(logger.release)
		}
	}()
	listener := NewPebbleEventLogger(logger, "test")
	listener.DiskSlow(pebble.DiskSlowInfo{})
	select {
	case <-logger.entered:
	case <-time.After(time.Second):
		t.Fatal("disk-slow logger was not called")
	}
	for range 64 {
		listener.DiskSlow(pebble.DiskSlowInfo{})
	}
	close(logger.release)
	select {
	case record := <-logger.errorLogs:
		require.Equal(t, "Pebble event logs dropped", record.message)
		require.Equal(t, uint64(32), record.fields["droppedEvents"])
	case <-time.After(time.Second):
		t.Fatal("dropped Pebble event logs were not reported")
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
