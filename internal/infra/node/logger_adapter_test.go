package node

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestLoggerAdapter_ImplementsRaftLogger(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	// Verify it implements the raft.Logger interface
	var _ = adapter
}

func TestLoggerAdapter_Debug(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	// Should not panic
	adapter.Debug("hello", " ", "world")
}

func TestLoggerAdapter_Debugf(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Debugf("count: %d", 42)
}

func TestLoggerAdapter_Error(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Error("something failed")
}

func TestLoggerAdapter_Errorf(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Errorf("error: %s", "bad thing")
}

func TestLoggerAdapter_Info(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Info("info message")
}

func TestLoggerAdapter_Infof(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Infof("info: %d", 7)
}

func TestLoggerAdapter_Warning(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	// Warning routes to Infof with "WARN: " prefix
	adapter.Warning("watch out")
}

func TestLoggerAdapter_Warningf(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	adapter.Warningf("warn: %s", "danger")
}

func TestLoggerAdapter_Fatal(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	require.PanicsWithValue(t, "fatal error", func() {
		adapter.Fatal("fatal error")
	})
}

func TestLoggerAdapter_Fatalf(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	require.PanicsWithValue(t, "fatal: boom", func() {
		adapter.Fatalf("fatal: %s", "boom")
	})
}

func TestLoggerAdapter_Panic(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	require.PanicsWithValue(t, "panic now", func() {
		adapter.Panic("panic now")
	})
}

func TestLoggerAdapter_Panicf(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	adapter := NewLoggerAdapter(logger)

	require.PanicsWithValue(t, "panic: 99", func() {
		adapter.Panicf("panic: %d", 99)
	})
}
