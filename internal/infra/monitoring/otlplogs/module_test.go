package otlplogs

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// threadSafeBuf wraps a bytes.Buffer with a mutex so the OTLP no-op
// exporter goroutine and assertions can read/write concurrently
// without tripping the race detector.
type threadSafeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *threadSafeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func TestLogger_TraceEmittedWhenLevelTrace(t *testing.T) {
	t.Parallel()

	out := &threadSafeBuf{}
	logger, err := Logger(ModuleConfig{
		Output:     out,
		Level:      logging.TraceLevel,
		FormatJSON: true,
	})
	require.NoError(t, err)

	logger.Trace("trace-record")
	logger.Tracef("trace-fmt-%d", 7)
	logger.Debug("debug-record")
	logger.Info("info-record")

	s := out.String()
	for _, want := range []string{
		`"level":"trace"`,
		`trace-record`,
		`trace-fmt-7`,
		`debug-record`,
		`info-record`,
	} {
		assert.Containsf(t, s, want, "expected %q in output:\n%s", want, s)
	}
}

func TestLogger_TraceDroppedAtDebugLevel(t *testing.T) {
	t.Parallel()

	out := &threadSafeBuf{}
	logger, err := Logger(ModuleConfig{
		Output:     out,
		Level:      logging.DebugLevel,
		FormatJSON: true,
	})
	require.NoError(t, err)

	logger.Trace("should-not-appear")
	logger.Debug("debug-record")

	s := out.String()
	assert.NotContains(t, s, "should-not-appear", "trace must not be emitted at debug level:\n%s", s)
	assert.Contains(t, s, "debug-record")
}

func TestLogger_LevelGates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		configured logging.Level
		emit       func(logging.Logger)
		marker     string
		wantOut    bool
	}{
		{logging.InfoLevel, func(l logging.Logger) { l.Debug("d-at-info") }, "d-at-info", false},
		{logging.InfoLevel, func(l logging.Logger) { l.Info("i-at-info") }, "i-at-info", true},
		{logging.ErrorLevel, func(l logging.Logger) { l.Info("i-at-error") }, "i-at-error", false},
		{logging.ErrorLevel, func(l logging.Logger) { l.Error("e-at-error") }, "e-at-error", true},
	}

	for _, tc := range cases {
		t.Run(tc.marker, func(t *testing.T) {
			t.Parallel()
			out := &threadSafeBuf{}
			logger, err := Logger(ModuleConfig{
				Output:     out,
				Level:      tc.configured,
				FormatJSON: true,
			})
			require.NoError(t, err)

			tc.emit(logger)
			got := strings.Contains(out.String(), tc.marker)
			assert.Equalf(t, tc.wantOut, got, "marker=%q output=%q", tc.marker, out.String())
		})
	}
}

func TestLogger_DefaultFieldsApplied(t *testing.T) {
	t.Parallel()

	out := &threadSafeBuf{}
	logger, err := Logger(ModuleConfig{
		Output:     out,
		Level:      logging.InfoLevel,
		FormatJSON: true,
		Fields:     map[string]any{"node-id": 42},
	})
	require.NoError(t, err)

	logger.Info("hello")
	assert.Contains(t, out.String(), `"node-id":42`)
}

func TestLogger_EnabledRespectsTrace(t *testing.T) {
	t.Parallel()

	out := &threadSafeBuf{}
	traceLogger, err := Logger(ModuleConfig{Output: out, Level: logging.TraceLevel, FormatJSON: true})
	require.NoError(t, err)
	assert.True(t, traceLogger.Enabled(logging.TraceLevel), "trace level should be enabled")
	assert.True(t, traceLogger.Enabled(logging.DebugLevel), "debug should also be enabled below trace")

	debugLogger, err := Logger(ModuleConfig{Output: out, Level: logging.DebugLevel, FormatJSON: true})
	require.NoError(t, err)
	assert.False(t, debugLogger.Enabled(logging.TraceLevel), "trace must be disabled at debug level")
	assert.True(t, debugLogger.Enabled(logging.DebugLevel))
}
