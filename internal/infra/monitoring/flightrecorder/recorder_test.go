package flightrecorder

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/logging"
)

// NOTE: these tests are NOT parallel because only one flight recorder
// can be active at a time (runtime limitation).

func TestRecorder(t *testing.T) {
	logger := logging.Testing()
	cfg := Config{
		Enabled:  true,
		MinAge:   200 * time.Millisecond,
		MaxBytes: 1 << 20,
	}

	r := New(cfg, logger)
	require.False(t, r.Enabled())

	r.Start()
	require.True(t, r.Enabled())

	// Let some trace data accumulate
	time.Sleep(50 * time.Millisecond)

	var buf bytes.Buffer
	err := r.Snapshot(&buf)
	require.NoError(t, err)
	require.Greater(t, buf.Len(), 0)

	r.Stop()
	require.False(t, r.Enabled())

	// Snapshot after stop should fail
	err = r.Snapshot(&buf)
	require.Error(t, err)
}

func TestSnapshotHandler(t *testing.T) {
	logger := logging.Testing()
	cfg := Config{
		Enabled:  true,
		MinAge:   200 * time.Millisecond,
		MaxBytes: 1 << 20,
	}

	r := New(cfg, logger)
	handler := SnapshotHandler(r)

	// Before start: should return 503
	req := httptest.NewRequest(http.MethodGet, "/debug/flight-recorder", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	// After start: should return 200 with trace data
	r.Start()
	defer r.Stop()

	time.Sleep(50 * time.Millisecond)

	req = httptest.NewRequest(http.MethodGet, "/debug/flight-recorder", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "application/octet-stream")
	require.Contains(t, w.Header().Get("Content-Disposition"), "snapshot-")
	require.Greater(t, w.Body.Len(), 0)
}
