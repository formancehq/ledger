package flightrecorder

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
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

	// Trace data accumulates asynchronously; poll until a snapshot has content
	// rather than sleeping a fixed duration.
	var buf bytes.Buffer
	require.Eventually(t, func() bool {
		buf.Reset()

		return r.Snapshot(&buf) == nil && buf.Len() > 0
	}, 2*time.Second, 10*time.Millisecond)

	r.Stop()
	require.False(t, r.Enabled())

	// Snapshot after stop should fail
	err := r.Snapshot(&buf)
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

	// After start: should return 200 with trace data. Trace data accumulates
	// asynchronously; poll the handler until it serves a non-empty snapshot.
	r.Start()
	defer r.Stop()

	require.Eventually(t, func() bool {
		req = httptest.NewRequest(http.MethodGet, "/debug/flight-recorder", nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		return w.Code == http.StatusOK && w.Body.Len() > 0
	}, 2*time.Second, 10*time.Millisecond)

	require.Contains(t, w.Header().Get("Content-Type"), "application/octet-stream")
	require.Contains(t, w.Header().Get("Content-Disposition"), "snapshot-")
}
