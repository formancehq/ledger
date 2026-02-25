package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
)

func TestHandleSetChartEnforcementMode_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"mode":"AUDIT"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts/enforcement-mode", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartEnforcementMode(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSetChartEnforcementMode_InvalidMode(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"mode":"INVALID"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts/enforcement-mode", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetChartEnforcementMode_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts/enforcement-mode", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetChartEnforcementMode_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"mode":"STRICT"}`)
	r := newRequest(t, http.MethodPut, "/chart-of-accounts/enforcement-mode", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleSetChartEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetChartEnforcementMode_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"mode":"STRICT"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts/enforcement-mode", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartEnforcementMode(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
