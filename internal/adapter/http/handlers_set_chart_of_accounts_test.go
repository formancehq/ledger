package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleSetChartOfAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"roots":{"bank":{"account":true}}}`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartOfAccounts(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSetChartOfAccounts_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartOfAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetChartOfAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"roots":{"bank":{"account":true}}}`)
	r := newRequest(t, http.MethodPut, "/chart-of-accounts", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleSetChartOfAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetChartOfAccounts_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"roots":{"bank":{"account":true}}}`)
	r := newRequest(t, http.MethodPut, "/ledger1/chart-of-accounts", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleSetChartOfAccounts(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
