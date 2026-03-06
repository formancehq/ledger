package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestHandleGetChartOfAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{
				Name: name,
				ChartOfAccounts: &commonpb.ChartOfAccounts{
					Roots: map[string]*commonpb.ChartSegment{
						"bank": {Account: true},
					},
				},
				EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/chart-of-accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleGetChartOfAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[chartOfAccountsJSON]](t, w)
	require.NotNil(t, resp.Data.ChartOfAccounts)
	require.Contains(t, resp.Data.ChartOfAccounts.Roots, "bank")
	require.Equal(t, "STRICT", resp.Data.EnforcementMode)
}

func TestHandleGetChartOfAccounts_NoChart(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: name}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/chart-of-accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleGetChartOfAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	resp := decodeResponse[BaseResponse[chartOfAccountsJSON]](t, w)
	require.Nil(t, resp.Data.ChartOfAccounts)
	require.Equal(t, "STRICT", resp.Data.EnforcementMode)
}

func TestHandleGetChartOfAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chart-of-accounts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetChartOfAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetChartOfAccounts_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/chart-of-accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleGetChartOfAccounts(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
