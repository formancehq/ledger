package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListAccountTypes_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{
				Name: "ledger1",
				AccountTypes: map[string]*commonpb.AccountType{
					"users": {
						Name:    "users",
						Pattern: "users:*",
					},
					"banks": {
						Name:    "banks",
						Pattern: "banks:*",
					},
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/account-types", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccountTypes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAccountTypes_Empty(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/account-types", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccountTypes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAccountTypes_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/account-types", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListAccountTypes(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccountTypes_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/account-types", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleListAccountTypes(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
