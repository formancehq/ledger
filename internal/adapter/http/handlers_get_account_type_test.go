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

func TestHandleGetAccountType_Success(t *testing.T) {
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
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/account-types/users", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "users",
	})

	srv.handleGetAccountType(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetAccountType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/account-types/users", nil, map[string]string{
		"ledgerName": "",
		"typeName":   "users",
	})

	srv.handleGetAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetAccountType_MissingTypeName(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/account-types/", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "",
	})

	srv.handleGetAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetAccountType_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{
				Name:         "ledger1",
				AccountTypes: map[string]*commonpb.AccountType{},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/account-types/missing", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "missing",
	})

	srv.handleGetAccountType(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetAccountType_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/account-types/users", nil, map[string]string{
		"ledgerName": "missing",
		"typeName":   "users",
	})

	srv.handleGetAccountType(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
