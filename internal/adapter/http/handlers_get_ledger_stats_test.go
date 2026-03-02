package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
)

func TestHandleGetLedgerStats_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
			require.Equal(t, "my-ledger", ledgerName)
			return &commonpb.LedgerStats{
				AccountCount:     42,
				TransactionCount: 100,
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[ledgerStatsJSON]](t, w)
	require.Equal(t, uint64(42), wrapper.Data.AccountCount)
	require.Equal(t, uint64(100), wrapper.Data.TransactionCount)
}

func TestHandleGetLedgerStats_EmptyLedger(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return &commonpb.LedgerStats{
				AccountCount:     0,
				TransactionCount: 0,
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/empty-ledger/stats", nil, map[string]string{
		"ledgerName": "empty-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[ledgerStatsJSON]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.AccountCount)
	require.Equal(t, uint64(0), wrapper.Data.TransactionCount)
}

func TestHandleGetLedgerStats_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/stats", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLedgerStats_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, errors.New("internal error")
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleGetLedgerStats_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/stats", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetLedgerStats_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestHandleGetLedgerStats_FullRouteIntegration tests that the route is correctly
// registered in NewHandler and accessible via a full HTTP request.
func TestHandleGetLedgerStats_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return &commonpb.LedgerStats{
				AccountCount:     5,
				TransactionCount: 10,
			}, nil
		},
	}

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/my-ledger/stats", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
